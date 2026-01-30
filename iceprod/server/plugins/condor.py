"""
The Condor plugin.  Allows submission to
`HTCondor <http://research.cs.wisc.edu/htcondor/>`_.

Note: Condor was renamed to HTCondor in 2012.
"""
import asyncio
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
import enum
import importlib.resources
import json
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time
from typing import Any, Generator, NamedTuple

import classad2 as classad  # type: ignore
import htcondor2 as htcondor  # type: ignore
from wipac_dev_tools.prometheus_tools import GlobalLabels, AsyncPromWrapper, PromWrapper, AsyncPromTimer, PromTimer

from iceprod.core.config import Task
from iceprod.core.exe import WriteToScript, Transfer, Data
from iceprod.common.prom_utils import HistogramBuckets
from iceprod.server.config import IceProdConfig
from iceprod.server import grid
from iceprod.server.util import str2datetime

logger = logging.getLogger('condor')


def check_call_clean_env(*args, **kwargs):
    env = os.environ.copy()
    del env['LD_LIBRARY_PATH']
    kwargs['env'] = env
    return subprocess.check_call(*args, **kwargs)


def check_output_clean_env(*args, **kwargs):
    env = os.environ.copy()
    del env['LD_LIBRARY_PATH']
    kwargs['env'] = env
    return subprocess.check_output(*args, **kwargs)


@enum.unique
class JobStatus(enum.StrEnum):
    IDLE = enum.auto()       # job is waiting in the queue
    RUNNING = enum.auto()    # job is running
    FAILED = enum.auto()     # job needs cleanup
    COMPLETED = enum.auto()  # job is out of the queue

    @staticmethod
    def from_condor_status(num):
        match num:
            case 0 | 1: return JobStatus.IDLE
            case 2 | 6: return JobStatus.RUNNING
            case 3 | 4: return JobStatus.COMPLETED
            case _: return JobStatus.FAILED


JOB_EVENT_STATUS_TRANSITIONS = {
    htcondor.JobEventType.SUBMIT: JobStatus.IDLE,
    htcondor.JobEventType.JOB_STAGE_IN: JobStatus.RUNNING,
    htcondor.JobEventType.JOB_STAGE_OUT: JobStatus.RUNNING,
    htcondor.JobEventType.FILE_TRANSFER: JobStatus.RUNNING,
    htcondor.JobEventType.EXECUTE: JobStatus.RUNNING,
    htcondor.JobEventType.JOB_EVICTED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_UNSUSPENDED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RELEASED: JobStatus.IDLE,
    htcondor.JobEventType.SHADOW_EXCEPTION: JobStatus.IDLE,
    htcondor.JobEventType.JOB_RECONNECT_FAILED: JobStatus.IDLE,
    htcondor.JobEventType.JOB_TERMINATED: JobStatus.FAILED,
    htcondor.JobEventType.JOB_HELD: JobStatus.FAILED,
    htcondor.JobEventType.JOB_SUSPENDED: JobStatus.FAILED,
    htcondor.JobEventType.JOB_ABORTED: JobStatus.FAILED,
}


RESET_CONDOR_REASONS = [
    # condor file transfer plugin failed
    '_condor_stdout: (errno 2) No such file',
    'transfer input files failure',
    'transfer output files failure',
    # resource limits
    'cpu consumption limit exceeded',
    'memory limit exceeded',
    'cgroup memory limit',
    'local storage limit on worker node exceeded',
    'execution time limit exceeded',
    # general retries
    'exceeded max iceprod queue time',
    'job has failed',
    'python-initiated action (by user ice3simusr)',
]


RESET_STDERR_REASONS = [
    # glidein died
    'sigterm',
    'killed',
    # hopefully transient errors
    'bus error (core dumped)',
    'segmentation fault (core dumped)',
    'operation timed out',
    'connection timed out',
    'globus_ftp_client: the operation was aborted',
    # GPU errors
    'opencl error: could not set up context',
    'opencl error: could build the opencl program',
    # CVMFS errors
    'python: command not found',
    'cannot read file data: Stale file handle',
    'setenv: command not found',
    # Pelican errors
    'Cannot contact Pelican director',
]


def parse_usage(usage: str) -> int:
    """
    Parse HTCondor usage expression

    Example input: "Usr 0 00:00:00, Sys 0 00:00:00"

    Args:
        usage: usage expression

    Returns:
        usage sum in seconds
    """
    total = 0
    for part in usage.split(','):
        _, days, leftovers = part.strip().split(' ')
        hours, minutes, seconds = leftovers.split(':')
        total += int(days)*86400 + int(hours)*3600 + int(minutes)*60 + int(seconds)
    return total


@dataclass(kw_only=True, slots=True)
class CondorJob(grid.GridTask):
    """Holds the job states for an HTCondor cluster."""
    dataset_id: str | None = None
    task_id: str | None = None
    instance_id: str | None = None
    submit_dir: Path | None = None
    status: JobStatus = JobStatus.IDLE
    extra: htcondor.classad.ClassAd | dict | None = None


class CondorJobId(NamedTuple):
    """Represents an HTCondor job id"""
    cluster_id: int
    proc_id: int

    def __str__(self):
        return f'{self.cluster_id}.{self.proc_id}'


class CondorSubmit:
    """Factory for submitting HTCondor jobs"""
    AD_DEFAULTS = {
        'request_cpus': 1,
        'request_gpus': 'UNDEFINED',
        'request_memory': 1000,
        'request_disk': 1000000,
        '+OriginalTime': 3600,
        'requirements': '',
        'PreCmd': 'UNDEFINED',
        'PreArguments': 'UNDEFINED',
        'transfer_input_files': [],
        'transfer_output_files': [],
        'transfer_output_remaps': [],
    }

    _GENERIC_ADS = ['Iwd', 'IceProdDatasetId', 'IceProdTaskId', 'IceProdTaskInstanceId', 'MATCH_EXP_JOBGLIDEIN_ResourceName']
    AD_INFO = [
        'RemotePool', 'RemoteHost',
        'HoldReason', 'RemoveReason', 'Reason', 'MachineAttrGLIDEIN_Site0',
    ] + _GENERIC_ADS
    AD_PROJECTION_QUEUE = ['JobStatus', 'RemotePool', 'RemoteHost'] + _GENERIC_ADS
    AD_PROJECTION_HISTORY = [
        'JobStatus', 'ExitCode', 'RemoveReason', 'LastHoldReason', 'CpusUsage', 'RemoteSysCpu', 'RemoteUserCpu',
        'GpusUsage', 'ResidentSetSize_RAW', 'DiskUsage_RAW', 'LastRemoteWallClockTime',
        'LastRemoteHost', 'LastRemotePool', 'MachineAttrGLIDEIN_Site0',
    ] + _GENERIC_ADS

    def __init__(self, cfg: IceProdConfig, submit_dir: Path, credentials_dir: Path, prom_global=None):
        self.cfg = cfg
        self.submit_dir = submit_dir
        self.credentials_dir = credentials_dir
        self.condor_schedd = htcondor.Schedd()
        self.prometheus = prom_global if prom_global else GlobalLabels({
            "type": "condor"
        })

        submit_dir.mkdir(parents=True, exist_ok=True)
        self.precmd = submit_dir / 'pre.py'
        self.precmd.write_text((importlib.resources.files('iceprod.server')/'data'/'condor_input_precmd.py').read_text())
        self.precmd.chmod(0o777)
        self.transfer_plugins = self.condor_plugin_discovery()
        logger.info('transfer plugins installed: %s', list(self.transfer_plugins.keys()))

        self.default_container = self.cfg['queue'].get('default_container', 'Undefined')

        # a mapping of url prefix to oauth service name
        self.oauth_service_mapping = self.cfg['oauth_services']

    def _restart_schedd(self):
        self.condor_schedd = htcondor.Schedd()

    def condor_plugin_discovery(self):
        """Find all available HTCondor transfer plugins, and copy them to the submit_dir"""
        ret = {}
        with importlib.resources.as_file(importlib.resources.files('iceprod.server')/'data'/'condor_transfer_plugins') as src_dir:
            if src_dir.is_dir():
                dest_dir = self.submit_dir / 'transfer_plugins'
                shutil.copytree(src_dir, dest_dir, dirs_exist_ok=True)
                for p in dest_dir.iterdir():
                    p.chmod(0o777)
                    logger.debug('transfer plugin %s', p)
                    for line in subprocess.run([str(p), '-classad'], capture_output=True, text=True, check=True).stdout.split('\n'):
                        logger.debug('transfer plugin output: %s', line)
                        if line.startswith('SupportedMethods'):
                            ret[line.split('=')[-1].strip(' ";')] = str(p)
        return ret

    def condor_oauth_url_transform(self, handle_name: str, token_scopes: dict[str, str]) -> dict[str, str]:
        """
        Convert from token scopes to HTCondor oauth url prefixes.

        Returns: transfer prefix transform
        """
        transfer_transforms = {}
        for url_prefix in token_scopes:
            if service_name := self.oauth_service_mapping.get(url_prefix, None):
                transfer_transforms[url_prefix] = f'{service_name}.{handle_name}+{url_prefix}'
            else:
                raise RuntimeError('unknown token scope url prefix: %r', url_prefix)
        return transfer_transforms

    def condor_oauth_scratch(self, task: Task) -> tuple[str, dict[str, Any]] | None:
        """
        Test if scratch is in use, and if so ask for a token.
        """
        cred_path = self.credentials_dir / 'scratch'
        if cred_path.exists():
            with cred_path.open() as f:
                creds = json.load(f)
            scratch_cred = creds[0]
        else:
            return None

        # should have filled from grid._convert_to_task()
        site_temp = task.dataset.config['options']['site_temp']
        for url_prefix in self.oauth_service_mapping:
            if site_temp.startswith(url_prefix):
                logger.info('site_temp: %s', site_temp)
                logger.info('url_prefix: %s', url_prefix)
                return url_prefix, scratch_cred
        return None

    @staticmethod
    def condor_os_container(os_arch: list[str] | str) -> str:
        """Convert from OS_ARCH to container image"""
        if isinstance(os_arch, list):
            os_arch = os_arch[0]
        os_arch = os_arch.rsplit('_',2)[0].rsplit('.',1)[0]
        if 'RHEL' not in os_arch:
            raise Exception('unknown OS_ARCH specified')
        version = os_arch.split('_')[-1]
        container = f'/cvmfs/singularity.opensciencegrid.org/opensciencegrid/osgvo-el{version}:latest'
        return container

    @staticmethod
    def condor_resource_reqs(task: Task) -> dict[str, str]:
        """Convert from Task requirements to HTCondor requirements"""
        ads = {}
        requirements = []
        if 'cpu' in task.requirements and task.requirements['cpu']:
            ads['request_cpus'] = task.requirements['cpu']
        if 'gpu' in task.requirements and task.requirements['gpu']:
            ads['request_gpus'] = task.requirements['gpu']
            requirements.append('GPUs_Capability >= 6.1')
        if 'memory' in task.requirements and task.requirements['memory']:
            ads['request_memory'] = int(task.requirements['memory']*1000)
        if 'disk' in task.requirements and task.requirements['disk']:
            # add 1G spare for log files and other misc
            ads['request_disk'] = int(task.requirements['disk']*1000000+1000000)
        if 'time' in task.requirements and task.requirements['time']:
            ads['+OriginalTime'] = int(task.requirements['time']*3600)
            requirements.append('TargetTime > OriginalTime')
        if requirements:
            ads['requirements'] = '('+')&&('.join(requirements)+')'
        return ads

    def condor_infiles(self, infiles: Iterable[Data], transfer_transforms: dict[str, str]) -> dict[str, str | list[str]]:
        """Convert from set[Data] to HTCondor classads for input files"""
        files = []
        mapping: list[tuple[str,str]] = []
        x509_proxy = self.cfg['queue'].get('x509proxy', None)
        if x509_proxy:
            files.append(x509_proxy)
        for infile in infiles:
            if infile.url.startswith('gsiftp:') and not x509_proxy:
                raise RuntimeError('need x509 proxy for gridftp!')
            if infile.url[0] == '/':
                url = infile.url
            else:
                if infile.transfer == Transfer.MAYBE:
                    url = 'iceprod-plugin://maybe-' + infile.url
                else:
                    url = 'iceprod-plugin://true-' + infile.url
                basename = Path(infile.url).name
                if basename != infile.local:
                    url += '?mapping='+infile.local
                    # mapping.append((basename,infile.local))
            files.append(url)
        ads: dict[str, str | list[str]] = {}
        if mapping:
            ads['PreCmd'] = f'"{self.precmd.name}"'
            ads['PreArguments'] = '"'+' '.join(f"'{k}'='{v}'" for k,v in mapping)+'"'
            files.append(str(self.precmd))
        if files:
            ads['transfer_input_files'] = files
        return ads

    def condor_outfiles(self, outfiles: Iterable[Data], transfer_transforms: dict[str, str]) -> dict[str, str | list[str]]:
        """Convert from set[Data] to HTCondor classads for output files"""
        files = []
        mapping = []
        for outfile in outfiles:
            files.append(outfile.local)
            if outfile.transfer == Transfer.MAYBE:
                url = 'iceprod-plugin://maybe-' + outfile.url
            else:
                url = outfile.url
            mapping.append((outfile.local, url))
        ads: dict[str, str | list[str]] = {}
        ads['transfer_output_files'] = files
        ads['transfer_output_remaps'] = ';'.join(f'{name} = {url}' for name,url in mapping)
        return ads

    def create_submit_dir(self, task: Task, jel_dir: Path) -> Path:
        """
        Create the submit dir
        """
        path = jel_dir / task.task_id
        i = 1
        while path.exists():
            path = jel_dir / f'{task.task_id}_{i}'
            i += 1
        path.mkdir(parents=True)
        return path

    def add_oauth_tokens(self, provider_transforms: dict, tokens: list[Any]):
        """
        Add OAuth tokens to HTCondor
        """
        now = time.time()
        logger.info('add_tokens: %r', tokens)
        for data in tokens:
            if data.get('type', 'oauth') != 'oauth':
                logger.warning('unhandled type for token: %r', data)
                continue
            if 'transfer_prefix' not in data:
                logger.warning('missing transfer_prefix from token: %r', data)
                break
            prefix = data['transfer_prefix']
            if transform := provider_transforms.get(prefix, None):
                s_h = transform.split('+',1)[0]
                service,handle = s_h.split('.')
                if token := data.get('access_token', None):
                    htcondor.Credd().add_user_service_cred(
                        credtype=htcondor.CredType.OAuth,
                        credential=json.dumps({
                            "access_token": token,
                            "token_type": "bearer",
                            "expires_in": int(data['expiration'] - now),
                            "expires_at": data['expiration'],
                            "scope": data['scope'].split(),
                        }).encode('utf-8'),
                        service=service,
                        handle=handle,
                        refresh=False,
                    )
                if token := data.get('refresh_token', None):
                    htcondor.Credd().add_user_service_cred(
                        credtype=htcondor.CredType.OAuth,
                        credential=json.dumps({
                            "refresh_token": token,
                            "scopes": data['scope'],
                        }).encode('utf-8'),
                        service=service,
                        handle=handle,
                        refresh=True,
                    )
            else:
                logger.warning('unused token for transfer_prefix %r', data['transfer_prefix'])

    def oauth_submit(self, task: Task) -> tuple[dict[str, str], str, str]:
        """Do oauth stuff for submit"""
        task_config = task.get_task_config()

        handle = re.sub(r'[^a-zA-Z0-9]', '', f'{task.dataset.dataset_id}{task.name}')
        token_transform = self.condor_oauth_url_transform(handle, task_config['token_scopes'])

        services_used = defaultdict(set)
        for service in token_transform.values():
            s,h = service.split('+',1)[0].split('.',1)
            services_used[s].add(h)

        scratch = self.condor_oauth_scratch(task)
        if scratch:
            site_temp = task.dataset.config['options']['site_temp']
            prefix,cred = scratch
            svc = self.oauth_service_mapping[prefix]
            services_used[svc].add('scratch')
            transform = f'{svc}.scratch+{site_temp}'
            self.add_oauth_tokens({site_temp: transform}, [cred])
            # token_transform[site_temp] = transform

        block = f"""+oauth_file_transform = False
+OAuthServicesNeeded = "{' '.join(f'{s}*{h}' for s,handles in services_used.items() for h in handles)}"
"""

        reqs = ' && '.join(f'stringListMember("{name}", HasFileTransferPluginMethods)' for name in services_used)

        logger.info('oauth_submit lines: \n%s', block)
        logger.info('oauth_submit reqs: %s', reqs)

        return token_transform, block, reqs

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_condor_submit', 'IceProd grid condor.submit calls', buckets=HistogramBuckets.MINUTE))
    @AsyncPromWrapper(lambda self: self.prometheus.histogram('iceprod_grid_condor_schedd_submit', 'IceProd grid htcondor.schedd.submit calls', buckets=HistogramBuckets.MINUTE))
    async def submit(self, prom_histogram, tasks: list[Task], jel: Path) -> dict[CondorJobId, CondorJob]:
        """
        Submit multiple jobs to Condor as a single batch.

        Assumes that the resource requirements are identical.

        Args:
            tasks: IceProd Tasks to submit
            jel: common job event log

        Returns:
            dict of new jobs
        """
        jel_dir = jel.parent
        transfer_plugin_str = ';'.join(f'{k}={v}' for k,v in self.transfer_plugins.items())

        # the tasks all have the same basic config, so just get the first one
        task_config = tasks[0].get_task_config()

        oauth_file_transform, oauth_block, oauth_reqs = self.oauth_submit(tasks[0])

        submitfile = f"""
output = $(initialdir)/condor.out
error = $(initialdir)/condor.err
log = {jel}

notification = never
job_ad_information_attrs = {" ".join(self.AD_INFO)}
batch_name = Dataset {tasks[0].dataset.dataset_num}

+IsIceProdJob = True
+IceProdSite = "{self.cfg["queue"].get("site", "unknown")}"
+IceProdDatasetId = $(datasetid)
+IceProdDataset = $(dataset)
+IceProdJobId = $(jobid)
+IceProdJobIndex = $(jobindex)
+IceProdTaskId = $(taskid)
+IceProdTaskName = $(taskname)
+IceProdTaskInstanceId = $(taskinstance)

request_cpus = $(cpus)
request_gpus = $(gpus)
request_memory = $(memory)
request_disk = $(disk)
+OriginalTime = $(time)
+TargetTime = (!isUndefined(Target.PYGLIDEIN_TIME_TO_LIVE) ? Target.PYGLIDEIN_TIME_TO_LIVE : Target.TimeToLive)
requirements = $($(reqs))
+SingularityImage= $(container)

{oauth_block}

transfer_plugins = {transfer_plugin_str}
when_to_transfer_output = ON_EXIT
should_transfer_files = YES
infiles_expr = replaceall(";", $(infiles), ",")
transfer_input_files = $STRING(infiles_expr)
+PreCmd = $(prec)
+PreArguments = $(prea)
outfiles_expr = replaceall(";", $(outfiles), ",")
transfer_output_files = $STRING(outfiles_expr)
transfer_output_remaps = $(outremaps)

"""

        for k,v in task_config['batchsys'].get('condor', {}).items():
            if k.lower() != 'requirements':
                submitfile += f'+{k} = {v}\n'

        reqs = ''
        for k,v in self.cfg['queue'].get('batchopts', {}).items():
            if k.lower() == 'requirements':
                reqs = v
            else:
                submitfile += f'+{k} = {v}\n'

        jobset = []
        for task in tasks:
            submit_dir = self.create_submit_dir(task, jel_dir)
            script = WriteToScript(task=task, workdir=submit_dir)
            executable = await script.convert(transfer=True)
            logger.debug('running task with exe %r', executable)

            ads = self.AD_DEFAULTS.copy()
            ads.update(self.condor_infiles(script.infiles, oauth_file_transform))
            ads.update(self.condor_outfiles(script.outfiles, oauth_file_transform))
            ads.update(self.condor_resource_reqs(task))

            if task.oauth_tokens:
                self.add_oauth_tokens(oauth_file_transform, task.oauth_tokens)

            container = task_config.get('container', None)
            if not container:
                if os_arch := task.requirements.get('os'):
                    container = self.condor_os_container(os_arch)
                else:
                    container = self.default_container
            if container != 'Undefined':
                container = f'"{container}"'

            reqs2 = reqs
            for k,v in task_config['batchsys'].get('condor', {}).items():
                if k.lower() == 'requirements':
                    reqs2 = f'({reqs}) && ({v})' if reqs else f'({v})'
                    break

            if reqs2:
                ads['requirements'] = f'{ads["requirements"]} && {reqs2}' if ads.get('requirements', None) else reqs2
            # ignore oauth_reqs for now
            # todo: when we actually start using condor file transfer, this needs to be re-enabled
            # if oauth_reqs:
            #     ads['requirements'] = f'{ads["requirements"]} && {oauth_reqs}' if ads.get('requirements', None) else oauth_reqs
            if ads['requirements']:
                submitfile += f'reqs{task.task_id} = {ads["requirements"]}\n'
            # stringify everything, quoting the real strings
            jobset.append({
                'datasetid': f'"{task.dataset.dataset_id}"',
                'dataset': f'{task.dataset.dataset_num}',
                'jobid': f'"{task.job.job_id}"',
                'jobindex': f'{task.job.job_index}',
                'taskid': f'"{task.task_id}"',
                'taskname': f'"{task.name}"',
                'taskinstance': f'"{task.instance_id if task.instance_id else ""}"',
                'initialdir': f'{submit_dir}',
                'executable': f'{executable}',
                'cpus': f'{ads["request_cpus"]}',
                'gpus': f'{ads["request_gpus"]}',
                'memory': f'{ads["request_memory"]}',
                'disk': f'{ads["request_disk"]}',
                'time': f'{ads["+OriginalTime"]}',
                'reqs': f'reqs{task.task_id}',
                'container': f'{container}',
                'prec': f'{ads["PreCmd"]}',
                'prea': f'{ads["PreArguments"]}',
                'infiles': f'"{";".join(ads["transfer_input_files"])}"',  # type: ignore
                'outfiles': f'"{";".join(ads["transfer_output_files"])}"',  # type: ignore
                'outremaps': f'"{ads["transfer_output_remaps"]}"',
            })

        submitfile += '\n\nqueue '+', '.join(jobset[0].keys())+' from (\n'
        for job in jobset:
            submitfile += '  '+', '.join(job.values())+'\n'
        submitfile += ')\n'

        logger.debug("submitfile:\n%s", submitfile)

        with prom_histogram.time():
            s = htcondor.Submit(submitfile)
            try:
                submit_result = self.condor_schedd.submit(s, count=1, itemdata=s.itemdata())
            except htcondor.HTCondorException:
                self._restart_schedd()
                submit_result = self.condor_schedd.submit(s, count=1, itemdata=s.itemdata())

        cluster_id = int(submit_result.cluster())
        ret = {}
        for i,job in enumerate(jobset):
            ret[CondorJobId(cluster_id=cluster_id, proc_id=i)] = CondorJob(
                dataset_id=job['datasetid'].strip('"'),
                task_id=job['taskid'].strip('"'),
                instance_id=job['taskinstance'].strip('"'),
                submit_dir=Path(job['initialdir']),
            )
        return ret

    @PromTimer(lambda self: self.prometheus.histogram('iceprod_grid_condor_get_jobs', 'IceProd grid condor.get_jobs calls', buckets=HistogramBuckets.MINUTE))
    def get_jobs(self) -> dict[CondorJobId, CondorJob]:
        """
        Get all jobs currently on the condor queue.
        """
        ret = {}
        for ad in self.condor_schedd.query(
            constraint=f'IceProdSite =?= "{self.cfg["queue"].get("site", "unknown")}"',
            projection=['ClusterId', 'ProcId'] + self.AD_PROJECTION_QUEUE,
        ):
            job_id = CondorJobId(cluster_id=ad['ClusterId'], proc_id=ad['ProcId'])
            submit_dir = None
            if s := ad.get('Iwd'):
                submit_dir = Path(s)
            status = JobStatus.IDLE
            if s := ad.get('JobStatus'):
                status = JobStatus.from_condor_status(s)
            job = CondorJob(
                dataset_id=ad.get('IceProdDatasetId'),
                task_id=ad.get('IceProdTaskId'),
                instance_id=ad.get('IceProdTaskInstanceId'),
                submit_dir=submit_dir,
                status=status,
                extra=ad,
            )
            ret[job_id] = job
        return ret

    @PromTimer(lambda self: self.prometheus.histogram('iceprod_grid_condor_get_history', 'IceProd grid condor.get_history calls', buckets=HistogramBuckets.MINUTE))
    def get_history(self, since: int | None = None) -> Generator[tuple[CondorJobId, CondorJob], None, None]:
        """
        Get all jobs currently on the condor history.

        Args:
            since: how far back to look in the history (unix time)
        """
        for ad in self.condor_schedd.history(
            constraint=f'IceProdSite =?= "{self.cfg["queue"].get("site", "unknown")}"',
            projection=['ClusterId', 'ProcId'] + self.AD_PROJECTION_HISTORY,
            since=classad.ExprTree(f'CompletionDate<{since}') if since else None,  # type: ignore
        ):
            job_id = CondorJobId(cluster_id=ad['ClusterId'], proc_id=ad['ProcId'])

            submit_dir = None
            if s := ad.get('Iwd'):
                submit_dir = Path(s)

            status = JobStatus.IDLE
            if s := ad.get('JobStatus'):
                status = JobStatus.from_condor_status(s)

            job = CondorJob(
                dataset_id=ad.get('IceProdDatasetId'),
                task_id=ad.get('IceProdTaskId'),
                instance_id=ad.get('IceProdTaskInstanceId'),
                submit_dir=submit_dir,
                status=status,
                extra=ad,
            )
            yield (job_id, job)

    @PromTimer(lambda self: self.prometheus.histogram('iceprod_grid_condor_remove', 'IceProd grid condor.remove calls', buckets=HistogramBuckets.SECOND))
    def remove(self, job_id: str | CondorJobId, reason: str | None = None):
        """
        Remove a job from condor.

        Args:
            job_id: condor job id
            reason: reason for removal
        """
        logger.info('removing job %s', job_id)
        self.condor_schedd.act(htcondor.JobAction.Remove, str(job_id), reason=reason if reason else '')


class Grid(grid.BaseGrid):
    """HTCondor grid plugin"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs = {}
        self.jels = {str(filename): htcondor.JobEventLog(str(filename)).events(0) for filename in self.submit_dir.glob('*/*.jel')}
        self.submitter = CondorSubmit(self.cfg, submit_dir=self.submit_dir, credentials_dir=self.credentials_dir, prom_global=self.prometheus)

        # save last event.timestamp, on restart only process >= timestamp
        self.last_event_timestamp = 0.
        self.load_timestamp()

    def load_timestamp(self):
        timestamp_path = self.submit_dir / 'last_event_timestamp'
        if timestamp_path.exists():
            with timestamp_path.open('r') as f:
                self.last_event_timestamp = float(f.read().strip())

    def save_timestamp(self):
        timestamp_path = self.submit_dir / 'last_event_timestamp'
        with timestamp_path.open('w') as f:
            f.write(str(self.last_event_timestamp))

    async def run(self, forever=True):
        # initial job load
        try:
            await self.check()
            self.save_timestamp()
        except Exception:
            logger.warning('failed to check', exc_info=True)
        check_time = time.monotonic()
        try:
            await self.wait(timeout=0)
        except Exception:
            logger.warning('failed to wait', exc_info=True)

        logger.info('active JELs: %r', list(self.jels.keys()))

        while True:
            start = time.monotonic()
            try:
                await self.submit()
            except Exception:
                logger.warning('failed to submit', exc_info=True)
            wait_time = max(0, self.cfg['queue']['submit_interval'] - (time.monotonic() - start))
            try:
                await self.wait(timeout=wait_time)
            except Exception:
                logger.warning('failed to wait', exc_info=True)

            now = time.monotonic()
            if now - check_time >= self.cfg['queue']['check_time']:
                try:
                    await self.check()
                    self.save_timestamp()
                except Exception:
                    logger.warning('failed to check', exc_info=True)
                check_time = now

            if not forever:
                break

    def queue_dataset_status(self) -> dict[grid.GridStatus, Counter[str]]:
        """Get the current queue job counts by dataset and job status."""
        ret: dict[grid.GridStatus, Counter[str]] = defaultdict(Counter)
        for job in self.jobs.values():
            if not job.dataset_id:
                continue
            if job.status == JobStatus.IDLE:
                ret[grid.GridStatus.QUEUED][job.dataset_id] += 1
            elif job.status == JobStatus.RUNNING:
                ret[grid.GridStatus.PROCESSING][job.dataset_id] += 1
        return ret

    # Submit to Condor #

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_submit', 'IceProd grid submit calls', buckets=HistogramBuckets.TENMINUTE))
    @AsyncPromWrapper(lambda self: self.prometheus.counter('iceprod_grid_submit_datasets', 'IceProd grid submit dataset counter', labels=['dataset', 'task', 'success'], finalize=False))
    async def submit(self, prom_counter):
        num_to_submit = self.get_queue_num()
        logger.info("Attempting to submit %d tasks", num_to_submit)
        if num_to_submit < 1:
            return
        tasks = await self.get_tasks_to_queue(num_to_submit)
        cur_jel = self.get_current_JEL()

        # pre-load the scratch credentials
        await self.get_scratch_credentials()

        # split into datasets and task types
        tasks_by_dataset = defaultdict(list)
        for task in tasks:
            tasks_by_dataset[f'{task.dataset.dataset_num}-{task.name}'].append(task)
        for key in tasks_by_dataset:
            tasks = tasks_by_dataset[key]
            dataset_num, task_name = key.split('-', 1)
            success = 'success'
            try:
                ret = await self.submitter.submit(tasks, cur_jel)
                self.jobs.update(ret)
            except Exception as e:
                logger.warning('submit failed for dataset %s task %s', dataset_num, task_name, exc_info=True)
                async with asyncio.TaskGroup() as tg:
                    for task in tasks:
                        j = CondorJob(dataset_id=task.dataset.dataset_id, task_id=task.task_id, instance_id=task.instance_id)
                        tg.create_task(self.task_reset(j, reason=f'HTCondor submit failed: {e}'))
                success = 'fail'
            else:
                logger.info('submit succeeded for dataset %s task %s', dataset_num, task_name)
            prom_counter.labels({'dataset': dataset_num, 'task': task_name, 'success': success}).inc(len(tasks))

    @PromWrapper(lambda self: self.prometheus.gauge('iceprod_grid_queue_num', 'IceProd grid queue status gauges', labels=['status'], finalize=False))
    def get_queue_num(self, prom_counter) -> int:
        """Determine how many tasks to queue."""
        counts = {s: 0 for s in JobStatus}
        for job in self.jobs.values():
            counts[job.status] += 1

        idle_jobs = counts[JobStatus.IDLE]
        prom_counter.labels({'status': 'idle'}).set(idle_jobs)
        logger.info('idle jobs: %r', idle_jobs)
        processing_jobs = counts[JobStatus.RUNNING]
        prom_counter.labels({'status': 'processing'}).set(processing_jobs)
        logger.info('processing jobs: %r', processing_jobs)
        queue_tot_max = self.cfg['queue']['max_total_tasks_on_queue'] - idle_jobs - processing_jobs
        queue_idle_max = self.cfg['queue']['max_idle_tasks_on_queue'] - idle_jobs
        queue_interval_max = self.cfg['queue']['max_tasks_per_submit']
        queue_num = max(0, min(queue_tot_max, queue_idle_max, queue_interval_max))
        return queue_num

    def get_current_JEL(self) -> Path:
        """
        Get the current Job Event Log, possibly creating a new one
        for every hour.

        Returns:
            Path: filename to current JEL
        """
        day = datetime.now(UTC).strftime('%Y-%m-%dT%H')
        day_submit_dir = self.submit_dir / day
        if not day_submit_dir.exists():
            day_submit_dir.mkdir(mode=0o700, parents=True)
        cur_jel = day_submit_dir / 'jobs.jel'
        if not cur_jel.exists():
            cur_jel.touch(mode=0o600)
        cur_jel_str = str(cur_jel)
        if cur_jel_str not in self.jels:
            self.jels[cur_jel_str] = htcondor.JobEventLog(cur_jel_str).events(0)
        return cur_jel

    # JEL processing #

    @AsyncPromWrapper(lambda self: self.prometheus.counter('iceprod_grid_wait', 'IceProd grid wait counter', labels=['job_status'], finalize=False))
    async def wait(self, prom_counter, timeout):
        """
        Wait for jobs to complete from the Job Event Logs.

        Args:
            timeout: wait up to N seconds
        """
        start = time.monotonic()

        # make sure we have the latest JEL loaded
        self.get_current_JEL()

        while True:
            for filename, events in self.jels.items():
                try:
                    for event in events:
                        if float(event.timestamp) < self.last_event_timestamp:
                            continue

                        job_id = CondorJobId(cluster_id=event.cluster, proc_id=event.proc)

                        if event.type == htcondor.JobEventType.SUBMIT:
                            self.jobs[job_id] = CondorJob()
                            continue
                        elif job_id not in self.jobs:
                            logger.debug('reloaded job %s', job_id)
                            self.jobs[job_id] = CondorJob()

                        job = self.jobs[job_id]

                        if event.type == htcondor.JobEventType.JOB_AD_INFORMATION:
                            if not job.dataset_id:
                                job.dataset_id = event['IceProdDatasetId']
                                job.task_id = event['IceProdTaskId']
                                job.instance_id = event['IceProdTaskInstanceId']
                                job.submit_dir = Path(event['Iwd'])

                            type_ = event['TriggerEventTypeNumber']
                            if type_ == htcondor.JobEventType.JOB_TERMINATED:
                                logger.info("job %s %s.%s exited on its own", job_id, job.dataset_id, job.task_id)

                                success = event.get('ReturnValue', 1) == 0
                                job.status = JobStatus.COMPLETED if success else JobStatus.FAILED
                                prom_counter.labels({"job_status": str(job.status)}).inc()

                                # there's a bug where not all the classads are updated before the event fires
                                # so ignore this and let the cross-check take care of it
                                continue

                                """
                                # get stats
                                cpu = event.get('CpusUsage', None)
                                gpu = event.get('GpusUsage', None)
                                memory = event.get('ResidentSetSize_RAW', None)  # KB
                                if memory is None:
                                    memory = event.get('MemoryUsage', None)*1000  # MB
                                disk = event.get('DiskUsage_RAW', None)  # KB
                                if disk is None:
                                    disk = event.get('DiskUsage', None)  # KB
                                time_ = event.get('RemoteWallClockTime', None)  # seconds
                                if time_ is None:
                                    time_ = parse_usage(event.get('RunRemoteUsage', '')) / event.get('RequestCpus', 1)
                                elif cpu is None and time_:
                                    cpu = parse_usage(event.get('RunRemoteUsage', '')) / time_
                                # data_in = event['ReceivedBytes']  # KB
                                # data_out = event['SentBytes']  # KB

                                resources = {}
                                if cpu is not None:
                                    resources['cpu'] = cpu
                                if gpu is not None:
                                    resources['gpu'] = gpu
                                if memory is not None:
                                    resources['memory'] = memory/1000000.
                                if disk is not None:
                                    resources['disk'] = disk/1000000.
                                if time_ is not None:
                                    resources['time'] = time_/3600.

                                stats = {}
                                if site := event.get('MachineAttrGLIDEIN_Site0'):
                                    stats['site'] = site
                                elif site := event.get('MATCH_EXP_JOBGLIDEIN_ResourceName'):
                                    stats['site'] = site

                                reason = None
                                if r := event.get('HoldReason'):
                                    reason = r
                                elif r := event.get('RemoveReason'):
                                    reason = r
                                elif r := event.get('Reason'):
                                    reason = r

                                # finish job
                                await self.finish(job_id, success=success, resources=resources, stats=stats, reason=reason)
                                """

                            elif type_ == htcondor.JobEventType.JOB_ABORTED:
                                job.status = JobStatus.FAILED
                                prom_counter.labels({"job_status": str(job.status)}).inc()
                                reason = event.get('Reason', None)
                                logger.info("job %s %s.%s removed: %r", job_id, job.dataset_id, job.task_id, reason)

                                # there's a bug where not all the classads are updated before the event fires
                                # so ignore this and let the cross-check take care of it
                                continue

                                """
                                await self.finish(job_id, success=False, reason=reason)
                                """

                            else:
                                # update status
                                new_status = JOB_EVENT_STATUS_TRANSITIONS.get(type_, None)
                                if new_status is not None and job.status != new_status:
                                    job.status = new_status
                                    prom_counter.labels({"job_status": str(job.status)}).inc()
                                    if new_status == JobStatus.FAILED:
                                        self.submitter.remove(job_id, reason=event.get('HoldReason', 'Job has failed'))
                                    else:
                                        await self.job_update(job)
                except Exception:
                    logger.warning('error processing condor log', exc_info=True)

            if time.monotonic() - start >= timeout:
                break
            await asyncio.sleep(.1)

    async def job_update(self, job: CondorJob):
        """
        Send updated info from the batch system to the IceProd API.

        Must handle dup calls.
        """
        if job.status not in (JobStatus.IDLE, JobStatus.RUNNING):
            logging.warning("unknown job status: %r", job.status)
            return

        try:
            if job.status == JobStatus.IDLE:
                logging.info("task %s.%s is now queued", job.dataset_id, job.task_id)
                await self.task_idle(job)
            elif job.status == JobStatus.RUNNING:
                logging.info("task %s.%s is now processing", job.dataset_id, job.task_id)
                await self.task_processing(job)
        except Exception:
            logging.warning("failed to update task status", exc_info=True)

    async def finish(self, job_id: CondorJobId, success: bool = True, resources: dict | None = None, reason: str | None = None, stats: dict | None = None):
        """
        Run cleanup actions after a batch job completes.

        Must handle dup calls.
        """
        if job_id not in self.jobs:
            logger.debug('dup call: %s not in job dict', job_id)
            return

        if not stats:
            stats = {}
        if resources:
            stats['resources'] = resources

        job = self.jobs[job_id]
        logger.info('finish for condor=%s iceprod=%s.%s', job_id, job.dataset_id, job.task_id)

        stdout = None
        stderr = None
        if job.submit_dir and job.submit_dir.is_dir():
            stdout = job.submit_dir / 'condor.out'
            stderr = job.submit_dir / 'condor.err'

        # do global actions
        try:
            if success:
                await self.task_success(job, stats=stats, stdout=stdout, stderr=stderr)
            else:
                future = None
                if reason:
                    stats['error_summary'] = reason
                    # check condor error for reset reason
                    for text in RESET_CONDOR_REASONS:
                        if text.lower() in reason.lower():
                            future = self.task_reset(job, stats=stats, reason=reason)
                            break
                if future is None and stderr and stderr.is_file():
                    # check stderr for reset reason
                    data = stderr.open().read()
                    for text in RESET_STDERR_REASONS:
                        if text.lower() in data.lower():
                            future = self.task_reset(job, stats=stats, reason=reason)
                            break
                if future is None:
                    future = self.task_failure(job, stats=stats, reason=reason, stdout=stdout, stderr=stderr)
                await future
        except Exception:
            logger.warning('failed to update REST', exc_info=True)

        # internal job cleanup
        del self.jobs[job_id]

    # Longer checks #

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_check', 'IceProd grid check calls'))
    async def check(self):
        """
        Do a cross-check, to verify `self.jobs` vs the submit dir and IceProd API.
        """
        logger.info('starting cross-check')
        self.cross_check_start = time.monotonic()

        all_jobs = self.submitter.get_jobs()

        # swap job dicts
        for job_id in set(self.jobs) - set(all_jobs):
            logger.info('removing job %s from cross-check', job_id)
        old_jobs = self.jobs
        self.jobs = all_jobs

        # process any updates
        async with asyncio.TaskGroup() as tg:
            for job_id, job in all_jobs.items():
                if job_id not in old_jobs or job.status != old_jobs[job_id].status:
                    if job.status == JobStatus.FAILED:
                        extra = job.extra if job.extra else {}
                        reason = extra.get('HoldReason', 'Job has failed')
                        logger.info("job %s %s.%s removed from cross-check: %r", job_id, job.dataset_id, job.task_id, reason)
                        self.submitter.remove(job_id, reason=reason)
                    else:
                        tg.create_task(self.job_update(job))

        await self.check_history()

        await self.check_iceprod()

        # check for old jobs and dirs
        async for path in self.check_submit_dir():
            for job_id, job in self.jobs.items():
                if job.submit_dir == path:
                    self.submitter.remove(job_id, reason='exceeded max iceprod queue time')

        logger.info('finished cross-check')

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_check_history', 'IceProd grid check calls', buckets=HistogramBuckets.MINUTE))
    @AsyncPromWrapper(lambda self: self.prometheus.histogram('iceprod_grid_check_history_per_job', 'IceProd grid check history per job', buckets=HistogramBuckets.SECOND))
    async def check_history(self, prom_histogram):
        """Check condor_history"""
        now = time.time()
        hist_jobs_iter = self.submitter.get_history(since=int(self.last_event_timestamp))
        self.last_event_timestamp = now
        async with asyncio.TaskGroup() as tg:
            last_time = time.monotonic()
            for job_id, job in hist_jobs_iter:
                if job_id not in self.jobs:
                    self.jobs[job_id] = job

                logger.info("job %s %s.%s exited on its own from cross-check", job_id, job.dataset_id, job.task_id)
                extra = job.extra if job.extra else {}

                # get stats
                cpu = extra.get('CpusUsage', None)
                if not cpu and (wall := extra.get('LastRemoteWallClockTime', None)):
                    cpu = (extra.get('RemoteSysCpu', 0) + extra.get('RemoteUserCpu', 0)) * 1. / wall
                gpu = extra.get('GpusUsage', None)
                memory = extra.get('ResidentSetSize_RAW', None)  # KB
                disk = extra.get('DiskUsage_RAW', None)  # KB
                time_ = extra.get('LastRemoteWallClockTime', None)  # seconds

                resources = {}
                if cpu is not None:
                    resources['cpu'] = cpu
                if gpu is not None:
                    resources['gpu'] = gpu
                if memory is not None:
                    resources['memory'] = memory/1000000.
                if disk is not None:
                    resources['disk'] = disk/1000000.
                if time_ is not None:
                    resources['time'] = time_/3600.

                success = extra.get('JobStatus') == 4 and extra.get('ExitCode', 1) == 0
                job.status = JobStatus.COMPLETED if success else JobStatus.FAILED

                stats = {}
                if site := extra.get('MachineAttrGLIDEIN_Site0'):
                    stats['site'] = site
                elif site := extra.get('MATCH_EXP_JOBGLIDEIN_ResourceName'):
                    stats['site'] = site

                reason = None
                if r := extra.get('LastHoldReason'):
                    reason = r
                elif r := extra.get('RemoveReason'):
                    reason = r

                # finish job
                tg.create_task(self.finish(job_id, success=success, resources=resources, stats=stats, reason=reason))

                # do timing manually to also count generator time per job
                next_time = time.monotonic()
                prom_histogram.observe(next_time - last_time)
                last_time = next_time

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_check_iceprod', 'IceProd grid check calls', buckets=HistogramBuckets.TENSECOND))
    async def check_iceprod(self):
        """
        Sync with iceprod server status.
        """
        fut = self.get_tasks_on_queue()
        queue_tasks = {j.task_id: j for j in self.jobs.values()}
        server_tasks = await fut
        # buffer time takes the time since cross check started + 60 seconds
        buffer_time = datetime.now(UTC) - timedelta(seconds=(time.monotonic() - self.cross_check_start + 60))
        async with asyncio.TaskGroup() as tg:
            for task in server_tasks:
                if task['task_id'] not in queue_tasks:
                    # ignore anything too recent
                    if str2datetime(task['status_changed']) >= buffer_time:
                        continue
                    logger.info(f'task {task["dataset_id"]}.{task["task_id"]} in iceprod but not in queue')
                    job = CondorJob(
                        dataset_id=task['dataset_id'],
                        task_id=task['task_id'],
                        instance_id=task['instance_id'],
                    )
                    tg.create_task(self.task_reset(job, reason='task missing from HTCondor queue'))

    @PromWrapper(lambda self: self.prometheus.histogram('iceprod_grid_check_submit_dir', 'IceProd grid check calls', buckets=HistogramBuckets.TENSECOND))
    async def check_submit_dir(self, prom_histogram):
        """
        Return directory paths that should be cleaned up.
        """
        with prom_histogram.time():
            # get time limits
            queue_tasks = {j.task_id for j in self.jobs.values()}
            queued_time = self.cfg['queue'].get('max_task_queued_time', 86400*2)
            processing_time = self.cfg['queue'].get('max_task_processing_time', 86400*2)
            suspend_time = self.cfg['queue'].get('suspend_submit_dir_time', 86400)
            now = time.time()
            job_clean_logs_time = now - suspend_time
            job_old_time = now - (queued_time + processing_time)
            dir_old_time = now - (queued_time + processing_time + suspend_time)
            logger.debug('now: %r, job_clean_logs_time: %r, job_old_time: %r, dir_old_time: %r', now, job_clean_logs_time, job_old_time, dir_old_time)

            for daydir in self.submit_dir.glob('[0-9][0-9][0-9][0-9]*'):
                logger.debug('looking at daydir %s', daydir)
                if daydir.is_dir():
                    empty = True
                    for path in daydir.iterdir():
                        job_active = path.name.split('_')[0] in queue_tasks
                        logger.debug('looking at path %s, active: %r', path, job_active)
                        st = path.lstat()
                        logger.debug('stat: %r', st)
                        if stat.S_ISDIR(st.st_mode):
                            empty = False
                            if not job_active:
                                if st.st_mtime < job_clean_logs_time:
                                    logger.info('cleaning up submit dir %s', path)
                                    shutil.rmtree(path)
                            elif st.st_mtime < job_old_time:
                                yield path
                                if st.st_mtime < dir_old_time:
                                    logger.info('cleaning up submit dir %s', path)
                                    shutil.rmtree(path)
                    if empty:
                        logger.info('cleaning up daydir %s', daydir)
                        for path in self.jels.copy():
                            if Path(path).parent == daydir:
                                logger.info('removing JEL')
                                self.jels[path].close()
                                del self.jels[path]
                        shutil.rmtree(daydir)
                        continue
                    # let other processing happen
                    await asyncio.sleep(0)
