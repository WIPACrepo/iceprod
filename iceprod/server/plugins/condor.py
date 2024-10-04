"""
The Condor plugin.  Allows submission to
`HTCondor <http://research.cs.wisc.edu/htcondor/>`_.

Note: Condor was renamed to HTCondor in 2012.
"""
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, UTC
import enum
import importlib
import logging
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time
from typing import NamedTuple

import htcondor  # type: ignore

from iceprod.core.config import Task
from iceprod.core.exe import WriteToScript, Transfer
from iceprod.server.config import IceProdConfig
from iceprod.server import grid

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
class JobStatus(enum.Enum):
    IDLE = enum.auto()       # job is waiting in the queue
    RUNNING = enum.auto()    # job is running
    FAILED = enum.auto()     # job needs cleanup
    COMPLETED = enum.auto()  # job is out of the queue


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


RESET_REASONS = [
    'SIGTERM',
    'killed',
    'Transfer input files failure',
    'Transfer output files failure',
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

    def __init__(self, cfg: IceProdConfig, submit_dir: Path, credentials_dir: Path):
        self.cfg = cfg
        self.submit_dir = submit_dir
        self.credentials_dir = credentials_dir
        self.condor_schedd = htcondor.Schedd()

        submit_dir.mkdir(parents=True, exist_ok=True)
        self.precmd = submit_dir / 'pre.py'
        self.precmd.write_text((importlib.resources.files('iceprod.server')/'data'/'condor_input_precmd.py').read_text())
        self.precmd.chmod(0o777)
        self.transfer_plugins = self.condor_plugin_discovery()
        logger.info('transfer plugins installed: %s', list(self.transfer_plugins.keys()))

        self.default_container = self.cfg['queue'].get('default_container', 'Undefined')

    def condor_plugin_discovery(self):
        """Find all available HTCondor transfer plugins, and copy them to the submit_dir"""
        src_dir = importlib.resources.files('iceprod.server')/'data'/'condor_transfer_plugins'
        if src_dir.is_dir():
            dest_dir = self.submit_dir / 'transfer_plugins'
            shutil.copytree(src_dir, dest_dir, dirs_exist_ok=True)
            ret = {}
            for p in dest_dir.iterdir():
                p.chmod(0o777)
                logger.debug('transfer plugin %s', p)
                for line in subprocess.run([str(p), '-classad'], capture_output=True, text=True, check=True).stdout.split('\n'):
                    logger.debug('transfer plugin output: %s', line)
                    if line.startswith('SupportedMethods'):
                        ret[line.split('=')[-1].strip(' "')] = str(p)
        return ret

    @staticmethod
    def condor_os_reqs(os_arch):
        """Convert from OS_ARCH to Condor OS requirements"""
        os_arch = os_arch.rsplit('_',2)[0].rsplit('.',1)[0]
        reqs = 'OpSysAndVer =?= "{}"'.format(os_arch.replace('RHEL','CentOS').replace('_',''))
        reqs = reqs + '|| OpSysAndVer =?= "{}"'.format(os_arch.replace('RHEL','SL').replace('_',''))
        reqs = 'isUndefined(OSGVO_OS_STRING) ? ('+reqs+') : OSGVO_OS_STRING =?= "{}"'.format(os_arch.replace('_',' '))
        return reqs

    @staticmethod
    def condor_resource_reqs(task: Task):
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
        if 'os' in task.requirements and task.requirements['os']:
            if len(task.requirements['os']) == 1:
                requirements.append(CondorSubmit.condor_os_reqs(task.requirements['os'][0]))
            else:
                requirements.append('('+')||('.join(CondorSubmit.condor_os_reqs(os) for os in task.requirements['os'])+')')
        if requirements:
            ads['requirements'] = '('+')&&('.join(requirements)+')'
        return ads

    def condor_infiles(self, infiles):
        """Convert from set[Data] to HTCondor classads for input files"""
        files = []
        mapping = []
        x509_proxy = self.cfg['queue'].get('x509proxy', None)
        if x509_proxy:
            files.append(x509_proxy)
        for infile in infiles:
            if infile.url.startswith('gsiftp:') and not x509_proxy:
                raise RuntimeError('need x509 proxy for gridftp!')
            if infile.transfer == Transfer.MAYBE:
                url = 'iceprod://maybe-' + infile.url
            else:
                url = infile.url
            files.append(url)
            basename = Path(infile.url).name
            if basename != infile.local:
                mapping.append((basename,infile.local))
        ads = {}
        if mapping:
            ads['PreCmd'] = f'"{self.precmd.name}"'
            ads['PreArguments'] = '"'+' '.join(f"'{k}'='{v}'" for k,v in mapping)+'"'
            files.append(str(self.precmd))
        if files:
            ads['transfer_input_files'] = files
        return ads

    def condor_outfiles(self, outfiles):
        """Convert from set[Data] to HTCondor classads for output files"""
        files = []
        mapping = []
        for outfile in outfiles:
            files.append(outfile.local)
            if outfile.transfer == Transfer.MAYBE:
                url = 'iceprod://maybe-' + outfile.url
            else:
                url = outfile.url
            mapping.append((outfile.local, url))
        ads = {}
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

    async def submit(self, tasks: list[Task], jel: Path):
        """
        Submit multiple jobs to Condor as a single batch.

        Assumes that the resource requirements are identical.

        Args:
            tasks: IceProd Tasks to submit
            jel: common job event log
        """
        jel_dir = jel.parent
        transfer_plugin_str = ';'.join(f'{k}={v}' for k,v in self.transfer_plugins.items())
        submitfile = f"""
output = $(initialdir)/condor.out
error = $(initialdir)/condor.err
log = {jel}

notification = never
job_ad_information_attrs = Iwd IceProdDatasetId IceProdTaskId IceProdTaskInstanceId CommittedTime RemotePool RemoteHost MATCH_EXP_JOBGLIDEIN_ResourceName
batch_name = Dataset {tasks[0].dataset.dataset_num}

+IsIceProdJob = True
+IceProdSite = {self.cfg["queue"].get("site", "unknown")}
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

        for k,v in tasks[0].get_task_config()['batchsys'].get('condor', {}).items():
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
            s = WriteToScript(task=task, workdir=submit_dir)
            executable = await s.convert()
            logger.debug('running task with exe %r', executable)

            ads = self.AD_DEFAULTS.copy()
            ads.update(self.condor_infiles(s.infiles))
            ads.update(self.condor_outfiles(s.outfiles))
            ads.update(self.condor_resource_reqs(task))

            container = task.get_task_config().get('container', self.default_container)
            if container != 'Undefined':
                container = f'"{container}"'

            reqs2 = reqs
            for k,v in tasks[0].get_task_config()['batchsys'].get('condor', {}).items():
                if k.lower() == 'requirements':
                    reqs2 = f'({reqs}) && ({v})' if reqs else v
                    break

            if reqs2:
                ads["requirements"] = f'({ads["requirements"]}) && ({reqs2})'
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
                'infiles': f'"{";".join(ads["transfer_input_files"])}"',
                'outfiles': f'"{";".join(ads["transfer_output_files"])}"',
                'outremaps': f'"{ads["transfer_output_remaps"]}"',
            })

        submitfile += '\n\nqueue '+','.join(jobset[0].keys())+' from (\n'
        for job in jobset:
            submitfile += '  '+','.join(job.values())+'\n'
        submitfile += ')\n'

        logger.debug("submitfile:\n%s", submitfile)

        s = htcondor.Submit(submitfile)
        self.condor_schedd.submit(s, count=1, itemdata=s.itemdata())

    def remove(self, job_id: str | CondorJob, reason: str | None = None):
        logger.info('removing job %s', job_id)
        self.condor_schedd.act(htcondor.JobAction.Remove, str(job_id), reason=reason)


class Grid(grid.BaseGrid):
    """HTCondor grid plugin"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs = {}
        self.jels = {str(filename): htcondor.JobEventLog(str(filename)).events(0) for filename in self.submit_dir.glob('*/*.jel')}
        self.submitter = CondorSubmit(self.cfg, submit_dir=self.submit_dir, credentials_dir=self.credentials_dir)

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
        # initial job load from JELs
        try:
            await self.wait(timeout=0)
        except Exception:
            logger.warning('failed to wait', exc_info=True)

        logger.info('active JELs: %r', list(self.jels.keys()))

        check_time = time.monotonic()
        while True:
            start = time.monotonic()
            try:
                await self.submit()
            except Exception:
                logger.warning('failed to submit', exc_info=True)
            wait_time = max(0, self.cfg['queue']['submit_interval'] - (time.monotonic() - start))
            try:
                await self.wait(timeout=wait_time)
                self.save_timestamp()
            except Exception:
                logger.warning('failed to wait', exc_info=True)

            now = time.monotonic()
            if now - check_time >= self.cfg['queue']['check_time']:
                try:
                    await self.check()
                except Exception:
                    logger.warning('failed to check', exc_info=True)
                check_time = now

            if not forever:
                break

    # Submit to Condor #

    async def submit(self):
        num_to_submit = self.get_queue_num()
        logger.info("Attempting to submit %d tasks", num_to_submit)
        tasks = await self.get_tasks_to_queue(num_to_submit)
        cur_jel = self.get_current_JEL()

        # split into datasets and task types
        tasks_by_dataset = defaultdict(list)
        for task in tasks:
            tasks_by_dataset[f'{task.dataset.dataset_num}-{task.name}'].append(task)
        for key in tasks_by_dataset:
            tasks = tasks_by_dataset[key]
            try:
                await self.submitter.submit(tasks, cur_jel)
            except Exception as e:
                logger.warning('submit failed for dataset %s', key, exc_info=True)
                async with asyncio.TaskGroup() as tg:
                    for task in tasks:
                        j = CondorJob(dataset_id=task.dataset.dataset_id, task_id=task.task_id, instance_id=task.instance_id)
                        tg.create_task(self.task_reset(j, reason=f'HTCondor submit failed: {e}'))

    def get_queue_num(self) -> int:
        """Determine how many tasks to queue."""
        counts = {s: 0 for s in JobStatus}
        for job in self.jobs.values():
            counts[job.status] += 1

        idle_jobs = counts[JobStatus.IDLE]
        processing_jobs = counts[JobStatus.RUNNING]
        queue_tot_max = self.cfg['queue']['max_total_tasks_on_queue'] - idle_jobs - processing_jobs
        queue_idle_max = self.cfg['queue']['max_idle_tasks_on_queue'] - idle_jobs
        queue_interval_max = self.cfg['queue']['max_tasks_per_submit']
        queue_num = max(0, min(queue_tot_max, queue_idle_max, queue_interval_max))
        return queue_num

    def get_current_JEL(self) -> Path:
        """
        Get the current Job Event Log, possibly creating a new one
        if the day rolls over.

        Returns:
            Path: filename to current JEL
        """
        day = datetime.now(UTC).date().isoformat()
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

    async def wait(self, timeout):
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
                for event in events:
                    if float(event.timestamp) < self.last_event_timestamp:
                        continue
                    self.last_event_timestamp = event.timestamp

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

                            # get stats
                            cpu = event.get('CpusUsage', None)
                            if not cpu:
                                cpu = parse_usage(event.get('RunRemoteUsage', ''))
                            gpu = event.get('GpusUsage', None)
                            memory = event.get('MemoryUsage', None)  # MB
                            disk = event.get('DiskUsage', None)  # KB
                            time_ = event.get('LastRemoteWallClockTime', None)  # seconds
                            # data_in = event['ReceivedBytes']  # KB
                            # data_out = event['SentBytes']  # KB

                            resources = {}
                            if cpu is not None:
                                resources['cpu'] = cpu
                            if gpu is not None:
                                resources['gpu'] = gpu
                            if memory is not None:
                                resources['memory'] = memory/1000.
                            if disk is not None:
                                resources['disk'] = disk/1000000.
                            if time_ is not None:
                                resources['time'] = time_/3600.

                            success = event.get('ReturnValue', 1) == 0
                            job.status = JobStatus.COMPLETED if success else JobStatus.FAILED

                            # finish job
                            await self.finish(job_id, success=success, resources=resources)

                        elif type_ == htcondor.JobEventType.JOB_ABORTED:
                            job.status = JobStatus.FAILED
                            reason = event.get('Reason', None)
                            logger.info("job %s %s.%s removed: %r", job_id, job.dataset_id, job.task_id, reason)
                            await self.finish(job_id, success=False, reason=reason)

                        else:
                            # update status
                            new_status = JOB_EVENT_STATUS_TRANSITIONS.get(type_, None)
                            if new_status is not None and job.status != new_status:
                                job.status = new_status
                                if new_status == JobStatus.FAILED:
                                    self.submitter.remove(job_id, reason=event.get('HoldReason', None))
                                else:
                                    await self.job_update(job)

            if time.monotonic() - start >= timeout:
                break
            await asyncio.sleep(1)

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
                await self.task_idle(job)
            elif job.status == JobStatus.RUNNING:
                await self.task_processing(job)
        except Exception:
            pass

    async def finish(self, job_id: CondorJobId, success: bool = True, resources: dict | None = None, reason: str | None = None):
        """
        Run cleanup actions after a batch job completes.

        Must handle dup calls.
        """
        if job_id not in self.jobs:
            logger.debug('dup call: %s not in job dict', job_id)
            return

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
                    for text in RESET_REASONS:
                        if text in reason:
                            future = self.task_reset(job, reason=reason)
                            break
                if future is None:
                    future = self.task_failure(job, stats=stats, reason=reason, stdout=stdout, stderr=stderr)
                await future
        except Exception:
            logger.warning('failed to update REST', exc_info=True)

        # internal job cleanup
        del self.jobs[job_id]

    # Longer checks #

    async def check(self):
        """
        Do a cross-check, to verify `self.jobs` vs the submit dir and IceProd API.
        """
        # check for reset tasks

        # check for old jobs and dirs
        async for path in self.check_submit_dir():
            for job_id, job in self.jobs.items():
                if job.submit_dir == path:
                    self.submitter.remove(job_id, reason='exceeded max queue time')

    async def check_submit_dir(self):
        """
        Return directory paths that should be cleaned up.
        """
        # get time limits
        queued_time = self.cfg['queue'].get('max_task_queued_time', 86400*2)
        processing_time = self.cfg['queue'].get('max_task_processing_time', 86400*2)
        suspend_time = self.cfg['queue'].get('suspend_submit_dir_time', 86400)
        now = time.time()
        job_old_time = now - (queued_time + processing_time)
        dir_old_time = now - (queued_time + processing_time + suspend_time)

        for daydir in self.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'):
            logger.debug('looking at daydir %s', daydir)
            if daydir.is_dir():
                empty = True
                for path in daydir.iterdir():
                    logger.debug('looking at path %s', path)
                    st = path.lstat()
                    logger.debug('stat: %r', st)
                    if stat.S_ISDIR(st.st_mode):
                        empty = False
                        if st.st_mtime < job_old_time:
                            yield path
                            if st.st_mtime < dir_old_time:
                                logger.info('cleaning up submit dir %s', path)
                                shutil.rmtree(path)
                if empty:
                    st = daydir.lstat()
                    if st.st_mtime < dir_old_time:
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
