"""
The Condor plugin.  Allows submission to
`HTCondor <http://research.cs.wisc.edu/htcondor/>`_.

Note: Condor was renamed to HTCondor in 2012.
"""
from dataclasses import dataclass, asdict
from datetime import datetime
import enum
import getpass
import glob
from functools import partial, total_ordering
import importlib
import logging
import os
from pathlib import Path
import shutil
import subprocess
import time

import classad
import htcondor
from rest_tools.client import RestClient
from tornado.concurrent import run_on_executor

from iceprod.core.config import Task
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


JOB_EVENT_STATUS_TRANSITIONS = {
    htcondor.JobEventType.SUBMIT: grid.JobStatus.IDLE,
    htcondor.JobEventType.JOB_STAGE_IN: grid.JobStatus.TRANSFERRING_INPUT,
    htcondor.JobEventType.JOB_STAGE_OUT: grid.JobStatus.TRANSFERRING_OUTPUT,
    htcondor.JobEventType.EXECUTE: grid.JobStatus.RUNNING,
    htcondor.JobEventType.JOB_EVICTED: grid.JobStatus.IDLE,
    htcondor.JobEventType.JOB_UNSUSPENDED: grid.JobStatus.IDLE,
    htcondor.JobEventType.JOB_RELEASED: grid.JobStatus.IDLE,
    htcondor.JobEventType.SHADOW_EXCEPTION: grid.JobStatus.IDLE,
    htcondor.JobEventType.JOB_RECONNECT_FAILED: grid.JobStatus.IDLE,
    htcondor.JobEventType.JOB_TERMINATED: grid.JobStatus.FAILED,
    htcondor.JobEventType.JOB_HELD: grid.JobStatus.FAILED,
    htcondor.JobEventType.JOB_SUSPENDED: grid.JobStatus.FAILED,
    htcondor.JobEventType.JOB_ABORTED: grid.JobStatus.FAILED,
}

TRANSFER_EVENT_STATUS_TRANSITIONS = {
    htcondor.FileTransferEventType.IN_QUEUED: grid.JobStatus.TRANSFERRING_INPUT,
    htcondor.FileTransferEventType.IN_STARTED: grid.JobStatus.TRANSFERRING_INPUT,
    htcondor.FileTransferEventType.OUT_QUEUED: grid.JobStatus.TRANSFERRING_OUTPUT,
    htcondor.FileTransferEventType.OUT_STARTED: grid.JobStatus.TRANSFERRING_OUTPUT,
}


@total_ordering
@dataclass(kw_only=True, slots=True)
class CondorJob(grid.BaseGridJob):
    """Holds the job states for an HTCondor cluster."""
    raw_status: htcondor.JobEventType
    cluster_id: int
    proc_id: int = 0

    def __eq__(self, other):
        return self.cluster_id == other.cluster_id and self.proc_id == other.proc_id

    def __lt__(self, other):
        return self.cluster_id < other.cluster_id or self.cluster_id == other.cluster_id and self.proc_id < other.proc_id



class CondorJobActions(grid.GridJobActions):
    """HTCondor job actions"""
    def __init__(self, site: str, rest_client: RestClient, submit_dir: Path, cfg: IceProdConfig):
        super().__init__(site=site, rest_client=rest_client)
        self.submit_dir = submit_dir
        self.cfg = cfg
        self.condor_schedd = htcondor.Schedd()
        self.precmd = submit_dir / 'pre.sh'
        submit_dir.mkdir(parents=True, exist_ok=True)
        self.precmd.write_text((importlib.resources.files('iceprod.server')/'data'/'condor_input_precmd.py').read_text())
        self.precmd.chmod(0o777)
        self.transfer_plugins = self.condor_plugin_discovery()

    def condor_plugin_discovery(self):
        """Find all available HTCondor transfer plugins, and copy them to the submit_dir"""
        src_dir = importlib.resources.files('iceprod.server')/'data'/'condor_transfer_plugins'
        if src_dir.is_dir():
            dest_dir = self.submit_dir / 'transfer_plugins'
            shutil.copytree(src_dir, dest_dir, dirs_exist_ok=True)
            ret = {}
            for p in dest_dir.iterdir():
                p.chmod(0o777)
                for line in subprocess.run([str(p), '-classad'], capture_output=True, text=True, check=True).stdout:
                    if line.startswith('SupportedMethods'):
                        ret[line.split('=')[-1].strip()] = str(p)
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
        if 'memory' in task.requirements and task.requirements['memory']:
            ads['request_memory'] = int(task.requirements['memory']*1000)
        else:
            ads['request_memory'] = 1000
        if 'disk' in task.requirements and task.requirements['disk']:
            ads['request_disk'] = int(task.requirements['disk']*1000000)
        if 'time' in task.requirements and task.requirements['time']:
            ads['+OriginalTime'] = int(task.requirements['time']*3600)
            ads['+TargetTime'] = '(!isUndefined(Target.PYGLIDEIN_TIME_TO_LIVE) ? Target.PYGLIDEIN_TIME_TO_LIVE : Target.TimeToLive)'
            requirements.append('TargetTime > OriginalTime')
        if 'os' in task.requirements and task.requirements['os']:
            if len(task.requirements['os']) == 1:
                requirements.append(CondorJobActions.condor_os_reqs(task.requirements['os'][0]))
            else:
                requirements.append('('+')||('.join(CondorJobActions.condor_os_reqs(os) for os in task.requirements['os'])+')')
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
            files.append(infile.url)
            basename = Path(infile.url).name
            if basename != infile.local:
                mapping.append((basename,infile.local))
        ads = {}
        if mapping:
            ads['PreCmd'] = self.precmd.name
            ads['PreArguments'] = ' '.join(f"'{k}'='{v}'" for k,v in mapping)
            files.append(str(self.precmd))
        if files:
            ads['transfer_input_files'] = ','.join(files)
        return ads

    def condor_outfiles(infiles):
        """Convert from set[Data] to HTCondor classads for output files"""
        files = []
        mapping = []
        for infile in infiles:
            files.append(infile.local)
            mapping.append((infile.local,infile.url))
        ads = {}
        ads['transfer_output_files'] = ','.join(files)
        ads['transfer_output_remaps'] = ';'.join(f'{name} = {url}' for name,url in mapping)
        return ads

    async def submit(self, jobs: list[grid.BaseGridJob]):
        """
        Submit multiple jobs to Condor.

        Assumes that the resource requirements are identical.
        """
        cluster_ad = {
            'log': self.get_current_logfile(),
            'notification': 'never',
            '+IsIceProdJob': True,
            '+IceProdSite': self.site,
            'transfer_plugins': self.transfer_plugins,
            'when_to_transfer_output': 'ON_EXIT',
            'should_transfer_files': 'YES',
            'job_ad_information_attrs': 'Iwd, IceProdDataset, IceProdTaskId',
            'batch_name': 'IceProdDataset',
        }
        proc_ads = []
        for job in jobs:
            transfer_input = []
            transfer_output = []
            proc_ad = {
                '+IceProdDatasetId': job.task.dataset.dataset_id,
                '+IceProdDataset': job.task.dataset.dataset_num,
                '+IceProdJobId': job.task.job.job_id,
                '+IceProdJobIndex': job.task.job.job_index,
                '+IceProdTaskId': job.task.task_id,
                '+IceProdTaskName': job.task.name,
                'Iwd': job.submit_dir,
                'executable': job.executable,
            }
            proc_ad.update(self.condor_infiles(job.infiles))
            proc_ad.update(self.condor_outfiles(job.infiles))
            proc_ad.update(self.condor_resource_reqs(job.task))
            proc_ads.append((proc_ad, 1))

        cluster_id = self.condor_schedd.submitMany(cluster_ad, proc_ads)
        for proc_id,job in enumerate(jobs):
            task_id = job.task.task_id
            logger.info("task %s submitted as %d.%d", task_id, cluster_id, proc_id)
            cj = CondorJob(**asdict(job), cluster_id=cluster_id, proc_id=proc_id)
            self.jobs[task_id] = cj

    async def job_update(self, job: CondorJob):
        """
        Send updated info from the batch system to the IceProd API.

        Must handle dup calls.
        """
        await super().job_updates(job)

    async def finish(self, job: CondorJob):
        """
        Run cleanup actions after a batch job completes.

        Must handle dup calls.
        """
        if job.raw_status != htcondor.JobEventType.JOB_TERMINATED:
            # clean up queue if necessary
            pass  # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            

        # do global actions
        await super().finish(job)
        
    def get_current_logfile(self) -> str:
        """
        Get the current Job Event Log, possibly creating a new one
        if the day rolls over.

        Returns:
            Path: filename to current JEL
        """
        day = datetime.utcnow().date.isoformat()
        day_submit_dir = self.submit_dir / day
        if not day_submit_dir.exists():
            day_submit_dir.mkdir(mode=0o700, parents=True)
        cur_jel = day_submit_dir / 'jobs.jel'
        return cur_jel



class ActiveJobs(grid.BaseActiveJobs):
    """HTCondor active jobs plugin"""
    def __init__(self, jobs: CondorJobActions, submit_dir: Path, cfg: IceProdConfig):
        super().__init__(jobs, submit_dir)
        self.cfg = cfg
        self.jels = {filename: htcondor.JobEventLog(str(filename)).events(0) for filename in self.submit_dir.glob('*/*.jel')}
        self.job_states = {}

    def get_current_JEL(self) -> str:
        cur_jel = self.jobs.get_current_logfile()
        cur_jel_str = str(cur_jel)
        if cur_jel_str not in self.jels:
            self.jels[cur_jel_str] = htcondor.JobEventLog(cur_jel_str).events(0)
        return cur_jel

    async def load(self):
        """
        Load currently active jobs.

        Scan the Job Event Logs, then run a check().
        """
        await self.wait(0)
        await self.check()

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
                    new_status = JOB_EVENT_STATUS_TRANSITIONS.get(event.type, None)
                    if event.type == htcondor.JobEventType.FILE_TRANSFER and 'Type' in event:
                        new_status = TRANSFER_EVENT_STATUS_TRANSITIONS.get(event['Type'], None)
                    elif event.type == htcondor.JobEventType.JOB_TERMINATED:
                        if event['TerminatedNormally'] and event['ReturnValue'] == 0:
                            new_status = grid.JobStatus.COMPLETED
                        # get stats
                        mem = event['RunRemoteUsage']
                        data_in = event['ReceivedBytes']
                        data_out = event['SentBytes']

                    if new_status is None:
                        continue

                    # handle multiple jobs in a single cluster
                    cluster = self.job_states.setdefault(
                        event.cluster,
                        Cluster(
                            cluster_id=event.cluster,
                            event_log_path=filename,
                            procs={},
                        ),
                    )
                    cluster.procs[event.proc] = new_status
                    

            if time.monotonic() - start >= timeout:
                break
            await asyncio.sleep(1)

    async def check(self):
        """
        Do a cross-check, to verify `self.jobs` vs the submit dir and IceProd API.
        """
        pass

    async def check_submit_dir(self):
        """
        Return directory paths that should be cleaned up.
        """
        # get time limits
        try:
            queued_time = timedelta(seconds=self.cfg['queue']['max_task_queued_time'])
        except Exception:
            queued_time = timedelta(seconds=86400*2)
        try:
            processing_time = timedelta(seconds=self.cfg['queue']['max_task_processing_time'])
        except Exception:
            processing_time = timedelta(seconds=86400*2)
        try:
            suspend_time = timedelta(seconds=self.cfg['queue']['suspend_submit_dir_time'])
        except Exception:
            suspend_time = timedelta(seconds=86400)
        all_time = queued_time + processing_time + suspend_time
        now = time.time()

        for daydir in self.submit_dir.iterdir():
            if daydir.is_dir():
                for path in daydir.iterdir():
                    st = path.lstat()
                    if stat.S_ISDIR(st.st_mode):
                        if now - all_time > st.st_birthtime:
                            yield path
                # let other processing happen
                await asyncio.sleep(0)
        

    


class Grid(grid.BaseGrid):
    """HTCondor grid plugin"""
    def get_active_jobs(self):
        return ActiveJobs(CondorJobActions(self.site, self.rest_client, self.submit_dir, self.cfg), self.submit_dir, self.cfg)

    def get_submit_dir(self):
        # set local submit dir to match day/JEL
        jel = self.active_jobs.jobs.get_current_logfile()
        return jel.parent















# class condor(grid.BaseGrid):
    # """Plugin Overrides for HTCondor pilot submission"""

    # # let the basic plugin be dumb and implement as little as possible

    # @run_on_executor
    # def generate_submit_file(self, task, cfg=None, passkey=None,
                             # filelist=None):
        # """Generate queueing system submit file for task in dir."""
        # args = self.get_submit_args(task,cfg=cfg,passkey=passkey)

        # # get requirements and batchopts
        # requirements = []
        # batch_opts = {}
        # for b in self.queue_cfg['batchopts']:
            # if b.lower() == 'requirements':
                # requirements.append(self.queue_cfg['batchopts'][b])
            # else:
                # batch_opts[b] = self.queue_cfg['batchopts'][b]
        # if cfg:
            # if (cfg['steering'] and 'batchsys' in cfg['steering'] and
                    # cfg['steering']['batchsys']):
                # for b in cfg['steering']['batchsys']:
                    # if b.lower().startswith(self.__class__.__name__):
                        # # these settings apply to this batchsys
                        # for bb in cfg['steering']['batchsys'][b]:
                            # value = cfg['steering']['batchsys'][b][bb]
                            # if bb.lower() == 'requirements':
                                # requirements.append(value)
                            # else:
                                # batch_opts[bb] = value
            # if task['task_id'] != 'pilot':
                # if 'task' in cfg['options']:
                    # t = cfg['options']['task']
                    # if t in cfg['tasks']:
                        # alltasks = [cfg['tasks'][t]]
                    # else:
                        # alltasks = []
                        # try:
                            # for tt in cfg['tasks']:
                                # if t == tt['name']:
                                    # alltasks.append(tt)
                        # except Exception:
                            # logger.warning('error finding specified task to run for %r',
                                           # task,exc_info=True)
                # else:
                    # alltasks = cfg['tasks']
                # for t in alltasks:
                    # if 'batchsys' in t and t['batchsys']:
                        # for b in t['batchsys']:
                            # if b.lower().startswith(self.__class__.__name__):
                                # # these settings apply to this batchsys
                                # for bb in t['batchsys'][b]:
                                    # value = t['batchsys'][b][bb]
                                    # if bb.lower() == 'requirements':
                                        # requirements.append(value)
                                    # else:
                                        # batch_opts[bb] = value

        # # write the submit file
        # submit_file = os.path.join(task['submit_dir'],'condor.submit')
        # with open(submit_file,'w') as f:
            # p = partial(print,sep='',file=f)
            # p('universe = vanilla')
            # p('executable = {}'.format(os.path.join(task['submit_dir'],'loader.sh')))
            # p('log = condor.log')
            # p('output = condor.out.$(Process)')
            # p('error = condor.err.$(Process)')
            # p('notification = never')
            # p('+IsIceProdJob = True')  # mark as IceProd for monitoring
            # p('want_graceful_removal = True')
            # if filelist:
                # p('transfer_input_files = {}'.format(','.join(filelist)))
                # p('skip_filechecks = True')
                # p('should_transfer_files = always')
                # p('when_to_transfer_output = ON_EXIT_OR_EVICT')
                # p('+SpoolOnEvict = False')
            # p('transfer_output_files = iceprod_log, iceprod_out, iceprod_err')
            # if 'num' in task:
                # p('transfer_output_remaps = "iceprod_log=iceprod_log_$(Process)'
                  # ';iceprod_out=iceprod_out_$(Process)'
                  # ';iceprod_err=iceprod_err_$(Process)"')

            # # handle resources
            # p('+JobIsRunning = (JobStatus =!= 1) && (JobStatus =!= 5)')
            # if 'reqs' in task:
                # if 'cpu' in task['reqs'] and task['reqs']['cpu']:
                    # p('+OriginalCpus = {}'.format(task['reqs']['cpu']))
                    # p('+RequestResizedCpus = ((Cpus < OriginalCpus) ? OriginalCpus : Cpus)')
                    # p('+JOB_GLIDEIN_Cpus = "$$(Cpus:0)"')
                    # p('+JobIsRunningCpus = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Cpus)))')
                    # p('+JobCpus = (JobIsRunningCpus ? int(MATCH_EXP_JOB_GLIDEIN_Cpus) : OriginalCpus)')
                    # p('request_cpus = (!isUndefined(Cpus)) ? RequestResizedCpus : JobCpus')
                    # p('Rank = Rank + (isUndefined(Cpus) ? 0 : Cpus)/8')
                # if 'gpu' in task['reqs'] and task['reqs']['gpu']:
                    # p('+OriginalGpus = {}'.format(task['reqs']['gpu']))
                    # p('+RequestResizedGpus = (Gpus < OriginalGpus) ? OriginalGpus : Gpus')
                    # p('+JOB_GLIDEIN_Gpus = "$$(Gpus:0)"')
                    # p('+JobIsRunningGpus = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Gpus)))')
                    # p('+JobGpus = (JobIsRunningGpus ? int(MATCH_EXP_JOB_GLIDEIN_GPUs) : OriginalGpus)')
                    # p('request_gpus = !isUndefined(Gpus) ? RequestResizedGpus : JobGpus')
                # if 'memory' in task['reqs'] and task['reqs']['memory']:
                    # # extra 100MB for pilot
                    # p('+OriginalMemory = {}'.format(int(task['reqs']['memory']*1000+100)))
                    # p('+RequestResizedMemory = (Memory < OriginalMemory) ? OriginalMemory : Memory')
                    # p('+JOB_GLIDEIN_Memory = "$$(Memory:0)"')
                    # p('+JobIsRunningMemory = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Memory)))')
                    # p('+JobMemory = (JobIsRunningMemory ? int(MATCH_EXP_JOB_GLIDEIN_Memory) : OriginalMemory)')
                    # p('request_memory = !isUndefined(Memory) ? RequestResizedMemory : JobMemory')
                # else:
                    # p('request_memory = 1000')
                # if 'disk' in task['reqs'] and task['reqs']['disk']:
                    # p('+OriginalDisk = {}'.format(int(task['reqs']['disk']*1000000)))
                    # p('+RequestResizedDisk = (Disk-10000 < OriginalDisk) ? OriginalDisk : Disk-10000')
                    # p('+JOB_GLIDEIN_Disk = "$$(Disk:0)"')
                    # p('+JobIsRunningDisk = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Disk)))')
                    # p('+JobDisk = (JobIsRunningDisk ? int(MATCH_EXP_JOB_GLIDEIN_Disk) : OriginalDisk)')
                    # p('request_disk = !isUndefined(Disk) ? RequestResizedDisk : JobDisk')
                # if 'time' in task['reqs'] and task['reqs']['time']:
                    # # extra 10 min for pilot
                    # p('+OriginalTime = {}'.format(int(task['reqs']['time'])*3600+600))
                    # p('+TargetTime = (!isUndefined(Target.PYGLIDEIN_TIME_TO_LIVE) ? Target.PYGLIDEIN_TIME_TO_LIVE : Target.TimeToLive)')
                    # p('Rank = Rank + (TargetTime - OriginalTime)/86400')
                    # requirements.append('TargetTime > OriginalTime')
                # if 'os' in task['reqs'] and task['reqs']['os']:
                    # requirements.append(condor_os_reqs(task['reqs']['os']))

            # for b in batch_opts:
                # p(b+'='+batch_opts[b])
            # if requirements:
                # p('requirements = ('+')&&('.join(requirements)+')')

            # if task['task_id'] == 'pilot' and 'pilot_ids' in task:
                # for pilot_id in task['pilot_ids']:
                    # p('arguments = ',' '.join(args + ['--pilot_id', pilot_id]))
                    # p('queue')
            # elif 'num' in task:
                # p('arguments = ',' '.join(args))
                # p('queue {}'.format(task['num']))
            # else:
                # p('arguments = ',' '.join(args))
                # p('queue')

    # @run_on_executor
    # def submit(self,task):
        # """Submit task to queueing system."""
        # cmd = ['condor_submit','-terse','condor.submit']
        # out = check_output_clean_env(cmd, cwd=task['submit_dir'], universal_newlines=True)
        # grid_queue_id = []
        # for line in out.split('\n'):
            # # look for range
            # parts = [p.strip() for p in line.split('-') if p.strip()]
            # if len(parts) != 2:
                # continue
            # major = parts[0].split('.')[0]
            # minor_1 = int(parts[0].split('.')[1])
            # minor_2 = int(parts[1].split('.')[1])
            # for i in range(minor_1, minor_2+1):
                # grid_queue_id.append('{}.{}'.format(major,i))
        # task['grid_queue_id'] = ','.join(grid_queue_id)

    # @run_on_executor
    # def get_grid_status(self):
        # """Get all tasks running on the queue system.
           # Returns {grid_queue_id:{status,submit_dir}}
        # """
        # ret = {}
        # cmd = ['condor_q',getpass.getuser(),'-af:j','jobstatus','cmd']
        # out = check_output_clean_env(cmd, universal_newlines=True)
        # print('get_grid_status():',out)
        # for line in out.split('\n'):
            # if not line.strip():
                # continue
            # gid,status,cmd = line.split()
            # if 'loader.sh' not in cmd:
                # continue
            # if status == '1':
                # status = 'queued'
            # elif status == '2':
                # status = 'processing'
            # elif status == '4':
                # status = 'completed'
            # elif status in ('3','5','6'):
                # status = 'error'
            # else:
                # status = 'unknown'
            # ret[gid] = {'status':status,'submit_dir':os.path.dirname(cmd)}
        # return ret

    # @run_on_executor
    # def remove(self,tasks):
        # """Remove tasks from queueing system."""
        # if tasks:
            # check_call_clean_env(['condor_rm']+list(tasks))
