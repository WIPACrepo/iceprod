"""
The Condor plugin.  Allows submission to
`HTCondor <http://research.cs.wisc.edu/htcondor/>`_.

Note: Condor was renamed to HTCondor in 2012.
"""
from __future__ import print_function
import os
import sys
import random
import math
import logging
import getpass
from datetime import datetime,timedelta
import subprocess
from functools import partial

import tornado.gen
from tornado.concurrent import run_on_executor

from iceprod.core import dataclasses
from iceprod.core import functions
from iceprod.server import GlobalID
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

def condor_os_reqs(os_arch):
    """Convert from OS_ARCH to Condor OS requirements"""
    os_arch = os_arch.rsplit('_',2)[0].rsplit('.',1)[0]
    reqs = 'OpSysAndVer =?= "{}"'.format(os_arch.replace('RHEL','CentOS').replace('_',''))
    reqs = reqs + '|| OpSysAndVer =?= "{}"'.format(os_arch.replace('RHEL','SL').replace('_',''))
    reqs = reqs + ' || OSGVO_OS_STRING =?= "{}"'.format(os_arch.replace('_',' '))
    return '('+reqs+')'

class condor(grid.BaseGrid):

    ### Plugin Overrides ###

    # let the basic plugin be dumb and implement as little as possible

    @run_on_executor
    def generate_submit_file(self, task, cfg=None, passkey=None,
                             filelist=None):
        """Generate queueing system submit file for task in dir."""
        args = self.get_submit_args(task,cfg=cfg,passkey=passkey)

        # get requirements and batchopts
        requirements = []
        batch_opts = {}
        for b in self.queue_cfg['batchopts']:
            if b.lower() == 'requirements':
                requirements.append(self.queue_cfg['batchopts'][b])
            else:
                batch_opts[b] = self.queue_cfg['batchopts'][b]
        if cfg:
            if (cfg['steering'] and 'batchsys' in cfg['steering'] and
                cfg['steering']['batchsys']):
                for b in cfg['steering']['batchsys']:
                    if b.lower().startswith(self.__class__.__name__):
                        # these settings apply to this batchsys
                        for bb in cfg['steering']['batchsys'][b]:
                            value = cfg['steering']['batchsys'][b][bb]
                            if bb.lower() == 'requirements':
                                requirements.append(value)
                            else:
                                batch_opts[bb] = value
            if task['task_id'] != 'pilot':
                if 'task' in cfg['options']:
                    t = cfg['options']['task']
                    if t in cfg['tasks']:
                        alltasks = [cfg['tasks'][t]]
                    else:
                        alltasks = []
                        try:
                            for tt in cfg['tasks']:
                                if t == tt['name']:
                                    alltasks.append(tt)
                        except Exception:
                            logger.warning('error finding specified task to run for %r',
                                        task,exc_info=True)
                else:
                    alltasks = cfg['tasks']
                for t in alltasks:
                    if 'batchsys' in t and t['batchsys']:
                        for b in t['batchsys']:
                            if b.lower().startswith(self.__class__.__name__):
                                # these settings apply to this batchsys
                                for bb in t['batchsys'][b]:
                                    value = t['batchsys'][b][bb]
                                    if bb.lower() == 'requirements':
                                        requirements.append(value)
                                    else:
                                        batch_opts[bb] = value

        # write the submit file
        submit_file = os.path.join(task['submit_dir'],'condor.submit')
        with open(submit_file,'w') as f:
            p = partial(print,sep='',file=f)
            p('universe = vanilla')
            p('executable = {}'.format(os.path.join(task['submit_dir'],'loader.sh')))
            p('log = condor.log')
            p('output = condor.out.$(Process)')
            p('error = condor.err.$(Process)')
            p('notification = never')
            p('+IsIceProdJob = True') # mark as IceProd for monitoring
            p('want_graceful_removal = True')
            if filelist:
                p('transfer_input_files = {}'.format(','.join(filelist)))
                p('skip_filechecks = True')
                p('should_transfer_files = always')
                p('when_to_transfer_output = ON_EXIT_OR_EVICT')
                p('+SpoolOnEvict = False')
            p('transfer_output_files = iceprod_log, iceprod_out, iceprod_err')
            if 'num' in task:
                p('transfer_output_remaps = "iceprod_log=iceprod_log_$(Process)'
                  ';iceprod_out=iceprod_out_$(Process)'
                  ';iceprod_err=iceprod_err_$(Process)"')

            # handle resources
            p('+JobIsRunning = (JobStatus =!= 1) && (JobStatus =!= 5)')
            if 'reqs' in task:
                if 'cpu' in task['reqs'] and task['reqs']['cpu']:
                    p('+OriginalCpus = {}'.format(task['reqs']['cpu']))
                    p('+RequestResizedCpus = ((Cpus < OriginalCpus) ? OriginalCpus : Cpus)')
                    p('+JOB_GLIDEIN_Cpus = "$$(Cpus:0)"')
                    p('+JobIsRunningCpus = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Cpus)))')
                    p('+JobCpus = (JobIsRunningCpus ? int(MATCH_EXP_JOB_GLIDEIN_Cpus) : OriginalCpus)')
                    p('request_cpus = (!isUndefined(Cpus)) ? RequestResizedCpus : JobCpus')
                    p('Rank = Rank + (isUndefined(Cpus) ? 0 : Cpus)/8')
                if 'gpu' in task['reqs'] and task['reqs']['gpu']:
                    p('+OriginalGpus = {}'.format(task['reqs']['gpu']))
                    p('+RequestResizedGpus = (Gpus < OriginalGpus) ? OriginalGpus : Gpus')
                    p('+JOB_GLIDEIN_Gpus = "$$(Gpus:0)"')
                    p('+JobIsRunningGpus = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Gpus)))')
                    p('+JobGpus = (JobIsRunningGpus ? int(MATCH_EXP_JOB_GLIDEIN_GPUs) : OriginalGpus)')
                    p('request_gpus = !isUndefined(Gpus) ? RequestResizedGpus : JobGpus')
                if 'memory' in task['reqs'] and task['reqs']['memory']:
                    # extra 100MB for pilot
                    p('+OriginalMemory = {}'.format(int(task['reqs']['memory']*1000+100)))
                    p('+RequestResizedMemory = (Memory < OriginalMemory) ? OriginalMemory : Memory')
                    p('+JOB_GLIDEIN_Memory = "$$(Memory:0)"')
                    p('+JobIsRunningMemory = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Memory)))')
                    p('+JobMemory = (JobIsRunningMemory ? int(MATCH_EXP_JOB_GLIDEIN_Memory) : OriginalMemory)')
                    p('request_memory = !isUndefined(Memory) ? RequestResizedMemory : JobMemory')
                else:
                    p('request_memory = 1000')
                if 'disk' in task['reqs'] and task['reqs']['disk']:
                    p('+OriginalDisk = {}'.format(int(task['reqs']['disk']*1000000)))
                    p('+RequestResizedDisk = (Disk-10000 < OriginalDisk) ? OriginalDisk : Disk-10000')
                    p('+JOB_GLIDEIN_Disk = "$$(Disk:0)"')
                    p('+JobIsRunningDisk = (JobIsRunning && (!isUndefined(MATCH_EXP_JOB_GLIDEIN_Disk)))')
                    p('+JobDisk = (JobIsRunningDisk ? int(MATCH_EXP_JOB_GLIDEIN_Disk) : OriginalDisk)')
                    p('request_disk = !isUndefined(Disk) ? RequestResizedDisk : JobDisk')
                if 'time' in task['reqs'] and task['reqs']['time']:
                    # extra 10 min for pilot
                    p('+OriginalTime = {}'.format(int(task['reqs']['time'])*3600+600))
                    p('+TargetTime = (!isUndefined(Target.PYGLIDEIN_TIME_TO_LIVE) ? Target.PYGLIDEIN_TIME_TO_LIVE : Target.TimeToLive)')
                    p('Rank = Rank + (TargetTime - OriginalTime)/86400')
                    requirements.append('TargetTime > OriginalTime')
                if 'os' in task['reqs'] and task['reqs']['os']:
                    requirements.append(condor_os_reqs(task['reqs']['os']))

            for b in batch_opts:
                p(b+'='+batch_opts[b])
            if requirements:
                p('requirements = ('+')&&('.join(requirements)+')')

            if task['task_id'] == 'pilot' and 'pilot_ids' in task:
                for pilot_id in task['pilot_ids']:
                    p('arguments = ',' '.join(args + ['--pilot_id', pilot_id]))
                    p('queue')
            elif 'num' in task:
                p('arguments = ',' '.join(args))
                p('queue {}'.format(task['num']))
            else:
                p('arguments = ',' '.join(args))
                p('queue')

    @run_on_executor
    def submit(self,task):
        """Submit task to queueing system."""
        cmd = ['condor_submit','-terse','condor.submit']
        out = check_output_clean_env(cmd, cwd=task['submit_dir'], universal_newlines=True)
        grid_queue_id = []
        for line in out.split('\n'):
            # look for range
            parts = [p.strip() for p in line.split('-') if p.strip()]
            if len(parts) != 2:
                continue
            major = parts[0].split('.')[0]
            minor_1 = int(parts[0].split('.')[1])
            minor_2 = int(parts[1].split('.')[1])
            for i in range(minor_1, minor_2+1):
                grid_queue_id.append('{}.{}'.format(major,i))
        task['grid_queue_id'] = ','.join(grid_queue_id)

    @run_on_executor
    def get_grid_status(self):
        """Get all tasks running on the queue system.
           Returns {grid_queue_id:{status,submit_dir}}
        """
        ret = {}
        cmd = ['condor_q',getpass.getuser(),'-af:j','jobstatus','cmd']
        out = check_output_clean_env(cmd, universal_newlines=True)
        print('get_grid_status():',out)
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,cmd = line.split()
            if 'loader.sh' not in cmd:
                continue
            if status == '1':
                status = 'queued'
            elif status == '2':
                status = 'processing'
            elif status == '4':
                status = 'completed'
            elif status in ('3','5','6'):
                status = 'error'
            else:
                status = 'unknown'
            ret[gid] = {'status':status,'submit_dir':os.path.dirname(cmd)}
        return ret

    @run_on_executor
    def remove(self,tasks):
        """Remove tasks from queueing system."""
        if tasks:
            check_call_clean_env(['condor_rm']+list(tasks))
