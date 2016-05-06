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

from iceprod.core import dataclasses
from iceprod.core import functions
from iceprod.server import GlobalID
from iceprod.server import grid

logger = logging.getLogger('condor')

    
class condor(grid.grid):

    ### Plugin Overrides ###

    # let the basic plugin be dumb and implement as little as possible

    def generate_submit_file(self,task,cfg=None,passkey=None,
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
                        except:
                            logger.warn('error finding specified task to run for %r',
                                        task,exc_info=True)
                else:
                    alltasks = cfg['tasks']
                for t in alltasks:
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
            if filelist:
                p('transfer_input_files = {}'.format(','.join(filelist)))
                p('skip_filechecks = True')
                p('should_transfer_files = always')
            p('transfer_output_files = iceprod_log, iceprod_out, iceprod_err')
            if 'num' in task:
                p('transfer_output_remaps = "iceprod_log=iceprod_log_$(Process)'
                  ';iceprod_out=iceprod_out_$(Process)'
                  ';iceprod_err=iceprod_err_$(Process)"')
            p('arguments = ',' '.join(args))

            if 'reqs' in task:
                if 'cpu' in task['reqs']:
                    p('request_cpus = {}'.format(task['reqs']['cpu']))
                if 'gpu' in task['reqs']:
                    p('request_gpus = {}'.format(task['reqs']['gpu']))
                if 'memory' in task['reqs']:
                    p('request_memory = {}'.format(int(task['reqs']['memory']*1000)))
                if 'disk' in task['reqs']:
                    p('request_disk = {}'.format(int(task['reqs']['disk']*1000000)))

            for b in batch_opts:
                p(b+'='+batch_opts[b])
            if requirements:
                p('requirements = ('+')&&('.join(requirements)+')')

            if 'num' in task:
                p('queue {}'.format(task['num']))
            else:
                p('queue')

    def submit(self,task):
        """Submit task to queueing system."""
        cmd = ['condor_submit','condor.submit']
        out = subprocess.check_output(cmd,cwd=task['submit_dir'])
        for line in out.split('\n'):
            line = line.strip()
            if 'cluster' in line:
                task['grid_queue_id'] = line.split()[-1].strip('.')

    def get_grid_status(self):
        """Get all tasks running on the queue system.
           Returns {grid_queue_id:{status,submit_dir}}
        """
        ret = {}
        cmd = ['condor_q',getpass.getuser(),'-af:j','jobstatus','cmd']
        out = subprocess.check_output(cmd)
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,cmd = line.split()
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

    def remove(self,tasks):
        """Remove tasks from queueing system."""
        if tasks:
            subprocess.check_call(['condor_rm']+list(tasks))
