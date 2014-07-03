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
    
    def generate_submit_file(self,task,cfg=None,passkey=None):
        """Generate queueing system submit file for task in dir."""
        # get args
        args = ['-d {}'.format(self.web_address)]
        if 'platform' in self.queue_cfg and self.queue_cfg['platform']:
            args.append('-m {}'.format(self.queue_cfg['platform']))
        if self.cfg['download']['http_username']:
            args.append('-u {}'.format(self.cfg['download']['http_username']))
        if self.cfg['download']['http_password']:
            args.append('-p {}'.format(self.cfg['download']['http_password']))
        if self.x509:
            args.append('-x {}'.format(self.x509))
        if passkey:
            args.append('--passkey {}'.format(passkey))
        
        # get requirements and batchopts
        requirements = []
        batch_opts = {}
        for b in self.queue_cfg['batchopts']:
            if b.lower() == 'requirements':
                requirements.append(self.queue_cfg['batchopts'][b])
            else:
                batch_opts[b] = self.queue_cfg['batchopts'][b]
        if cfg:
            if cfg['steering']:
                for b in cfg['steering']['batchsys']:
                    if b.lower().startswith(self.__class__.__name__):
                        # these settings apply to this batchsys
                        for bb in cfg['steering']['batchsys'][b]:
                            value = cfg['steering']['batchsys'][b][bb]
                            if bb.lower() == 'requirements':
                                requirements.append(value)
                            else:
                                batch_opts[bb] = value
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
            p('output = condor.out')
            p('error = condor.err')
            p('notification = never')
            if task['task_id'] != 'pilot':
                p('transfer_input_files = {}'.format(
                    os.path.join(task['submit_dir'],'task.cfg')))
                p('should_transfer_files = always')
                args.append('--cfgfile task.cfg')
            else:
                args.append('--gridspec "{}"'.format(self.gridspec))
            p('arguments = ',' '.join(args))
            
            for b in batch_opts:
                p(b+'='+batch_opts[b])
            if requirements:
                p('requirements = ('+')&&('.join(requirements)+')')
            
            p('queue')
    
    def submit(self,task):
        """Submit task(s) to queueing system."""
        if isinstance(task,dict) and 'task_id' in task:
            cmd = 'condor_submit condor.submit'
            if subprocess.call(cmd,shell=True,cwd=task['submit_dir']):
                raise Exception('submit failed for task %r'%task['task_id'])
        else:
            raise NotImplementedError('multiple submission not implemented')
    
    