"""
A supercomputer plugin, specificially for
`Graham <https://docs.computecanada.ca/wiki/Graham>`_.
"""
from __future__ import print_function
import os
import sys
import stat
import random
import math
import logging
import getpass
import socket
from datetime import datetime,timedelta
import subprocess
import asyncio
from functools import partial

import tornado.gen
from tornado.concurrent import run_on_executor

import iceprod
from iceprod.core import dataclasses
from iceprod.core import constants
from iceprod.core import functions
from iceprod.core.exe_json import ServerComms
from iceprod.server import grid
from iceprod.server.globus import SiteGlobusProxy

logger = logging.getLogger('plugin-graham')

async def subprocess_ssh(host, args):
    """A subprocess call over ssh"""
    cmd = ['ssh', '-i', '~/.ssh/iceprod', host]
    if 'ICEPRODBASE' in os.environ:
        cmd.append(f'{os.environ["ICEPRODBASE"]}/env-shell.sh')
    cmd += args
    await asyncio.create_subprocess_exec(cmd, timeout=20*60, check=True)

class MyServerComms(ServerComms):
    def __init__(self, rest_client):
        self.rest = rest_client

class supercomp_graham(grid.BaseGrid):

    ### Plugin Overrides ###

    # graham gpu queue requirements
    resources = {
        'cpu': 2,
        'memory': 8.,
        'disk': 48.,
        'gpu': 1,
        'time': 4.,
    }

    def __init__(self, *args, **kwargs):
        super(supercomp_graham, self).__init__(*args, **kwargs)
        self.x509proxy = SiteGlobusProxy()

    async def upload_logfiles(self, task_id, dataset_id, submit_dir='', reason=''):
        """upload logfiles"""
        data = {'name': 'stdlog', 'task_id': task_id, 'dataset_id': dataset_id}

        # upload stdlog
        filename = os.path.join(submit_dir, constants['stdlog'])
        if os.path.exists(filename):
            with open(filename) as f:
                data['data'] = f.read()
            await self.rest_client.request('POST', '/logs', data)
        else:
            data['data'] = reason
            await self.rest_client.request('POST', '/logs', data)

        # upload stderr
        data['name'] = 'stderr'
        filename = os.path.join(submit_dir, constants['stderr'])
        if os.path.exists(filename):
            with open(filename) as f:
                data['data'] = f.read()
            await self.rest_client.request('POST', '/logs', data)
        else:
            data['data'] = ''
            await self.rest_client.request('POST', '/logs', data)

        # upload stdout
        data['name'] = 'stdout'
        filename = os.path.join(submit_dir, constants['stdout'])
        if os.path.exists(filename):
            with open(filename) as f:
                data['data'] = f.read()
            await self.rest_client.request('POST', '/logs', data)
        else:
            data['data'] = ''
            await self.rest_client.request('POST', '/logs', data)

    async def task_error(self, task_id, dataset_id, reason=''):
        """reset a task"""
        # search for reason, reasources in logfile
        resources = {}
        filename = os.path.join(submit_dir, constants['stdlog'])
        if os.path.exists(filename):
            with open(filename) as f:
                log_reason = ''
                resource_lines = False
                for line in f:
                    line = line.strip()
                    if line == 'Resources:':
                        resource_lines = True
                        continue
                    if resource_lines:
                        if ':' in line:
                            name,value = line.split(':',1)
                            if value.isdigit():
                                resources[name] = int(value)
                            else:
                                try:
                                    resources[name] = float(value)
                                except Exception:
                                    resources[name] = value
                            continue
                        else:
                            resource_lines = False
                    if not reason:
                        if 'failed to download' in line:
                            log_reason = 'failed to download input file(s)'
                        if 'failed to upload' in line:
                            log_reason = 'failed to upload output file(s)'
                        if 'Exception' in line and not log_reason:
                            log_reason = line
                        if 'return code:' in line and not log_reason:
                            log_reason = 'task error: return code '+line.rsplit(':',1)[-1]
                if log_reason and not reason:
                    reason = log_reason

        comms = MyServerComms(self.rest_client)
        await comms.task_kill(task_id, dataset_id=dataset_id, 
                              reason=reason, resources=resources)

    async def finish_task(self, task_id, dataset_id):
        """complete a task"""
        # search for reasources in logfile
        resources = {}
        filename = os.path.join(submit_dir, constants['stdlog'])
        if os.path.exists(filename):
            with open(filename) as f:
                resource_lines = False
                for line in f:
                    line = line.strip()
                    if line == 'Resources:':
                        resource_lines = True
                        continue
                    if resource_lines:
                        if ':' in line:
                            name,value = line.split(':',1)
                            if value.isdigit():
                                resources[name] = int(value)
                            else:
                                try:
                                    resources[name] = float(value)
                                except Exception:
                                    resources[name] = value
                            continue
                        else:
                            break

        comms = MyServerComms(self.rest_client)
        await comms.finish_task(task_id, dataset_id=dataset_id, 
                                resources=resources)

    async def check_and_clean(self):
        """Check and clean the grid"""
        host = socket.getfqdn()
        self.x509proxy.update_proxy()

        # first, check if anything has completed successfully
        grid_jobs = await self.get_grid_completions()

        if grid_jobs:
            # get pilots from iceprod
            ret = await self.rest_client.request('GET', '/pilots')
            pilot_futures = []
            pilots_to_delete = set()
            for pilot_id in ret:
                pilot = ret[pilot_id]
                if pilot['queue_host'] != host:
                    continue
                if 'grid_queue_id' in pilot and pilot['grid_queue_id'] in grid_jobs:
                    gid = pilot['grid_queue_id']
                    pilot['submit_dir'] = grid_jobs[gid]['submit_dir']
                    if pilot['tasks']:
                        task_id = pilot['tasks'][0]
                        ret = await self.rest_client.request('GET', f'/task/{task_id}')
                        if ret['status'] == 'processing':
                            pilot['dataset_id'] = ret['dataset_id']
                            if grid_jobs[gid]['status'] == 'ok':
                                # upload data - do other steps in next loop
                                pilot_futures.append(asyncio.ensure_future(self.upload_output(pilot)))
                            else:
                                await self.upload_logfiles(task_id, pilot['dataset_id'],
                                                           submit_dir=pilot['submit_dir'])
                                await self.task_error(task_id, pilot['dataset_id'])

                    pilots_to_delete.add(pilot_id)

            for fut in asyncio.as_completed(pilot_futures):
                pilot,e = await fut # upload is done

                task_id = pilot['tasks'][0]
                if e:
                    reason = f'failed to download input files\n{e}'
                    await self.upload_logfiles(task_id,
                                               dataset_id=pilot['dataset_id'],
                                               submit_dir=task['submit_dir'],
                                               reason=reason)
                    await self.task_error(task['task_id'],
                                          dataset_id=pilot['dataset_id'],
                                          reason=reason)
                else:
                    await self.upload_logfiles(task_id,
                                               dataset_id=pilot['dataset_id'],
                                               submit_dir=task['submit_dir'])
                    await self.finish_task(task['task_id'],
                                           dataset_id=pilot['dataset_id'])

            for pilot_id in pilots_to_delete:
                await self.rest_client.request('DELETE', f'/pilots/{pilot_id}')

        # now, do regular check_and_clean
        await super(supercomp_graham, self).check_and_clean()

    async def queue(self):
        """Submit a pilot for each task, up to the limit"""
        host = socket.getfqdn()
        self.x509proxy.update_proxy()
        resources = self.resources.copy()

        debug = False
        if ('queue' in self.cfg and 'debug' in self.cfg['queue']
            and self.cfg['queue']['debug']):
            debug = True

        dataset_cache = {}
        task_futures = []
        for _ in range(self.get_queue_num()):
            # get a processing task
            args = {'requirements': supercomp_graham.resources.copy()}
            args['requirements']['os'] = 'RHEL_7_x86_64'
            try:
                task = await self.rest_client.request('POST', f'/task_actions/process', args)
            except Exception:
                logger.info('no more tasks to queue')
                break

            # get full job, dataset, config info
            job = await self.rest_client.request('GET', f'/jobs/{task["job_id"]}')
            if task['dataset_id'] in dataset_cache:
                dataset, config = dataset_cache[task['dataset_id']]
            else:
                dataset = await self.rest_client.request('GET', f'/datasets/{task["dataset_id"]}')
                config = await self.rest_client.request('GET', f'/config/{task["dataset_id"]}')
                dataset_cache[task['dataset_id']] = (dataset, config)

            # check if we have any files in the task_files API
            task_cfg = None
            for t in config['tasks']:
                if t['name'] == task['name']:
                    task_cfg = t
                    break
            else:
                logger.warning('cannot find task in config for %s', task['task_id'])
                continue
            if task_cfg['task_files']:
                comms = MyServerComms(self.rest_client)
                files = await comms.task_files(task['dataset_id'],
                                               task['task_id'])
                task_cfg['data'].extend(files)

            config['dataset'] = dataset['dataset']
            task.update({
                'config': config,
                'job': job['job_index'],
                'jobs_submitted': dataset['jobs_submitted'],
                'tasks_submitted': dataset['tasks_submitted'],
                'debug': dataset['debug'],
                'reqs': args['requirements'],
            })

            # setup submit dir
            await self.setup_submit_directory(task)

            # create pilot
            resources_available = {'time': resources['time']}
            for k in ('cpu','gpu','memory','disk'):
                resources_available[k] = resources[k]-task['requirements'][k]
            pilot = {'resources': resources,
                     'resources_available': resources_available,
                     'resources_claimed': task['requirements'],
                     'tasks': [task['task_id']],
                     'queue_host': host,
                     'queue_version': iceprod.__version__,
                     'version': iceprod.__version__,
            }
            ret = await self.rest_client.request('POST', '/pilots', args)
            pilot['pilot_id'] = ret['result']
            task['pilot'] = pilot

            # get input files, all tasks in parallel
            task_futures.append(asyncio.ensure_future(self.download_input(task)))

        # wait for the futures
        for fut in asyncio.as_completed(task_futures):
            task,e = await fut
            pilot_id = task['pilot']['pilot_id']
            if e:
                reason = f'failed to download input files\n{e}'
                await self.upload_logfiles(task['task_id'],
                                           dataset_id=task['dataset_id'],
                                           submit_dir=task['submit_dir'],
                                           reason=reason)
                await self.task_error(task['task_id'],
                                      dataset_id=task['dataset_id'],
                                      reason=reason)
                await self.rest_client.request('DELETE', f'/pilots/{pilot_id}')
                continue

            # submit to queue
            await self.submit(task)

            # update pilot
            args = {'grid_queue_id': task['grid_queue_id']}
            await self.rest_client.request('PATCH', f'/pilots/{pilot_id}', args)

    async def download_input(task):
        """
        Download input files for task.

        Args:
            task (dict): task info
        Returns:
            dict: task info
        """
        try:
            proxy = self.x509proxy.get_proxy()
            await subprocess_ssh(
                    'gra-dtn1.computecanada.ca',
                    ['export', f'X509_USER_PROXY={proxy}',';',
                     'python', '-m', 'iceprod.core.data_transfer', '-f',
                     os.path.join(task['submit_dir'],'task.cfg'),
                     '-d', task['submit_dir'],
                     'input']
            )
        except Exception as e:
            return (task, e)
        return (task,None)

    async def upload_output(*args):
        """
        Upload output files for task.

        Args:
            task (dict): task info
        Returns:
            dict: task info
        """
        try:
            proxy = self.x509proxy.get_proxy()
            await subprocess_ssh(
                    'gra-dtn1.computecanada.ca',
                    ['export', f'X509_USER_PROXY={proxy}',';',
                     'python', '-m', 'iceprod.core.data_transfer', '-f',
                     os.path.join(task['submit_dir'],'task.cfg'),
                     '-d', task['submit_dir'],
                     'output']
            )
        except Exception as e:
            return (task,e)
        return (task,None)

    @run_on_executor
    def generate_submit_file(self, task, cfg=None, passkey=None,
                             filelist=None):
        """Generate queueing system submit file for task in dir."""
        args = self.get_submit_args(task,cfg=cfg,passkey=passkey)

        # write the submit file
        submit_file = os.path.join(task['submit_dir'],'submit.sh')
        with open(submit_file,'w') as f:
            p = partial(print,sep='',file=f)
            p('#!/bin/bash')
            p('#SBATCH --account=def-dgrant')
            p('#SBATCH --output={}'.format(os.path.join(task['submit_dir'],'slurm.out')))
            p('#SBATCH --error={}'.format(os.path.join(task['submit_dir'],'slurm.err')))
            p(f'#SBATCH --chdir={task["submit_dir"]}')
            p('#SBATCH --ntasks=1')
            p('#SBATCH --export=NONE')
            p('#SBATCH --mail-type=NONE')
            p('#SBATCH --job-name=iceprod_{}'.format(os.path.basename(task['submit_dir'])))

            # handle resource requests
            if 'reqs' in task:
                if 'cpu' in task['reqs'] and task['reqs']['cpu']:
                    p(f'#SBATCH --cpus-per-task={task["reqs"]["cpu"]}')
                if 'gpu' in task['reqs'] and task['reqs']['gpu']:
                    p(f'#SBATCH --gres=gpu:{task["reqs"]["gpu"]}')
                if 'memory' in task['reqs'] and task['reqs']['memory']:
                    p('#SBATCH --mem={}M'.format(int(task['reqs']['memory']*1000)))
                if 'disk' in task['reqs'] and task['reqs']['disk']:
                    p('#SBATCH --tmp={}M'.format(int(task['reqs']['disk']*1000)))
                if 'time' in task['reqs'] and task['reqs']['time']:
                    p('#SBATCH --time={}'.format(int(task['reqs']['time']*60)))

            # get batchopts
            for b in self.queue_cfg['batchopts']:
                p(b+'='+self.queue_cfg['batchopts'][b])

            # make resources explicit in env
            if 'reqs' in task:
                if 'cpu' in task['reqs'] and task['reqs']['cpu']:
                    p(f'export CPU={task["reqs"]["cpu"]}')
                if 'gpu' in task['reqs'] and task['reqs']['gpu']:
                    p(f'export GPU={task["reqs"]["gpu"]}')
                if 'memory' in task['reqs'] and task['reqs']['memory']:
                    p(f'export MEMORY={task["reqs"]["memory"]}')
                if 'disk' in task['reqs'] and task['reqs']['disk']:
                    p(f'export DISK={task["reqs"]["disk"]}')
                if 'time' in task['reqs'] and task['reqs']['time']:
                    p(f'export TIME={task["reqs"]["time"]}')

            p('{} {}'.format(os.path.join(task['submit_dir'],'loader.sh'),args))

        # make it executable
        st = os.stat(submit_file)
        os.chmod(submit_file, st.st_mode | stat.S_IEXEC)

    async def submit(self,task):
        """Submit task to queueing system."""
        cmd = ['sbatch','submit.sh']
        ret = await asyncio.create_subprocess_exec(cmd, cwd=task['submit_dir'],
                                                   check=True,
                                                   stdout=subprocess.PIPE)
        grid_queue_id = ''
        for line in ret.stdout.split('\n'):
            if 'Submitted batch job' in line:
                grid_queue_id = line.strip().rsplit(1)[-1]
                break
        else:
            raise Exception('did not get a grid_queue_id')
        task['grid_queue_id'] = grid_queue_id

    async def get_grid_status(self):
        """
        Get all tasks running on the queue system.
        
        Returns:
            dict: {grid_queue_id: {status, submit_dir} }
        """
        cmd = ['squeue', '-u', getpass.getuser(), '-h', '-o', '%A %t %j %o']
        ret = await asyncio.create_subprocess_exec(cmd, check=True,
                                                   stdout=subprocess.PIPE)
        out = ret.out
        ret = {}
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,name,cmd = line.split()
            if not name.startswith('iceprod'):
                continue
            if status == 'PD':
                status = 'queued'
            elif status == 'R':
                status = 'processing'
            elif status == 'CD':
                status = 'completed'
            else:
                status = 'error'
            ret[gid] = {'status':status,'submit_dir':os.path.dirname(cmd)}
        return ret

    async def get_grid_completions(self):
        """
        Get completions in the last 4 days.
        
        Returns:
            dict: {grid_queue_id: {status, submit_dir} }
        """
        date = (datetime.now()-timedelta(days=4)).isoformat().split('.',1)[0]
        cmd = ['sacct', '-u', getpass.getuser(), '-n', '-P', '-S', date, '-o', 'JobIDRaw,State,JobName,ExitCode,Workdir']
        ret = await asyncio.create_subprocess_exec(cmd, check=True,
                                                   stdout=subprocess.PIPE)
        out = ret.out
        ret = {}
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,name,exit_code,workdir = line.strip().split('|')
            if status != 'COMPLETED' or not name.startswith('iceprod'):
                continue
            if exit_code == '0:0':
                status = 'ok'
            else:
                status = 'error'
            ret[gid] = {'status':status,'submit_dir':workdir}
        return ret

    async def remove(self,tasks):
        """Remove tasks from queueing system."""
        if tasks:
            cmd = ['scancel']+list(tasks)
            await asyncio.create_subprocess_exec(cmd, check=True)
