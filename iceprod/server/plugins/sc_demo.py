"""
A supercomputer-like plugin, specificially for the SC 2019 Demo.

Basically, task submission directly to condor.
"""
import os
import logging
import getpass
from datetime import datetime,timedelta
import subprocess
import asyncio
from functools import partial
import gzip

import tornado.gen
from tornado.concurrent import run_on_executor

import iceprod
from iceprod.core import dataclasses
from iceprod.core import constants
from iceprod.core import functions
from iceprod.core.exe_json import ServerComms
from iceprod.server import grid
from iceprod.server.globus import SiteGlobusProxy
from iceprod.server.plugins.condor import condor_os_reqs

logger = logging.getLogger('plugin-sc_demo')

async def check_call(*args, **kwargs):
    logger.info('subprocess_check_call: %r', args)
    p = await asyncio.create_subprocess_exec(*args, **kwargs)
    if p.returncode:
        raise Exception(f'command failed, return code {p.returncode}')
    return p

async def check_call_clean_env(*args, **kwargs):
    logger.info('subprocess_check_call: %r', args)
    env = os.environ.copy()
    del env['LD_LIBRARY_PATH']
    kwargs['env'] = env
    p = await asyncio.create_subprocess_exec(*args, **kwargs)
    if p.returncode:
        raise Exception(f'command failed, return code {p.returncode}')
    return p

async def check_output_clean_env(*args, **kwargs):
    logger.info('subprocess_check_output: %r', args)
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.STDOUT
    env = os.environ.copy()
    del env['LD_LIBRARY_PATH']
    kwargs['env'] = env
    p = await asyncio.create_subprocess_exec(*args, **kwargs)
    out,_ = await p.communicate()
    if p.returncode:
        raise Exception(f'command failed, return code {p.returncode}')
    return out.decode('utf-8')

class MyServerComms(ServerComms):
    def __init__(self, rest_client):
        self.rest = rest_client

class sc_demo(grid.BaseGrid):

    ### Plugin Overrides ###

    def __init__(self, *args, **kwargs):
        super(sc_demo, self).__init__(*args, **kwargs)
        self.x509proxy = SiteGlobusProxy()

        # SC demo queue requirements
        self.resources = {}
        self.queue_params = {
            'requirements.site': 'SC-Demo',
        }
        if 'site' in self.queue_cfg:
            self.queue_params['requirements.site'] = self.queue_cfg['site']
        if 'gpu' in self.queue_params['requirements.site'].lower():
            self.resources['gpu'] = 1
        logger.info('resources: %r', self.resources)
        logger.info('queue params: %r', self.queue_params)

    async def upload_logfiles(self, task_id, dataset_id, submit_dir='', reason=''):
        """upload logfiles"""
        data = {'name': 'stdlog', 'task_id': task_id, 'dataset_id': dataset_id}

        def read_filename(filename):
            if os.path.exists(filename):
                with open(filename) as f:
                    return f.read()
            elif os.path.exists(filename+'.gz'):
                try:
                    with gzip.open(filename+'.gz', 'rt', encoding='utf-8') as f:
                        return f.read()
                except EOFError:
                    pass
                except Exception:
                    logging.info('stdlog:', exc_info=True)
            return ''

        # upload stdlog
        data['data'] = read_filename(os.path.join(submit_dir, constants['stdlog']))
        if not data['data']:
            data['data'] = reason
        await self.rest_client.request('POST', '/logs', data)

        # upload stderr
        data['name'] = 'stderr'
        data['data'] = read_filename(os.path.join(submit_dir, constants['stderr']))
        await self.rest_client.request('POST', '/logs', data)

        # upload stdout
        data['name'] = 'stdout'
        data['data'] = read_filename(os.path.join(submit_dir, constants['stdout']))
        await self.rest_client.request('POST', '/logs', data)

    async def task_error(self, task_id, dataset_id, submit_dir, reason=''):
        """reset a task"""
        # search for resources in stdout
        resources = {}
        filename = os.path.join(submit_dir, 'condor.out')
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
        if not reason:
            # search for reason in logfile
            filename = os.path.join(submit_dir, constants['stdlog'])
            if os.path.exists(filename):
                with open(filename) as f:
                    for line in f:
                        line = line.strip()
                        if 'failed to download' in line:
                            reason = 'failed to download input file(s)'
                        if 'failed to upload' in line:
                            reason = 'failed to upload output file(s)'
                        if 'Exception' in line and not reason:
                            reason = line
                        if 'return code:' in line and not reason:
                            reason = 'task error: return code '+line.rsplit(':',1)[-1]
        if not reason:
            reason = 'unknown failure'

        site = None
        if 'site' in self.queue_cfg:
            site = self.queue_cfg['site']

        comms = MyServerComms(self.rest_client)
        await comms.task_error(task_id, dataset_id=dataset_id, 
                               reason=reason, resources=resources,
                               site=site)

    async def finish_task(self, task_id, dataset_id, submit_dir):
        """complete a task"""
        # search for reasources in slurm stdout
        resources = {}
        filename = os.path.join(submit_dir, 'condor.out')
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
        host = grid.get_host()
        #self.x509proxy.update_proxy()
        
        # get time limits
        try:
            queued_time = timedelta(seconds=self.queue_cfg['max_task_queued_time'])
        except Exception:
            queued_time = timedelta(seconds=86400*2)
        try:
            processing_time = timedelta(seconds=self.queue_cfg['max_task_processing_time'])
        except Exception:
            processing_time = timedelta(seconds=86400*2)
        try:
            suspend_time = timedelta(seconds=self.queue_cfg['suspend_submit_dir_time'])
        except Exception:
            suspend_time = timedelta(seconds=86400)
        all_time = queued_time + processing_time + suspend_time
        time_dict = {'queued': queued_time,
                     'processing': queued_time+processing_time,
                     'completed': all_time,
                     'error': all_time,
                     'unknown': all_time}
        for t in time_dict:
            logger.debug("time limit: %s - %s",t,time_dict[t])
        now = datetime.utcnow()
        
        # get pilots from iceprod
        ret = await self.rest_client.request('GET', '/pilots')

        # filter by queue host
        # index by grid_queue_id
        pilots = {}
        for pilot_id in ret:
            if (ret[pilot_id]['queue_host'] == host
                and 'grid_queue_id' in ret[pilot_id]
                and ret[pilot_id]['grid_queue_id']):
                pilots[ret[pilot_id]['grid_queue_id']] = ret[pilot_id]

        # first, check if anything has completed successfully
        # important to get grid status before completions, so we don't reset
        # newly completed jobs
        grid_jobs = await self.get_grid_status()
        grid_history = await self.get_grid_completions()

        logger.info("iceprod pilots: %r", list(pilots))
        logger.info("grid jobs: %r", list(grid_jobs))
        logger.info("grid history: %r", list(grid_history))

        if grid_history:
            pilot_futures = []
            pilots_to_delete = set()
            for gid in pilots:
                if gid in grid_history:
                    try:
                        pilot = pilots[gid]
                        pilot['submit_dir'] = grid_history[gid]['submit_dir']
                        if pilot['tasks']:
                            task_id = pilot['tasks'][0]
                            logger.info('post-processing task %s', task_id)
                            ret = await self.rest_client.request('GET', f'/tasks/{task_id}')
                            if ret['status'] == 'processing':
                                pilot['dataset_id'] = ret['dataset_id']
                                if grid_history[gid]['status'] == 'ok':
                                    logger.info('uploading logs for task %s', task_id)
                                    pilot_futures.append(asyncio.ensure_future(self.upload_output(pilot)))
                                else:
                                    logger.info('error in task %s', task_id)
                                    await self.upload_logfiles(task_id, pilot['dataset_id'],
                                                               submit_dir=pilot['submit_dir'])
                                    await self.task_error(task_id, pilot['dataset_id'],
                                                          submit_dir=pilot['submit_dir'])
                    except Exception:
                        logger.error('error handling task', exc_info=True)

                    pilots_to_delete.add(pilot_id)

            for fut in asyncio.as_completed(pilot_futures):
                pilot,e = await fut # upload is done

                try:
                    task_id = pilot['tasks'][0]
                    if e:
                        logger.info('failed to upload output files for task %s', task_id)
                        reason = f'failed to upload output files\n{e}'
                        await self.upload_logfiles(task_id,
                                                   dataset_id=pilot['dataset_id'],
                                                   submit_dir=pilot['submit_dir'],
                                                   reason=reason)
                        await self.task_error(task_id,
                                              dataset_id=pilot['dataset_id'],
                                              submit_dir=pilot['submit_dir'],
                                              reason=reason)
                    else:
                        logger.info('finishing task %s', task_id)
                        await self.upload_logfiles(task_id,
                                                   dataset_id=pilot['dataset_id'],
                                                   submit_dir=pilot['submit_dir'])
                        await self.finish_task(task_id,
                                               dataset_id=pilot['dataset_id'],
                                               submit_dir=pilot['submit_dir'])
                except Exception:
                    logger.error('error handling task', exc_info=True)

            for pilot_id in pilots_to_delete:
                await self.rest_client.request('DELETE', f'/pilots/{pilot_id}')
                if pilot_id in pilots:
                    del pilots[pilot_id]


        ### Now do the regular check and clean
        reset_pilots = set(pilots).difference(grid_jobs)
        remove_grid_jobs = set(grid_jobs).difference(pilots)
        prechecked_dirs = set()

        # check the queue
        grid_idle = 0
        for grid_queue_id in set(grid_jobs).intersection(pilots):
            status = grid_jobs[grid_queue_id]['status']
            submit_time = pilots[grid_queue_id]['submit_date']
            if '.' in submit_time:
                submit_time = datetime.strptime(submit_time, '%Y-%m-%dT%H:%M:%S.%f')
            else:
                submit_time = datetime.strptime(submit_time, '%Y-%m-%dT%H:%M:%S')

            if now - submit_time > time_dict[status]:
                logger.info('pilot over time: %r', pilots[grid_queue_id]['pilot_id'])
                reset_pilots.add(pilots[grid_queue_id]['pilot_id'])
            elif status == 'queued':
                grid_idle += 1

            submit_dir = grid_jobs[grid_queue_id]['submit_dir']
            if submit_dir:
                # queueing systems don't like deleteing directories they know
                # about, so put them on a list of "don't touch"
                prechecked_dirs.add(submit_dir)
        self.grid_idle = grid_idle
        self.grid_processing = len(pilots)-len(reset_pilots)-grid_idle

        # check submit directories
        delete_dirs = set()
        for x in os.listdir(self.submit_dir):
            d = os.path.join(self.submit_dir,x)
            if d in prechecked_dirs:
                continue
            if os.path.isdir(d) and '_' in x:
                logger.debug('found submit_dir %s',d)
                mtime = datetime.utcfromtimestamp(os.path.getmtime(d))
                # use all_time instead of suspend_time because the
                # dir will have the submit time, not the last time
                if now-mtime < all_time:
                    continue # skip for suspended or failed tasks
                delete_dirs.add(d)

        logger.info('%d processing pilots', self.grid_processing)
        logger.info('%d queued pilots', self.grid_idle)
        logger.info('%d ->reset', len(reset_pilots))
        logger.info('%d ->grid remove', len(remove_grid_jobs))
        logger.info('%d ->submit clean', len(delete_dirs))
        self.statsd.gauge('processing_pilots', self.grid_processing)
        self.statsd.gauge('queued_pilots', self.grid_idle)
        self.statsd.incr('reset_pilots', len(reset_pilots))
        self.statsd.incr('grid_remove', len(remove_grid_jobs))
        self.statsd.incr('clean_dirs', len(delete_dirs))

        if reset_pilots:
            logger.info('reset %r',reset_pilots)
            for grid_queue_id in reset_pilots:
                try:
                    pilot_id = pilots[grid_queue_id]['pilot_id']
                    await self.rest_client.request('DELETE', '/pilots/{}'.format(pilot_id))
                except KeyError:
                    pass

        # remove grid tasks
        if remove_grid_jobs:
            logger.info('remove %r',remove_grid_jobs)
            await asyncio.ensure_future(self.remove(remove_grid_jobs))

        if delete_dirs:
            await asyncio.ensure_future(self._delete_dirs(delete_dirs))

    async def queue(self):
        """Submit a pilot for each task, up to the limit"""
        host = grid.get_host()
        #self.x509proxy.update_proxy()
        resources = self.resources.copy()

        debug = False
        if ('queue' in self.cfg and 'debug' in self.cfg['queue']
            and self.cfg['queue']['debug']):
            debug = True

        dataset_cache = {}
        task_futures = []
        for _ in range(self.get_queue_num()):
            # get a processing task
            args = {
                'requirements': self.resources.copy(),
                'query_params': self.queue_params,
            }
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
            if 'task_files' in task_cfg and task_cfg['task_files']:
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
                'requirements': args['requirements'],
            })

            # setup submit dir
            await self.setup_submit_directory(task)

            # create pilot
            if 'time' in resources:
                resources_available = {'time': resources['time']}
            else:
                resources_available = {'time': 1}
            for k in ('cpu','gpu','memory','disk'):
                if k in resources:
                    resources_available[k] = resources[k]-task['requirements'][k]
                else:
                    resources_available[k] = 0
            pilot = {'resources': resources,
                     'resources_available': resources_available,
                     'resources_claimed': task['requirements'],
                     'tasks': [task['task_id']],
                     'queue_host': host,
                     'queue_version': iceprod.__version__,
                     'version': iceprod.__version__,
            }
            ret = await self.rest_client.request('POST', '/pilots', pilot)
            pilot['pilot_id'] = ret['result']
            task['pilot'] = pilot

            if any(f['movement'] in ('download','both') for f in task_cfg['data']):
                # get input files, all tasks in parallel
                task_futures.append(asyncio.ensure_future(self.download_input(task)))
            else:
                async def f(task):
                    return task,None
                task_futures.append(asyncio.ensure_future(f(task)))

        # wait for the futures
        grid_queue_ids = []
        for fut in asyncio.as_completed(task_futures):
            task,e = await fut
            try:
                pilot_id = task['pilot']['pilot_id']
                if e is not None:
                    reason = f'failed to download input files\n{e}'
                    await self.upload_logfiles(task['task_id'],
                                               dataset_id=task['dataset_id'],
                                               submit_dir=task['submit_dir'],
                                               reason=reason)
                    await self.task_error(task['task_id'],
                                          dataset_id=task['dataset_id'],
                                          submit_dir=task['submit_dir'],
                                          reason=reason)
                    await self.rest_client.request('DELETE', f'/pilots/{pilot_id}')
                    continue

                # submit to queue
                await self.submit(task)
                grid_queue_ids.append(task['grid_queue_id'])

                # update pilot
                pilot_id = task['pilot']['pilot_id']
                args = {'grid_queue_id': task['grid_queue_id']}
                await self.rest_client.request('PATCH', f'/pilots/{pilot_id}', args)

            except Exception as e:
                try:
                    reason = f'failed to submit pilot:\n{e}'
                    await self.upload_logfiles(task['task_id'],
                                               dataset_id=task['dataset_id'],
                                               submit_dir=task['submit_dir'],
                                               reason=reason)
                    await self.task_error(task['task_id'],
                                          dataset_id=task['dataset_id'],
                                          submit_dir=task['submit_dir'],
                                          reason=reason)
                except Exception:
                    pass
                logger.error('error handling pilot', exc_info=True)

        # DEMO: put jobs on hold, to releaes when it's time
        cmd = ['condor_hold']+grid_queue_ids
        out = await check_output_clean_env(*cmd)

    async def download_input(self, task):
        """
        Download input files for task.

        Args:
            task (dict): task info
        Returns:
            dict: task info
        """
        try:
            await check_call(
                'python', '-m', 'iceprod.core.data_transfer', '-f',
                 os.path.join(task['submit_dir'],'task.cfg'),
                 '-d', task['submit_dir'],
                 'input'
            )
        except Exception as e:
            logger.info('error downloading', exc_info=True)
            return (task, e)
        return (task,None)

    async def upload_output(self, task):
        """
        Upload output files for task.

        Args:
            task (dict): task info
        Returns:
            dict: task info
        """
        try:
            await check_call(
                'python', '-m', 'iceprod.core.data_transfer', '-f',
                 os.path.join(task['submit_dir'],'task.cfg'),
                 '-d', task['submit_dir'],
                 'output'
            )
        except Exception as e:
            logger.info('error uploading', exc_info=True)
            return (task,e)
        return (task,None)

    async def generate_submit_file(self, task, cfg=None, passkey=None,
                             filelist=None):
        """Generate queueing system submit file for task in dir."""
        args = self.get_submit_args(task,cfg=cfg)
        args.extend(['--offline', '--gzip-logs'])

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

        # DEMO: igor's test scripts
        filelist.extend([
            os.path.abspath(os.path.expanduser('~/igor_test_support/pre_test.sh')),
            os.path.abspath(os.path.expanduser('~/igor_test_support/post_test.sh')),
        ])

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
            p('+IsIceProdJob = True') # mark as IceProd for monitoring
            p('want_graceful_removal = True')
            if filelist:
                p('transfer_input_files = {}'.format(','.join(filelist)))
            p('skip_filechecks = True')
            p('should_transfer_files = always')
            p('when_to_transfer_output = ON_EXIT_OR_EVICT')
            p('+SpoolOnEvict = False')
            p('transfer_output_files = iceprod_log.gz, iceprod_out.gz, iceprod_err.gz')

            # handle resources
            p('+JobIsRunning = (JobStatus =!= 1) && (JobStatus =!= 5)')
            if 'reqs' in task:
                if 'cpu' in task['reqs'] and task['reqs']['cpu']:
                    p('request_cpus = {}'.format(task['reqs']['cpu']))
                if 'gpu' in task['reqs'] and task['reqs']['gpu']:
                    p('request_gpus = {}'.format(task['reqs']['gpu']))
                    # DEMO: gpu jobs need region set by condor_qedit
                    p('+CHUNK_Locations="NONE"')
                    requirements.append('stringListIMember($(CLOUD_DATARegion), $(CHUNK_Locations))')
                if 'memory' in task['reqs'] and task['reqs']['memory']:
                    p('request_memory = {}'.format(int(task['reqs']['memory']*1000+100)))
                else:
                    p('request_memory = 1000')
                if 'disk' in task['reqs'] and task['reqs']['disk']:
                    p('request_disk = {}'.format(int(task['reqs']['disk']*1000000)))
                if 'time' in task['reqs'] and task['reqs']['time']:
                    # extra 10 min for pilot
                    p('+OriginalTime = {}'.format(int(task['reqs']['time'])*3600+600))
                if 'os' in task['reqs'] and task['reqs']['os']:
                    requirements.append(condor_os_reqs(task['reqs']['os']))

            for b in batch_opts:
                p(f'{b}={batch_opts[b]}')
            if requirements:
                p('requirements = ('+')&&('.join(requirements)+')')

            p('arguments = ',' '.join(args))
            p('queue')

    async def submit(self,task):
        """Submit task to queueing system."""
        cmd = ['condor_submit','-terse','condor.submit']
        out = await check_output_clean_env(*cmd, cwd=task['submit_dir'])
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

        return task

    async def get_grid_status(self):
        """Get all tasks running on the queue system.
           Returns {grid_queue_id:{status,submit_dir}}
        """
        ret = {}
        cmd = ['condor_q',getpass.getuser(),'-af:j','jobstatus','cmd']
        out = await check_output_clean_env(*cmd)
        print('get_grid_status():',out)
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,cmd = line.split()
            if 'loader.sh' not in cmd:
                continue
            if status in ('0', '1', '5'): # DEMO: treat held jobs as queued
                status = 'queued'
            elif status == '2':
                status = 'processing'
            elif status == '4':
                status = 'completed'
            elif status == '3':
                continue # skip already removed jobs
            elif status in ('5', '6'):
                status = 'error'
            else:
                status = 'unknown'
            ret[gid] = {'status':status,'submit_dir':os.path.dirname(cmd)}
        return ret

    async def remove(self,tasks):
        """Remove tasks from queueing system."""
        if tasks:
            cmd = ['condor_rm']+list(tasks)
            await check_call_clean_env(*cmd)

    async def get_grid_completions(self):
        """
        Get completions in the last 4 days.

        Returns:
            dict: {grid_queue_id: {status, submit_dir} }
        """
        ret = {}
        cmd = ['condor_history',getpass.getuser(),'-match','50000','-af:j','jobstatus','exitcode','exitbysignal','cmd']
        out = await check_output_clean_env(*cmd)
        print('get_grid_completions():',out)
        for line in out.split('\n'):
            if not line.strip():
                continue
            gid,status,exitstatus,exitsignal,cmd = line.split()
            if 'loader.sh' not in cmd:
                continue
            if status == '4' and exitstatus == '0' and exitsignal == 'false':
                status = 'ok'
            else:
                status = 'error'
            ret[gid] = {'status':status,'submit_dir':os.path.dirname(cmd)}
        return ret
