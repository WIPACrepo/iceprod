"""
Task submission directly to condor.
"""
import os
import logging
import getpass
from datetime import datetime,timedelta
import subprocess
import asyncio
from functools import partial
import gzip

import requests.exceptions

import iceprod
from iceprod.core import constants
from iceprod.core.exe import Config
from iceprod.core.resources import sanitized_requirements, rounded_requirements
from iceprod.core.exe_json import ServerComms
from iceprod.server import grid
from iceprod.server.globus import SiteGlobusProxy

logger = logging.getLogger('plugin-condor_direct')


async def check_call(*args, **kwargs):
    logger.info('subprocess_check_call: %r', args)
    p = await asyncio.create_subprocess_exec(*args, **kwargs)
    if await p.wait():
        raise Exception(f'command failed, return code {p.returncode}')
    return p


async def check_call_clean_env(*args, **kwargs):
    logger.info('subprocess_check_call: %r', args)
    env = os.environ.copy()
    del env['LD_LIBRARY_PATH']
    kwargs['env'] = env
    p = await asyncio.create_subprocess_exec(*args, **kwargs)
    if await p.wait():
        raise Exception(f'command failed, return code {p.returncode}')
    return p


async def check_output(*args, **kwargs):
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.STDOUT
    logger.info('subprocess_check_output: %r', args)
    p = await asyncio.create_subprocess_exec(*args, **kwargs)
    out,_ = await p.communicate()
    if p.returncode:
        raise Exception(f'command failed, return code {p.returncode}')
    return out.decode('utf-8')


async def check_output_clean_env(*args, **kwargs):
    logger.info('subprocess_check_output: %r', args)
    kwargs['stdout'] = subprocess.PIPE
    kwargs['stderr'] = subprocess.PIPE
    env = os.environ.copy()
    del env['LD_LIBRARY_PATH']
    kwargs['env'] = env
    p = await asyncio.create_subprocess_exec(*args, **kwargs)
    out,err = await p.communicate()
    out = out+b'\n'+err
    if p.returncode:
        raise Exception(f'command failed, return code {p.returncode}')
    return out.decode('utf-8')


class MyServerComms(ServerComms):
    def __init__(self, rest_client):
        self.rest = rest_client


class TaskInfo(dict):
    def __init__(self, **kwargs):
        self['dataset_id'] = None
        self['job_id'] = None
        self['task_id'] = None
        self['config'] = None
        self['pilot'] = None
        self['submit_dir'] = None
        super(TaskInfo, self).__init__(**kwargs)


def condor_os_reqs(os_arch):
    """Convert from OS_ARCH to Condor OS requirements"""
    os_arch = os_arch.rsplit('_',2)[0].rsplit('.',1)[0]
    reqs = 'OpSysAndVer =?= "{}"'.format(os_arch.replace('RHEL','CentOS').replace('_',''))
    reqs = reqs + '|| OpSysAndVer =?= "{}"'.format(os_arch.replace('RHEL','SL').replace('_',''))
    reqs = reqs + ' || OSGVO_OS_STRING =?= "{}"'.format(os_arch.replace('_',' '))
    return '('+reqs+')'


def run_async(func,*args,**kwargs):
    async def myfunc():
        return_obj = {'args':args,'kwargs':kwargs}
        try:
            return_obj['return'] = await func(*args,**kwargs)
        except Exception as e:
            logger.info('error running async', exc_info=True)
            return_obj['exception'] = e
        return return_obj
    return asyncio.ensure_future(myfunc())


def read_filename(filename):
    """Read a file that may potentially be gzipped"""
    data = ''
    if os.path.exists(filename):
        with open(filename) as f:
            data = f.read()
    elif os.path.exists(filename+'.gz'):
        try:
            with gzip.open(filename+'.gz', 'rt', encoding='utf-8') as f:

                try:
                    while True:
                        pos = f.tell()
                        ret = f.read(256)
                        if not ret:
                            break
                        data += ret
                except EOFError:
                    f.seek(pos)
                    try:
                        while True:
                            ret = f.read(1)
                            if not ret:
                                break
                            data += ret
                    except EOFError:
                        pass
        except Exception:
            logger.info('error reading gzip file', exc_info=True)
    if len(data) > 10**8:
        logger.warning(f'logfile {filename} has length {len(data)} and will be trimmed')
        data = data[-1*10**8:]
    return data


class condor_direct(grid.BaseGrid):
    """Plugin Overrides for HTCondor direct job submission"""

    batch_site = 'CondorDirect'
    batch_outfile = 'condor.out'
    batch_resources = {}

    def __init__(self, *args, **kwargs):
        super(condor_direct, self).__init__(*args, **kwargs)
        self.x509proxy = SiteGlobusProxy()

        # queue requirements
        self.site = self.batch_site
        if 'site' in self.queue_cfg:
            self.site = self.queue_cfg['site']
        self.resources = self.batch_resources.copy()
        self.resources['site'] = self.site
        if 'gpu' in self.site.lower():
            self.resources['gpu'] = 1
        elif 'cpu' in self.site.lower():
            self.resources['gpu'] = 0
        self.queue_params = {}
        if 'exclusive' in self.queue_cfg and self.queue_cfg['exclusive']:
            self.queue_params['requirements.site'] = self.site
        logger.info('resources: %r', self.resources)
        logger.info('queue params: %r', self.queue_params)

        self.grid_remove_once = set()

    async def upload_logfiles(self, task_id, dataset_id, submit_dir=None, reason=''):
        """
        Upload logfiles

        Args:
            task_id (str): task id
            dataset_id (str): dataset id
            submit_dir (str): (optional) submit dir for task
            reason (str): (optional) reason to inject into stdlog if it does not exist

        Returns:
            payload_failure (bool): indicate if the task had a payload failure
        """
        if submit_dir is None:
            submit_dir = ''

        payload_failure = False

        data = {'name': 'stdlog', 'task_id': task_id, 'dataset_id': dataset_id}

        # upload stdlog
        data['data'] = read_filename(os.path.join(submit_dir, constants['stdlog']))
        for line in data['data'].split('\n'):
            if 'task exe' in line and 'return code' in line:
                return_code = int(line.rsplit(':', 1)[1].strip())
                if return_code != 0 and return_code != 132:  # ignore SIGILL
                    payload_failure = True
                    break

        if not data['data']:
            data['data'] = reason
        await self.rest_client.request('POST', '/logs', data)

        # upload stderr
        data['name'] = 'stderr'
        data['data'] = read_filename(os.path.join(submit_dir, constants['stderr']))
        await self.rest_client.request('POST', '/logs', data)
        if payload_failure:
            for line in data['data'].split('\n'):
                # find cases where it's probably a node failure
                if ('No such file or directory' in line
                        or 'No space left on device' in line
                        or 'Illegal instruction' in line
                        or 'Input/output error' in line
                        or ('Killed' in line and 'env-shell.sh' in line)
                        or ('python: command not found' in line and 'env-shell.sh' in line)
                        or 'py3-v4.1.1/RHEL_8_x86_64/lib/libCore.so.6.18: undefined symbol: usedToIdentifyRootClingByDlSym' in line
                        or 'OpenCL ERROR: clGetPlatformIDs' in line):
                    payload_failure = False
                    break

        # upload stdout
        data['name'] = 'stdout'
        data['data'] = read_filename(os.path.join(submit_dir, constants['stdout']))
        await self.rest_client.request('POST', '/logs', data)

        return payload_failure

    async def get_hold_reason(self, submit_dir, resources=None):
        """Search for a hold reason in the condor.log"""
        if submit_dir is None:
            submit_dir = ''
        reason = None
        submit_filename = os.path.join(submit_dir, 'condor.submit')
        submit_data = {}
        if os.path.exists(submit_filename):
            with open(submit_filename) as f:
                for line in f:
                    line = line.strip().lower()
                    if '=' in line:
                        key, value = line.split('=', 1)
                        submit_data[key.strip()] = value.strip()
        filename = os.path.join(submit_dir, 'condor.log')
        if os.path.exists(filename):
            with open(filename) as f:
                for line in f:
                    line = line.strip().lower()
                    if 'policy violation' in line:
                        resource_type = None
                        val = 0
                        if 'memory limit' in line:
                            resource_type = 'memory'
                            try:
                                val = float(line.split('used')[-1].split('mb')[0].strip())/1024.
                            except Exception:
                                try:
                                    val = float(line.split(':')[-1].split('mb')[0].strip())/1024.
                                except Exception:
                                    pass
                        elif 'memory usage exceeded' in line:
                            resource_type = 'memory'
                        elif 'cpu limit' in line or 'cpu consumption limit':
                            resource_type = 'cpu'
                            try:
                                val = float(line.split('used')[-1].split('cores')[0].strip())
                            except Exception:
                                try:
                                    val = float(line.split('used')[-1].split('usr')[0].strip())
                                except Exception:
                                    pass
                        elif 'cpu usage exceeded' in line:
                            resource_type = 'cpu'
                        elif 'execution time limit' in line:
                            resource_type = 'time'
                            try:
                                val = float(line.split('used')[-1].split('.')[0].strip())/3600.
                            except Exception:
                                pass
                        elif 'local storage limit' in line:
                            resource_type = 'disk'
                            try:
                                val = float(line.split('used')[-1].split('mb')[0].strip())/1024.
                            except Exception:
                                try:
                                    val = float(line.split('used')[-1].split('gb')[0].strip())
                                except Exception:
                                    pass
                        elif 'disk usage exceeded' in line:
                            resource_type = 'disk'
                        if resource_type:
                            if val:
                                resources[resource_type] = val
                            reason = f'Resource overusage for {resource_type}: '
                            if resource_type in resources:
                                reason += f'{resources[resource_type]}'
                            break
                    elif 'cpu usage exceeded request_cpus' in line:
                        reason = 'Resource overusage for cpu: '
                        try:
                            reason += str(int(submit_data['request_cpus']))
                        except Exception:
                            pass
                        break
                    elif 'memory usage exceeded request_memory' in line:
                        reason = 'Resource overusage for memory: '
                        try:
                            reason += str(float(submit_data['request_memory'])/1000.)
                        except Exception:
                            pass
                        break
                    elif 'disk usage exceeded request_disk' in line:
                        reason = 'Resource overusage for disk: '
                        try:
                            reason += str(float(submit_data['request_disk'])/1000000.)
                        except Exception:
                            pass
                        break
                    elif 'runtime exceeded maximum' in line:
                        reason = 'Resource overusage for time: '
                        try:
                            reason += str(float(line.split()[-2].strip('('))/3600)
                        except Exception:
                            pass
                        break
                    elif 'Transfer output files failure' in line:
                        reason = 'Failed to transfer output files'
                        break
                    elif 'Transfer input files failure' in line:
                        reason = 'Failed to transfer input files'
                        break
                    elif 'failed due to remote transfer hook error' in line:
                        if 'failed to send file' in line:
                            reason = 'Failed to transfer output files'
                        elif 'failed to receive file' in line:
                            reason = 'Failed to transfer input files'
                        else:
                            reason = 'Failed to transfer files'
                        break
        return reason

    async def task_error(self, task_id, dataset_id, submit_dir, reason='',
                         site=None, pilot_id=None, kill=False, failed=False):
        """reset a task"""
        if submit_dir is None:
            submit_dir = ''
        # search for resources in stdout
        resources = {}
        batch_job_id = None
        filename = os.path.join(submit_dir, self.batch_outfile)
        if os.path.exists(filename):
            with open(filename) as f:
                resource_lines = False
                for line in f:
                    line = line.strip()
                    if (not batch_job_id) and 'Job submitted from host' in line:
                        batch_job_id = '.'.join(line.split('(', 1)[1].split('.')[0:2])
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

        if batch_job_id:
            resources.update(await self.get_grid_resources(batch_job_id))

        if (not reason) and failed:
            reason = 'payload failure'
        if not reason:
            # search for reason in logfile
            filename = os.path.join(submit_dir, constants['stdlog'])
            for line in read_filename(filename):
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
            # check the batch system logs
            reason = await self.get_hold_reason(submit_dir, resources=resources)
        if not reason:
            reason = 'unknown failure'

        if not site:
            site = self.site

        comms = MyServerComms(self.rest_client)
        if kill:
            host = None
            filename = os.path.join(submit_dir, 'condor.log')
            if os.path.exists(filename):
                with open(filename) as f:
                    for line in f:
                        line = line.strip()
                        if 'Job executing on host' in line:
                            host = line.split('<')[-1].split(':')[0]
                        if 'Error from' in line:
                            host = line.split()[2].strip(':')
            submitter = grid.get_host()
            message = reason + f'\n\npilot_id: {pilot_id}\nhostname: {host}\nsubmitter: {submitter}\nsite: {site}'
            await comms.task_kill(task_id, dataset_id=dataset_id, reason=reason,
                                  resources=resources, message=message, site=site)
        else:
            await comms.task_error(task_id, dataset_id=dataset_id, reason=reason,
                                   resources=resources, site=site, failed=failed)

    async def finish_task(self, task_id, dataset_id, submit_dir, site=None):
        """complete a task"""
        if submit_dir is None:
            submit_dir = ''
        # search for reasources in slurm stdout
        resources = {}
        filename = os.path.join(submit_dir, self.batch_outfile)
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

        if not site:
            site = self.site

        comms = MyServerComms(self.rest_client)
        await comms.finish_task(task_id, dataset_id=dataset_id,
                                resources=resources, site=site)

    async def check_and_clean(self):
        """Check and clean the grid"""
        host = grid.get_host()

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
        args = {
            'queue_host': host,
            'host': self.site,
            'keys': 'pilot_id|queue_host|grid_queue_id|submit_date|tasks',
        }
        ret = await self.rest_client.request('GET', '/pilots', args)

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

        logger.debug("iceprod pilots: %r", list(pilots))
        logger.debug("grid jobs: %r", list(grid_jobs))
        logger.debug("grid history: %r", list(grid_history))

        async def post_process(task):
            task_id = task['task_id']
            ret = await self.rest_client.request('GET', f'/tasks/{task_id}')
            if ret['status'] == 'processing':
                task['dataset_id'] = ret['dataset_id']

                logger.info('uploading logs for task %s', task_id)
                payload_failure = await self.upload_logfiles(task_id, task['dataset_id'],
                                                             submit_dir=task['submit_dir'])
                if task['grid']['status'] == 'ok':
                    # upload files (may be a no-op)
                    await self.upload_output(task)

                    logger.info('finished task %s', task_id)
                    await self.finish_task(task_id,
                                           dataset_id=task['dataset_id'],
                                           submit_dir=task['submit_dir'],
                                           site=task['grid']['site'])
                else:
                    logger.info('error in task %s', task_id)
                    if payload_failure:
                        logger.info('payload failed')
                    await self.task_error(task_id, task['dataset_id'],
                                          submit_dir=task['submit_dir'],
                                          site=task['grid']['site'],
                                          failed=payload_failure)

        async def post_process_complete(fut):
            ret = await fut
            task = ret['args'][0]
            if 'exception' in ret:
                reason = f'failed post-processing task\n{ret["exception"]}'
                logger.warning(reason)
                await self.upload_logfiles(task['task_id'],
                                           dataset_id=task['dataset_id'],
                                           submit_dir=task['submit_dir'],
                                           reason=reason)
                await self.task_error(task['task_id'],
                                      dataset_id=task['dataset_id'],
                                      submit_dir=task['submit_dir'],
                                      reason=reason)

        if grid_history:
            pilots_to_delete = {}
            awaitables = set()
            for gid in pilots:
                if gid in grid_history:
                    pilot_id = None
                    try:
                        pilot = pilots[gid]
                        pilot_id = pilot['pilot_id']
                        pilot['submit_dir'] = grid_history[gid]['submit_dir']
                        if pilot['tasks']:
                            task_id = pilot['tasks'][0]
                            logger.info('post-processing task %s', task_id)
                            task = TaskInfo(task_id=task_id, pilot=pilot,
                                            submit_dir=pilot['submit_dir'],
                                            grid=grid_history[gid])
                            awaitables.add(run_async(post_process, task))
                            while len(awaitables) >= 10:
                                done, pending = await asyncio.wait(awaitables, return_when=asyncio.FIRST_COMPLETED)
                                awaitables = pending
                                for fut in done:
                                    await post_process_complete(fut)
                    except Exception:
                        logger.error('error handling task', exc_info=True)

                    if pilot_id:
                        logger.info('deleting completed pilot %s, with gid %s', pilot_id, gid)
                        pilots_to_delete[pilot_id] = gid

            for fut in asyncio.as_completed(awaitables):
                await post_process_complete(fut)

            for pilot_id in pilots_to_delete:
                try:
                    await self.rest_client.request('DELETE', f'/pilots/{pilot_id}')
                except Exception:
                    logger.info('delete pilot error', exc_info=True)
                gid = pilots_to_delete[pilot_id]
                if gid in pilots:
                    del pilots[gid]

        # Now do the regular check and clean
        reset_pilots = set(pilots).difference(grid_jobs)
        prechecked_dirs = set()

        # give two attempts to find a grid job before removing it
        remove_grid_jobs = set()
        remove_once = set()
        for gid in set(grid_jobs).difference(pilots):
            if gid in self.grid_remove_once:
                remove_grid_jobs.add(gid)
            else:
                remove_once.add(gid)
        self.grid_remove_once = remove_once

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
                # reset_pilots.add(grid_queue_id)
                remove_grid_jobs.add(grid_queue_id)
            elif status == 'error':
                logger.info('job error. pilot_id: %r, grid_id: %r',
                            pilots[grid_queue_id]['pilot_id'], grid_queue_id)
                # reset_pilots.add(grid_queue_id)
                remove_grid_jobs.add(grid_queue_id)

                pilot = pilots[grid_queue_id]
                if pilot['tasks']:
                    task_id = pilot['tasks'][0]
                    logger.info('post-processing task %s', task_id)
                    ret = await self.rest_client.request('GET', f'/tasks/{task_id}')
                    if ret['status'] == 'processing':
                        await self.task_error(task_id, ret['dataset_id'], pilot_id=pilot['pilot_id'],
                                              submit_dir=grid_jobs[grid_queue_id]['submit_dir'],
                                              kill=True, site=grid_jobs[grid_queue_id]['site'])
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
                    continue  # skip for suspended or failed tasks
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
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        continue  # a missing pilot is the point of deleting it
                    logger.info('delete pilot error', exc_info=True)
                except Exception:
                    logger.info('delete pilot error', exc_info=True)

        # remove grid tasks
        if remove_grid_jobs:
            logger.info('remove %r',remove_grid_jobs)
            await asyncio.ensure_future(self.remove(remove_grid_jobs))

        if delete_dirs:
            await asyncio.ensure_future(self._delete_dirs(delete_dirs))

    async def queue(self):
        """Submit a pilot for each task, up to the limit"""
        host = grid.get_host()

        dataset_cache = {}

        async def make_pilot(task):
            """take a TaskInfo as input"""
            resources = self.resources.copy()

            # get full job, dataset, config info
            job = await self.rest_client.request('GET', f'/jobs/{task["job_id"]}')
            if task['dataset_id'] in dataset_cache:
                dataset, config = dataset_cache[task['dataset_id']]
            else:
                dataset = await self.rest_client.request('GET', f'/datasets/{task["dataset_id"]}')
                config = await self.rest_client.request('GET', f'/config/{task["dataset_id"]}')
                dataset_cache[task['dataset_id']] = (dataset, config)

            # update config with options, and parse it
            config['dataset'] = dataset['dataset']
            task.update({
                'config': config,
                'job': job['job_index'],
                'dataset': dataset['dataset'],
                'jobs_submitted': dataset['jobs_submitted'],
                'tasks_submitted': dataset['tasks_submitted'],
                'debug': dataset['debug'],
            })
            config = self.create_config(task)
            parser = Config(config)
            config = parser.parseObject(config, {})
            task['config'] = config

            task_cfg = None
            for t in config['tasks']:
                if t['name'] == task['name']:
                    task_cfg = t
                    break
            else:
                raise Exception(f'cannot find task in config for {task["task_id"]}')

            # get task requirements
            if 'requirements' in task:
                reqs = sanitized_requirements(task['requirements'])
            else:
                reqs = self.resources.copy()
            if task_cfg and 'requirements' in task_cfg:
                config_reqs = sanitized_requirements(task_cfg['requirements'])
                for k in config_reqs:
                    if k in reqs:
                        if isinstance(config_reqs[k], (int, float)) and reqs[k] < config_reqs[k]:
                            reqs[k] = config_reqs[k]
                        elif isinstance(config_reqs[k], str):
                            reqs[k] = config_reqs[k]
                    else:
                        reqs[k] = config_reqs[k]
            reqs = rounded_requirements(reqs)
            task['requirements'] = reqs

            # get task files
            if 'task_files' in task_cfg and task_cfg['task_files']:
                logger.info("getting task_files for %s", task['task_id'])
                comms = MyServerComms(self.rest_client)
                files = await comms.task_files(task['dataset_id'],
                                               task['task_id'])
                task_cfg['data'].extend(files)
                task_cfg['task_files'] = False

            # task config customizations
            await self.customize_task_config(task_cfg, job_cfg=task['config'], dataset=dataset)

            # create pilot
            if 'time' in resources:
                resources_available = {'time': resources['time']}
            else:
                resources_available = {'time': 1}
            resources_claimed = {}
            for k in ('cpu','gpu','memory','disk'):
                if k in resources and k in reqs:
                    resources_available[k] = resources[k]-reqs[k]
                else:
                    resources_available[k] = 0
                if k in reqs:
                    resources_claimed[k] = reqs[k]
                elif k in resources:
                    resources_claimed[k] = resources[k]
                else:
                    resources_claimed[k] = 0
            pilot = {
                'resources': resources,
                'resources_available': resources_available,
                'resources_claimed': resources_claimed,
                'tasks': [task['task_id']],
                'queue_host': host,
                'queue_version': iceprod.__version__,
                'host': self.site,
                'version': iceprod.__version__,
            }
            ret = await self.rest_client.request('POST', '/pilots', pilot)
            pilot['pilot_id'] = ret['result']
            task['pilot'] = pilot

            await self.setup_submit_directory(task)
            task['pilot']['submit_dir'] = task['submit_dir']

            # download files (may be a no-op)
            await self.download_input(task)

            # submit to queue
            await self.submit(task['pilot'])

            # update pilot
            pilot_id = task['pilot']['pilot_id']
            args = {'grid_queue_id': task['pilot']['grid_queue_id']}
            await self.rest_client.request('PATCH', f'/pilots/{pilot_id}', args)

        awaitables = set()
        queue_num = self.get_queue_num()
        args = {
            'requirements': self.resources.copy(),
            'query_params': self.queue_params,
        }
        logger.info(f'attempting to queue {queue_num} tasks, with args {args}')
        for _ in range(queue_num):
            # get a processing task
            try:
                ret = await self.rest_client.request('POST', '/task_actions/process', args)
            except Exception:
                logger.info('no more tasks to queue')
                break
            awaitables.add(run_async(make_pilot, TaskInfo(**ret)))

        grid_queue_ids = []
        for fut in asyncio.as_completed(awaitables):
            ret = await fut
            task = ret['args'][0]

            if 'exception' in ret:
                reason = f'failed queue task\n{ret["exception"]}'
                logger.warning(reason)
                await self.upload_logfiles(task['task_id'],
                                           dataset_id=task['dataset_id'],
                                           submit_dir=task['submit_dir'],
                                           reason=reason)
                await self.task_error(task['task_id'],
                                      dataset_id=task['dataset_id'],
                                      submit_dir=task['submit_dir'],
                                      reason=reason)
                if task['pilot'] and task['pilot']['pilot_id']:
                    logger.info("deleting just submitted pilot: %s", task["pilot"]["pilot_id"])
                    await self.rest_client.request('DELETE', f'/pilots/{task["pilot"]["pilot_id"]}')
                if task['pilot'] and 'grid_queue_id' in task['pilot'] and task['pilot']['grid_queue_id']:
                    logger.info("deleting just submitted job: %s", task['pilot']['grid_queue_id'])
                    await self.remove([task['pilot']['grid_queue_id']])
            else:
                if 'grid_queue_id' in task['pilot'] and task['pilot']['grid_queue_id']:
                    grid_queue_ids.append(task['pilot']['grid_queue_id'])

    async def download_input(self, task):
        pass

    async def upload_output(self, task):
        pass

    async def generate_submit_file(self, task, cfg=None, passkey=None,
                                   filelist=None):
        """Generate queueing system submit file for task in dir."""
        args = self.get_submit_args(task,cfg=cfg)
        args.extend(['--offline', '--offline_transfer', 'True', '--gzip-logs'])

        # get requirements and batchopts
        requirements = []
        batch_opts = {}
        input_files = list(filelist)
        output_files = []
        output_remaps = []
        for b in self.queue_cfg['batchopts']:
            if b.lower() == 'requirements':
                requirements.append(self.queue_cfg['batchopts'][b])
            else:
                batch_opts[b] = self.queue_cfg['batchopts'][b]
        if cfg:
            if (cfg['steering'] and 'batchsys' in cfg['steering'] and
                    cfg['steering']['batchsys']):
                for b in cfg['steering']['batchsys']:
                    if self.__class__.__name__.startswith(b.lower()):
                        # these settings apply to this batchsys
                        for bb in cfg['steering']['batchsys'][b]:
                            value = cfg['steering']['batchsys'][b][bb]
                            if bb.lower() == 'requirements':
                                logger.info(f'steering batchsys requirement: {value}')
                                requirements.append(value)
                            else:
                                logger.info(f'steering batchsys other: {bb} = {value}')
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
            logger.info(f'{task["task_id"]} selected tasks: {alltasks}')
            for t in alltasks:
                if 'batchsys' in t and t['batchsys']:
                    logger.info(f'{task["task_id"]} task batchsys: {t["batchsys"]}')
                    for b in t['batchsys']:
                        if self.__class__.__name__.startswith(b.lower()):
                            # these settings apply to this batchsys
                            for bb in t['batchsys'][b]:
                                value = t['batchsys'][b][bb]
                                if bb.lower() == 'requirements':
                                    logger.info(f'{task["task_id"]} task batchsys requirement: {value}')
                                    requirements.append(value)
                                elif bb.lower() == 'transfer_input_files':
                                    input_files.extend(value.split(','))
                                elif bb.lower() == 'transfer_output_files':
                                    output_files.extend(value.split(','))
                                elif bb.lower() == 'transfer_output_remaps':
                                    output_remaps.extend(value.split(','))
                                else:
                                    logger.info(f'{task["task_id"]} task batchsys other: {bb}={value}')
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
            p('+IsIceProdJob = True')  # mark as IceProd for monitoring
            p('want_graceful_removal = True')
            if input_files:
                p('transfer_input_files = {}'.format(','.join(input_files)))
            p('skip_filechecks = True')
            p('should_transfer_files = always')
            p('when_to_transfer_output = ON_EXIT_OR_EVICT')
            p('+SpoolOnEvict = False')
            output_files.extend(['iceprod_log.gz', 'iceprod_out.gz', 'iceprod_err.gz'])
            p('transfer_output_files = {}'.format(','.join(output_files)))
            if output_remaps:
                p('transfer_output_remaps = "{}"'.format(';'.join(output_remaps)))

            # put some info about the task in the classads
            p(f'+IceProdDatasetId = "{task["dataset_id"]}"')
            p(f'+IceProdDataset = {task["dataset"]}')
            p(f'+IceProdJobId = "{task["job_id"]}"')
            p(f'+IceProdJobIndex = {task["job"]}')
            p(f'+IceProdTaskId = "{task["task_id"]}"')
            p(f'+IceProdTaskIndex = {task["task_index"]}')
            p(f'+IceProdTaskName = "{task["name"]}"')
            p(f'+IceProdSiteId = "{self.cfg["site_id"]}"')

            # handle resources
            p('+JobIsRunning = (JobStatus =!= 1) && (JobStatus =!= 5)')
            if 'cpu' in task['requirements'] and task['requirements']['cpu']:
                p('request_cpus = {}'.format(task['requirements']['cpu']))
            if 'gpu' in task['requirements'] and task['requirements']['gpu']:
                p('request_gpus = {}'.format(task['requirements']['gpu']))
            else:
                requirements.append('(!isUndefined(Target.GPUs) ? Target.GPUs == 0 : True)')
            if 'memory' in task['requirements'] and task['requirements']['memory']:
                p('request_memory = {}'.format(int(task['requirements']['memory']*1000)))
            if 'disk' in task['requirements'] and task['requirements']['disk']:
                p('request_disk = {}'.format(int(task['requirements']['disk']*1000000)))
            if 'time' in task['requirements'] and task['requirements']['time']:
                p('+OriginalTime = {}'.format(int(task['requirements']['time']*3600)))
                p('+TargetTime = (!isUndefined(Target.PYGLIDEIN_TIME_TO_LIVE) ? Target.PYGLIDEIN_TIME_TO_LIVE : Target.TimeToLive)')
                p('Rank = Rank + (TargetTime - OriginalTime)/86400')
                requirements.append('TargetTime > OriginalTime')
            if 'os' in task['requirements'] and task['requirements']['os']:
                if isinstance(task['requirements']['os'], list):
                    os_type = task['requirements']['os'][0]
                else:
                    os_type = task['requirements']['os']
                if 'singularity' in self.queue_cfg and not self.queue_cfg['singularity']:
                    requirements.append(condor_os_reqs(os_type))
                else:
                    imageoptions = {
                        'RHEL_6_x86_64': 'osgvo-el6:latest',
                        'RHEL_7_x86_64': 'osgvo-el7:latest',
                        'RHEL_8_x86_64': 'osgvo-el8:latest',
                    }
                    if os_type in imageoptions:
                        image = imageoptions[os_type]
                    else:
                        raise Exception(f'bad OS selection: {os_type}')
                    p(f'+SingularityImage="/cvmfs/singularity.opensciencegrid.org/opensciencegrid/{image}"')

            for b in batch_opts:
                p(f'{b}={batch_opts[b]}')
            if requirements:
                p('requirements = ('+')&&('.join(requirements)+')')

            p('arguments = ',' '.join(args))
            p('queue')

    async def submit(self,task):
        """Submit task to queueing system."""
        cmd = ['condor_submit','-terse','condor.submit']
        for tries in range(3):  # make three attempts
            try:
                out = await check_output_clean_env(*cmd, cwd=task['submit_dir'])
                break
            except Exception:
                if tries >= 2:
                    raise
                await asyncio.sleep(1)  # backoff a bit
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

    async def get_grid_resources(self, job_id):
        """Get resource information from a running/held task on the queue system"""
        ret = {}
        cmd = ['condor_q', job_id, '-af:,', 'CpusUsage', 'GPUsUsage', 'ResidentSetSize_RAW', 'DiskUsage_RAW', 'LastRemoteWallClockTime']
        out = await check_output_clean_env(*cmd)
        print('get_grid_status():',out)
        cpu, gpu, memory, disk, time = out.strip().split(',')
        if cpu != 'undefined':
            ret['cpu'] = float(cpu)
        if gpu != 'undefined':
            ret['gpu'] = float(gpu)
        if memory != 'undefined':
            ret['memory'] = float(memory)/1024/1024.
        if disk != 'undefined':
            ret['disk'] = float(disk)/1024/1024.
        if time != 'undefined':
            ret['time'] = float(time)/3600.
        return ret

    async def get_grid_status(self):
        """Get all tasks running on the queue system.
           Returns {grid_queue_id:{status,submit_dir}}
        """
        ret = {}
        cmd = ['condor_q', '-constraint', f'Owner == "{getpass.getuser()}" && IceProdSiteId == "{self.cfg["site_id"]}"',
               '-af:j,', 'jobstatus', 'MATCH_EXP_JOBGLIDEIN_ResourceName', 'cmd']
        out = await check_output_clean_env(*cmd)
        print('get_grid_status():',out)
        for line in out.split('\n'):
            if not line.strip():
                continue
            try:
                gid,status,site,cmd = [x.strip() for x in line.split(',') if x.strip()]
            except ValueError:
                logger.warning('bad line: %r', line)
                continue
            if 'loader.sh' not in cmd:
                continue
            if status in ('0', '1'):
                status = 'queued'
            elif status in ('2', '3', '6', '7'):
                status = 'processing'
            elif status == '4':
                status = 'completed'
            elif status in ('5',):
                status = 'error'
            else:
                status = 'unknown'
            ret[gid] = {'status':status,'submit_dir':os.path.dirname(cmd),'site':site}
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
            dict: {grid_queue_id: {status, submit_dir, site} }
        """
        ret = {}
        cmd = ['condor_history', '-constraint', f'Owner == "{getpass.getuser()}" && IceProdSiteId == "{self.cfg["site_id"]}"',
               '-match', '50000', '-af:j,', 'jobstatus', 'exitcode', 'exitbysignal',
               'MATCH_EXP_JOBGLIDEIN_ResourceName', 'cmd']
        out = await check_output_clean_env(*cmd)
        print('get_grid_completions():',out)
        for line in out.split('\n'):
            if not line.strip():
                continue
            try:
                gid,status,exitstatus,exitsignal,site,cmd = [x.strip() for x in line.split(',') if x.strip()]
            except ValueError:
                logger.warning('bad line: %r', line)
                continue
            if 'loader.sh' not in cmd:
                continue
            if status == '4' and exitstatus == '0' and exitsignal == 'false':
                status = 'ok'
            else:
                status = 'error'
            if site == 'undefined':
                site = None
            ret[gid] = {'status':status,'submit_dir':os.path.dirname(cmd),'site':site}
        return ret
