"""
Interface for configuring and submitting jobs on a computing cluster.
Do not use this class directly. Instead use one of the implementations
that inherit from this class.
"""

import os
import sys
import random
import math
import logging
import time
from copy import deepcopy
from io import BytesIO
from datetime import datetime,timedelta
from collections import namedtuple, Counter, defaultdict
import socket
import asyncio

import tornado.gen
from tornado.concurrent import run_on_executor

import iceprod
import iceprod.core.exe
from iceprod.core import dataclasses
from iceprod.core import functions
from iceprod.core import serialization
from iceprod.core.resources import Resources, group_hasher
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import module
from iceprod.server import get_pkg_binary,GlobalID
from iceprod.server import dataset_prio


logger = logging.getLogger('grid')


def get_host():
    """Cache the host fqdn for 1 hour"""
    t = time.time()
    if get_host.history and get_host.history[0]+3600 < t:
        return get_host.history[1]
    host = socket.getfqdn()
    get_host.history = [t, host]
    return host
get_host.history = None

class BaseGrid(object):
    """
    Interface for a generic job distribution system.
    Do not use this class directly.  Use one of the plugins.
    """

    # use only these grid states when defining grid status
    GRID_STATES = ('queued','processing','completed','error','unknown')

    def __init__(self, gridspec, queue_cfg, cfg, modules, io_loop, executor, statsd, rest_client):
        self.gridspec = gridspec
        self.queue_cfg = queue_cfg
        self.cfg = cfg
        self.modules = modules
        self.io_loop = io_loop
        self.executor = executor
        self.statsd = statsd
        self.rest_client = rest_client

        self.site = None
        if 'site' in self.queue_cfg:
            self.site = self.queue_cfg['site']

        self.submit_dir = os.path.expanduser(os.path.expandvars(
                self.cfg['queue']['submit_dir']))
        if not os.path.exists(self.submit_dir):
            try:
                os.makedirs(self.submit_dir)
            except Exception as e:
                logger.warning('error making submit dir %s',self.submit_dir,
                            exc_info=True)

        self.grid_processing = 0
        self.grid_idle = 0

    ### Public Functions ###

    async def check_and_clean(self):
        """Check and clean the grid"""
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
        host = get_host()
        args = {
            'queue_host': host,
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

        # get grid status
        grid_jobs = await asyncio.ensure_future(self.get_grid_status())

        logger.debug("iceprod pilots: %r", list(pilots))
        logger.debug("grid jobs: %r", list(grid_jobs))

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

        # reset tasks
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
        """Queue tasks to the grid"""
        # get tasks on the queue
        args = {
            'status': 'queued',
            'keys': 'task_id|dataset_id|status_changed|requirements',
        }
        ret = await self.rest_client.request('GET', '/tasks', args)
        tasks = []
        for t in ret['tasks']:
            if 'site' in t['requirements'] and t['requirements']['site'] != self.site:
                continue
            tasks.append(t)
        dataset_ids = set(row['dataset_id'] for row in tasks)

        # get dataset priorities
        dataset_prios = {}
        for d in dataset_ids:
            dataset = await self.rest_client.request('GET', '/datasets/{}'.format(d))
            dataset_prios[d] = dataset['priority']

        # sort by dataset priority, status changed time
        tasks.sort(key=lambda t:(-1*dataset_prios[t['dataset_id']],t['status_changed'],t['task_id']))

        # queue new pilots
        await self.setup_pilots(tasks)


    ### Private Functions ###

    def get_queue_num(self, available=100000):
        """Determine how many pilots to queue."""
        tasks_on_queue = self.queue_cfg['pilots_on_queue']
        queue_tot_max = tasks_on_queue[1] - self.grid_processing - self.grid_idle
        queue_idle_max = tasks_on_queue[0] - self.grid_idle
        queue_interval_max = tasks_on_queue[2] if len(tasks_on_queue) > 2 else tasks_on_queue[0]
        queue_num = max(0,min(available - self.grid_idle, queue_tot_max,
                              queue_idle_max, queue_interval_max))
        logger.info('queueing %d pilots', queue_num)
        self.statsd.incr('queueing_pilots', queue_num)
        return queue_num

    @run_on_executor
    def _delete_dirs(self, dirs):
        # delete dirs that need deleting
        for t in dirs:
            if not t.startswith(self.submit_dir):
                # some security against nefarious things
                raise Exception('directory %s not in submit_dir %s'%(t, self.submit_dir))
            try:
                logger.info('deleting submit_dir %s', t)
                functions.removedirs(t)
            except Exception:
                logger.warning('could not delete submit dir %s', t, exc_info=True)
                continue

    @staticmethod
    def _get_resources(tasks):
        """yield resource information for each task in a list"""
        default_resource = deepcopy(Resources.defaults)
        for k in default_resource:
            if isinstance(default_resource[k],list):
                default_resource[k] = len(default_resource[k])
        for t in tasks:
            values = {}
            try:
                for k in t['reqs']:
                    if k in default_resource and t['reqs'][k]:
                        try:
                            if isinstance(default_resource[k], int):
                                values[k] = int(t['reqs'][k])
                            elif isinstance(default_resource[k], float):
                                values[k] = float(t['reqs'][k])
                            else:
                                values[k] = t['reqs'][k]
                        except Exception:
                            logger.warning('bad reqs value for task %r', t)
                    elif k == 'os' and t['reqs'][k]:
                        logger.debug('OS req: %s', t['reqs'][k])
                        values['os'] = ','.join(t['reqs'][k]) if isinstance(t['reqs'][k], list) else t['reqs'][k]
            except TypeError:
                logger.warning('t[reqs]: %r',t['reqs'])
                raise
            resource = deepcopy(default_resource)
            resource.update(values)
            yield resource

    async def setup_pilots(self, tasks):
        """Setup pilots for the task reqs"""
        host = get_host()

        debug = False
        if ('queue' in self.cfg and 'debug' in self.cfg['queue']
            and self.cfg['queue']['debug']):
            debug = True

        # convert to resource requests and group them
        groups = defaultdict(list)
        for resources in self._get_resources({'reqs':t['requirements']} for t in tasks):
            k = group_hasher(resources)
            groups[k].append(resources)

        # get already queued pilots
        pilot_groups = Counter()
        ret = await self.rest_client.request('GET', '/pilots', {'host': '', 'keys':'resources'})
        for pilot in ret.values():
            k = group_hasher(pilot['resources'])
            pilot_groups[k] += 1

        # remove already queued groups from consideration
        groups_considered = Counter()
        for k in groups:
            n = len(groups[k]) - pilot_groups[k]
            if n > 0:
                groups_considered[k] = n

        # select at least one from each resource group
        queue_num = self.get_queue_num(available=len(tasks))
        groups_to_queue = Counter()
        keys = set(groups_considered.keys())
        while queue_num > 0 and keys:
            for k in list(keys):
                if groups_considered[k] < 1:
                    keys.remove(k)
                else:
                    groups_to_queue[k] += 1
                    groups_considered[k] -= 1
                    queue_num -= 1
                    if queue_num < 1:
                        break
        logger.debug('groups_to_queue: %r', groups_to_queue)

        for r in groups_to_queue:
            try:
                resources = defaultdict(list)
                for x in groups[r]:
                    for k in x:
                        if x[k] is not None:
                            resources[k].append(x[k])
                resources = {k:resources[k][0] if isinstance(resources[k][0],dataclasses.String) else max(resources[k])
                             for k in resources}
                logger.info('submitting %d pilots for resource %r',
                            groups_to_queue[r], resources)
                for name in resources:
                    self.statsd.incr('pilot_resources.'+name, resources[name])
                pilot = {'task_id': 'pilot',
                         'name': 'pilot',
                         'debug': debug,
                         'reqs': resources,
                         'num': groups_to_queue[r],
                         'pilot_ids': [],
                }

                args = {
                    'queue_host': host,
                    'queue_version': iceprod.__version__,
                    'resources': resources,
                }
                for _ in range(groups_to_queue[r]):
                    ret = await self.rest_client.request('POST', '/pilots', args)
                    pilot['pilot_ids'].append(ret['result'])

                await self.setup_submit_directory(pilot)
                await asyncio.ensure_future(self.submit(pilot))

                grid_queue_ids = pilot['grid_queue_id'].split(',')
                for i,pilot_id in enumerate(pilot['pilot_ids']):
                    ret = await self.rest_client.request('PATCH',
                            '/pilots/{}'.format(pilot_id),
                            {'grid_queue_id': grid_queue_ids[i]})

            except Exception:
                logger.error('error submitting pilots', exc_info=True)

    async def setup_submit_directory(self,task):
        """Set up submit directory"""
        # create directory for task
        submit_dir = self.submit_dir
        task_dir = os.path.join(submit_dir,task['task_id']+'_'+str(random.randint(0,1000000)))
        while os.path.exists(task_dir):
            task_dir = os.path.join(submit_dir,task['task_id']+'_'+str(random.randint(0,1000000)))
        task_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(task_dir)))
        os.makedirs(task_dir)
        task['submit_dir'] = task_dir

        # symlink or copy the .sh file
        src = get_pkg_binary('iceprod','loader.sh')
        try:
            os.symlink(src,os.path.join(task_dir,'loader.sh'))
        except Exception as e:
            try:
                functions.copy(src,os.path.join(task_dir,'loader.sh'))
            except Exception as e:
                logger.error('Error creating symlink or copy of .sh file: %s',e,exc_info=True)
                raise

        # get passkey
        expiration = self.queue_cfg['max_task_queued_time']
        expiration += self.queue_cfg['max_task_processing_time']
        expiration += self.queue_cfg['max_task_reset_time']

        data = {
            'type': 'system',
            'role': 'pilot',
            'exp': expiration,
        }
        passkey = await self.rest_client.request('POST', '/create_token', data)
        passkey = passkey['result']

        # write cfg
        cfg, filelist = self.write_cfg(task)

        # create submit file
        try:
            await asyncio.ensure_future(self.generate_submit_file(task,
                    cfg=cfg, passkey=passkey, filelist=filelist))
        except Exception:
            logger.error('Error generating submit file',exc_info=True)
            raise

    def write_cfg(self, task):
        """Write the config file for a task-like object"""
        filename = os.path.join(task['submit_dir'],'task.cfg')

        if 'config' in task and task['config']:
            config = serialization.dict_to_dataclasses(task['config'])
        else:
            config = dataclasses.Job()
        filelist = [filename]

        if 'reqs' in task:
            # add resources
            config['options']['resources'] = {}
            for r in task['reqs']:
                config['options']['resources'][r] = task['reqs'][r]

        # add server options
        config['options']['task_id'] = task['task_id']
        config['options']['task'] = task['name']
        if 'job' in task:
            config['options']['job'] = task['job']
        if 'jobs_submitted' in task:
            config['options']['jobs_submitted'] = task['jobs_submitted']
        if 'dataset_id' in task:
            config['options']['dataset_id'] = task['dataset_id']
        if 'dataset' in task:
            config['options']['dataset'] = task['dataset']
        config['options']['stillrunninginterval'] = self.queue_cfg['ping_interval']
        config['options']['debug'] = task['debug']
        config['options']['upload'] = 'logging'
        config['options']['gridspec'] = self.gridspec
        if 'site_temp' in self.cfg['queue']:
            config['options']['site_temp'] = self.cfg['queue']['site_temp']
        if ('download' in self.cfg and 'http_username' in self.cfg['download']
            and self.cfg['download']['http_username']):
            config['options']['username'] = self.cfg['download']['http_username']
        if ('download' in self.cfg and 'http_password' in self.cfg['download']
            and self.cfg['download']['http_password']):
            config['options']['password'] = self.cfg['download']['http_password']
        if 'system' in self.cfg and 'remote_cacert' in self.cfg['system']:
            config['options']['ssl'] = {}
            config['options']['ssl']['cacert'] = os.path.basename(self.cfg['system']['remote_cacert'])
            src = self.cfg['system']['remote_cacert']
            dest = os.path.join(task['submit_dir'],config['options']['ssl']['cacert'])
            try:
                os.symlink(src,dest)
            except Exception as e:
                try:
                    functions.copy(src,dest)
                except Exception:
                    logger.error('Error creating symlink or copy of remote_cacert',
                                 exc_info=True)
                    raise
            filelist.append(dest)
        if 'x509proxy' in self.cfg['queue'] and self.cfg['queue']['x509proxy']:
            config['options']['x509'] = os.path.basename(self.cfg['queue']['x509proxy'])
            src = self.cfg['queue']['x509proxy']
            logger.info('submit_dir %r  x509 %r', task['submit_dir'], config['options']['x509'])
            dest = os.path.join(task['submit_dir'],config['options']['x509'])
            try:
                os.symlink(src,dest)
            except Exception as e:
                try:
                    functions.copy(src,dest)
                except Exception:
                    logger.error('Error creating symlink or copy of x509 proxy',
                                 exc_info=True)
                    raise
            filelist.append(dest)
        if 'extra_file_tranfers' in self.cfg['queue'] and self.cfg['queue']['extra_file_tranfers']:
            for f in self.cfg['queue']['extra_file_tranfers']:
                logger.info('submit_dir %r  extra_files %r', task['submit_dir'], f)
                dest = os.path.join(task['submit_dir'],os.path.basename(f))
                try:
                    os.symlink(os.path.abspath(f),dest)
                except Exception as e:
                    try:
                        functions.copy(f,dest)
                    except Exception:
                        logger.error('Error creating symlink or copy of extra file %s',
                                     f, exc_info=True)
                        raise
                filelist.append(dest)
        if 'data_movement_stats' in self.cfg['queue'] and self.cfg['queue']['data_movement_stats']:
            config['options']['data_movement_stats'] = self.cfg['queue']['data_movement_stats']
        if 'upload_checksum' in self.cfg['queue']:
            config['options']['upload_checksum'] = self.cfg['queue']['upload_checksum']

        # write to file
        serialization.serialize_json.dump(config,filename)

        c = iceprod.core.exe.Config(config)
        config = c.parseObject(config, {})

        return (config, filelist)

    # not async: called from executor
    def get_submit_args(self,task,cfg=None,passkey=None):
        """Get the submit arguments to start the loader script."""
        # get website address
        if ('rest_api' in self.cfg and self.cfg['rest_api'] and
            'url' in self.cfg['rest_api'] and self.cfg['rest_api']['url']):
            web_address = self.cfg['rest_api']['url']
        else:
            raise Exception('no web address for rest calls')

        args = []
        if 'software_dir' in self.queue_cfg and self.queue_cfg['software_dir']:
            args.append('-s {}'.format(self.queue_cfg['software_dir']))
        if 'iceprod_dir' in self.queue_cfg and self.queue_cfg['iceprod_dir']:
            args.append('-e {}'.format(self.queue_cfg['iceprod_dir']))
        if 'x509proxy' in self.cfg['queue'] and self.cfg['queue']['x509proxy']:
            args.append('-x {}'.format(os.path.basename(self.cfg['queue']['x509proxy'])))
        if ('download' in self.cfg and 'http_proxy' in self.cfg['download']
            and self.cfg['download']['http_proxy']):
            args.apend('-c {}'.format(self.cfg['download']['http_proxy']))
        args.append('--url {}'.format(web_address))
        if passkey:
            args.append('--passkey {}'.format(passkey))
        if cfg:
            args.append('--cfgfile task.cfg')
        if 'debug' in task and task['debug']:
            args.append('--debug')
        return args

    ### Plugin Overrides ###

    async def get_grid_status(self):
        """Get all tasks running on the queue system.
           Returns {grid_queue_id:{status,submit_dir}}
        """
        return {}

    async def generate_submit_file(self,task,cfg=None,passkey=None,filelist=None):
        """Generate queueing system submit file for task in dir."""
        raise NotImplementedError()

    async def submit(self,task):
        """Submit task to queueing system."""
        raise NotImplementedError()

    async def remove(self,tasks):
        """Remove tasks from queueing system."""
        pass
