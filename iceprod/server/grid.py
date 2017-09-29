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
from copy import deepcopy
from io import BytesIO
from datetime import datetime,timedelta
from collections import namedtuple, Counter, defaultdict
import itertools

import tornado.gen
from tornado.concurrent import run_on_executor

from iceprod.core import dataclasses
from iceprod.core import functions
from iceprod.core import serialization
from iceprod.core.resources import Resources, group_hasher
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import module
from iceprod.server import get_pkg_binary,GlobalID
from iceprod.server import dataset_prio


logger = logging.getLogger('grid')


class BaseGrid(object):
    """
    Interface for a generic job distribution system.
    Do not use this class directly.  Use one of the plugins.
    """

    # use only these grid states when defining grid status
    GRID_STATES = ('queued','processing','completed','error','unknown')

    def __init__(self, gridspec, queue_cfg, cfg, modules, io_loop, executor, statsd):
        self.gridspec = gridspec
        self.queue_cfg = queue_cfg
        self.cfg = cfg
        self.modules = modules
        self.io_loop = io_loop
        self.executor = executor
        self.statsd = statsd

        if self.gridspec:
            self.grid_id, self.name = self.gridspec.split('.', 1)
        else:
            self.grid_id = self.cfg['site_id']
            self.name = ''

        self.submit_dir = os.path.expanduser(os.path.expandvars(
                self.cfg['queue']['submit_dir']))
        if not os.path.exists(self.submit_dir):
            try:
                os.makedirs(self.submit_dir)
            except Exception as e:
                logger.warn('error making submit dir %s',self.submit_dir,
                            exc_info=True)

        self.hostname = 'localhost'

        self.tasks_queued = 0
        self.tasks_processing = 0
        self.grid_processing = 0
        self.grid_idle = 0

    ### Public Functions ###

    @tornado.gen.coroutine
    def check_and_clean(self):
        """Check and Clean the Grid"""
        yield self.check_iceprod()
        yield self.check_grid()
        yield self.metrics()

    @tornado.gen.coroutine
    def queue(self):
        """Queue tasks to the grid"""
        tasks = None

        # update hostname once a submit cycle
        self.hostname = functions.gethostname()

        if ('submit_pilots' in self.cfg['queue'] and
            self.cfg['queue']['submit_pilots']):
            pilots = True
        else:
            pilots = False
        
        # calculate num tasks to queue
        tasks_on_queue = self.queue_cfg['tasks_on_queue']
        min_tasks = tasks_on_queue[0]
        max_tasks = tasks_on_queue[1]
        change = min_tasks
        if len(tasks_on_queue) > 2:
            change = tasks_on_queue[2]


        num_to_queue = min(max_tasks - self.tasks_processing - self.tasks_queued,
                           min_tasks - self.tasks_queued)
        change = max(0,min(change,num_to_queue))
        logger.info('can queue up to %d tasks', change)
        self.statsd.gauge('can_queue', change)

        # get queueing datasets from database
        datasets = yield self.modules['db']['queue_get_queueing_datasets']()
        if not isinstance(datasets,dict):
            raise Exception('db.queue_get_queueing_datasets(%s) did not return a dict'%self.gridspec)

        if datasets:
            groups = yield self.modules['db']['rpc_get_groups']()

            if groups:
                filters = None
                if 'group_filters' in self.cfg:
                    filters = self.cfg['group_filters']
                datasets = yield dataset_prio.apply_group_prios(datasets,
                        groups=groups, filters=filters)

            # get priority factors
            qf_p = qf_d = qf_t = 1.0
            if 'queueing_factor_priority' in self.queue_cfg:
                qf_p = self.queue_cfg['queueing_factor_priority']
            if 'queueing_factor_dataset' in self.queue_cfg:
                qf_d = self.queue_cfg['queueing_factor_dataset']
            if 'queueing_factor_tasks' in self.queue_cfg:
                qf_t = self.queue_cfg['queueing_factor_tasks']

            # assign each dataset a priority
            dataset_prios = dataset_prio.calc_datasets_prios(datasets,
                    queueing_factor_priority=qf_p,
                    queueing_factor_dataset=qf_d,
                    queueing_factor_tasks=qf_t)
            logger.debug('dataset prios: %r',dataset_prios)

            # get tasks to queue
            tasks = yield self.modules['db']['queue_get_queueing_tasks'](
                    dataset_prios=dataset_prios,
                    gridspec_assignment=self.gridspec,
                    num=change)
            if not isinstance(tasks,dict):
                raise Exception('db.queue_get_queueing_tasks(%s) did not return a dict'%self.gridspec)

        self.statsd.gauge('did_queue', len(tasks) if tasks else 0)

        if tasks:
            if pilots:
                yield self.add_tasks_to_pilot_lookup(tasks)
            else:
                for t in tasks:
                    # set up submit directory
                    yield self.setup_submit_directory(tasks[t])
                    # submit to queueing system
                    yield self.submit(tasks[t])
            self.tasks_queued += len(tasks)

        if pilots:
            # now try to queue pilots
            tasks = yield self.modules['db']['queue_get_task_lookup']()
            yield self.setup_pilots(tasks)


    ### Private Functions ###

    @tornado.gen.coroutine
    def check_iceprod(self):
        """check if any task is in a state for too long"""
        tasks = yield self.modules['db']['queue_get_active_tasks'](gridspec=self.gridspec)
        logger.debug('active tasks: %r',tasks)
        if tasks is None:
            raise Exception('db.queue_get_active_tasks(%s) returned none'%self.gridspec)
        elif isinstance(tasks,Exception):
            raise tasks
        elif not isinstance(tasks,dict):
            raise Exception('db.queue_get_active_tasks(%s) did not return a dict'%self.gridspec)

        now = datetime.utcnow()
        reset_tasks = []
        waiting_tasks = []
        idle_tasks = []

        # check the waiting status
        tasks_waiting = 0
        if 'waiting' in tasks:
            max_task_waiting_time = self.queue_cfg['max_task_waiting_time']
            for t in tasks['waiting'].values():
                try:
                    if now - t['status_changed'] > timedelta(seconds=max_task_waiting_time):
                        reset_tasks.append(t)
                    else:
                        tasks_waiting += 1
                except Exception:
                    logging.warn('error waiting->reset for %r', t,
                                 exc_info=True)

        # check the queued status
        tasks_queued = 0
        if 'queued' in tasks:
            max_task_queued_time = self.queue_cfg['max_task_queued_time']
            for t in tasks['queued'].values():
                try:
                    if now - t['status_changed'] > timedelta(seconds=max_task_queued_time):
                        reset_tasks.append(t)
                    else:
                        tasks_queued += 1
                except Exception:
                    logging.warn('error queued->reset for %r', t,
                                 exc_info=True)
        self.tasks_queued = tasks_queued

        # check the processing status
        tasks_processing = 0
        if 'processing' in tasks:
            max_task_processing_time = self.queue_cfg['max_task_processing_time']
            for t in tasks['processing'].values():
                try:
                    if now - t['status_changed'] > timedelta(seconds=max_task_processing_time):
                        reset_tasks.append(t)
                    else:
                        tasks_processing += 1
                except Exception:
                    logging.warn('error processing->reset for %r', t,
                                 exc_info=True)
        self.tasks_processing = tasks_processing

        # check the resume,reset status
        max_task_reset_time = self.queue_cfg['max_task_reset_time']
        if 'reset' in tasks:
            for t in tasks['reset'].values():
                if t['failures'] >= 3 or not self.gridspec:
                    idle_tasks.append(t)
                else:
                    waiting_tasks.append(t)
        if 'resume' in tasks:
            for t in tasks['resume'].values():
                idle_tasks.append(t)

        logger.info('%d processing tasks',self.tasks_processing)
        logger.info('%d queued tasks',self.tasks_queued)
        logger.info('%d waiting tasks',tasks_waiting)
        logger.info('%d ->idle',len(idle_tasks))
        logger.info('%d ->waiting',len(waiting_tasks))
        logger.info('%d ->reset',len(reset_tasks))
        self.statsd.gauge('processing_tasks', self.tasks_processing)
        self.statsd.gauge('queued_tasks', self.tasks_queued)
        self.statsd.gauge('waiting_tasks', tasks_waiting)
        self.statsd.incr('idle_tasks', len(idle_tasks))
        self.statsd.incr('waiting_tasks', len(waiting_tasks))
        self.statsd.incr('reset_tasks', len(reset_tasks))

        if idle_tasks:
            # change status to idle
            # TODO: this should also flush any caches
            #       but how to do that is in question
            ret = yield self.modules['db']['queue_set_task_status'](task={t['task_id'] for t in idle_tasks},
                                                status='idle')
            if isinstance(ret,Exception):
                raise ret

        if waiting_tasks:
            # change status to waiting
            ret = yield self.modules['db']['queue_set_task_status'](task={t['task_id'] for t in waiting_tasks},
                                                status='waiting')
            if isinstance(ret,Exception):
                raise ret

        if reset_tasks:
            # reset some tasks
            max_resets = self.cfg['queue']['max_resets']
            failures = []
            resets = []
            for t in reset_tasks:
                if t['failures'] >= max_resets:
                    failures.append(t['task_id'])
                else:
                    resets.append(t['task_id'])
            ret = yield self.modules['db']['queue_reset_tasks'](reset=resets,fail=failures)
            if isinstance(ret,Exception):
                raise ret

    @tornado.gen.coroutine
    def check_grid(self):
        """check the queueing system for problems"""
        if ('submit_pilots' in self.cfg['queue'] and
            self.cfg['queue']['submit_pilots']):
            pilots = True
        else:
            pilots = False
        
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
            logger.debug("time limit: %s - %r",t,time_dict[t])
        now = datetime.utcnow()

        # get tasks from iceprod
        if pilots:
            tasks = yield self.modules['db']['queue_get_pilots']()
        else:
            tasks = yield self.modules['db']['queue_get_grid_tasks'](gridspec=self.gridspec)
        if isinstance(tasks,Exception):
            raise tasks

        # convert to dict for fast lookup
        tasks = {t['grid_queue_id']:t for t in tasks}

        # get grid status
        grid_tasks = yield self.get_grid_status()
        if not isinstance(grid_tasks,dict):
            raise Exception('get_task_status() on %s did not return a dict'%self.gridspec)

        reset_tasks = set()
        remove_grid_tasks = set()
        delete_dirs = set()

        prechecked_dirs = set()

        # check the grid tasks
        grid_idle = 0
        for grid_queue_id in set(grid_tasks).union(tasks):
            if grid_queue_id in grid_tasks:
                status = grid_tasks[grid_queue_id]['status']
                submit_dir = grid_tasks[grid_queue_id]['submit_dir']
            else:
                status = 'unknown'
                submit_dir = None
            if grid_queue_id in tasks:
                # iceprod knows about this one
                if ((not submit_dir) or
                    now - tasks[grid_queue_id]['submit_time'] > time_dict[status]):
                    # grid doesn't know
                    logger.info('task not on grid, or over time: %r',
                                tasks[grid_queue_id]['submit_dir'])
                    submit_dir = tasks[grid_queue_id]['submit_dir']
                    if pilots:
                        reset_tasks.add(tasks[grid_queue_id]['pilot_id'])
                    else:
                        reset_tasks.add(tasks[grid_queue_id]['task_id'])
                elif submit_dir != tasks[grid_queue_id]['submit_dir']:
                    # mixup - delete both
                    logger.warn('submit dirs not equal: %r != %r', submit_dir,
                                tasks[grid_queue_id]['submit_dir'])
                    remove_grid_tasks.add(grid_queue_id)
                    if pilots:
                        reset_tasks.add(tasks[grid_queue_id]['pilot_id'])
                    else:
                        reset_tasks.add(tasks[grid_queue_id]['task_id'])
                elif status == 'queued':
                    grid_idle += 1
            else: # must be in grid_tasks
                # what iceprod doesn't know must be killed
                if status in ('queued','processing','unknown'):
                    remove_grid_tasks.add(grid_queue_id)
            if submit_dir:
                # queueing systems don't like deleteing directories they know
                # about, so put them on a list of "don't touch"
                prechecked_dirs.add(submit_dir)
        self.grid_idle = grid_idle
        self.grid_processing = len(tasks)-len(reset_tasks)-grid_idle

        # check submit directories
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
                    logger.debug('skip submit_dir for recent suspended task')
                    continue # skip for suspended or failed tasks
                delete_dirs.add(d)

        if pilots:
            logger.info('%d processing pilots', self.grid_processing)
            logger.info('%d queued pilots', self.grid_idle)
            logger.info('%d ->reset', len(reset_tasks))
            logger.info('%d ->grid remove', len(remove_grid_tasks))
            logger.info('%d ->submit clean', len(delete_dirs))
            self.statsd.gauge('processing_pilots', self.grid_processing)
            self.statsd.gauge('queued_pilots', self.grid_idle)
            self.statsd.incr('reset_pilots', len(reset_tasks))
            self.statsd.incr('grid_remove', len(remove_grid_tasks))
            self.statsd.incr('clean_dirs', len(delete_dirs))

        # reset tasks
        if reset_tasks:
            logger.info('reset %r',reset_tasks)
            if pilots:
                ret = yield self.modules['db']['queue_del_pilots'](pilots=reset_tasks)
            else:
                ret = yield self.modules['db']['queue_set_task_status'](task=reset_tasks,
                                                    status='reset')
            if isinstance(ret,Exception):
                raise ret

        # remove grid tasks
        if remove_grid_tasks:
            logger.info('remove %r',remove_grid_tasks)
            yield self.remove(remove_grid_tasks)

        yield self._delete_dirs(delete_dirs)

    @tornado.gen.coroutine
    def metrics(self):
        """send metrics about queue status, if we haven't somewhere else"""
        datasets = yield self.modules['db']['queue_get_active_dataset_tasks'](gridspec=self.gridspec)
        for d in datasets:
            for name in datasets[d]:
                for status in datasets[d][name]:
                    dataset_num = GlobalID.localID_ret(d,type='int')
                    stat_name = 'datasets.'+str(dataset_num)+'.'+name+'.'+status
                    self.statsd.gauge(stat_name, datasets[d][name][status])

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
                logger.warn('could not delete submit dir %s', t, exc_info=True)
                continue

    def _get_resources(self, tasks):
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
                            logger.warn('bad reqs value for task %r', t)
            except TypeError:
                logger.warn('t[reqs]: %r',t['reqs'])
                raise
            resource = deepcopy(default_resource)
            resource.update(values)
            yield resource

    @tornado.gen.coroutine
    def add_tasks_to_pilot_lookup(self, tasks):
        task_reqs = {}
        task_iter = itertools.izip(tasks.keys(),
                                   self._get_resources(tasks.values()))
        for task_id, resources in task_iter:
            task_reqs[task_id] = resources
        logger.info('adding %d tasks to pilot lookup', len(task_reqs))
        self.statsd.incr('add_to_task_lookup', len(task_reqs))
        ret = yield self.modules['db']['queue_add_task_lookup'](tasks=task_reqs)
        if isinstance(ret,Exception):
            logger.error('error add_task_lookup')
            raise ret

    @tornado.gen.coroutine
    def setup_pilots(self, tasks):
        """Setup pilots for the task reqs"""
        debug = False
        if ('queue' in self.cfg and 'debug' in self.cfg['queue']
            and self.cfg['queue']['debug']):
            debug = True
            
        groups = defaultdict(list)
        if isinstance(tasks,dict):
            tasks = tasks.values()
        for resources in self._get_resources({'reqs':v} for v in tasks):
            k = group_hasher(resources)
            groups[k].append(resources)

        # get already queued requirements
        pilots = yield self.modules['db']['queue_get_pilots'](active=False)
        pilot_groups = Counter()
        for p in pilots:
            k = group_hasher(p['requirements'])
            pilot_groups[k] += 1

        # remove already queued groups from consideration
        groups_considered = Counter()
        for k in groups:
            n = len(groups[k]) - pilot_groups[k]
            if n > 0:
                groups_considered[k] = n

        # determine how many pilots to queue
        tasks_on_queue = self.queue_cfg['pilots_on_queue']
        queue_tot_max = tasks_on_queue[1] - self.grid_processing - self.grid_idle
        queue_idle_max = tasks_on_queue[0] - self.grid_idle
        queue_interval_max = tasks_on_queue[2] if len(tasks_on_queue) > 2 else tasks_on_queue[0]
        queue_num = max(0,min(len(tasks) - self.grid_idle, queue_tot_max,
                              queue_idle_max, queue_interval_max))
        logger.info('queueing %d pilots', queue_num)
        self.statsd.incr('queueing_pilots', queue_num)

        # select at least one from each resource group
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

        for r in groups_to_queue:
            try:
                resources = defaultdict(list)
                for x in groups[r]:
                    for k in x:
                        resources[k].append(x[k])
                resources = {k:max(resources[k]) for k in resources}
                logger.info('submitting %d pilots for resource %r',
                            groups_to_queue[r], resources)
                for name in resources:
                    self.statsd.incr('pilot_resources.'+name, resources[name])
                pilot = {'task_id': 'pilot',
                         'name': 'pilot',
                         'debug': debug,
                         'reqs': resources,
                         'num': groups_to_queue[r],
                }
                pilot_ids = yield self.modules['db']['queue_new_pilot_ids'](num=pilot['num'])
                pilot['pilot_ids'] = pilot_ids
                yield self.setup_submit_directory(pilot)
                yield self.submit(pilot)
                ret = yield self.modules['db']['queue_add_pilot'](pilot=pilot)
                if isinstance(ret,Exception):
                    logger.error('error updating DB with pilots')
                    raise ret
            except Exception:
                logger.error('error submitting pilots', exc_info=True)

    @tornado.gen.coroutine
    def setup_submit_directory(self,task):
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
        expiration = datetime.utcnow()
        expiration += timedelta(seconds=self.queue_cfg['max_task_queued_time'])
        expiration += timedelta(seconds=self.queue_cfg['max_task_processing_time'])
        expiration += timedelta(seconds=self.queue_cfg['max_task_reset_time'])
        ret = yield self.modules['db']['auth_new_passkey'](expiration=expiration)
        if isinstance(ret,Exception):
            logger.error('error getting passkey for task_id %r',
                         task['task_id'])
            raise ret
        passkey = ret

        # write cfg
        cfg, filelist = yield self.write_cfg(task)

        if task['task_id'] != 'pilot':
            # update DB
            logger.info('task %s has new submit_dir %s',task['task_id'],task_dir)
            ret = yield self.modules['db']['queue_set_submit_dir'](task=task['task_id'],
                                         submit_dir=task_dir)
            if isinstance(ret,Exception):
                logger.error('error updating DB with submit_dir')
                raise ret

        # create submit file
        try:
            yield self.generate_submit_file(task, cfg=cfg, passkey=passkey,
                                            filelist=filelist)
        except Exception:
            logger.error('Error generating submit file',exc_info=True)
            raise

    @tornado.gen.coroutine
    def write_cfg(self,task):
        """Write the config file for a task"""
        filename = os.path.join(task['submit_dir'],'task.cfg')

        if task['task_id'] == 'pilot':
            config = dataclasses.Job()
        else:
            # get config from database
            ret = yield self.modules['db']['queue_get_cfg_for_task'](task_id=task['task_id'])
            if isinstance(ret,Exception):
                logger.error('error getting task cfg for task_id %r',
                             task['task_id'])
                raise ret
            config = serialization.serialize_json.loads(ret)
            config['options']['dataset_id'] = task['dataset_id']
            config['options']['job'] = task['job']
            config['options']['jobs_submitted'] = task['jobs_submitted']

        filelist = [filename]

        if 'reqs' in task:
            # add resources
            config['options']['resources'] = {}
            for r in task['reqs']:
                config['options']['resources'][r] = task['reqs'][r]

        # add server options
        config['options']['task_id'] = task['task_id']
        config['options']['task'] = task['name']
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

        # write to file
        serialization.serialize_json.dump(config,filename)

        ret = (config, filelist)
        raise tornado.gen.Return(ret)

    # not async: called from executor
    def get_submit_args(self,task,cfg=None,passkey=None):
        """Get the submit arguments to start the loader script."""
        # get website address
        if ('monitor_address' in self.queue_cfg and
            self.queue_cfg['monitor_address']):
            web_address = self.queue_cfg['monitor_address']
        else:
            host = self.hostname
            if 'system' in self.cfg and 'remote_cacert' in self.cfg['system']:
                web_address = 'https://'+host
            else:
                web_address = 'http://'+host
            if ('webserver' in self.cfg and 'port' in self.cfg['webserver'] and
                self.cfg['webserver']['port']):
                web_address += ':'+str(self.cfg['webserver']['port'])

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

    @tornado.gen.coroutine
    def get_grid_status(self):
        """Get all tasks running on the queue system.
           Returns {grid_queue_id:{status,submit_dir}}
        """
        raise tornado.gen.Return({})

    @tornado.gen.coroutine
    def generate_submit_file(self,task,cfg=None,passkey=None,filelist=None):
        """Generate queueing system submit file for task in dir."""
        raise NotImplementedError()

    @tornado.gen.coroutine
    def submit(self,task):
        """Submit task to queueing system."""
        raise NotImplementedError()

    @tornado.gen.coroutine
    def remove(self,tasks):
        """Remove tasks from queueing system."""
        pass