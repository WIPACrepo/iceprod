"""
The queue module is responsible for interacting with the local batch or
queueing system, putting tasks on the queue and removing them as necessary.
"""

import os
import time
import logging
from contextlib import contextmanager
from itertools import izip

import tornado.httpclient
import tornado.gen
from tornado.concurrent import run_on_executor
import certifi

import iceprod.server
from iceprod.server import module
from iceprod.server.master_communication import send_master
from iceprod.server.globus import SiteGlobusProxy
import iceprod.core.functions

class StopException(Exception):
    pass

logger = logging.getLogger('modules_queue')

class queue(module.module):
    """
    Run the queue module, which queues jobs onto the local grid system(s).
    """

    def __init__(self,*args,**kwargs):
        # run default init
        super(queue,self).__init__(*args,**kwargs)

        self.proxy = None

        self.max_duration = 3600*12

    def start(self):
        """Start the queue"""
        super(queue,self).start()

        # set up x509 proxy
        proxy_kwargs = {}
        if 'gridftp_cfgfile' in self.cfg['queue']:
            proxy_kwargs['cfgfile'] = self.cfg['queue']['gridftp_cfgfile']
        self.proxy = SiteGlobusProxy(**proxy_kwargs)

        # set up job cacert
        use_ssl = 'system' in self.cfg and 'ssl' in self.cfg['system'] and self.cfg['system']['ssl']
        if (use_ssl and 'cert' in self.cfg['system']['ssl']):
            if 'I3PROD' in os.environ:
                remote_cacert = os.path.expandvars(os.path.join('$I3PROD','etc','remote_cacert'))
            else:
                remote_cacert = os.path.expandvars(os.path.join('$PWD','remote_cacert'))
            with open(remote_cacert,'w') as f:
                f.write(open(certifi.where()).read())
                f.write('\n# IceProd local cert\n')
                f.write(open(self.cfg['system']['ssl']['cert']).read())
            self.cfg['system']['remote_cacert'] = remote_cacert

        # some setup
        self.plugins = []
        plugin_names = [x for x in self.cfg['queue'] if isinstance(self.cfg['queue'][x],dict)]
        plugin_cfg = [self.cfg['queue'][x] for x in plugin_names]
        plugin_types = [x['type'] for x in plugin_cfg]
        logger.info('queueing plugins in cfg: %r',{x:y for x,y in izip(plugin_names,plugin_types)})
        if not plugin_names:
            logger.debug('%r',self.cfg['queue'])
            logger.warn('no queueing plugins found. deactivating queue')
            self.stop()
            return

        # try to find plugins
        raw_types = iceprod.server.listmodules('iceprod.server.plugins')
        logger.info('available modules: %r',raw_types)
        plugins_tmp = []
        for i,t in enumerate(plugin_types):
            t = t.lower()
            p = None
            for r in raw_types:
                r_name = r.rsplit('.',1)[1].lower()
                if r_name == t:
                    # exact match
                    logger.debug('exact plugin match - %s',r)
                    p = r
                    break
                elif t.startswith(r_name):
                    # partial match
                    if p is None:
                        logger.debug('partial plugin match - %s',r)
                        p = r
                    else:
                        name2 = p.rsplit('.',1)[1]
                        if len(r_name) > len(name2):
                            logger.debug('better plugin match - %s',r)
                            p = r
            if p is not None:
                plugins_tmp.append((p,plugin_names[i],plugin_cfg[i]))
            else:
                logger.error('Cannot find plugin for grid %s of type %s',plugin_names[i],t)

        # instantiate all plugins that are required
        gridspec_types = {}
        if 'max_task_queued_time' in self.cfg['queue']:
            self.max_duration += self.cfg['queue']['max_task_queued_time']
        if 'max_task_processing_time' in self.cfg['queue']:
            self.max_duration += self.cfg['queue']['max_task_processing_time']
        for p,p_name,p_cfg in plugins_tmp:
            logger.warn('queueing plugin found: %s = %s', p_name, p_cfg['type'])
            # try instantiating the plugin
            args = (self.cfg['site_id']+'.'+p_name, p_cfg, self.cfg,
                    self.modules, self.io_loop, self.executor, self.statsd)
            try:
                self.plugins.append(iceprod.server.run_module(p,*args))
            except Exception as e:
                logger.error('Error importing plugin',exc_info=True)
            else:
                desc = p_cfg['description'] if 'description' in p_cfg else ''
                gridspec_types[self.cfg['site_id']+'.'+p_name] = {
                        'type': p_cfg['type'],
                        'description': desc,
                }
                duration = 0
                if 'max_task_queued_time' in p_cfg:
                    duration += p_cfg['max_task_queued_time']
                if 'max_task_processing_time' in p_cfg:
                    duration += p_cfg['max_task_processing_time']
                if duration > self.max_duration:
                    self.max_duration = duration

        @tornado.gen.coroutine
        def cb():
            # add gridspec and types to the db
            try:
                yield self.modules['db']['queue_set_site_queues'](
                        site_id=self.cfg['site_id'], queues=gridspec_types)
            except Exception:
                logger.warn('error setting site queues',exc_info=True)
            # start queue loop
            yield self.queue_loop()
        self.io_loop.add_callback(cb)

    @tornado.gen.coroutine
    def queue_loop(self):
        """Run the queueing loop"""
        try:
            # check and clean grids
            for p in self.plugins:
                try:
                    yield p.check_and_clean()
                except Exception:
                    logger.error('plugin %s.check_and_clean() raised exception',
                                 p.__class__.__name__,exc_info=True)

            # check proxy cert
            try:
                yield self.check_proxy(self.max_duration)
            except Exception:
                logger.error('error checking proxy',exc_info=True)

            # buffer jobs and tasks for active datasets
            if ('master' in self.cfg and 'url' in self.cfg['master']
                and self.cfg['master']['url']):
                gridspecs = [p.gridspec for p in self.plugins]
            else:
                # queue for all gridspecs, since we don't have a master
                gridspecs = None
            try:
                yield self.buffer_jobs_tasks(gridspecs)
            except Exception:
                logger.error('error buffering jobs and tasks',
                             exc_info=True)

            # queue tasks to grids
            num_queued = 0
            for p in self.plugins:
                try:
                    yield p.queue()
                    num_queued += p.tasks_queued + p.tasks_processing
                except Exception:
                    logger.error('plugin %s.queue() raised exception',
                                 p.__class__.__name__,exc_info=True)

            # do global queueing
            try:
                plugin_cfg = self.plugins[0].queue_cfg

                # get num tasks to queue
                tasks_on_queue = plugin_cfg['tasks_on_queue']
                num = min(tasks_on_queue[1] - num_queued, tasks_on_queue[0])
                if len(tasks_on_queue) > 2:
                    num = min(num, tasks_on_queue[2])
                if num > 0:
                    # get priority factors
                    qf_p = 1.0
                    qf_d = 1.0
                    qf_t = 1.0
                    if 'queueing_factor_priority' in self.cfg['queue']:
                        qf_p = self.cfg['queue']['queueing_factor_priority']
                    elif 'queueing_factor_priority' in plugin_cfg:
                        qf_p = plugin_cfg['queueing_factor_priority']
                    if 'queueing_factor_dataset' in self.cfg['queue']:
                        qf_d = self.cfg['queue']['queueing_factor_dataset']
                    elif 'queueing_factor_dataset' in plugin_cfg:
                        qf_d = plugin_cfg['queueing_factor_dataset']
                    if 'queueing_factor_tasks' in self.cfg['queue']:
                        qf_t = self.cfg['queue']['queueing_factor_tasks']
                    elif 'queueing_factor_tasks' in plugin_cfg:
                        qf_t = plugin_cfg['queueing_factor_tasks']
                    yield self.global_queueing(qf_p,qf_d,qf_t,num=num)
            except Exception:
                logger.error('error in global queueing', exc_info=True)

        except Exception:
            logger.error('queue_loop stopped because of exception',
                        exc_info=True)
        else:
            # set timeout
            if 'queue' in self.cfg and 'queue_interval' in self.cfg['queue']:
                timeout = self.cfg['queue']['queue_interval']
                if timeout <= 0:
                    timeout = 300
            else:
                timeout = 300
            self.io_loop.call_later(timeout, self.queue_loop)

    @run_on_executor
    def check_proxy(self, duration=None):
        """Check the x509 proxy"""
        try:
            if duration:
                self.proxy.set_duration(duration//3600)
            self.proxy.update_proxy()
            self.cfg['queue']['x509proxy'] = self.proxy.get_proxy()
        except Exception:
            logger.warn('cannot setup x509 proxy', exc_info=True)

    @tornado.gen.coroutine
    def global_queueing(self, queueing_factor_priority=1.0,
                        queueing_factor_dataset=1.0,
                        queueing_factor_tasks=1.0,
                        num=100):
        """
        Do global queueing.

        Fetch tasks from the global server that match the local resources
        and add them to the local DB. This is non-blocking, but only
        one at a time can run.

        :param queueing_factor_priority: queueing factor for priority
        :param queueing_factor_dataset: queueing factor for dataset id
        :param queueing_factor_tasks: queueing factor for number of tasks
        :param num: number of tasks to queue
        """
        if ('master' not in self.cfg or 'url' not in self.cfg['master'] or
            not self.cfg['master']['url']):
            logger.debug('no master url, so skip global queueing')
            return

        #resources = yield self.modules['db']['node_get_site_resources'](
        #        site_id=self.cfg['site_id'])

        url = self.cfg['master']['url']
        params = {#'resources':resources,
                  'queueing_factor_priority':queueing_factor_priority,
                  'queueing_factor_dataset':queueing_factor_dataset,
                  'queueing_factor_tasks':queueing_factor_tasks,
                  'num':num,
                 }
        if 'group_filters' in self.cfg:
            params['filters'] = self.cfg['group_filters']
        ret = yield send_master(self.cfg, 'queue_master', **params)

        yield self.modules['db']['misc_update_tables'](tables=ret)

    @tornado.gen.coroutine
    def buffer_jobs_tasks(self,gridspecs):
        """Make sure active datasets have jobs and tasks defined"""
        buffer = self.cfg['queue']['task_buffer']
        if buffer > 0:
            yield self.modules['db']['queue_buffer_jobs_tasks'](gridspec=gridspecs,
                                                                num_jobs=buffer)