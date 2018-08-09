"""
The queue module is responsible for interacting with the local batch or
queueing system, putting tasks on the queue and removing them as necessary.
"""

import os
import time
import logging
from contextlib import contextmanager
import socket

import tornado.httpclient
import tornado.gen
from tornado.concurrent import run_on_executor
import certifi

import iceprod
import iceprod.server
from iceprod.server import module
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
        logger.info('queueing plugins in cfg: %r',{x:y for x,y in zip(plugin_names,plugin_types)})
        if not plugin_names:
            logger.debug('%r',self.cfg['queue'])
            logger.warning('no queueing plugins found. deactivating queue')
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
            logger.warning('queueing plugin found: %s = %s', p_name, p_cfg['type'])
            # try instantiating the plugin
            args = (self.cfg['site_id']+'.'+p_name, p_cfg, self.cfg,
                    self.modules, self.io_loop, self.executor, self.statsd,
                    self.rest_client)
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

        # add gridspec and types to the db
        args = {
            'host': socket.getfqdn(),
            'queues': {p_name:p_cfg['type'] for p,p_name,p_cfg in plugins_tmp},
            'version': iceprod.__version__,
        }
        if 'grid_id' in self.cfg and self.cfg['grid_id']:
            try:
                self.rest_client.request_seq('GET',
                        '/grids/{}'.format(self.cfg['grid_id']))
            except Exception:
                logger.warning('grid_id %s not present in DB',
                                self.cfg['grid_id'], exc_info=True)
                del self.cfg['grid_id']
        if 'grid_id' not in self.cfg:
            # register grid
            try:
                ret = self.rest_client.request_seq('POST',
                        '/grids', args)
                self.cfg['grid_id'] = ret['result']
            except Exception:
                logger.fatal('cannot register grid in DB', exc_info=True)
                raise
        else:
            # update grid
            try:
                ret = self.rest_client.request_seq('PATCH',
                        '/grids/{}'.format(self.cfg['grid_id']), args)
            except Exception:
                logger.warning('error updating grid in DB', exc_info=True)

        self.io_loop.add_callback(self.queue_loop)

    async def queue_loop(self):
        """Run the queueing loop"""
        # check and clean grids
        for p in self.plugins:
            try:
                await p.check_and_clean()
            except Exception:
                logger.error('plugin %s.check_and_clean() raised exception',
                             p.__class__.__name__,exc_info=True)

        # check proxy cert
        try:
             self.check_proxy(self.max_duration)
        except Exception:
            logger.error('error checking proxy',exc_info=True)

        # queue tasks to grids
        for p in self.plugins:
            try:
                await p.queue()
            except Exception:
                logger.error('plugin %s.queue() raised exception',
                             p.__class__.__name__,exc_info=True)

        # set timeout
        if 'queue' in self.cfg and 'queue_interval' in self.cfg['queue']:
            timeout = self.cfg['queue']['queue_interval']
            if timeout <= 0:
                timeout = 300
        else:
            timeout = 300
        self.io_loop.call_later(timeout, self.queue_loop)

    def check_proxy(self, duration=None):
        """
        Check the x509 proxy.

        Blocking function.
        """
        try:
            if duration:
                self.proxy.set_duration(duration//3600)
            self.proxy.update_proxy()
            self.cfg['queue']['x509proxy'] = self.proxy.get_proxy()
        except Exception:
            logger.warning('cannot setup x509 proxy', exc_info=True)
