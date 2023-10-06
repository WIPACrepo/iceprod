"""
The queue module is responsible for interacting with the local batch or
queueing system, putting tasks on the queue and removing them as necessary.
"""
import asyncio
import os
import logging
import socket

import iceprod
import iceprod.server
from iceprod.server import module
from iceprod.server.globus import SiteGlobusProxy
import iceprod.core.functions


class StopException(Exception):
    pass


logger = logging.getLogger('queue')


class Queue:
    """
    Run the queue module, which queues jobs onto the local grid system(s).
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.proxy = None

        self.max_duration = 3600*12

        self.grid = None
        self.queue_task = None

    async def start(self):
        """Start the queue"""
        super(queue,self).start()

        # set up x509 proxy
        proxy_kwargs = {}
        if 'gridftp_cfgfile' in self.cfg['queue']:
            proxy_kwargs['cfgfile'] = self.cfg['queue']['gridftp_cfgfile']
        self.proxy = SiteGlobusProxy(**proxy_kwargs)

        # some setup
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
                    self.modules, self.executor, self.statsd,
                    self.rest_client, self.cred_client)
            try:
                self.plugins.append(iceprod.server.run_module(p,*args))
            except Exception:
                logger.error('Error importing plugin', exc_info=True)
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

        self.queue_task = asyncio.create_task(self.queue_loop())

    async def queue_loop(self, run_once=False):
        """Run the queueing loop"""
        while True:
            # check and clean grid
            try:
                await self.grid.check_and_clean()
            except Exception:
                logger.error('plugin %s.check_and_clean() raised exception',
                             self.grid.__class__.__name__,exc_info=True)

            # check proxy cert
            try:
                self.check_proxy(self.max_duration)
            except Exception:
                logger.error('error checking proxy',exc_info=True)

            # queue tasks to grids
            try:
                await self.grid.queue()
            except Exception:
                logger.error('plugin %s.queue() raised exception',
                             self.grid.__class__.__name__,exc_info=True)

            if run_once:
                break

            # set timeout
            timeout = self.cfg.get('queue', {}).get('queue_interval', 300)
            if timeout <= 0:
                timeout = 300
            await asyncio.sleep(timeout)

    async def stop(self):
        if self.queue_task:
            self.queue_task.cancel()
            self.queue_task = None

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
