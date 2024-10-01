"""
The queue module is responsible for interacting with the local batch or
queueing system, putting tasks on the queue and removing them as necessary.
"""
import asyncio
import importlib
import logging
from pathlib import Path
import pkgutil
import time

from rest_tools.client import ClientCredentialsAuth

from iceprod.server.globus import SiteGlobusProxy


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
        self.proxy_task = None

        self.max_duration = 3600*12

        self.grid = self.get_grid_plugin()

    async def run(self):
        # set up x509 proxy
        proxy_kwargs = {'duration': self.max_duration}
        if 'gridftp_cfgfile' in self.cfg['queue']:
            proxy_kwargs['cfgfile'] = self.cfg['queue']['gridftp_cfgfile']
        self.proxy = SiteGlobusProxy(**proxy_kwargs)

        self.proxy_task = asyncio.create_task(self.check_proxy())

        try:
            await self.grid.run()
        finally:
            if self.proxy_task:
                self.proxy_task.cancel()
                self.proxy_task = None
            await asyncio.sleep(0)

    def get_grid_plugin(self):
        """
        Get the grid plugin called for in the config.

        Returns:
            Grid plugin instantiated class
        """
        plugin_desc = self.cfg['queue']['description']
        plugin_type = self.cfg['queue']['type']
        logger.info('queueing plugin in cfg: %r %r', plugin_desc, plugin_type)

        # try to find installed plugins
        plugin_path = str(Path(__file__).parent / 'plugins')
        raw_types = [name for _, name, _ in pkgutil.iter_modules([plugin_path])]

        logger.info('installed plugins: %r', raw_types)
        plugin_type = plugin_type.lower()
        p = None
        for raw_name in raw_types:
            if raw_name == plugin_type:
                # exact match
                logger.debug('exact plugin match - %s', raw_name)
                p = raw_name
                break
            elif plugin_type.startswith(raw_name):
                # partial match
                if p is None:
                    logger.debug('partial plugin match - %s', raw_name)
                    p = raw_name
                else:
                    if len(raw_name) > len(p):
                        logger.debug('better plugin match - %s', raw_name)
                        p = raw_name
        if p is not None:
            plugin_module = importlib.import_module(f'iceprod.server.plugins.{p}')
        else:
            raise RuntimeError(f'Cannot find plugin for type {plugin_type}')

        # get duration
        duration = 0
        if 'max_task_queued_time' in self.cfg['queue']:
            duration += self.cfg['queue']['max_task_queued_time']
        if 'max_task_processing_time' in self.cfg['queue']:
            duration += self.cfg['queue']['max_task_processing_time']
        if duration > self.max_duration:
            self.max_duration = duration

        # instantiate plugin
        kwargs = {
            'cfg': self.cfg,
            'rest_client': None,
            'cred_client': None,
        }

        if ('rest_api' in self.cfg and 'url' in self.cfg['rest_api']
                and 'oauth_url' in self.cfg['rest_api']
                and 'oauth_client_id' in self.cfg['rest_api']
                and 'oauth_client_secret' in self.cfg['rest_api']):
            try:
                kwargs['rest_client'] = ClientCredentialsAuth(
                    address=self.cfg['rest_api']['url'],
                    token_url=self.cfg['rest_api']['oauth_url'],
                    client_id=self.cfg['rest_api']['oauth_client_id'],
                    client_secret=self.cfg['rest_api']['oauth_client_secret'],
                )
                kwargs['cred_client'] = ClientCredentialsAuth(
                    address=self.cfg['rest_api']['cred_url'],
                    token_url=self.cfg['rest_api']['oauth_url'],
                    client_id=self.cfg['rest_api']['oauth_client_id'],
                    client_secret=self.cfg['rest_api']['oauth_client_secret'],
                )
            except Exception:
                logger.warning('failed to connect to rest api: %r',
                               self.cfg['rest_api'].get('url',''), exc_info=True)

        try:
            return getattr(plugin_module, 'Grid')(**kwargs)
        except Exception:
            logger.error('Error calling plugin class', exc_info=True)
            raise RuntimeError('Error calling plugin class')

    async def check_proxy(self, duration=None):
        """
        Check the x509 proxy every 5 minutes.
        """
        def blocking():
            try:
                if duration:
                    self.proxy.set_duration(duration//3600)
                self.proxy.update_proxy()
                self.cfg['queue']['x509proxy'] = self.proxy.get_proxy()
            except Exception:
                logger.warning('cannot setup x509 proxy', exc_info=True)
                raise RuntimeError('cannot setup x509 proxy')

        while True:
            start = time.monotonic()
            await asyncio.to_thread(blocking())
            end = time.monotonic()
            await asyncio.sleep(max(1, 300 - (end-start)))
