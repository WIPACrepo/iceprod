"""
Server for queue management
"""
import asyncio
import importlib
import logging
import pkgutil
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import Any

import pymongo
from prometheus_client import Info, start_http_server
from rest_tools.server import RestServer
from tornado.web import HTTPError, RequestHandler

from iceprod.common.mongo import Mongo
from iceprod.common.prom_utils import AsyncMonitor
from iceprod.s3 import S3, boto3
from iceprod.util import VERSION_STRING

from .base_handler import IceProdRestConfig
from .config import get_config

logger = logging.getLogger('rest-server')


class Error(RequestHandler):
    def prepare(self):
        raise HTTPError(404, 'invalid api route')


class Health(RequestHandler):
    def get(self):  # ty: ignore
        self.write({})


class Server:
    def __init__(self, s3_override=None):
        config = get_config()

        rest_config: dict[str, Any] = {
            'debug': config.DEBUG,
            'route_stats': {
                'window_size': config.ROUTE_STATS_WINDOW_SIZE,
                'window_time': config.ROUTE_STATS_WINDOW_TIME,
                'timeout': config.ROUTE_STATS_TIMEOUT,
            }
        }
        if config.OPENID_URL:
            logging.info(f'enabling auth via {config.OPENID_URL} for aud "{config.OPENID_AUDIENCE}"')
            rest_config.update({
                'auth': {
                    'openid_url': config.OPENID_URL,
                    'audience': config.OPENID_AUDIENCE,
                }
            })
        elif config.CI_TESTING:
            logging.warning('CI_TESTING is enabled!')
            rest_config.update({
                'auth': {
                    'secret': 'secret',
                }
            })
        else:
            raise RuntimeError('OPENID_URL not specified, and CI_TESTING not enabled!')

        # enable monitoring
        self.prometheus_port = config.PROMETHEUS_PORT if config.PROMETHEUS_PORT > 0 else None
        self.async_monitor = None

        s3conn = None
        if s3_override:
            logging.warning('S3 in testing mode')
            s3conn = S3('', '', '', mock_s3=s3_override)
        elif boto3 and config.S3_ACCESS_KEY and config.S3_SECRET_KEY:
            s3conn = S3(config.S3_ADDRESS, config.S3_ACCESS_KEY, config.S3_SECRET_KEY)
        else:
            logger.warning('S3 is not available!')

        self.db_client = Mongo(
            url=config.DB_URL,
            timeout=config.DB_TIMEOUT,
            write_concern=config.DB_WRITE_CONCERN
        )
        self.indexes = defaultdict(partial(defaultdict, dict))
        self._index_task: asyncio.Task | None = None

        kwargs = IceProdRestConfig(rest_config, database=self.db_client.client, s3conn=s3conn)

        server = RestServer(debug=config.DEBUG, max_body_size=config.MAX_BODY_SIZE)

        handler_path = str(Path(__file__).parent / 'handlers')
        for _, name, _ in pkgutil.iter_modules([handler_path]):
            mod = importlib.import_module(f'iceprod.rest.handlers.{name}')
            ret = mod.setup(kwargs)
            for route,cls,kw in ret['routes']:
                kw2 = kw.copy()
                kw2['database'] = kw['database'][ret['database']]
                kw2['db_client'] = self.db_client.client
                server.add_route(route, cls, kw2)
            ii = self.indexes[ret['database']]
            for col in ret['indexes']:
                ii[col].update(ret['indexes'][col])

        server.add_route('/healthz', Health)
        server.add_route(r'/(.*)', Error)

        server.startup(address=config.HOST, port=config.PORT)

        self.server = server

    async def _create_indexes(self):
        """
        Create Mongo indexes.

        Runs without the client-wide DB_TIMEOUT, because index builds on large
        collections can take much longer. Errors are logged, not raised, so a
        failed build does not take down the server.
        """
        try:
            # timeout(0) means "no timeout". timeout(None) would NOT work: pymongo
            # then falls back to the client-wide timeoutMS (DB_TIMEOUT).
            with pymongo.timeout(0):
                for database, indexes in self.indexes.items():
                    await self.db_client.create_indexes(db_name=database, indexes=indexes)
            logger.info('DB: index creation complete')
        except asyncio.CancelledError:
            logger.info('DB: index creation cancelled')
            raise
        except Exception:
            logger.exception('DB: index creation failed')

    async def start(self, wait_for_indexes: bool = False):
        """
        Start the server.

        Index creation runs in a background task so startup is not blocked.

        Args:
            wait_for_indexes: wait for index creation to finish before returning (for tests)
        """
        if self.prometheus_port:
            logging.info("starting prometheus on %r", self.prometheus_port)
            start_http_server(self.prometheus_port)
            i = Info('iceprod', 'IceProd information')
            i.info({
                'version': VERSION_STRING,
                'type': 'api',
            })
            self.async_monitor = AsyncMonitor(labels={'type': 'api'})
            await self.async_monitor.start()

        await self.db_client.ping()
        self._index_task = asyncio.create_task(self._create_indexes())
        if wait_for_indexes:
            await self._index_task

    async def stop(self):
        if self._index_task and not self._index_task.done():
            self._index_task.cancel()
            try:
                await self._index_task
            except asyncio.CancelledError:
                pass
        await self.server.stop()
        if self.async_monitor:
            await self.async_monitor.stop()
        await self.db_client.close()
