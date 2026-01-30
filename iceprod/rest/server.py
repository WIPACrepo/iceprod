"""
Server for queue management
"""
from collections import defaultdict
from functools import partial
import importlib
import logging
from pathlib import Path
import pkgutil

from prometheus_client import Info, start_http_server
from rest_tools.server import RestServer
from tornado.web import RequestHandler, HTTPError

from iceprod.common.mongo import Mongo
from iceprod.common.prom_utils import AsyncMonitor
from iceprod.s3 import boto3, S3
from iceprod.util import VERSION_STRING
from .config import get_config
from .base_handler import IceProdRestConfig

logger = logging.getLogger('rest-server')


class Error(RequestHandler):
    def prepare(self):
        raise HTTPError(404, 'invalid api route')


class Health(RequestHandler):
    def get(self):
        self.write({})


class Server:
    def __init__(self, s3_override=None):
        config = get_config()

        rest_config = {
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

    async def start(self):
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
        for database, indexes in self.indexes.items():
            await self.db_client.create_indexes(db_name=database, indexes=indexes)

    async def stop(self):
        await self.server.stop()
        if self.async_monitor:
            await self.async_monitor.stop()
        await self.db_client.close()
