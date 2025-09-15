"""
Server for queue management
"""
from collections import defaultdict
import dataclasses
from functools import partial
import importlib
import logging
from pathlib import Path
import pkgutil

import motor.motor_asyncio
from prometheus_client import Info, start_http_server
from rest_tools.server import RestServer
from tornado.web import RequestHandler, HTTPError
from wipac_dev_tools import from_environment_as_dataclass

from iceprod.util import VERSION_STRING
from ..prom_utils import AsyncMonitor
from ..s3 import boto3, S3
from .base_handler import IceProdRestConfig

logger = logging.getLogger('rest-server')


class Error(RequestHandler):
    def prepare(self):
        raise HTTPError(404, 'invalid api route')


class Health(RequestHandler):
    def get(self):
        self.write({})


@dataclasses.dataclass
class DefaultConfig:
    HOST: str = 'localhost'
    PORT: int = 8080
    DEBUG: bool = False
    OPENID_URL: str = ''
    OPENID_AUDIENCE: str = ''
    DB_URL: str = 'mongodb://localhost/iceprod'
    DB_TIMEOUT: int = 60
    DB_WRITE_CONCERN: int = 1
    PROMETHEUS_PORT: int = 0
    S3_ADDRESS: str = ''
    S3_ACCESS_KEY: str = ''
    S3_SECRET_KEY: str = ''
    MAX_BODY_SIZE: int = 10**9
    ROUTE_STATS_WINDOW_SIZE: int = 1000
    ROUTE_STATS_WINDOW_TIME: int = 3600
    ROUTE_STATS_TIMEOUT: int = 60
    CI_TESTING: bool = True


class Server:
    def __init__(self, s3_override=None):
        config = from_environment_as_dataclass(DefaultConfig)

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

        logging_url = config.DB_URL.split('@')[-1] if '@' in config.DB_URL else config.DB_URL
        logging.info(f'DB: {logging_url}')
        db_url, db_name = config.DB_URL.rsplit('/', 1)
        self.db = motor.motor_asyncio.AsyncIOMotorClient(
            db_url,
            timeoutMS=config.DB_TIMEOUT*1000,
            w=config.DB_WRITE_CONCERN,
        )
        logging.info(f'DB name: {db_name}')
        self.indexes = defaultdict(partial(defaultdict, dict))

        kwargs = IceProdRestConfig(rest_config, database=self.db, s3conn=s3conn)

        server = RestServer(debug=config.DEBUG, max_body_size=config.MAX_BODY_SIZE)

        handler_path = str(Path(__file__).parent / 'handlers')
        for _, name, _ in pkgutil.iter_modules([handler_path]):
            mod = importlib.import_module(f'iceprod.rest.handlers.{name}')
            ret = mod.setup(kwargs)
            for route,cls,kw in ret['routes']:
                kw2 = kw.copy()
                kw2['database'] = kw['database'][ret['database']]
                kw2['auth_database'] = kw['database']['auth']
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

        for database in self.indexes:
            db = self.db[database]
            for collection in self.indexes[database]:
                existing = await db[collection].index_information()
                for name in self.indexes[database][collection]:
                    if name not in existing:
                        kwargs = self.indexes[database][collection][name]
                        logging.info('DB: creating index %s/%s:%s %r', database, collection, name, kwargs)
                        await db[collection].create_index(name=name, **kwargs)

    async def stop(self):
        await self.server.stop()
        if self.async_monitor:
            await self.async_monitor.stop()
