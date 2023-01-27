"""
Server for queue management
"""
import asyncio
from collections import defaultdict
from datetime import datetime
import importlib
import logging
import os
from pathlib import Path
import pkgutil
import string

import pymongo.errors
import motor.motor_asyncio
from rest_tools.server import RestServer, RestHandler
from rest_tools.server import keycloak_role_auth, catch_error
from tornado.escape import json_decode
from tornado.web import RequestHandler, HTTPError
from wipac_dev_tools import from_environment

from ..s3 import boto3, S3, FakeS3
from ..server.module import FakeStatsClient, StatsClientIgnoreErrors
from .base_handler import IceProdRestConfig



class Error(RequestHandler):
    def prepare(self):
        raise HTTPError(404, 'invalid api route')


class Health(RequestHandler):
    def get(self):
        self.write({})


class Server:
    def __init__(self, s3_override=None):
        default_config = {
            'HOST': 'localhost',
            'PORT': 8080,
            'DEBUG': False,
            'OPENID_URL': '',
            'OPENID_AUDIENCE': '',
            'DB_URL': 'mongodb://localhost/iceprod',
            'STATSD_ADDRESS': '',
            'STATSD_PREFIX': 'rest_api',
            'S3_ADDRESS': '',
            'S3_ACCESS_KEY': '',
            'S3_SECRET_KEY': '',
            'CI_TESTING': '',
        }
        config = from_environment(default_config)

        rest_config = {
            'debug': config['DEBUG'],
        }
        if config['OPENID_URL']:
            logging.info(f'enabling auth via {config["OPENID_URL"]} for aud "{config["OPENID_AUDIENCE"]}"')
            rest_config.update({
                'auth': {
                    'openid_url': config['OPENID_URL'],
                    'audience': config['OPENID_AUDIENCE'],
                }
            })
        elif config['CI_TESTING']:
            rest_config.update({
                'auth': {
                    'secret': 'secret',
                }
            })
        else:
            raise RuntimeError('OPENID_URL not specified, and CI_TESTING not enabled!')

        statsd = FakeStatsClient()
        if config['STATSD_ADDRESS']:
            try:
                addr = config['STATSD_ADDRESS']
                port = 8125
                if ':' in addr:
                    addr,port = addr.split(':')
                    port = int(port)
                statsd = StatsClientIgnoreErrors(addr, port=port, prefix=config['STATSD_PREFIX'])
            except Exception:
                logger.warning('failed to connect to statsd: %r', config['STATSD_ADDRESS'], exc_info=True)

        if s3_override:
            logging.warning('S3 in testing mode')
            s3conn = FakeS3(s3_override)
        elif boto3 and config['S3_ACCESS_KEY'] and config['S3_SECRET_KEY']:
            s3conn = S3(config['S3_ADDRESS'], config['S3_ACCESS_KEY'], config['S3_SECRET_KEY'])
        else:
            logger.warning('S3 is not available!')

        logging_url = config["DB_URL"].split('@')[-1] if '@' in config["DB_URL"] else config["DB_URL"]
        logging.info(f'DB: {logging_url}')
        db_url, db_name = config['DB_URL'].rsplit('/', 1)
        db = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        logging.info(f'DB name: {db_name}')
        self.db = db[db_name]
        self.indexes = defaultdict(dict)

        kwargs = IceProdRestConfig(rest_config, statsd=statsd, database=self.db, s3conn=s3conn)

        server = RestServer(debug=config['DEBUG'])

        handler_path = str(Path(__file__).parent / 'handlers')
        for _, name, _ in pkgutil.iter_modules([handler_path]):
            mod = importlib.import_module(f'iceprod.rest.handlers.{name}')
            ret = mod.setup(kwargs)
            for route,cls,kw in ret['routes']:
                server.add_route(route, cls, kw)
            for col in ret['indexes']:
                self.indexes[col].update(ret['indexes'][col])

        server.add_route('/healthz', Health)
        server.add_route(r'/(.*)', Error)

        server.startup(address=config['HOST'], port=config['PORT'])

        self.server = server

    async def start(self):
        for collection in self.indexes:
            existing = await self.db[collection].index_information()
            for name in self.indexes[collection]:
                if name not in existing:
                    kwargs = self.indexes[collection][name]
                    logging.info('DB: creating index %s:%s %r', collection, name, kwargs)
                    await self.db[collection].create_index(name=name, **kwargs)

    async def stop(self):
        await self.server.stop()
