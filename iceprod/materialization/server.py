"""
Materialization service for dataset late materialization.
"""
import asyncio
from datetime import datetime
import json
import logging
import time
import uuid

import pymongo
import pymongo.errors
import motor.motor_asyncio
import requests.exceptions
from rest_tools.client import RestClient, ClientCredentialsAuth
from rest_tools.server import RestServer
from tornado.web import HTTPError
from tornado.web import RequestHandler as TornadoRequestHandler
from wipac_dev_tools import from_environment

from iceprod.rest.auth import authorization, attr_auth
from iceprod.rest.base_handler import IceProdRestConfig, APIBase
from iceprod.server.module import FakeStatsClient, StatsClientIgnoreErrors
from iceprod.server.util import nowstr, datetime2str
from .service import MaterializationService

logger = logging.getLogger('server')


class BaseHandler(APIBase):
    def initialize(self, materialization_service=None, rest_client=None, **kwargs):
        super().initialize(**kwargs)
        self.materialization_service = materialization_service
        self.rest_client = rest_client

    async def new_request(self, args):
        # validate first
        fields = {
            'dataset_id': str,
            'set_status': str,
            'num': int,
        }
        if set(args)-set(fields):  # don't let random args through
            raise HTTPError(400, reason='invalid params')
        for k in fields:
            if k in args and not isinstance(args[k], fields[k]):
                r = 'key "{}" should be of type {}'.format(k, fields[k].__name__)
                raise HTTPError(400, reason=r)

        # set some fields
        now = nowstr()
        data = {
            'materialization_id': uuid.uuid1().hex,
            'status': 'waiting',
            'create_timestamp': now,
            'modify_timestamp': now,
            'creator': self.auth_data.get('username', None),
            'role': self.auth_data.get('role', None),
        }
        for k in fields:
            if k in args:
                data[k] = args[k]

        # insert
        await self.db.materialization.insert_one(data)
        return data

    async def check_attr_auth(self, arg, val, role):
        """
        Based on the request groups or username, check if they are allowed to
        access `arg`:`role`.

        Runs a remote query to the IceProd API.

        Args:
            arg (str): attribute name to check
            val (str): attribute value
            role (str): the role to check for (read|write)
        """
        args = {
            'name': arg,
            'value': val,
            'role': role,
            'username': self.current_user,
            'groups': self.auth_groups,
        }
        try:
            await self.rest_client.request('POST', '/auths', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                raise HTTPError(403, 'auth failed')
            else:
                raise HTTPError(500, 'auth could not be completed')


class StatusHandler(BaseHandler):
    """
    Handle materialization status requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, materialization_id):
        """
        Get materialization status.

        If materialization_id is invalid, returns http code 404.

        Args:
            materialization_id (str): materialization request id

        Returns:
            dict: materialization metadata
        """
        ret = await self.db.materialization.find_one({'materialization_id':materialization_id},
                                                     projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Materialization request not found")
        else:
            self.write(ret)


class RequestHandler(BaseHandler):
    """
    Handle basic materialization requests.
    """
    @authorization(roles=['admin', 'system'])
    async def post(self):
        """
        Create basic materialization request.

        On success, returns http code 201.

        Params:
            num: number of jobs to buffer per dataset

        Returns:
            dict: {result: materialization_id}
        """
        args = json.loads(self.request.body) if self.request.body else {}

        data = await self.new_request(args)

        # return success
        self.set_status(201)
        self.write({'result': data['materialization_id']})


class RequestDatasetHandler(BaseHandler):
    """
    Handle dataset materialization requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id):
        """
        Create dataset materialization request.

        On success, returns http code 201.

        Params:
            num: number of jobs to buffer per dataset

        Args:
            dataset_id (str): dataset_id to materialize

        Returns:
            dict: {result: materialization_id}
        """
        args = json.loads(self.request.body) if self.request.body else {}
        args['dataset_id'] = dataset_id

        data = await self.new_request(args)

        # return success
        self.set_status(201)
        self.write({'result': data['materialization_id']})


class DatasetStatusHandler(BaseHandler):
    """
    Handle dataset materialization request statuses.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id):
        """
        Get latest materialization status for a dataset, if any.

        If dataset_id is invalid, returns http code 404.

        Args:
            dataset_id (str): dataset id to check

        Returns:
            dict: materialization metadata
        """
        ret = await self.db.materialization.find_one(
            {'dataset_id': dataset_id},
            projection={'_id':False},
            sort=[('modify_timestamp', pymongo.DESCENDING)],
        )
        if not ret:
            self.send_error(404, reason="Materialization request not found")
        else:
            self.write(ret)


class HealthHandler(BaseHandler):
    """
    Handle health requests.
    """
    async def get(self):
        """
        Get health status.

        Returns based on exit code, 200 = ok, 400 = failure
        """
        now = time.time()
        status = {
            'now': nowstr(),
            'start_time': datetime2str(datetime.utcfromtimestamp(self.materialization_service.start_time)),
            'last_run_time': "",
            'last_success_time': "",
            'last_cleanup_time': "",
            'num_requests': -1,
        }
        try:
            if self.materialization_service.last_run_time is None and self.materialization_service.start_time + 3600 < now:
                self.send_error(500, reason='materialization was never run')
                return
            if self.materialization_service.last_run_time is not None:
                if self.materialization_service.last_run_time + 3600 < now:
                    self.send_error(500, reason='materialization has stopped running')
                    return
                status['last_run_time'] = datetime2str(datetime.utcfromtimestamp(self.materialization_service.last_run_time))
            if self.materialization_service.last_success_time is None and self.materialization_service.start_time + 86400 < now:
                self.send_error(500, reason='materialization was never successful')
                return
            if self.materialization_service.last_success_time is not None:
                if self.materialization_service.last_success_time + 86400 < now:
                    self.send_error(500, reason='materialization has stopped being successful')
                    return
                status['last_success_time'] = datetime2str(datetime.utcfromtimestamp(self.materialization_service.last_success_time))
            if self.materialization_service.last_cleanup_time is not None:
                status['last_cleanup_time'] = datetime2str(datetime.utcfromtimestamp(self.materialization_service.last_cleanup_time))
        except Exception:
            logger.info('error from materialization service', exc_info=True)
            self.send_error(500, reason='error from materialization service')
            return

        try:
            ret = await self.db.materialization.count_documents({'status':{'$in':['waiting','processing']}}, maxTimeMS=1000)
        except Exception:
            logger.info('bad db request', exc_info=True)
            self.send_error(500, reason='bad db request')
            return
        if ret is None:
            self.send_error(500, reason='bad db result')
        else:
            status['num_requests'] = ret
            self.write(status)


class Error(TornadoRequestHandler):
    def prepare(self):
        raise HTTPError(404, 'invalid api route')


class Server:
    def __init__(self):
        default_config = {
            'HOST': 'localhost',
            'PORT': 8080,
            'DEBUG': False,
            'OPENID_URL': '',
            'OPENID_AUDIENCE': '',
            'ICEPROD_API_ADDRESS': 'https://iceprod2-api.icecube.wisc.edu',
            'ICEPROD_API_CLIENT_ID': '',
            'ICEPROD_API_CLIENT_SECRET': '',
            'DB_URL': 'mongodb://localhost/datasets',
            'STATSD_ADDRESS': '',
            'STATSD_PREFIX': 'rest_api',
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

        logging_url = config["DB_URL"].split('@')[-1] if '@' in config["DB_URL"] else config["DB_URL"]
        logging.info(f'DB: {logging_url}')
        db_url, db_name = config['DB_URL'].rsplit('/', 1)
        db = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        logging.info(f'DB name: {db_name}')
        self.db = db[db_name]
        self.indexes = {
            'materialization': {
                'materialization_id_index': {'keys': 'materialization_id', 'unique': True},
                'dataset_id_index': {'keys': 'dataset_id', 'unique': False},
                'status_timestamp_index': {'keys': [('status', pymongo.ASCENDING), ('timestamp', pymongo.ASCENDING)], 'unique': False},
            }
        }

        if config['ICEPROD_API_CLIENT_ID'] and config['ICEPROD_API_CLIENT_SECRET']:
            logging.info(f'enabling auth via {config["OPENID_URL"]} for aud "{config["OPENID_AUDIENCE"]}"')
            rest_client = ClientCredentialsAuth(
                address=config['ICEPROD_API_ADDRESS'],
                token_url=config['OPENID_URL'],
                client_id=config['ICEPROD_API_CLIENT_ID'],
                client_secret=config['ICEPROD_API_CLIENT_SECRET'],
            )
        elif config['CI_TESTING']:
            rest_client = RestClient(config['ICEPROD_API_ADDRESS'], timeout=1, retries=0)
        else:
            raise RuntimeError('ICEPROD_API_CLIENT_ID or ICEPROD_API_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

        self.materialization_service = MaterializationService(self.db, rest_client)
        self.materialization_service_task = None

        kwargs = IceProdRestConfig(rest_config, statsd=statsd, database=self.db)
        kwargs['materialization_service'] = self.materialization_service
        kwargs['rest_client'] = rest_client

        server = RestServer(debug=config['DEBUG'])

        server.add_route(r'/status/(?P<materialization_id>\w+)', StatusHandler, kwargs)
        server.add_route('/', RequestHandler, kwargs)
        server.add_route(r'/request/(?P<dataset_id>\w+)', RequestDatasetHandler, kwargs)
        server.add_route(r'/request/(?P<dataset_id>\w+)/status', DatasetStatusHandler, kwargs)
        server.add_route('/healthz', HealthHandler, kwargs)
        server.add_route(r'/(.*)', Error)

        server.startup(address=config['HOST'], port=config['PORT'])

        self.server = server

    async def start(self):
        for collection in self.indexes:
            existing = await self.db[collection].index_information()
            for name in self.indexes[collection]:
                if name not in existing:
                    logging.info('DB: creating index %s:%s', collection, name)
                    kwargs = self.indexes[collection][name]
                    await self.db[collection].create_index(name=name, **kwargs)

        if not self.materialization_service_task:
            self.materialization_service_task = asyncio.create_task(self.materialization_service.run())

    async def stop(self):
        await self.server.stop()
        if self.materialization_service_task:
            self.materialization_service_task.cancel()
            try:
                await self.materialization_service_task
            except asyncio.CancelledError:
                pass
            finally:
                self.materialization_service_task = None
