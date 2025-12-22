"""
Materialization service for dataset late materialization.
"""
from dataclasses import asdict
import json
import logging
from typing import Any, Self
import uuid

from prometheus_client import Info, start_http_server
import pymongo
import requests.exceptions
from rest_tools.client import RestClient, ClientCredentialsAuth
from rest_tools.server import RestServer
from tornado.web import HTTPError
from tornado.web import RequestHandler as TornadoRequestHandler

from .config import get_config
from iceprod.common.mongo_queue import AsyncMongoQueue
from iceprod.util import VERSION_STRING
from iceprod.common.prom_utils import AsyncMonitor
from iceprod.rest.auth import authorization, attr_auth
from iceprod.rest.base_handler import IceProdRestConfig, APIBase
from iceprod.server.util import nowstr

logger = logging.getLogger('server')


class BaseHandler(APIBase):
    def initialize(self, *args, materialization_queue: AsyncMongoQueue, rest_client: RestClient, **kwargs):
        super().initialize(*args, **kwargs)
        self.materialization_queue = materialization_queue
        self.rest_client = rest_client

    async def new_request(self, args):
        """
        Validates a new materialization request.
        
        Deduplicates with existing requests with the same args that are in 'queued' status.

        Arguments:
            args: dict of args for request

        Returns:
            dict: data fields
        """
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
            'creator': self.auth_data.get('username', None),
            'role': self.auth_data.get('role', None),
        }
        for k in fields:
            if k in args:
                data[k] = args[k]

        # check if exists
        ret = await self.materialization_queue.lookup_by_payload(data)
        if ret and ret.status == 'queued':
            data['materialization_id'] = ret.uuid
        else:
            # insert
            ret = await self.materialization_queue.push(data)
            data['materialization_id'] = ret

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
        assert self.rest_client
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
        ret = await self.materialization_queue.get_status(materialization_id)
        if not ret:
            self.send_error(404, reason="Materialization request not found")
        else:
            self.write({
                'materialization_id': materialization_id,
                'status': ret,
            })


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
        ret = await self.materialization_queue.lookup_by_payload(
            {'dataset_id': dataset_id},
            sort=[('modify_timestamp', pymongo.DESCENDING)],
        )
        if not ret:
            self.send_error(404, reason="Materialization request not found")
        else:
            data = ret.payload
            data['materialization_id'] = ret.uuid
            data['status'] = ret.status
            data['create_date'] = ret.created_at.isoformat()
            self.write(data)


class HealthHandler(BaseHandler):
    """
    Handle health requests.
    """
    async def get(self):
        """
        Get health status.

        Returns based on exit code, 200 = ok, 400 = failure
        """
        status = {
            'now': nowstr(),
            'num_requests': -1,
        }

        try:
            ret = await self.materialization_queue.count()
        except Exception:
            logger.info('bad db request', exc_info=True)
            self.send_error(500, reason='bad db request')
        else:
            if ret is None:
                self.send_error(500, reason='bad db result')
            else:
                status['num_requests'] = ret
                self.write(status)


class Error(TornadoRequestHandler):
    def prepare(self):
        raise HTTPError(404, 'invalid api route')


class Server:
    def __init__(self: Self):
        config = get_config()

        rest_config: dict[str, Any] = {
            'debug': config.DEBUG,
        }
        if config.OPENID_URL:
            logging.info(f'enabling auth via {config.OPENID_URL} for aud "{config.OPENID_AUDIENCE}"')
            rest_config['auth'] = {
                'openid_url': config.OPENID_URL,
                'audience': config.OPENID_AUDIENCE,
            }
        elif config.CI_TESTING:
            rest_config['auth'] = {
                'secret': 'secret',
            }
        else:
            raise RuntimeError('OPENID_URL not specified, and CI_TESTING not enabled!')

        # enable monitoring
        self.prometheus_port = config.PROMETHEUS_PORT if config.PROMETHEUS_PORT > 0 else None
        self.async_monitor = None

        self.message_queue = AsyncMongoQueue(
            url=config.DB_URL,
            collection_name='materialization_queue',
            extra_indexes={'dataset_id_index': {'keys': 'dataset_id', 'unique': False}},
            timeout=config.DB_TIMEOUT,
            write_concern=config.DB_WRITE_CONCERN
        )

        rest_client: RestClient
        if config.ICEPROD_API_CLIENT_ID and config.ICEPROD_API_CLIENT_SECRET:
            logging.info(f'enabling auth via {config.OPENID_URL} for aud "{config.OPENID_AUDIENCE}"')
            rest_client = ClientCredentialsAuth(
                address=config.ICEPROD_API_ADDRESS,
                token_url=config.OPENID_URL,
                client_id=config.ICEPROD_API_CLIENT_ID,
                client_secret=config.ICEPROD_API_CLIENT_SECRET,
            )
        elif config.CI_TESTING:
            rest_client = RestClient(config.ICEPROD_API_ADDRESS, timeout=1, retries=0)
        else:
            raise RuntimeError('ICEPROD_API_CLIENT_ID or ICEPROD_API_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

        kwargs = IceProdRestConfig(rest_config)
        kwargs['materialization_queue'] = self.message_queue
        kwargs['rest_client'] = rest_client

        server = RestServer(debug=config.DEBUG)

        server.add_route(r'/status/(?P<materialization_id>\w+)', StatusHandler, kwargs)
        server.add_route('/', RequestHandler, kwargs)
        server.add_route(r'/request/(?P<dataset_id>\w+)', RequestDatasetHandler, kwargs)
        server.add_route(r'/request/(?P<dataset_id>\w+)/status', DatasetStatusHandler, kwargs)
        server.add_route('/healthz', HealthHandler, kwargs)
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
                'type': 'materialization',
            })
            self.async_monitor = AsyncMonitor(labels={'type': 'materialization'})
            await self.async_monitor.start()

        await self.message_queue.setup()

        # do migration
        # TODO: remove this code after one version release
        collection = self.message_queue.client.db['materialization']
        payload_fields = ['dataset_id', 'set_status', 'num', 'creator', 'role']
        async for row in collection.find({}):
            payload = {k: row[k] for k in payload_fields}
            await self.message_queue.push(payload=payload, priority=0)
        await collection.delete_many({})

    async def stop(self):
        await self.server.stop()
        if self.async_monitor:
            await self.async_monitor.stop()
        await self.message_queue.close()
