"""
Server for iceprod background services.
"""
import json
import logging
from pathlib import Path
import pkgutil
from typing import Any, Self

from prometheus_client import Info, start_http_server
from rest_tools.client import RestClient, ClientCredentialsAuth
from rest_tools.server import RestServer
from tornado.web import HTTPError
from tornado.web import RequestHandler as TornadoRequestHandler

from .config import get_config
from .base import AuthData, BaseAction, BaseHandler
from iceprod.common.mongo_queue import AsyncMongoQueue
from iceprod.util import VERSION_STRING
from iceprod.common.prom_utils import AsyncMonitor
from iceprod.rest.auth import authorization
from iceprod.rest.base_handler import IceProdRestConfig
from iceprod.server.util import nowstr

logger = logging.getLogger('server')


class RequestHandler(BaseHandler):
    """
    Handle basic requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self):
        """
        Get a count of request statuses.

        Returns:
            dict: {status: count}
        """
        assert self.action
        type_ = self.action.action_type
        counts = {}
        for status in ('queued', 'processing', 'error', 'complete'):
            ret = await self.message_queue.count({'payload.type': type_, 'status': status})
            counts[status] = ret
        self.write(counts)

    @authorization(roles=['admin', 'system', 'user'])
    async def post(self):
        """
        Create basic request.

        On success, returns http code 201.

        Returns:
            dict: {result: id}
        """
        assert self.action

        args = json.loads(self.request.body) if self.request.body else {}

        auth = AuthData(
            username=self.current_user,
            groups=self.auth_groups,
            roles=self.auth_roles,
            token=self.auth_data,
        )

        id_ = await self.action.create(args, auth_data=auth)

        # return success
        self.set_status(201)
        self.write({'result': id_})


class StatusHandler(BaseHandler):
    """
    Handle status requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self, id_):
        """
        Get the status of a request.

        If id is invalid, returns http code 404.

        Args:
            id_: request id

        Returns:
            dict: metadata
        """
        ret = await self.message_queue.get_status(id_)
        if not ret:
            self.send_error(404, reason="Request not found")
        else:
            payload = await self.message_queue.get_payload(id_)
            if ret == 'error':
                error_message = await self.message_queue.get_error(id_)
            else:
                error_message = ''
            self.write({
                'id': id_,
                'status': ret,
                'error_message': error_message,
                'payload': payload,
            })


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
            ret = await self.message_queue.count()
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
            collection_name='services_queue',
            extra_indexes={'type_index': {'keys': 'type', 'unique': False}},
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

        cred_client: RestClient
        if config.ICEPROD_CRED_CLIENT_ID and config.ICEPROD_CRED_CLIENT_SECRET:
            logging.info(f'enabling auth via {config.OPENID_URL} for aud "{config.OPENID_AUDIENCE}"')
            cred_client = ClientCredentialsAuth(
                address=config.ICEPROD_CRED_ADDRESS,
                token_url=config.OPENID_URL,
                client_id=config.ICEPROD_CRED_CLIENT_ID,
                client_secret=config.ICEPROD_CRED_CLIENT_SECRET,
            )
        elif config.CI_TESTING:
            cred_client = RestClient(config.ICEPROD_CRED_ADDRESS, timeout=1, retries=0)
        else:
            raise RuntimeError('ICEPROD_CRED_CLIENT_ID or ICEPROD_CRED_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

        handler_args = IceProdRestConfig(rest_config)
        handler_args['message_queue'] = self.message_queue
        handler_args['rest_client'] = rest_client

        server = RestServer(debug=config.DEBUG)

        # find all actions
        plugin_path = str(Path(__file__).parent / 'actions')
        logger.info('action path %s', plugin_path)
        for _, name, _ in pkgutil.iter_modules([plugin_path]):
            logger.info('configuring action %s', name)
            action_class = pkgutil.resolve_name(f'iceprod.services.actions.{name}:Action')
            action : BaseAction = action_class(queue=self.message_queue, logger=logger, api_client=rest_client, cred_client=cred_client)

            handler_args2 = handler_args.copy()
            handler_args2['action'] = action
            server.add_route(f'/actions/{name}', RequestHandler, handler_args2)
            server.add_route(rf'/actions/{name}/(?P<id_>\w+)', StatusHandler, handler_args2)

            for route, handler, args in action.extra_handlers():
                assert route != '/' and route.startswith('/')
                handler_args3 = handler_args2.copy()
                handler_args3.update(args)
                server.add_route(f'/actions/{name}' + route, handler, handler_args3)

        server.add_route('/healthz', HealthHandler, handler_args)
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

        await self.message_queue.setup()

    async def stop(self):
        await self.server.stop()
        if self.async_monitor:
            await self.async_monitor.stop()
        await self.message_queue.close()
