"""
Main website
------------

This is the external website users will see when interacting with IceProd.
It has been broken down into several sub-handlers for easier maintenance.
"""

import importlib.resources
import logging
import os
import random
import re

from prometheus_client import Info, start_http_server
import tornado.web
from rest_tools.client import RestClient, ClientCredentialsAuth
from rest_tools.server import catch_error, RestServer, RestHandlerSetup, OpenIDLoginHandler
from rest_tools.server.session import Session

from iceprod.util import VERSION_STRING
from iceprod.common.prom_utils import AsyncMonitor, PromRequestMixin
from iceprod.core.config import ConfigSchema as DATASET_SCHEMA
from iceprod.server.config import CONFIG_SCHEMA as SERVER_SCHEMA
from iceprod.server import documentation
from iceprod.server.util import nowstr

from .config import get_config
from .handlers.base import authenticated, LoginMixin, PublicHandler
from .handlers.submit import Config, ConfigStatus, Submit, SubmitStatus
from .handlers.dataset import Dataset, DatasetBrowse
from .handlers.job import Job, JobBrowse
from .handlers.task import Task, TaskBrowse

logger = logging.getLogger('website')


class Default(PublicHandler):
    """Handle / urls"""
    @catch_error
    async def get(self):
        # try to get the user, if available
        await self.get_current_user_async()
        self.render('main.html')


class Schemas(PublicHandler):
    """Handle /schemas/v3/(.*) urls"""
    @catch_error
    async def get(self, schema):
        if schema == 'dataset.schema.json':
            self.write(DATASET_SCHEMA.schema())
        elif schema == 'config.schema.json':
            self.write(SERVER_SCHEMA)
        elif ver := re.match(r'dataset_v(\d\.\d).schema.json', schema):
            ver = float(ver.group(1))
            self.write(DATASET_SCHEMA.schema(ver))
        else:
            raise tornado.web.HTTPError(404, reason='unknown schema')


class Documentation(PublicHandler):
    @catch_error
    async def get(self, url):
        # try to get the user, if available
        await self.get_current_user_async()
        doc_path = str(importlib.resources.files('iceprod.server')/'data'/'docs')
        full_path = os.path.join(doc_path, url)
        if not full_path.startswith(doc_path):
            self.set_status(404)
            self.render('404.html', path='bad docs path')
            return
        self.write(documentation.load_doc(full_path))
        self.flush()


class Log(PublicHandler):
    @authenticated
    async def get(self, dataset_id, log_id):
        assert self.rest_client
        ret = await self.rest_client.request('GET','/datasets/{}/logs/{}'.format(dataset_id, log_id))
        log_text = ret['data']
        html = '<html><head><title>' + ret['name'] + '</title></head><body>'
        html += log_text.replace('\n', '<br/>')
        html += '</body></html>'
        self.write(html)
        self.flush()


class Help(PublicHandler):
    """Help Page"""
    @catch_error
    async def get(self):
        # try to get the user, if available
        await self.get_current_user_async()
        self.render('help.html')


class Other(PublicHandler):
    """Handle any other urls - this is basically all 404"""
    @catch_error
    async def get(self):
        # try to get the user, if available
        await self.get_current_user_async()
        path = self.request.path
        self.set_status(404)
        self.render('404.html', path=path)


class Profile(PublicHandler):
    """Handle user profile page"""
    @authenticated
    async def get(self):
        username = self.current_user
        groups = self.auth_groups
        group_creds = {}
        for g in groups:
            if g != 'users':
                group_creds[g] = await self.get_cred_group_tokens(group_name=g)
        user_creds = await self.get_cred_user_tokens(username=username)
        self.render('profile.html', username=username, groups=groups,
                    group_creds=group_creds, user_creds=user_creds)


class Login(LoginMixin, PromRequestMixin, OpenIDLoginHandler):  # type: ignore
    pass


class Logout(PublicHandler):
    @catch_error
    async def get(self):
        self.clear_tokens()
        self.current_user = None
        self.request.uri = '/'  # for login redirect, fake the main page
        self.render('logout.html', status=None)


class HealthHandler(PublicHandler):
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
        }

        try:
            await self.system_rest_client.request('GET', '/dataset_summaries/status')
            status['rest_api'] = 'OK'
        except Exception:
            logger.info('error from REST API', exc_info=True)
            self.send_error(500, reason='error from REST API')
            return

        try:
            await self.cred_rest_client.request('GET', '/healthz')
            status['cred_service'] = 'OK'
        except Exception:
            logger.info('error from REST API', exc_info=True)
            self.send_error(500, reason='error from credential service')
            return

        self.write(status)


class Server:
    def __init__(self):
        config = get_config()

        # get package data
        static_path = str(importlib.resources.files('iceprod.website')/'data'/'www')
        if static_path is None or not os.path.exists(static_path):
            logger.info('static path: %r',static_path)
            raise Exception('bad static path')
        template_path = str(importlib.resources.files('iceprod.website')/'data'/'www_templates')
        if template_path is None or not os.path.exists(template_path):
            logger.info('template path: %r',template_path)
            raise Exception('bad template path')

        # set IceProd REST API
        if config.ICEPROD_API_ADDRESS:
            rest_address = config.ICEPROD_API_ADDRESS
        else:
            raise RuntimeError('ICEPROD_API_ADDRESS not specified')

        rest_config = {
            'debug': config.DEBUG,
            'server_header': 'IceProd/' + VERSION_STRING,
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

        handler_args = RestHandlerSetup(rest_config)
        if config.CI_TESTING:
            self.session = Session()
        else:
            kwargs = {}
            if config.REDIS_USER:
                kwargs['username'] = config.REDIS_USER
            if config.REDIS_PASSWORD:
                kwargs['password'] = config.REDIS_PASSWORD
            if config.REDIS_TLS:
                kwargs['tls'] = True
            self.session = Session(storage_type='redis', host=config.REDIS_HOST, **kwargs)  # type: ignore
        handler_args['session'] = self.session

        full_url = config.ICEPROD_WEB_URL
        login_url = full_url+'/login'

        login_handler_args = handler_args.copy()
        if config.ICEPROD_API_CLIENT_ID and config.ICEPROD_API_CLIENT_SECRET:
            logging.info('enabling system rest client and website logins"')
            rest_client = ClientCredentialsAuth(
                address=config.ICEPROD_API_ADDRESS,
                token_url=config.OPENID_URL,
                client_id=config.ICEPROD_API_CLIENT_ID,
                client_secret=config.ICEPROD_API_CLIENT_SECRET,
            )
            login_handler_args['oauth_client_id'] = config.ICEPROD_API_CLIENT_ID
            login_handler_args['oauth_client_secret'] = config.ICEPROD_API_CLIENT_SECRET
            login_handler_args['oauth_client_scope'] = 'offline_access posix profile'
        elif config.CI_TESTING:
            logger.info('CI_TESTING: no login for testing')
            rest_client = RestClient(config.ICEPROD_API_ADDRESS, timeout=1, retries=0)
        else:
            raise RuntimeError('ICEPROD_API_CLIENT_ID or ICEPROD_API_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

        handler_args.update({
            'rest_api': rest_address,
            'cred_rest_client': cred_client,
            'system_rest_client': rest_client,
            'auth_url': config.OPENID_URL,
            'auth_client_id': config.ICEPROD_API_CLIENT_ID,
            'auth_client_secret': config.ICEPROD_API_CLIENT_SECRET,
        })
        if config.COOKIE_SECRET:
            cookie_secret = config.COOKIE_SECRET
            log_cookie_secret = cookie_secret[:4] + 'X'*(len(cookie_secret)-8) + cookie_secret[-4:]
            logger.info('using supplied cookie secret %r', log_cookie_secret)
        else:
            cookie_secret = ''.join(hex(random.randint(0,15))[-1] for _ in range(64))

        server = RestServer(
            debug=config.DEBUG,
            cookie_secret=cookie_secret,
            login_url=login_url,
            template_path=template_path,
            static_path=static_path,
        )

        server.add_route("/", Default, handler_args)
        server.add_route('/config', Config, handler_args)
        server.add_route(r'/config/status/(\w+)', ConfigStatus, handler_args)
        server.add_route(r"/schemas/v3/([\w\.]+)", Schemas, handler_args)
        server.add_route('/dataset', DatasetBrowse, handler_args)
        server.add_route(r"/dataset/(\w+)", Dataset, handler_args)
        server.add_route(r"/dataset/(\w+)/task", TaskBrowse, handler_args)
        server.add_route(r"/dataset/(\w+)/task/(\w+)", Task, handler_args)
        server.add_route(r"/dataset/(\w+)/job", JobBrowse, handler_args)
        server.add_route(r"/dataset/(\w+)/job/(\w+)", Job, handler_args)
        server.add_route('/help', Help, handler_args)
        server.add_route(r"/docs/(.*)", Documentation, handler_args)
        server.add_route(r"/dataset/(\w+)/log/(\w+)", Log, handler_args)
        server.add_route('/submit', Submit, handler_args)
        server.add_route(r'/submit/status/(\w+)', SubmitStatus, handler_args)
        server.add_route('/profile', Profile, handler_args)
        server.add_route('/login', Login, login_handler_args)
        server.add_route('/logout', Logout, handler_args)

        server.add_route('/healthz', HealthHandler, handler_args)
        server.add_route(r"/.*", Other, handler_args)

        server.startup(address=config.HOST, port=config.PORT)

        self.server = server

    async def start(self):
        if self.prometheus_port:
            logging.info("starting prometheus on %r", self.prometheus_port)
            start_http_server(self.prometheus_port)
            i = Info('iceprod', 'IceProd information')
            i.info({
                'version': VERSION_STRING,
                'type': 'website',
            })
            self.async_monitor = AsyncMonitor(labels={'type': 'website'})
            await self.async_monitor.start()

    async def stop(self):
        await self.server.stop()
        if self.async_monitor:
            await self.async_monitor.stop()
        self.session.close()
