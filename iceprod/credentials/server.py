"""
Credentials store and refresh.
"""
import asyncio
from datetime import datetime
import logging
import time

import pymongo
import pymongo.errors
import motor.motor_asyncio
import requests.exceptions
from rest_tools.client import RestClient, ClientCredentialsAuth
from rest_tools.server import RestServer, ArgumentHandler, ArgumentSource
from tornado.web import HTTPError
from tornado.web import RequestHandler as TornadoRequestHandler
from wipac_dev_tools import from_environment

from iceprod.rest.auth import authorization
from iceprod.rest.base_handler import IceProdRestConfig, APIBase
from iceprod.server.module import FakeStatsClient, StatsClientIgnoreErrors
from iceprod.server.util import nowstr, datetime2str
from .service import RefreshService, get_expiration, is_expired

logger = logging.getLogger('server')


class BaseCredentialsHandler(APIBase):
    def initialize(self, refresh_service=None, rest_client=None, **kwargs):
        super().initialize(**kwargs)
        self.refresh_service = refresh_service
        self.rest_client = rest_client

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

    async def create(self, db, base_data):
        now = time.time()
        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument('url', type=str, required=True)
        argo.add_argument('type', type=str, choices=['s3', 'oauth'], required=True)
        argo.add_argument('buckets', type=list, default=[], required=False)
        argo.add_argument('access_key', type=str, default='', required=False)
        argo.add_argument('secret_key', type=str, default='', required=False)
        argo.add_argument('access_token', type=str, default='', required=False)
        argo.add_argument('refresh_token', type=str, default='', required=False)
        argo.add_argument('expire_date', type=float, default=now, required=False)
        argo.add_argument('last_use', type=float, default=now, required=False)
        args = vars(argo.parse_args())
        url = args['url']
        credential_type = args['type']

        base_data['url'] = url
        data = base_data.copy()
        data.update({
            'type': credential_type,
        })

        if credential_type == 's3':
            if not args['buckets']:
                raise HTTPError(400, reason='must specify bucket(s)')
            if not args['access_key']:
                raise HTTPError(400, reason='must specify access_key')
            if not args['secret_key']:
                raise HTTPError(400, reason='must specify secret_key')

            data['buckets'] = args['buckets']
            data['access_key'] = args['access_key']
            data['secret_key'] = args['secret_key']

        elif credential_type == 'oauth':
            if (not args['access_token']) and not args['refresh_token']:
                raise HTTPError(400, reason='must specify either access or refresh tokens')
            data['access_token'] = args['access_token']
            data['refresh_token'] = args['refresh_token']
            data['expiration'] = args['expire_date']
            data['last_use'] = args['last_use']

            if 'refresh_token' in data and not data.get('access_token', ''):
                new_cred = await self.refresh_service.refresh_cred(data)
                data.update(new_cred)
            if (not data.get('access_token', '')) and data.get('expiration') == now:
                data['expiration'] = get_expiration(data['access_token'])

        else:
            raise HTTPError(400, 'bad credential type')

        await db.update_one(
            base_data,
            {'$set': data},
            upsert=True,
        )

    async def patch_cred(self, db, base_data):
        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument('url', type=str, required=True)
        argo.add_argument('buckets', type=list, default=[], required=False)
        argo.add_argument('access_key', type=str, default='', required=False)
        argo.add_argument('secret_key', type=str, default='', required=False)
        argo.add_argument('access_token', type=str, default='', required=False)
        argo.add_argument('refresh_token', type=str, default='', required=False)
        argo.add_argument('expiration', type=float, default=0, required=False)
        argo.add_argument('last_use', type=float, default=0, required=False)
        args = vars(argo.parse_args())
        base_data['url'] = args['url']

        data = {}
        for key in ('buckets', 'access_key', 'secret_key', 'access_token', 'refresh_token', 'expiration', 'last_use'):
            if val := args[key]:
                data[key] = val

        if 'refresh_token' in data and 'access_token' not in data:
            new_cred = await self.refresh_service.refresh_cred(data)
            data.update(new_cred)
        if 'access_token' in data and 'expiration' not in data:
            data['expiration'] = get_expiration(data['access_token'])

        ret = await db.find_one_and_update(
            base_data,
            {'$set': data},
        )
        if not ret:
            raise HTTPError(404, 'credential not found')

    async def search_creds(self, db, base_data):
        if url := self.get_argument('url', None):
            base_data['url'] = url

        refresh = self.get_argument('norefresh', None) is None

        if refresh:
            update_data = {'last_use': time.time()}
            filters = base_data.copy()
            filters['type'] = 'oauth'
            await db.update_many(filters, {'$set': update_data})

        ret = {}
        async for row in db.find(base_data, projection={'_id': False}):
            ret[row['url']] = row

        for key in list(ret):
            cred = ret[key]
            if refresh and is_expired(cred) and cred['refresh_token']:
                try:
                    new_cred = await self.refresh_service.refresh_cred(cred)
                    filters = base_data.copy()
                    filters['url'] = key
                    ret[key] = await db.find_one_and_update(filters, {'$set': new_cred}, projection={'_id': False})
                except Exception:
                    del ret[key]

        return ret


class GroupCredentialsHandler(BaseCredentialsHandler):
    """
    Handle group credentials requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self, groupname):
        """
        Get a group's credentials.

        Args:
            groupname (str): group name
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise HTTPError(403, 'unauthorized')

        ret = await self.search_creds(self.db.group_creds, {'groupname': groupname})
        self.write(ret)

    @authorization(roles=['admin', 'system', 'user'])
    async def post(self, groupname):
        """
        Set a group credential.  Overwrites an existing credential for the specified url.

        Common body args:
            url (str): url of controlled resource
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            expire_date (str): access token expiration, ISO date time in UTC (optional)

        Args:
            groupname (str): group name
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise HTTPError(403, 'unauthorized')

        await self.create(self.db.group_creds, {'groupname': groupname})
        self.write({})

    @authorization(roles=['admin', 'system', 'user'])
    async def patch(self, groupname):
        """
        Update a group credential.  Usually used to update a specifc field.

        Required body args:
            url (str): url of controlled resource

        Other body args will update a credential.

        Args:
            groupname (str): group name
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise HTTPError(403, 'unauthorized')

        await self.patch_cred(self.db.group_creds, {'groupname': groupname})
        self.write({})

    @authorization(roles=['admin', 'system', 'user'])
    async def delete(self, groupname):
        """
        Delete a group's credentials.

        Args:
            groupname (str): groupname
        Body args:
            url (str): (optional) url of controlled resource
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise HTTPError(403, 'unauthorized')

        args = {'groupname': groupname}

        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument('url', type=str, default='', required=False)
        body_args = argo.parse_args()
        if body_args.url:
            args['url'] = body_args.url

        await self.db.group_creds.delete_many(args)
        self.write({})


class UserCredentialsHandler(BaseCredentialsHandler):
    """
    Handle user credentials requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self, username):
        """
        Get a user's credentials.

        Args:
            username (str): username
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise HTTPError(403, 'unauthorized')

        ret = await self.search_creds(self.db.user_creds, {'username': username})
        self.write(ret)

    @authorization(roles=['admin', 'system', 'user'])
    async def post(self, username):
        """
        Set a user credential.  Overwrites an existing credential for the specified url.

        Common body args:
            url (str): url of controlled resource
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            expire_date (str): access token expiration, ISO date time in UTC (optional)

        Args:
            username (str): username
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise HTTPError(403, 'unauthorized')

        await self.create(self.db.user_creds, {'username': username})
        self.write({})

    @authorization(roles=['admin', 'system', 'user'])
    async def patch(self, username):
        """
        Update a user credential.  Usually used to update a specifc field.

        Required body args:
            url (str): url of controlled resource

        Other body args will update a credential.

        Args:
            username (str): username
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise HTTPError(403, 'unauthorized')

        await self.patch_cred(self.db.user_creds, {'username': username})
        self.write({})

    @authorization(roles=['admin', 'system', 'user'])
    async def delete(self, username):
        """
        Delete a user's credentials.

        Args:
            username (str): username
        Body args:
            url (str): (optional) url of controlled resource
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise HTTPError(403, 'unauthorized')

        args = {'username': username}

        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument('url', type=str, default='', required=False)
        body_args = argo.parse_args()
        if body_args.url:
            args['url'] = body_args.url

        await self.db.user_creds.delete_many(args)
        self.write({})


class HealthHandler(BaseCredentialsHandler):
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
            'start_time': datetime2str(datetime.utcfromtimestamp(self.refresh_service.start_time)),
            'last_run_time': "",
            'last_success_time': "",
        }
        try:
            if self.refresh_service.last_run_time is None and self.refresh_service.start_time + 3600 < now:
                self.send_error(500, reason='refresh was never run')
                return
            if self.refresh_service.last_run_time is not None:
                if self.refresh_service.last_run_time + 3600 < now:
                    self.send_error(500, reason='refresh has stopped running')
                    return
                status['last_run_time'] = datetime2str(datetime.utcfromtimestamp(self.refresh_service.last_run_time))
            if self.refresh_service.last_success_time is None and self.refresh_service.start_time + 86400 < now:
                self.send_error(500, reason='refresh was never successful')
                return
            if self.refresh_service.last_success_time is not None:
                if self.refresh_service.last_success_time + 86400 < now:
                    self.send_error(500, reason='refresh has stopped being successful')
                    return
                status['last_success_time'] = datetime2str(datetime.utcfromtimestamp(self.refresh_service.last_success_time))
        except Exception:
            logger.info('error from refresh service', exc_info=True)
            self.send_error(500, reason='error from refresh service')
            return

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
            'TOKEN_CLIENTS': '{}',
            'TOKEN_REFRESH_WINDOW': 72.0,
            'TOKEN_EXPIRE_BUFFER': 24.0,
            'TOKEN_SERVICE_CHECK_INTERVAL': 180,
            'DB_URL': 'mongodb://localhost/creds',
            'STATSD_ADDRESS': '',
            'STATSD_PREFIX': 'credentials',
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
            'group_creds': {
                'group_index': {'keys': [('groupname', pymongo.DESCENDING), ('url', pymongo.DESCENDING)], 'unique': True},
            },
            'user_creds': {
                'username_index': {'keys': [('username', pymongo.DESCENDING), ('url', pymongo.DESCENDING)], 'unique': True},
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

        self.refresh_service = RefreshService(
            database=self.db,
            clients=config['TOKEN_CLIENTS'],
            refresh_window=config['TOKEN_REFRESH_WINDOW'],
            expire_buffer=config['TOKEN_EXPIRE_BUFFER'],
            service_run_interval=config['TOKEN_SERVICE_CHECK_INTERVAL'],
        )
        self.refresh_service_task = None

        kwargs = IceProdRestConfig(rest_config, statsd=statsd, database=self.db)
        kwargs['refresh_service'] = self.refresh_service
        kwargs['rest_client'] = rest_client

        server = RestServer(debug=config['DEBUG'])

        server.add_route(r'/groups/(?P<groupname>\w+)/credentials', GroupCredentialsHandler, kwargs)
        server.add_route(r'/users/(?P<username>\w+)/credentials', UserCredentialsHandler, kwargs)
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

        if not self.refresh_service_task:
            self.refresh_service_task = asyncio.create_task(self.refresh_service.run())

    async def stop(self):
        await self.server.stop()
        if self.refresh_service_task:
            self.refresh_service_task.cancel()
            try:
                await self.refresh_service_task
            except asyncio.CancelledError:
                pass  # ignore cancellations
            finally:
                self.refresh_service_task = None
