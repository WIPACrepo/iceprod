"""
Credentials store and refresh.
"""
import asyncio
import dataclasses
from datetime import datetime, UTC
import logging
import time
from typing import Any, Self

import jwt
from prometheus_client import Info, start_http_server
import pymongo
import pymongo.errors
import requests.exceptions
from rest_tools.client import RestClient, ClientCredentialsAuth
from rest_tools.server import RestServer, ArgumentHandler, ArgumentSource
from tornado.web import HTTPError
from tornado.web import RequestHandler as TornadoRequestHandler
from wipac_dev_tools import from_environment_as_dataclass

from iceprod.util import VERSION_STRING
from iceprod.common.mongo import Mongo
from iceprod.common.prom_utils import AsyncMonitor
from iceprod.rest.auth import authorization
from iceprod.rest.base_handler import IceProdRestConfig, APIBase
from iceprod.server.util import nowstr, datetime2str
from .service import ExchangeException, RefreshService
from .util import get_expiration, is_expired

logger = logging.getLogger('server')


class BaseCredentialsHandler(APIBase):
    def initialize(self, *args, refresh_service: RefreshService, rest_client: RestClient, **kwargs):  # type: ignore[override]
        super().initialize(*args, **kwargs)
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

    async def create(self, db, base_data):
        assert self.refresh_service
        now = time.time()
        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument('url', type=str, required=True)
        argo.add_argument('type', type=str, choices=['s3', 'oauth'], required=True)
        argo.add_argument('transfer_prefix', type=str, default='', required=False)
        argo.add_argument('buckets', type=list, default=[], required=False)
        argo.add_argument('access_key', type=str, default='', required=False)
        argo.add_argument('secret_key', type=str, default='', required=False)
        argo.add_argument('access_token', type=str, default='', required=False)
        argo.add_argument('refresh_token', type=str, default='', required=False)
        argo.add_argument('scope', type=str, default=None, required=False)
        argo.add_argument('expiration', type=float, default=now, required=False)
        argo.add_argument('last_use', type=float, default=now, required=False)
        args = vars(argo.parse_args())
        url = args['url']
        credential_type = args['type']

        base_data['url'] = url
        base_data['transfer_prefix'] = args['transfer_prefix']
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
            if args['scope']:
                base_data['scope'] = args['scope']
            data['scope'] = args['scope']
            data['expiration'] = args['expiration']
            if data['access_token'] and data.get('expiration') == now:
                data['expiration'] = get_expiration(data['access_token'])
            data['last_use'] = args['last_use']
            if data['access_token'] and data['scope'] is None:
                data['scope'] = jwt.decode(data['access_token'], options={"verify_signature": False}).get('scope', '')

            if 'refresh_token' in data and not data['access_token']:
                new_cred = await self.refresh_service.refresh_cred(data)
                data.update(new_cred)

        else:
            raise HTTPError(400, reason='bad credential type')

        try:
            await db.update_one(
                base_data,
                {'$set': data},
                upsert=True,
            )
        except pymongo.errors.DuplicateKeyError:
            raise HTTPError(409, reason='credential already exists')

    async def patch_cred(self, db, base_data):
        assert self.refresh_service
        argo = ArgumentHandler(ArgumentSource.JSON_BODY_ARGUMENTS, self)
        argo.add_argument('url', type=str, required=True)
        argo.add_argument('type', type=str, choices=['s3', 'oauth'], required=True)
        argo.add_argument('transfer_prefix', type=str, default='', required=False)
        argo.add_argument('buckets', type=list, default=[], required=False)
        argo.add_argument('access_key', type=str, default='', required=False)
        argo.add_argument('secret_key', type=str, default='', required=False)
        argo.add_argument('access_token', type=str, default='', required=False)
        argo.add_argument('refresh_token', type=str, default='', required=False)
        argo.add_argument('scope', type=str, default='', required=False)
        argo.add_argument('expiration', type=float, default=0, required=False)
        argo.add_argument('last_use', type=float, default=0, required=False)
        args = vars(argo.parse_args())
        base_data['url'] = args['url']
        base_data['type'] = args['type']
        if args['transfer_prefix']:
            base_data['transfer_prefix'] = args['transfer_prefix']

        data = {}
        for key in ('buckets', 'access_key', 'secret_key', 'access_token', 'refresh_token', 'scope', 'expiration', 'last_use'):
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
        assert self.refresh_service
        if url := self.get_argument('url', None):
            base_data['url'] = url
        if transfer_prefix := self.get_argument('transfer_prefix', None):
            base_data['transfer_prefix'] = transfer_prefix
        if scope := self.get_argument('scope', None):
            base_data['scope'] = scope
        logger.info('base_data: %r', base_data)

        refresh = self.get_argument('norefresh', None) is None

        if refresh:
            update_data = {'last_use': time.time()}
            filters = base_data.copy()
            filters['type'] = 'oauth'
            await db.update_many(filters, {'$set': update_data})

        ret = []
        async for row in db.find(base_data, projection={'_id': False}):
            if refresh and is_expired(row) and row['refresh_token']:
                try:
                    new_cred = await self.refresh_service.refresh_cred(row)
                    filters = base_data.copy()
                    filters['url'] = row['url']
                    cred = await db.find_one_and_update(filters, {'$set': new_cred}, projection={'_id': False})
                    ret.append(cred)
                except Exception:
                    logging.debug('ignore expired token %r', row)
            else:
                ret.append(row)

        return ret

    async def delete_cred(self, db, base_data):
        assert self.refresh_service
        if url := self.get_argument('url', None):
            base_data['url'] = url
        if transfer_prefix := self.get_argument('transfer_prefix', None):
            base_data['transfer_prefix'] = transfer_prefix
        if scope := self.get_argument('scope', None):
            base_data['scope'] = scope
        logger.info('base_data: %r', base_data)

        await db.delete_many(base_data)

    async def exchange_cred(self, db, base_data):
        assert self.refresh_service

        client_id = self.get_argument('client_id')

        # must be oauth for exchange
        base_data['type'] = 'oauth'

        new_scope = self.get_argument('new_scope', None)

        # get unexpired tokens
        creds = await self.search_creds(db, base_data)

        ret = []
        for row in creds:
            try:
                c = await self.refresh_service.exchange_cred(row, client_id=client_id, new_scope=new_scope)
            except ExchangeException:
                continue
            ret.append(c)
        logger.info('exchange_cred found %d creds and exchanged %d creds', len(creds), len(ret))
        return ret


class CreateHandler(BaseCredentialsHandler):
    """
    Handle requests for a new credential.  Just return it, don't store it.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def post(self):
        """
        Generate an oauth credential

        Args:
            username (str): the username for the credential
            url (str): url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): scope of access token
        """
        username = self.get_argument('username')
        scope = self.get_argument('scope')
        url = self.get_argument('url')
        transfer_prefix = self.get_argument('transfer_prefix')
        if self.auth_roles == ['user'] and username != self.current_user:
            raise HTTPError(403, 'unauthorized')

        try:
            new_cred = await self.refresh_service.create_cred(
                url=url,
                transfer_prefix=transfer_prefix,
                username=username,
                scope=scope
            )
        except Exception as e:
            raise HTTPError(400, reason=str(e))
        self.write(new_cred)


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
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
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
            transfer_prefix (str): transfer prefix for file transfer
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            scope (str): scope of access token
            expiration (float): access token expiration, in unix time (optional)

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

        Body args:
            url (str): url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token

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
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise HTTPError(403, 'unauthorized')

        await self.delete_cred(self.db.group_creds, {'groupname': groupname})
        self.write({})


class GroupExchangeHandler(BaseCredentialsHandler):
    """
    Handle group exchange requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self, groupname):
        """
        Get a group's exchange credentials.

        Args:
            groupname (str): group name
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
            client_id (str): client_id to exchange to
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise HTTPError(403, 'unauthorized')

        ret = await self.exchange_cred(self.db.group_creds, {'groupname': groupname})
        self.write(ret)


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
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
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
            transfer_prefix (str): transfer prefix for file transfer
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            scope (str): scope of access token
            expiration (float): access token expiration, in unix time (optional)

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

        Body args:
            url (str): url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token

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
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise HTTPError(403, 'unauthorized')

        await self.delete_cred(self.db.user_creds, {'username': username})
        self.write({})


class UserExchangeHandler(BaseCredentialsHandler):
    """
    Handle user exchange requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self, username):
        """
        Get a user's exchange credentials.

        Args:
            username (str): user name
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
            client_id (str): client_id to exchange to
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise HTTPError(403, 'unauthorized')

        ret = await self.exchange_cred(self.db.user_creds, {'username': username})
        self.write(ret)


class DatasetCredentialsHandler(BaseCredentialsHandler):
    """
    Handle dataset credentials requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, dataset_id):
        """
        Get a datasets's credentials.

        Args:
            dataset_id (str): dataset_id
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
        Returns:
            dict: url: credential dict
        """
        ret = await self.search_creds(self.db.dataset_creds, {'dataset_id': dataset_id})
        self.write(ret)

    @authorization(roles=['admin', 'system'])
    async def post(self, dataset_id):
        """
        Set a dataset credential.  Overwrites an existing credential for the specified url.

        Common body args:
            url (str): url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            scope (str): scope of access token
            expiration (float): access token expiration, in unix time (optional)

        Args:
            dataset_id (str): dataset_id
        """
        await self.create(self.db.dataset_creds, {'dataset_id': dataset_id})
        self.write({})

    @authorization(roles=['admin', 'system'])
    async def patch(self, dataset_id):
        """
        Update a dataset credential.  Usually used to update a specifc field.

        Body args:
            url (str): url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token

        Other body args will update a credential.

        Args:
            dataset_id (str): dataset_id
        """
        await self.patch_cred(self.db.dataset_creds, {'dataset_id': dataset_id})
        self.write({})

    @authorization(roles=['admin', 'system'])
    async def delete(self, dataset_id):
        """
        Delete a dataset's credentials.

        Args:
            dataset_id (str): dataset_id
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
        Returns:
            dict: url: credential dict
        """
        await self.delete_cred(self.db.dataset_creds, {'dataset_id': dataset_id})
        self.write({})


class DatasetExchangeHandler(BaseCredentialsHandler):
    """
    Handle dataset exchange requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, dataset_id):
        """
        Get a dataset's exchange credentials.

        Args:
            dataset_id (str): dataset_id
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
            client_id (str): client_id to exchange to
        Returns:
            dict: url: credential dict
        """
        ret = await self.exchange_cred(self.db.dataset_creds, {'dataset_id': dataset_id})
        self.write(ret)


class DatasetTaskCredentialsHandler(BaseCredentialsHandler):
    """
    Handle dataset/task credentials requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, dataset_id, task_name):
        """
        Get a datasets's credentials.

        Args:
            dataset_id (str): dataset_id
            task_name (str): task name
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
        Returns:
            dict: url: credential dict
        """
        ret = await self.search_creds(self.db.dataset_creds, {'dataset_id': dataset_id, 'task_name': task_name})
        self.write(ret)

    @authorization(roles=['admin', 'system'])
    async def post(self, dataset_id, task_name):
        """
        Set a dataset credential.  Overwrites an existing credential for the specified url.

        Common body args:
            url (str): url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            scope (str): scope of access token
            expiration (float): access token expiration, in unix time (optional)

        Args:
            dataset_id (str): dataset_id
            task_name (str): task name
        """
        await self.create(self.db.dataset_creds, {'dataset_id': dataset_id, 'task_name': task_name})
        self.write({})

    @authorization(roles=['admin', 'system'])
    async def patch(self, dataset_id, task_name):
        """
        Update a dataset credential.  Usually used to update a specifc field.

        Body args:
            url (str): url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token

        Other body args will update a credential.

        Args:
            dataset_id (str): dataset_id
            task_name (str): task name
        """
        await self.patch_cred(self.db.dataset_creds, {'dataset_id': dataset_id, 'task_name': task_name})
        self.write({})

    @authorization(roles=['admin', 'system'])
    async def delete(self, dataset_id, task_name):
        """
        Delete a dataset's credentials.

        Args:
            dataset_id (str): dataset_id
            task_name (str): task name
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
        Returns:
            dict: url: credential dict
        """
        await self.delete_cred(self.db.dataset_creds, {'dataset_id': dataset_id, 'task_name': task_name})
        self.write({})


class DatasetTaskExchangeHandler(BaseCredentialsHandler):
    """
    Handle dataset task exchange requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, dataset_id, task_name):
        """
        Get a dataset's exchange credentials.

        Args:
            dataset_id (str): dataset_id
            task_name (str): task name
        Param args:
            url (str): (optional) url of controlled resource
            transfer_prefix (str): transfer prefix for file transfer
            scope (str): (optional) scope of access token
            client_id (str): client_id to exchange to
        Returns:
            dict: url: credential dict
        """
        ret = await self.exchange_cred(self.db.dataset_creds, {'dataset_id': dataset_id, 'task_name': task_name})
        self.write(ret)


class HealthHandler(BaseCredentialsHandler):
    """
    Handle health requests.
    """
    async def get(self):
        """
        Get health status.

        Returns based on exit code, 200 = ok, 400 = failure
        """
        assert self.refresh_service
        now = time.time()
        status = {
            'now': nowstr(),
            'start_time': datetime2str(datetime.fromtimestamp(self.refresh_service.start_time, tz=UTC)),
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


@dataclasses.dataclass
class DefaultConfig:
    HOST: str = 'localhost'
    PORT: int = 8080
    DEBUG: bool = False
    OPENID_URL: str = ''
    OPENID_AUDIENCE: str = ''
    ICEPROD_API_ADDRESS: str = 'https://api.iceprod.icecube.aq'
    ICEPROD_API_CLIENT_ID: str = ''
    ICEPROD_API_CLIENT_SECRET: str = ''
    TOKEN_CLIENTS: str = '{}'
    TOKEN_REFRESH_WINDOW: float = 168.0  # hours
    TOKEN_EXPIRE_BUFFER: int = 15  # minutes
    TOKEN_SERVICE_CHECK_INTERVAL: int = 180
    DB_URL: str = 'mongodb://localhost/datasets'
    DB_TIMEOUT: int = 60
    DB_WRITE_CONCERN: int = 1
    PROMETHEUS_PORT: int = 0
    CI_TESTING: str = ''


class Server:
    def __init__(self: Self):
        config = from_environment_as_dataclass(DefaultConfig)

        rest_config: dict[str, Any] = {
            'debug': config.DEBUG,
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

        self.db_client = Mongo(
            url=config.DB_URL,
            timeout=config.DB_TIMEOUT,
            write_concern=config.DB_WRITE_CONCERN
        )
        self.indexes = {
            'group_creds': {
                'group_index2': {'keys': [('groupname', pymongo.DESCENDING), ('transfer_prefix', pymongo.DESCENDING)], 'unique': True},
            },
            'user_creds': {
                'username_index2': {'keys': [('username', pymongo.DESCENDING), ('transfer_prefix', pymongo.DESCENDING)], 'unique': True},
            },
            'dataset_creds': {
                'dataset_index2': {
                    'keys': [
                        ('dataset_id', pymongo.DESCENDING),
                        ('task_name', pymongo.DESCENDING),
                        ('transfer_prefix', pymongo.DESCENDING)
                    ],
                    'unique': True,
                },
            }
        }

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

        self.refresh_service = RefreshService(
            database=self.db_client.db,
            clients=config.TOKEN_CLIENTS,
            refresh_window=config.TOKEN_REFRESH_WINDOW,
            expire_buffer=config.TOKEN_EXPIRE_BUFFER,
            service_run_interval=config.TOKEN_SERVICE_CHECK_INTERVAL,
        )
        self.refresh_service_task = None

        kwargs = IceProdRestConfig(rest_config, database=self.db_client.db)
        kwargs['refresh_service'] = self.refresh_service
        kwargs['rest_client'] = rest_client

        server = RestServer(debug=config.DEBUG)

        server.add_route('/create', CreateHandler, kwargs)
        server.add_route(r'/groups/(?P<groupname>\w+)/credentials', GroupCredentialsHandler, kwargs)
        server.add_route(r'/groups/(?P<groupname>\w+)/exchange', GroupExchangeHandler, kwargs)
        server.add_route(r'/users/(?P<username>\w+)/credentials', UserCredentialsHandler, kwargs)
        server.add_route(r'/users/(?P<username>\w+)/exchange', UserExchangeHandler, kwargs)
        server.add_route(r'/datasets/(?P<dataset_id>\w+)/credentials', DatasetCredentialsHandler, kwargs)
        server.add_route(r'/datasets/(?P<dataset_id>\w+)/exchange', DatasetExchangeHandler, kwargs)
        server.add_route(r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_name>[^\/\?\#]+)/credentials', DatasetTaskCredentialsHandler, kwargs)
        server.add_route(r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_name>[^\/\?\#]+)/exchange', DatasetTaskExchangeHandler, kwargs)
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
                'type': 'credentials',
            })
            self.async_monitor = AsyncMonitor(labels={'type': 'credentials'})
            await self.async_monitor.start()

        await self.db_client.ping()
        await self.db_client.create_indexes(indexes=self.indexes)

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
        if self.async_monitor:
            await self.async_monitor.stop()
        await self.db_client.close()
