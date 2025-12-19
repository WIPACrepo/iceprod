import functools
import logging
import re
import time
import traceback
from typing import Any
from urllib.parse import urlencode

from iceprod.core.jsonUtil import json_encode

import tornado.web
import jwt
import requests.exceptions
from rest_tools.client import RestClient, OpenIDRestClient
from rest_tools.server import catch_error, RestHandler
from rest_tools.server.session import SessionMixin

from iceprod.credentials.util import get_expiration
from iceprod.util import VERSION_STRING
from iceprod.common.prom_utils import PromRequestMixin
from iceprod.roles_groups import GROUPS
import iceprod.server.states

logger = logging.getLogger('website_base')


def authenticated(method):
    """Decorate methods with this to require that the user be logged in.

    If the user is not logged in, they will be redirected to the configured
    `login url <RequestHandler.get_login_url>`.

    If you configure a login url with a query parameter, Tornado will
    assume you know what you're doing and use it as-is.  If not, it
    will add a `next` parameter so the login page knows where to send
    you once you're logged in.
    """
    @catch_error
    @functools.wraps(method)
    async def wrapper(self, *args, **kwargs):
        if not await self.get_current_user_async():
            if self.request.method in ("GET", "HEAD"):
                try:
                    url = self.get_login_url()
                    if "?" not in url:
                        next_url = self.request.uri
                        url += "?" + urlencode(dict(next=next_url))
                    self.redirect(url)
                    return None
                except Exception:
                    logger.warning('failed to make redirect', exc_info=True)
                    raise tornado.web.HTTPError(403, reason='auth failed')
            raise tornado.web.HTTPError(403, reason='auth failed')
        return await method(self, *args, **kwargs)
    return wrapper


def eval_expression(token, e):
    """from rest_tools.server.decorators.token_attribute_role_mapping_auth"""
    name, val = e.split('=',1)
    if name == 'scope':
        # special handling to split into string
        token_val = token.get('scope','').split()
    else:
        prefix = name.split('.')[:-1]
        while prefix:
            token = token.get(prefix[0], {})
            prefix = prefix[1:]
        token_val = token.get(name.split('.')[-1], None)

    logger.debug('token_val = %r', token_val)
    if token_val is None:
        return []

    prog = re.compile(val)
    if isinstance(token_val, list):
        ret = (prog.fullmatch(v) for v in token_val)
    else:
        ret = [prog.fullmatch(token_val)]
    return [r for r in ret if r]


class LoginMixin(SessionMixin, RestHandler):  # type: ignore[misc]
    """
    Store/load current user's `OpenIDLoginHandler` tokens in Redis.
    """
    def get_current_user(self) -> str | None:
        """Get the current username from the cookie"""
        try:
            username = self.get_secure_cookie('iceprod_username')
            if not username:
                raise RuntimeError('missing iceprod_username cookie')
            return username.decode('utf-8')
        except Exception:
            logger.info('failed to get username', exc_info=True)
        return None

    @property
    def auth_access_token(self) -> str | None:
        assert self.auth
        if self.session:
            ret = self.session.get('access_token', None)
            if not isinstance(ret, str):
                logger.info('bad access token type: not str')
                return None
            return ret
        else:
            logger.info('no session to get access token from')
        return None

    @auth_access_token.setter
    def auth_access_token(self, val: str):
        if self.session:
            self.session['access_token'] = val
        else:
            raise RuntimeError('no valid session')

    @property
    def auth_refresh_token(self) -> str | None:
        if self.session:
            ret = self.session.get('refresh_token', None)
            if not isinstance(ret, str):
                logger.info('bad access token type: not str')
                return None
            return ret
        else:
            logger.info('no session to get refresh token from')
        return None

    @auth_refresh_token.setter  # type: ignore[override]
    def auth_refresh_token(self, val: str):
        if self.session:
            self.session['refresh_token'] = val
        else:
            raise RuntimeError('no valid session')

    @property
    def auth_groups(self) -> list[str]:
        assert self.auth
        if not self.auth_access_token:
            return []
        try:
            data = self.auth.validate(self.auth_access_token)
        except jwt.ExpiredSignatureError:
            logger.debug('user access_token expired')
            return []

        # lookup groups
        groups: set[str] = set()
        try:
            for name in GROUPS:
                for expression in GROUPS[name]:
                    ret = eval_expression(data, expression)
                    groups.update(match.expand(name) for match in ret)
        except Exception:
            logger.info('cannot determine groups', exc_info=True)
        return sorted(groups)

    def store_tokens(
        self,
        access_token,
        access_token_exp,
        refresh_token=None,
        refresh_token_exp=None,
        user_info=None,
        user_info_exp=None,
    ):
        """
        Store jwt tokens and user info from OpenID-compliant auth source.

        Args:
            access_token (str): jwt access token
            access_token_exp (int): access token expiration in seconds
            refresh_token (str): jwt refresh token
            refresh_token_exp (int): refresh token expiration in seconds
            user_info (dict): user info (from id token or user info lookup)
            user_info_exp (int): user info expiration in seconds
        """
        assert self.auth
        if not user_info:
            user_info = self.auth.validate(access_token)
        username = user_info.get('preferred_username')
        if not username:
            username = user_info.get('upn')
        if not username:
            raise tornado.web.HTTPError(400, reason='no username in token')

        data = {
            'access_token': access_token,
            'refresh_token': refresh_token,
        }
        self._session_mgr.set(username, data)

        self.set_secure_cookie('iceprod_username', username, expires_days=30)

    def clear_tokens(self):
        """
        Clear token data, usually on logout.
        """
        self.clear_cookie('iceprod_username')
        if username := self.current_user:
            self._session_mgr.delete_session(username)


class TokenStorageMixin(RestHandler):
    """
    Store/load current user's tokens in iceprod credentials API.
    """
    TokenResult = list[dict[str, Any]]

    def initialize(self, *args, cred_rest_client, **kwargs):
        super().initialize(**kwargs)
        self.cred_rest_client = cred_rest_client

    async def _get_cred_tokens(self, path: str, url: str | None = None, scope: str | None = None) -> TokenResult:
        """Get selected tokens from the credential service."""
        try:
            assert self.auth
            args = {'url': url}
            if scope:
                args['scope'] = scope
            creds = await self.cred_rest_client.request('GET', path, args)
            return creds
        except requests.exceptions.RequestException:
            logger.warning('failed to get credentials', exc_info=True)
        return []

    async def get_cred_group_tokens(self, group_name: str, url: str | None = None, scope: str | None = None) -> TokenResult:
        return await self._get_cred_tokens(
            path=f'/groups/{group_name}/credentials',
            url=url,
            scope=scope,
        )

    async def get_cred_user_tokens(self, username: str, url: str | None = None, scope: str | None = None) -> TokenResult:
        return await self._get_cred_tokens(
            path=f'/users/{username}/credentials',
            url=url,
            scope=scope,
        )

    async def get_cred_dataset_tokens(self, dataset_id: str, url: str | None = None, scope: str | None = None) -> TokenResult:
        return await self._get_cred_tokens(
            path=f'/datasets/{dataset_id}/credentials',
            url=url,
            scope=scope,
        )

    async def get_cred_dataset_task_tokens(self, dataset_id: str, task_name: str, url: str | None = None, scope: str | None = None) -> TokenResult:
        return await self._get_cred_tokens(
            path=f'/datasets/{dataset_id}/tasks/{task_name}/credentials',
            url=url,
            scope=scope,
        )

    async def _put_cred_tokens(
        self,
        path: str,
        url: str,
        access_token: str,
        refresh_token: str | None = None
    ):
        """
        Store jwt tokens from OpenID-compliant auth source.

        Args:
            url (str): site url
            access_token (str): jwt access token
            refresh_token (str): jwt refresh token
        """
        assert self.auth
        args = {
            'url': url,
            'type': 'oauth',
            'access_token': access_token,
        }
        if refresh_token:
            args['refresh_token'] = refresh_token

        await self.cred_rest_client.request('POST', path, args)

    async def put_cred_group_tokens(
            self,
            group_name: str,
            url: str,
            access_token: str,
            refresh_token: str | None = None
    ):
        return await self._put_cred_tokens(
            path=f'/groups/{group_name}/credentials',
            url=url,
            access_token=access_token,
            refresh_token=refresh_token,
        )

    async def put_cred_user_tokens(
            self,
            username: str,
            url: str,
            access_token: str,
            refresh_token: str | None = None
    ):
        return await self._put_cred_tokens(
            path=f'/users/{username}/credentials',
            url=url,
            access_token=access_token,
            refresh_token=refresh_token,
        )

    async def put_cred_dataset_task_tokens(
            self,
            dataset_id: str,
            task_name: str,
            url: str,
            access_token: str,
            refresh_token: str | None = None
    ):
        return await self._put_cred_tokens(
            path=f'/datasets/{dataset_id}/tasks/{task_name}/credentials',
            url=url,
            access_token=access_token,
            refresh_token=refresh_token,
        )

    async def _clear_cred_tokens(self, path: str, url: str | None = None, scope: str | None = None):
        """
        Clear all token data.
        """
        args = {}
        if url:
            args['url'] = url
        if scope:
            args['scope'] = scope
        await self.cred_rest_client.request('DELETE', path, args)

    async def clear_cred_group_tokens(self, group_name: str, url: str, scope: str | None = None):
        return await self._clear_cred_tokens(
            path=f'/groups/{group_name}/credentials',
            url=url,
            scope=scope,
        )

    async def clear_cred_user_tokens(self, username: str, url: str, scope: str | None = None):
        return await self._clear_cred_tokens(
            path=f'/users/{username}/credentials',
            url=url,
            scope=scope,
        )

    async def clear_cred_dataset_tokens(self, dataset_id: str, url: str, scope: str | None = None):
        return await self._clear_cred_tokens(
            path=f'/datasets/{dataset_id}/credentials',
            url=url,
            scope=scope,
        )

    async def clear_cred_dataset_task_tokens(self, dataset_id: str, task_name: str, url: str, scope: str | None = None):
        return await self._clear_cred_tokens(
            path=f'/datasets/{dataset_id}/tasks/{task_name}/credentials',
            url=url,
            scope=scope,
        )


class PublicHandler(LoginMixin, TokenStorageMixin, PromRequestMixin, RestHandler):  # type: ignore[override]
    """Default Handler"""
    def initialize(  # type: ignore[override]
        self,
        *args,
        rest_api: str,
        system_rest_client: RestClient,
        auth_url: str,
        auth_client_id: str,
        auth_client_secret: str | None = None,
        **kwargs
    ):
        """
        Get some params from the website module

        :param rest_api: the rest api url
        :param system_rest_client: the rest client for the system role
        """
        super().initialize(*args, **kwargs)
        self.rest_api = rest_api
        self.system_rest_client = system_rest_client
        self.rest_client: RestClient | None = None
        self.auth_url = auth_url
        self.auth_client_id = auth_client_id
        self.auth_client_secret = auth_client_secret

    def get_template_namespace(self) -> dict[str, Any]:
        namespace = super().get_template_namespace()
        namespace['version'] = VERSION_STRING
        if self.request.uri:
            namespace['section'] = self.request.uri.lstrip('/').split('?')[0].split('/')[0]
        namespace['json_encode'] = json_encode
        namespace['states'] = iceprod.server.states
        namespace['rest_api'] = self.rest_api
        return namespace

    def update_refresh_token(self, access: str | bytes, refresh: str | bytes | None):
        if access:
            if isinstance(access, bytes):
                access = access.decode('utf-8')
            self.auth_access_token = access  # type: ignore
        if refresh:
            if isinstance(refresh, bytes):
                refresh = refresh.decode('utf-8')
            self.auth_refresh_token = refresh  # type: ignore

    async def get_current_user_async(self) -> str | None:
        try:
            self.current_user = LoginMixin.get_current_user(self)
            if self.current_user and self.auth_refresh_token:
                self.rest_client = OpenIDRestClient(
                    address=self.rest_api,
                    token_url=self.auth_url,
                    refresh_token=self.auth_refresh_token,
                    client_id=self.auth_client_id,
                    client_secret=self.auth_client_secret,
                    update_func=self.update_refresh_token,
                    timeout=50,
                    retries=1,
                )
                # verify refresh works  (note: this contacts Keycloak every time!)
                if not self.rest_client._openid_token():
                    logger.info('cannot refresh token')
                    return None
            elif self.current_user and self.auth_access_token:
                if get_expiration(self.auth_access_token) <= time.time():
                    logger.info('access token expired')
                    return None
                self.rest_client = RestClient(self.rest_api, self.auth_access_token, timeout=50, retries=1)
            elif not self.current_user:
                logger.info('no current user')
                return None
            else:
                logger.info('no access token')
                return None
        except Exception:
            logger.info('failed to create user rest client', exc_info=True)
            return None
        return self.current_user

    def write_error(self, status_code: int = 500, **kwargs) -> None:
        """Write out custom error page."""
        self.set_status(status_code)
        if status_code >= 500:
            self.write('<h2>Internal Error</h2>')
        else:
            self.write('<h2>Request Error</h2>')
        if 'exc_info' in kwargs:
            exception = kwargs["exc_info"][1]
            logger.info(''.join(traceback.format_exception(exception)))
            if isinstance(exception, tornado.web.HTTPError) and exception.reason:
                kwargs['reason'] = exception.reason
        if 'reason' in kwargs:
            self.write('<br />'.join(kwargs['reason'].split('\n')))
        elif 'message' in kwargs:
            self.write('<br />'.join(kwargs['message'].split('\n')))
        elif self._reason:
            self.write('<br />'.join(self._reason.split('\n')))
        self.finish()
