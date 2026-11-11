import functools
import logging
import re
import secrets
import time
import traceback
from typing import Any
from urllib.parse import urlencode

import jwt
import requests.exceptions
import tornado.web
from requests.auth import HTTPBasicAuth
from rest_tools.client import RestClient
from rest_tools.server import RestHandler, catch_error
from rest_tools.server.session import SessionMixin
from rest_tools.utils.auth import OpenIDAuth

import iceprod.server.states
from iceprod.common.prom_utils import PromRequestMixin
from iceprod.core.jsonUtil import json_encode
from iceprod.credentials.util import get_expiration
from iceprod.roles_groups import GROUPS
from iceprod.util import VERSION_STRING

logger = logging.getLogger('website_base')

#: refresh the user's access token when it has less than this many seconds left
REFRESH_MARGIN = 60

#: timeout (seconds) for OpenID discovery / token endpoint calls
TOKEN_REQUEST_TIMEOUT = 10

#: cache of OpenID base url -> token endpoint (discovered once per process)
_TOKEN_ENDPOINTS: dict[str, str] = {}


def get_token_endpoint(auth_url: str) -> str:
    """
    Get the OpenID token endpoint, using .well-known discovery once per process.

    Fallback for `PublicHandler._token_endpoint`, used only when the token
    validator (`self.auth`) did not already discover it.  Failures are not
    cached, so the next refresh retries discovery.

    Args:
        auth_url: OpenID provider base url

    Returns:
        the token endpoint url
    """
    if not auth_url:
        raise RuntimeError('no auth url')
    if auth_url not in _TOKEN_ENDPOINTS:
        url = auth_url if auth_url.endswith('/') else auth_url + '/'
        r = requests.get(url + '.well-known/openid-configuration', timeout=TOKEN_REQUEST_TIMEOUT)
        r.raise_for_status()
        _TOKEN_ENDPOINTS[auth_url] = r.json()['token_endpoint']
    return _TOKEN_ENDPOINTS[auth_url]


def _token_scopes(refresh_token: str) -> str:
    """
    Scopes of a (possibly opaque) refresh token, for the refresh request.

    The signature is deliberately not verified: the scope is only echoed back
    to the identity provider, which validates the refresh token itself and
    grants no more than the token allows.
    """
    try:
        return jwt.decode(refresh_token, options={'verify_signature': False}).get('scope', '')
    except Exception:
        return ''


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

        data: dict[str, str] = {
            'access_token': access_token,
        }
        if refresh_token:
            data['refresh_token'] = refresh_token
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
        return namespace

    def prepare(self):
        super().prepare()

        # set a content security policy, and use a nonce for inline style/script tags
        self.csp_nonce = secrets.token_urlsafe(16)
        csp_policy = (
            "default-src 'none'; "
            f"script-src 'self' 'nonce-{self.csp_nonce}' cdnjs.cloudflare.com; "
            "script-src-attr 'unsafe-inline'; "
            "connect-src 'self'; "
            "img-src 'self'; "
            f"style-src 'self' 'nonce-{self.csp_nonce}' cdnjs.cloudflare.com; "
            "style-src-attr 'unsafe-inline'; "
            "frame-ancestors 'none'; "
            "form-action 'self'; "
            "upgrade-insecure-requests; "
        )
        self.set_header('Content-Security-Policy', csp_policy)

    def update_refresh_token(self, access: str | bytes, refresh: str | bytes | None):
        if access:
            if isinstance(access, bytes):
                access = access.decode('utf-8')
            self.auth_access_token = access
        if refresh:
            if isinstance(refresh, bytes):
                refresh = refresh.decode('utf-8')
            self.auth_refresh_token = refresh

    def _token_endpoint(self) -> str:
        """
        Get the OpenID token endpoint for refreshes.

        In production `self.auth` is an `OpenIDAuth`, which already ran
        `.well-known` discovery at startup (it validates tokens with the same
        provider), so its `token_url` is used with no extra request.

        Falls back to `get_token_endpoint` (cached discovery from
        `self.auth_url`, the provider base url) when:
        * discovery failed at startup: `OpenIDAuth` logs the error and leaves
          `token_url` empty
        * `self.auth` is not OpenID, e.g. the shared-secret `Auth` in CI testing

        Returns:
            the token endpoint url
        """
        if isinstance(self.auth, OpenIDAuth) and self.auth.token_url:
            return self.auth.token_url
        return get_token_endpoint(self.auth_url or '')

    def refresh_access_token(self) -> str:
        """
        Exchange the session refresh token for a new access token.

        Makes exactly one token endpoint call, and stores the new tokens in
        the session via `update_refresh_token`.  Also used as the rest
        client's token callback, so long requests refresh mid-flight.

        The new access token is validated (signature, expiry, issuer and
        audience, via `self.auth`) and must belong to the logged in user
        before it is stored.  This is the same check `store_tokens` does at
        login, so every access token in the session has been verified once.
        Later expiry checks (`_access_token_valid`) can then decode without
        verification.

        Returns:
            the new access token

        Raises:
            Exception: on any failure (no refresh token, http error, bad
                response, invalid token, or token for a different user)
        """
        refresh_token = self.auth_refresh_token
        if not refresh_token:
            raise RuntimeError('no refresh token')
        args = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.auth_client_id,
            'scope': _token_scopes(refresh_token),
        }
        kwargs: dict[str, Any] = {'timeout': TOKEN_REQUEST_TIMEOUT}
        if self.auth_client_secret:
            kwargs['auth'] = HTTPBasicAuth(self.auth_client_id, self.auth_client_secret)
        r = requests.post(self._token_endpoint(), data=args, **kwargs)
        r.raise_for_status()
        ret = r.json()
        access_token = ret['access_token']

        # verify before storing; signing keys are cached by self.auth, so this
        # normally makes no network call
        assert self.auth
        data = self.auth.validate(access_token)
        username = data.get('preferred_username') or data.get('upn')
        if username != self.current_user:
            raise RuntimeError('refreshed token is for a different user')

        self.update_refresh_token(access_token, ret.get('refresh_token'))
        logger.debug('refreshed access token for %s', self.current_user)
        return access_token

    def _access_token_valid(self, access_token: str | None, margin: float = 0) -> bool:
        """
        Check if an access token exists and has more than `margin` seconds left.

        Decodes without verifying the signature.  This is safe because:
        * session tokens are server-side only, and were validated when stored
          (`store_tokens` at login, `refresh_access_token` on refresh)
        * the result only decides whether to refresh early; it grants nothing.
          The REST API validates the token on every request.
        """
        if not access_token:
            return False
        try:
            return get_expiration(access_token) - time.time() > margin
        except Exception:
            logger.info('cannot decode access token', exc_info=True)
            return False

    async def get_current_user_async(self) -> str | None:
        """
        Authenticate the user and build `self.rest_client`.

        Token handling:
        * cached access token valid for > REFRESH_MARGIN: use it, no Keycloak call
        * otherwise, with a refresh token: refresh once
        * the rest client refreshes again by itself if the token expires
          during a long request

        Returns:
            the username, or None if not authenticated
        """
        try:
            self.current_user = LoginMixin.get_current_user(self)
            if not self.current_user:
                logger.info('no current user')
                return None

            access_token = self.auth_access_token
            if self.auth_refresh_token:
                if not self._access_token_valid(access_token, REFRESH_MARGIN):
                    try:
                        access_token = self.refresh_access_token()
                    except Exception:
                        logger.info('cannot refresh token', exc_info=True)
                        return None
                self.rest_client = RestClient(self.rest_api, token=self.refresh_access_token, timeout=50, retries=1)
                # seed with the current token so no refresh happens until it expires
                self.rest_client.access_token = access_token
            elif access_token:
                if not self._access_token_valid(access_token):
                    logger.info('access token expired')
                    return None
                self.rest_client = RestClient(self.rest_api, access_token, timeout=50, retries=1)
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
