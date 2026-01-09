import binascii
import json
import logging
import random
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest_asyncio
from rest_tools.utils import Auth
from rest_tools.client import RestClient
import requests.exceptions
from tornado.web import create_signed_value

from iceprod.credentials.util import Client as CredClient
from iceprod.rest.auth import ROLES, GROUPS
from iceprod.website.server import Server


class ReqMock:
    def __init__(self):
        self.mocks = {}

    def add_mock(self, path: str, ret: Any) -> MagicMock:
        if callable(ret):
            m = MagicMock(side_effect=ret)
        else:
            m = MagicMock(return_value=ret)
        self.mocks[path] = m
        return m

    async def mock(self, method, path, *args, **kwargs):
        if path in self.mocks:
            return self.mocks[path](method, path, *args, **kwargs)
        else:
            response = MagicMock()
            response.status_code = 404
            raise requests.exceptions.HTTPError(response=response)


@pytest_asyncio.fixture
async def server(monkeypatch, port):
    monkeypatch.setenv('CI_TESTING', '1')
    monkeypatch.setenv('DEBUG', 'True')
    monkeypatch.setenv('PORT', str(port))

    monkeypatch.setenv('ICEPROD_CRED_ADDRESS', 'http://iceprod.test')
    monkeypatch.setenv('ICEPROD_API_ADDRESS', 'http://iceprod.test')

    token_clients = {
        'http://token.auth': {
            'client_id': 'client',
            'client_secret': 'secret',
            'transfer_prefix': ['token://'],
        }
    }
    monkeypatch.setenv('TOKEN_CLIENTS', json.dumps(token_clients))

    cookie = ''.join(hex(random.randint(0,15))[-1] for _ in range(64))
    monkeypatch.setenv('COOKIE_SECRET', cookie)
    cookie_secret = binascii.unhexlify(cookie)

    # set hostname
    hostname = b'localhost'
    address = f'http://localhost:{port}'
    monkeypatch.setenv('ICEPROD_WEB_URL', address)

    req_mock = ReqMock()
    monkeypatch.setattr(RestClient, 'request', req_mock.mock)

    def oauth_setup(self):
        self._OAUTH_AUTHORIZE_URL = 'http://idp.test/oauth/authorize'
        self._OAUTH_ACCESS_TOKEN_URL = 'http://idp.test/oauth/token'
        self._OAUTH_LOGOUT_URL = 'http://idp.test/oauth/logout'
        self._OAUTH_USERINFO_URL = 'http://idp.test/oauth/userinfo'

    auth = Auth('secret')
    monkeypatch.setattr(CredClient, 'auth', auth)

    s = Server()
    await s.start()

    def _add_to_data(data, attrs):
        logging.debug('attrs: %r', attrs)
        for item in attrs:
            key,value = item.split('=',1)
            d = data
            while '.' in key:
                k,key = key.split('.',1)
                if k not in d:
                    d[k] = {}
                d = d[k]
            if key not in d:
                d[key] = []
            d[key].append(value)

    #requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)

    class Request:
        def __init__(self, token_data, timeout=None):
            self.timeout = timeout
            self.req_mock = req_mock
            self.auth = auth

            username = token_data['preferred_username']
            s.session.set(username, {'access_token': auth.create_token(username, payload=token_data)})

            self.token_cookie = create_signed_value(cookie_secret, 'iceprod_username', username)
            logging.debug('Request cookie_secret: %r', cookie_secret)

        def get_http_client(self):
            return httpx.AsyncClient(timeout=self.timeout, cookies={'iceprod_username': self.token_cookie.decode('utf-8')})

        async def request_raw(
            self,
            client: httpx.AsyncClient,
            method: str,
            path: str,
            args: dict | None = None,
            form_data: dict | None = None,
            json: dict | None = None
        ) -> httpx.Response:
            logging.debug('website request %s %s', method, path)
            kwargs = {}
            if args:
                kwargs['params'] = args
            if form_data:
                assert method != 'GET'
                assert not json
                kwargs['data'] = form_data
            if json:
                assert not form_data
                kwargs['json'] = json
            ret = await client.request(method, address+path, **kwargs)  # type: ignore
            return ret

        async def request(self, *args, **kwargs) -> str:
            async with self.get_http_client() as client:
                ret = await self.request_raw(client, *args, **kwargs)
                ret.raise_for_status()
                return ret.text


    def client(timeout=1, username='user', roles=[], groups=[], exp=10):
        data = {'preferred_username': username}
        for r in roles:
            _add_to_data(data, ROLES[r])
        for g in groups:
            _add_to_data(data, GROUPS[g])
        return Request(data, timeout=timeout)

    try:
        yield client
    finally:
        await s.stop()
