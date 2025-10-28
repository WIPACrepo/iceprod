import logging
import re

import pytest
import pytest_asyncio
from rest_tools.client import RestClient
from rest_tools.utils import Auth
import requests.exceptions

from iceprod.rest.auth import ROLES, GROUPS
from iceprod.materialization.server import Server


@pytest_asyncio.fixture
async def server(monkeypatch, port, mongo_url, mongo_clear):
    monkeypatch.setenv('CI_TESTING', '1')
    monkeypatch.setenv('PORT', str(port))
    monkeypatch.setenv('ICEPROD_API_ADDRESS', 'http://test.iceprod')

    s = Server()
    # don't call start, because we don't want materialization to run
    # await s.start()

    auth = Auth('secret')

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

    def client(timeout=1, username='user', roles=[], groups=[], exp=10):
        data = {'preferred_username': username}
        for r in roles:
            _add_to_data(data, ROLES[r])
        for g in groups:
            _add_to_data(data, GROUPS[g])
        token = auth.create_token('username', expiration=exp, payload=data)
        return RestClient(f'http://localhost:{port}', token, timeout=timeout, retries=0)

    try:
        yield client
    finally:
        await s.stop()


async def test_materialization_server_bad_route(server):
    client = server(roles=['system'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/foo')
    assert exc_info.value.response.status_code == 404

async def test_materialization_server_bad_method(server):
    client = server(roles=['system'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/')
    assert exc_info.value.response.status_code == 405

async def test_materialization_server_health(server):
    client = server()
    ret = await client.request('GET', '/healthz')
    assert ret['num_requests'] == 0

async def test_materialization_server_request(server):
    client = server(roles=['system'])
    ret = await client.request('POST', '/', {})
    assert 'result' in ret

    ret = await client.request('GET', f'/status/{ret["result"]}')
    assert ret['status'] == 'waiting'

async def test_materialization_server_request_bad_role(server):
    client = server(roles=['user'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/', {})
    assert exc_info.value.response.status_code == 403

async def test_materialization_server_request_dataset(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})

    client = server(roles=['user'])
    ret = await client.request('POST', '/request/d123', {})
    ret = await client.request('POST', '/request/d123', {})
    mat_id = ret['result']
    
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', f'/status/{ret["result"]}')
    assert exc_info.value.response.status_code == 403

    ret = await client.request('GET', '/request/d123/status')
    assert ret['status'] == 'waiting'
    assert ret['dataset_id'] == 'd123'
    assert ret['materialization_id'] == mat_id

    # test no auth
    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/request/d123/status')
    assert exc_info.value.response.status_code == 403
