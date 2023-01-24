import os
import socket
from unittest.mock import AsyncMock, MagicMock

import motor.motor_asyncio
import pytest
import pytest_asyncio
from rest_tools.client import RestClient
import requests.exceptions

from iceprod.materialization.server import Server


@pytest.fixture(scope='module')
def monkeymodule():
    with pytest.MonkeyPatch.context() as mp:
        yield mp

@pytest.fixture(scope='module')
def mongo_url(monkeymodule):
    if 'DB_URL' not in os.environ:
        monkeymodule.setenv('DB_URL', 'mongodb://localhost/datasets')

@pytest_asyncio.fixture
async def server(monkeypatch, port, mongo_url, mongo_clear):
    monkeypatch.setenv('PORT', str(port))

    s = Server()
    # don't call start, because we don't want materialization to run
    # await s.start()

    def client(timeout=10):
        return RestClient(f'http://localhost:{port}', timeout=timeout, retries=0)

    try:
        yield client
    finally:
        await s.stop()


async def test_materialization_server_bad_route(server):
    client = server()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/foo')
    assert exc_info.value.response.status_code == 404

async def test_materialization_server_bad_method(server):
    client = server()
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/')
    assert exc_info.value.response.status_code == 405

async def test_materialization_server_health(server):
    client = server()
    ret = await client.request('GET', '/healthz')
    assert ret['num_requests'] == 0

async def test_materialization_server_request(server):
    client = server()
    ret = await client.request('POST', '/', {})
    assert 'result' in ret

    ret = await client.request('GET', f'/status/{ret["result"]}')
    assert ret['status'] == 'waiting'

async def test_materialization_server_request_dataset(server):
    client = server()
    ret = await client.request('POST', '/request/d123', {})
    assert 'result' in ret

    ret = await client.request('GET', f'/status/{ret["result"]}')
    assert ret['status'] == 'waiting'
    assert ret['dataset_id'] == 'd123'
