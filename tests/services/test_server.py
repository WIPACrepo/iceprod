import pytest
import requests


async def test_server_bad_route(server):
    client = server(roles=['system'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/foo')
    assert exc_info.value.response.status_code == 404

async def test_server_bad_method(server):
    client = server(roles=['system'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/healthz')
    assert exc_info.value.response.status_code == 405

async def test_server_health(server):
    client = server()
    ret = await client.request('GET', '/healthz')
    assert ret['num_requests'] == 0
