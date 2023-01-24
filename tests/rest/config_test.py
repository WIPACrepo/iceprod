import pytest
import requests.exceptions


async def test_rest_config_err(server):
    client = server(roles=['system'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/config/bar')
    assert exc_info.value.response.status_code == 404


async def test_rest_config(server):
    client = server(roles=['system'])
    data = {
        'name': 'foo'
    }
    await client.request('PUT', '/config/bar', data)
    
    ret = await client.request('GET', '/config/bar')
    assert ret == data
