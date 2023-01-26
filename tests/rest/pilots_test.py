import pytest
import requests.exceptions


async def test_rest_pilots_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', '/pilots')
    assert ret == {}


async def test_rest_pilots_post(server):
    client = server(roles=['system'])

    data = {
        'queue_host': 'foo.bar.baz',
        'queue_version': '1.2.3',
        'resources': {'foo':1}
    }
    ret = await client.request('POST', '/pilots', data)
    pilot_id = ret['result']

    ret = await client.request('GET', '/pilots')
    assert pilot_id in ret
    for k in data:
        assert k in ret[pilot_id]
        assert data[k] == ret[pilot_id][k]


async def test_rest_pilots_details(server):
    client = server(roles=['system'])

    data = {
        'queue_host': 'foo.bar.baz',
        'queue_version': '1.2.3',
        'resources': {'foo':1}
    }
    ret = await client.request('POST', '/pilots', data)
    pilot_id = ret['result']

    ret = await client.request('GET', f'/pilots/{pilot_id}')
    for k in data:
        assert k in ret
        assert data[k] == ret[k]

    assert 'tasks' in ret
    assert ret['tasks'] == []


async def test_rest_pilots_patch(server):
    client = server(roles=['system'])

    data = {
        'queue_host': 'foo.bar.baz',
        'queue_version': '1.2.3',
        'resources': {'foo':1}
    }
    ret = await client.request('POST', '/pilots', data)
    pilot_id = ret['result']

    new_data = {
        'queues': {'foo': 'HTCondor', 'bar': 'HTCondor'},
        'version': '1.2.8',
        'tasks': ['baz'],
    }
    ret = await client.request('PATCH', f'/pilots/{pilot_id}', new_data)
    for k in new_data:
        assert k in ret
        assert new_data[k] == ret[k]


async def test_rest_pilots_delete(server):
    client = server(roles=['system'])

    data = {
        'queue_host': 'foo.bar.baz',
        'queue_version': '1.2.3',
        'resources': {'foo':1}
    }
    ret = await client.request('POST', '/pilots', data)
    pilot_id = ret['result']

    ret = await client.request('DELETE', f'/pilots/{pilot_id}')

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', f'/pilots/{pilot_id}')
    assert exc_info.value.response.status_code == 404
