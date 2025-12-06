import logging
import time
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from rest_tools.client import RestClient
from rest_tools.utils import Auth
import requests.exceptions

from iceprod.server.util import nowstr
from iceprod.rest.auth import ROLES, GROUPS
from iceprod.credentials.server import Server


@pytest_asyncio.fixture
async def server(monkeypatch, port, mongo_url, mongo_clear):
    monkeypatch.setenv('CI_TESTING', '1')
    monkeypatch.setenv('PORT', str(port))
    monkeypatch.setenv('ICEPROD_API_ADDRESS', 'http://test.iceprod')

    s = Server()
    # don't call start, because we don't want the service to run
    # await s.start()

    s.refresh_service.refresh_cred = AsyncMock(return_value={})

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


GROUP = 'simprod'
USER = 'username'
DATASETS = ['dataset1', 'dataset2']
TASK_NAMES = ['alpha', 'bravo']


async def test_credentials_groups_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret == []


async def test_credentials_groups_s3(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    data['groupname'] = GROUP
    data['transfer_prefix'] = ''
    assert ret == [data]

    # test bucket in url
    data2 = {
        'url': 'http://bucket.bar',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bucket'],
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data2)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    data2['groupname'] = GROUP
    data2['transfer_prefix'] = ''
    assert ret == [data, data2]

    # now overwrite
    data3 = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['baz'],
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data3)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    data3_out = data3.copy()
    data3_out['groupname'] = GROUP
    data3_out['transfer_prefix'] = ''
    assert ret == [data3_out, data2]

    await client.request('DELETE', f'/groups/{GROUP}/credentials', {'url': 'http://foo'})

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret == [data2]

    await client.request('POST', f'/groups/{GROUP}/credentials', data3)
    await client.request('DELETE', f'/groups/{GROUP}/credentials')

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret == []
    
async def test_credentials_groups_s3_bad(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': [],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/groups/{GROUP}/credentials', data)
    assert exc_info.value.response.status_code == 400

    data = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/groups/{GROUP}/credentials', data)
    assert exc_info.value.response.status_code == 400

    data = {
        'url': 'http://foo',
        'type': 's3',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/groups/{GROUP}/credentials', data)
    assert exc_info.value.response.status_code == 400

    data = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'buckets': ['bar'],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/groups/{GROUP}/credentials', data)
    assert exc_info.value.response.status_code == 400


async def test_credentials_groups_oauth(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    for k in data:
        assert ret[0][k] == data[k]


async def test_credentials_groups_user(server):
    client = server(roles=['user'], groups=['users', GROUP])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    for k in data:
        assert ret[0][k] == data[k]

    data2 = {
        'url': 'http://foo',
        'type': 'oauth',
        'refresh_token': str(client.access_token),
        'expiration': time.time()+100,
    }
    await client.request('PATCH', f'/groups/{GROUP}/credentials', data2)
    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret[0]['refresh_token'] == data2['refresh_token']

    await client.request('DELETE', f'/groups/{GROUP}/credentials')

    # bad user
    client = server(roles=['user'], groups=['users'])

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/groups/{GROUP}/credentials', data)
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', f'/groups/{GROUP}/credentials')
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('DELETE', f'/groups/{GROUP}/credentials')
    assert exc_info.value.response.status_code == 403


async def test_credentials_users_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', f'/users/{USER}/credentials')
    assert ret == []


async def test_credentials_users_s3(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    await client.request('POST', f'/users/{USER}/credentials', data)

    ret = await client.request('GET', f'/users/{USER}/credentials')
    data['username'] = USER
    data['transfer_prefix'] = ''
    assert ret == [data]

    data2 = {
        'url': 'http://bar',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    await client.request('POST', f'/users/{USER}/credentials', data2)

    ret = await client.request('GET', f'/users/{USER}/credentials')
    data2['username'] = USER
    data2['transfer_prefix'] = ''
    assert ret == [data, data2]

    # now overwrite
    data3 = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['baz'],
    }
    await client.request('POST', f'/users/{USER}/credentials', data3)

    ret = await client.request('GET', f'/users/{USER}/credentials')
    data3_out = data3.copy()
    data3_out['username'] = USER
    data3_out['transfer_prefix'] = ''
    assert ret == [data3_out, data2]

    await client.request('DELETE', f'/users/{USER}/credentials', {'url': 'http://foo'})

    ret = await client.request('GET', f'/users/{USER}/credentials')
    assert ret == [data2]

    await client.request('POST', f'/users/{USER}/credentials', data3)
    await client.request('DELETE', f'/users/{USER}/credentials')

    ret = await client.request('GET', f'/users/{USER}/credentials')
    assert ret == []


async def test_credentials_users_oauth(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/users/{USER}/credentials', data)

    ret = await client.request('GET', f'/users/{USER}/credentials')
    for k in data:
        assert ret[0][k] == data[k]


async def test_credentials_users_user(server):
    client = server(username=USER, roles=['user'], groups=['users'])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/users/{USER}/credentials', data)

    ret = await client.request('GET', f'/users/{USER}/credentials')
    for k in data:
        assert ret[0][k] == data[k]

    data2 = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
        'last_use': time.time()
    }
    await client.request('PATCH', f'/users/{USER}/credentials', data2)
    ret = await client.request('GET', f'/users/{USER}/credentials', {'norefresh': 'true'})
    assert ret[0]['last_use'] == data2['last_use']

    await client.request('DELETE', f'/users/{USER}/credentials')

    # bad user
    client = server(username='foo', roles=['user'], groups=['users'])
    
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/users/{USER}/credentials', data)
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', f'/users/{USER}/credentials')
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('DELETE', f'/users/{USER}/credentials')
    assert exc_info.value.response.status_code == 403


async def test_credentials_datasets_user_fail(server):
    client = server(roles=['user'], groups=['users', GROUP])
    
    data = {
        'url': 'http://foo',
        'type': 'oauth',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', f'/datasets/{DATASETS[0]}/credentials', data)
    assert exc_info.value.response.status_code == 403

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data)
    assert exc_info.value.response.status_code == 403


async def test_credentials_datasets(server):
    client = server(roles=['system'])
    auth = Auth('secret')

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data)

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    for k in data:
        assert ret[0][k] == data[k]

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/credentials')
    for k in data:
        assert ret[0][k] == data[k]

    data2 = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
        'last_use': time.time()
    }
    await client.request('PATCH', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data2)
    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', {'norefresh': 'true'})
    assert ret[0]['last_use'] == data2['last_use']

    # now for the second task
    data3 = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': auth.create_token('sub2'),
    }
    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[1]}/credentials', data3)

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    assert len(ret) == 1
    assert ret[0]['access_token'] == data2['access_token']

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[1]}/credentials')
    assert len(ret) == 1
    assert ret[0]['access_token'] == data3['access_token']

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/credentials')
    assert len(ret) == 2
    assert ret[0]['access_token'] == data2['access_token']
    assert ret[1]['access_token'] == data3['access_token']

    # now for a different dataset
    data4 = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': auth.create_token('sub3'),
    }
    await client.request('POST', f'/datasets/{DATASETS[1]}/tasks/{TASK_NAMES[1]}/credentials', data4)

    ret = await client.request('GET', f'/datasets/{DATASETS[1]}/tasks/{TASK_NAMES[0]}/credentials')
    assert len(ret) == 0

    ret = await client.request('GET', f'/datasets/{DATASETS[1]}/tasks/{TASK_NAMES[1]}/credentials')
    assert len(ret) == 1
    assert ret[0]['access_token'] == data4['access_token']

    ret = await client.request('GET', f'/datasets/{DATASETS[1]}/credentials')
    assert len(ret) == 1
    assert ret[0]['access_token'] == data4['access_token']
    
    # delete
    await client.request('DELETE', f'/datasets/{DATASETS[0]}/credentials')
    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/credentials')
    assert len(ret) == 0
    
    ret = await client.request('GET', f'/datasets/{DATASETS[1]}/credentials')
    assert len(ret) == 1
    assert ret[0]['access_token'] == data4['access_token']

    await client.request('DELETE', f'/datasets/{DATASETS[1]}/credentials')
    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/credentials')
    assert len(ret) == 0
    ret = await client.request('GET', f'/datasets/{DATASETS[1]}/credentials')
    assert len(ret) == 0


async def test_credentials_datasets_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/credentials')
    assert ret == []


async def test_credentials_datasets_s3(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'transfer_prefix': 'http://bar',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data)

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/credentials')
    data['dataset_id'] = DATASETS[0]
    data['task_name'] = TASK_NAMES[0]
    assert ret == [data]

    data2 = {
        'url': 'http://bar',
        'transfer_prefix': 'http://bar',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data2)

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    data2['dataset_id'] = DATASETS[0]
    data2['task_name'] = TASK_NAMES[0]
    assert ret == [data, data2]

    data4 = {
        'url': 'http://foo',
        'transfer_prefix': 'http://baz',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data4)

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    data4['dataset_id'] = DATASETS[0]
    data4['task_name'] = TASK_NAMES[0]
    assert ret == [data, data2, data4]

    # now overwrite
    data3 = {
        'url': 'http://foo',
        'transfer_prefix': 'http://bar',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['baz'],
    }
    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data3)

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    data3_out = data3.copy()
    data3_out['dataset_id'] = DATASETS[0]
    data3_out['task_name'] = TASK_NAMES[0]
    assert ret == [data3_out, data2, data4]

    await client.request('DELETE', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', {'url': 'http://foo'})

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    assert ret == [data2]

    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data3)
    await client.request('DELETE', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    assert ret == []


async def test_credentials_datasets_oauth(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials', data)

    ret = await client.request('GET', f'/datasets/{DATASETS[0]}/tasks/{TASK_NAMES[0]}/credentials')
    for k in data:
        assert ret[0][k] == data[k]


async def test_credentials_healthz(server):
    client = server(username=USER, roles=['user'], groups=['users'])

    ret = await client.request('GET', '/healthz')

    now = nowstr()

    assert ret['now'][:16] == now[:16]
    assert ret['start_time'][:16] == now[:16]
    assert ret['last_run_time'] == ''
    assert ret['last_success_time'] == ''

    
async def test_credentials_bad_route(server):
    client = server(username=USER, roles=['user'], groups=['users'])

    with pytest.raises(requests.exceptions.HTTPError):
        await client.request('GET', '/foo')
        

