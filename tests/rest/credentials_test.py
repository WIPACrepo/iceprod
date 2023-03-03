from datetime import datetime, timezone

import jwt
import pytest
import requests.exceptions

from iceprod.rest.handlers.credentials import get_expiration

GROUP = 'simprod'
USER = 'username'

def test_get_expiration():
    exp = datetime.now(timezone.utc)
    t = exp.timestamp()
    tok = jwt.encode({'exp': t}, 'secret')
    e = get_expiration(tok)
    assert exp.strftime('%Y-%m-%dT%H:%M:%S') == e


async def test_rest_credentials_groups_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret == {}


async def test_rest_credentials_groups_s3(server):
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
    assert ret == {data['url']: data}

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
    assert ret == {data['url']: data, data2['url']: data2}

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
    data3['groupname'] = GROUP
    assert ret == {data['url']: data3, data2['url']: data2}

    await client.request('DELETE', f'/groups/{GROUP}/credentials', {'url': 'http://foo'})

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret == {data2['url']: data2}

    await client.request('POST', f'/groups/{GROUP}/credentials', data3)
    await client.request('DELETE', f'/groups/{GROUP}/credentials')

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret == {}
    
async def test_rest_credentials_groups_s3_bad(server):
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


async def test_rest_credentials_groups_oauth(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    for k in data:
        assert ret[data['url']][k] == data[k]


async def test_rest_credentials_groups_user(server):
    client = server(roles=['user'], groups=['users', GROUP])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    for k in data:
        assert ret[data['url']][k] == data[k]

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


async def test_rest_credentials_users_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', f'/users/{USER}/credentials')
    assert ret == {}


async def test_rest_credentials_users_s3(server):
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
    assert ret == {data['url']: data}

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
    assert ret == {data['url']: data, data2['url']: data2}

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
    data3['username'] = USER
    assert ret == {data['url']: data3, data2['url']: data2}

    await client.request('DELETE', f'/users/{USER}/credentials', {'url': 'http://foo'})

    ret = await client.request('GET', f'/users/{USER}/credentials')
    assert ret == {data2['url']: data2}

    await client.request('POST', f'/users/{USER}/credentials', data3)
    await client.request('DELETE', f'/users/{USER}/credentials')

    ret = await client.request('GET', f'/users/{USER}/credentials')
    assert ret == {}


async def test_rest_credentials_users_oauth(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/users/{USER}/credentials', data)

    ret = await client.request('GET', f'/users/{USER}/credentials')
    for k in data:
        assert ret[data['url']][k] == data[k]


async def test_rest_credentials_users_user(server):
    client = server(username=USER, roles=['user'], groups=['users'])

    data = {
        'url': 'http://foo',
        'type': 'oauth',
        'access_token': str(client.access_token),
    }
    await client.request('POST', f'/users/{USER}/credentials', data)

    ret = await client.request('GET', f'/users/{USER}/credentials')
    for k in data:
        assert ret[data['url']][k] == data[k]

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
