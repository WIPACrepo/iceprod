import pytest
import requests.exceptions

from iceprod.roles_groups import ROLES, GROUP_PRIORITIES


async def test_rest_auth_roles(server):
    client = server(roles=['admin'])
    ret = await client.request('GET', '/roles')
    assert ret['results'] == list(ROLES.keys())

    client = server(roles=['user'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/roles')
    assert exc_info.value.response.status_code == 403


async def test_rest_auth_groups(server):
    client = server(roles=['admin'])
    ret = await client.request('GET', '/groups')
    assert len(ret['results']) == len(GROUP_PRIORITIES)
    assert [x['name'] for x in ret['results']] == list(GROUP_PRIORITIES.keys())
    assert [x['priority'] for x in ret['results']] == list(GROUP_PRIORITIES.values())

    client = server(roles=['user'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/groups')
    assert exc_info.value.response.status_code == 403


async def test_rest_auth_users(server):
    client = server(roles=['admin'])
    ret = await client.request('GET', '/users')
    assert ret['results'] == []

    await client.request('PUT', '/users/foo')

    ret = await client.request('GET', '/users')
    assert len(ret['results']) == 1
    assert ret['results'][0]['username'] == 'foo'
    assert 'priority' in ret['results'][0]

    ret = await client.request('GET', '/users/foo')
    assert ret['username'] == 'foo'
    assert 'priority' in ret

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/foo')
    assert exc_info.value.response.status_code == 409

    await client.request('DELETE', '/users/foo')
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/users/foo')
    assert exc_info.value.response.status_code == 404


async def test_rest_auth_users_bad_role(server):
    client = server(roles=['user'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/users')
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/users/foo')
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/foo')
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('DELETE', '/users/foo')
    assert exc_info.value.response.status_code == 403

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/foo/priority')
    assert exc_info.value.response.status_code == 403


async def test_rest_auth_users_from_dataset(server):
    client = server(username='foo', roles=['user'], groups=['users', 'simprod'])

    # create dataset, which should set user prio
    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    await client.request('POST', '/datasets', data)

    client = server(roles=['admin'])
    ret = await client.request('GET', '/users')
    assert len(ret['results']) == 1
    assert ret['results'][0]['username'] == 'foo'
    assert 'priority' in ret['results'][0]


async def test_rest_auth_user_priority(server):
    client = server(roles=['admin'])
    await client.request('PUT', '/users/foo')
    await client.request('PUT', '/users/foo/priority', {'priority': 0.3})

    ret = await client.request('GET', '/users/foo')
    assert ret['username'] == 'foo'
    assert ret['priority'] == 0.3

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/foo/priority', {'priority': -1})
    assert exc_info.value.response.status_code == 400

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/foo/priority', {'priority': 10})
    assert exc_info.value.response.status_code == 400

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/foo/priority', {'priority': 'str'})
    assert exc_info.value.response.status_code == 400

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/foo/priority')
    assert exc_info.value.response.status_code == 400

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', '/users/bar/priority', {'priority': 0.5})
    assert exc_info.value.response.status_code == 400


async def test_rest_auth_auths(server):
    client = server(username='me', roles=['user'], groups=['users'])

    # create dataset first
    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result']

    # user can't do this
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/auths', {})
    assert exc_info.value.response.status_code == 403

    client = server(roles=['system'])
    args = {
        'name': 'dataset_id',
        'value': dataset_id,
        'role': 'read',
        'username': 'me',
        'groups': [],
    }
    await client.request('POST', '/auths', args)

    args = {
        'name': 'dataset_id',
        'value': dataset_id,
        'role': 'read',
        'username': 'you',
        'groups': ['users'],
    }
    await client.request('POST', '/auths', args)

    args = {
        'name': 'dataset_id',
        'value': dataset_id,
        'role': 'read',
        'username': 'you',
        'groups': ['foo'],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/auths', args)
    assert exc_info.value.response.status_code == 403

    # now check for write auth
    args = {
        'name': 'dataset_id',
        'value': dataset_id,
        'role': 'write',
        'username': 'me',
        'groups': [],
    }
    await client.request('POST', '/auths', args)

    args = {
        'name': 'dataset_id',
        'value': dataset_id,
        'role': 'write',
        'username': 'you',
        'groups': ['users'],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/auths', args)
    assert exc_info.value.response.status_code == 403

    args = {
        'name': 'dataset_id',
        'value': dataset_id,
        'role': 'write',
        'username': 'you',
        'groups': ['foo'],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/auths', args)
    assert exc_info.value.response.status_code == 403

    # bad dataset_id
    args = {
        'name': 'dataset_id',
        'value': 'foo',
        'role': 'read',
        'username': 'me',
        'groups': [],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/auths', args)
    assert exc_info.value.response.status_code == 403

    # bad role
    args = {
        'name': 'dataset_id',
        'value': dataset_id,
        'role': 'foo',
        'username': 'me',
        'groups': [],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/auths', args)
    assert exc_info.value.response.status_code == 403
