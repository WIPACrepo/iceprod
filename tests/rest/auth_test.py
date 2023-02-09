import pytest
import requests.exceptions

from iceprod.rest.auth import ROLES, GROUPS


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
    assert ret['results'] == list(GROUPS.keys())

    client = server(roles=['user'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/groups')
    assert exc_info.value.response.status_code == 403


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
    dataset_id = ret['result'].split('/')[-1]

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
