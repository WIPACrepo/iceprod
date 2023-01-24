import pytest
import requests.exceptions


async def test_rest_datasets_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', '/datasets')
    assert ret == {}


async def test_rest_datasets_post(server):
    client = server(roles=['user'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    ret = await client.request('GET', '/datasets')
    assert dataset_id in ret

    ret = await client.request('GET', '/datasets', {'status': 'suspended'})
    assert dataset_id not in ret

    ret = await client.request('GET', '/datasets', {'keys': 'dataset_id|dataset'})
    assert dataset_id in ret
    assert set(ret[dataset_id].keys()) == set(['dataset_id', 'dataset'])

    ret = await client.request('GET', f'/datasets/{dataset_id}')
    for k in data:
        assert k in ret
        assert data[k] == ret[k]


async def test_rest_datasets_post_bad_role(server):
    client = server(roles=['system'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/datasets', data)
    assert exc_info.value.response.status_code == 403


async def test_rest_datasets_err(server):
    client = server(roles=['system'])
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/datasets/bar')
    assert exc_info.value.response.status_code == 404


async def test_rest_datasets_update_description_user(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    data = {'description': 'foo bar baz'}
    await client.request('PUT', f'/datasets/{dataset_id}/description', data)

    ret = await client.request('GET', f'/datasets/{dataset_id}')
    assert data['description'] == ret['description']


async def test_rest_datasets_update_description_group(server):
    client = server(roles=['user'], groups=['simprod','users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'simprod',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    client = server(username='foo', roles=['user'], groups=['simprod','users'])
    data = {'description': 'foo bar baz'}
    await client.request('PUT', f'/datasets/{dataset_id}/description', data)

    ret = await client.request('GET', f'/datasets/{dataset_id}')
    assert data['description'] == ret['description']


async def test_rest_datasets_update_description_bad_user(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    client = server(username='foo', roles=['user'], groups=['users'])
    data = {'description': 'foo bar baz'}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', f'/datasets/{dataset_id}/description', data)
    assert exc_info.value.response.status_code == 403


async def test_rest_datasets_update_description_bad_role(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    client = server(roles=['system'])
    data = {'description': 'foo bar baz'}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', f'/datasets/{dataset_id}/description', data)
    assert exc_info.value.response.status_code == 403


async def test_rest_datasets_update_status(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    data = {'status': 'suspended'}
    await client.request('PUT', f'/datasets/{dataset_id}/status', data)

    ret = await client.request('GET', f'/datasets/{dataset_id}')
    assert data['status'] == ret['status']


async def test_rest_datasets_update_jobs_submitted(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    data = {'jobs_submitted': 2}
    await client.request('PUT', f'/datasets/{dataset_id}/jobs_submitted', data)

    ret = await client.request('GET', f'/datasets/{dataset_id}')
    assert data['jobs_submitted'] == ret['jobs_submitted']

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', f'/datasets/{dataset_id}/jobs_submitted')
    assert exc_info.value.response.status_code == 400

    data = {}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', f'/datasets/{dataset_id}/jobs_submitted', data)
    assert exc_info.value.response.status_code == 400

    
    data = {'jobs_submitted': 'foo'}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', f'/datasets/{dataset_id}/jobs_submitted', data)
    assert exc_info.value.response.status_code == 400

    data = {'jobs_submitted': 0}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', f'/datasets/{dataset_id}/jobs_submitted', data)
    assert exc_info.value.response.status_code == 400

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
        'jobs_immutable': True,
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    data = {'jobs_submitted': 2}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PUT', f'/datasets/{dataset_id}/jobs_submitted', data)
    assert exc_info.value.response.status_code == 400


async def test_rest_datasets_summaries_status(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result'].split('/')[-1]

    ret = await client.request('GET', '/dataset_summaries/status')
    assert ret == {'processing': [dataset_id]}
