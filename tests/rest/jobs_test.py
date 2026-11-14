import pytest
import requests.exceptions

import iceprod.server.states


async def test_rest_jobs_add(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    ret = await client.request('GET', f'/jobs/{job_id}')
    for k in data:
        assert k in ret
        assert data[k] == ret[k]
    for k in ('status','status_changed'):
        assert k in ret
    assert ret['status'] == 'processing'


async def test_rest_jobs_add_bad_role(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/jobs', data)
    assert exc_info.value.response.status_code == 403


async def test_rest_jobs_patch(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    new_data = {
        'status': 'suspended',
    }
    ret = await client.request('PATCH', f'/jobs/{job_id}', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == 'suspended'


async def test_rest_jobs_dataset_get(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/jobs')
    assert job_id in ret
    for k in data:
        assert k in ret[job_id]
        assert data[k] == ret[job_id][k]


async def test_rest_jobs_dataset_get_details(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/jobs/{job_id}')
    for k in data:
        assert k in ret
        assert data[k] == ret[k]
    for k in ('status','status_changed'):
        assert k in ret
    assert ret['status'] == 'processing'


async def test_rest_jobs_empty(server):
    client = server(roles=['system'])

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/datasets/foo/jobs/bar')
    assert exc_info.value.response.status_code == 404


async def test_rest_jobs_dataset_set_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    new_data = {'status': 'errors'}
    ret = await client.request('PUT', f'/datasets/{data["dataset_id"]}/jobs/{job_id}/status', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == new_data['status']


async def test_rest_jobs_dataset_bulk_set_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    new_data = {'jobs': [job_id]}
    ret = await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_status/errors', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == 'errors'


async def test_rest_jobs_dataset_bulk_suspend(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    new_data = {}
    ret = await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_suspend', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == 'suspended'


async def test_rest_jobs_dataset_bulk_suspend_specific(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_index': 1,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id2 = ret['result']

    new_data = {'jobs': [job_id]}
    ret = await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_suspend', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == 'suspended'
    ret = await client.request('GET', f'/jobs/{job_id2}')
    assert ret['status'] == iceprod.server.states.JOB_STATUS_START


async def test_rest_jobs_dataset_bulk_reset(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
        'status': 'errors',
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    new_data = {}
    ret = await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_reset', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == iceprod.server.states.JOB_STATUS_START


async def test_rest_jobs_dataset_bulk_reset_specific(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
        'status': 'errors',
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_index': 1,
        'status': 'errors',
    }
    ret = await client.request('POST', '/jobs', data)
    job_id2 = ret['result']

    new_data = {'jobs': [job_id]}
    ret = await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_reset', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == iceprod.server.states.JOB_STATUS_START
    ret = await client.request('GET', f'/jobs/{job_id2}')
    assert ret['status'] == 'errors'


async def test_rest_jobs_dataset_bulk_reset_blocked(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
        'status': 'complete',
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    with pytest.raises(Exception):
        await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_reset', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == 'complete'


async def test_rest_jobs_dataset_bulk_hardreset(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
        'status': 'complete',
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    new_data = {}
    ret = await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_hard_reset', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == iceprod.server.states.JOB_STATUS_START


async def test_rest_jobs_dataset_bulk__hardreset_specific(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
        'status': 'complete',
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_index': 1,
        'status': 'complete',
    }
    ret = await client.request('POST', '/jobs', data)
    job_id2 = ret['result']

    new_data = {'jobs': [job_id]}
    ret = await client.request('POST', f'/datasets/{data["dataset_id"]}/job_actions/bulk_hard_reset', new_data)

    ret = await client.request('GET', f'/jobs/{job_id}')
    assert ret['status'] == iceprod.server.states.JOB_STATUS_START
    ret = await client.request('GET', f'/jobs/{job_id2}')
    assert ret['status'] == 'complete'


async def test_rest_jobs_dataset_summaries_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/job_summaries/status')
    assert ret == {'processing': [job_id]}
    

async def test_rest_jobs_dataset_counts_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_index': 0,
    }
    ret = await client.request('POST', '/jobs', data)
    job_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/job_counts/status')
    assert ret == {'processing': 1}


async def test_rest_jobs_counts_dataset_status(server):
    client = server(roles=['system'])

    await client.request('POST', '/jobs', {'dataset_id': 'foo', 'job_index': 0})
    await client.request('POST', '/jobs', {'dataset_id': 'foo', 'job_index': 1})
    ret = await client.request('POST', '/jobs', {'dataset_id': 'baz', 'job_index': 0})
    await client.request('PATCH', f'/jobs/{ret["result"]}', {'status': 'suspended'})

    ret = await client.request('GET', '/job_counts/dataset_status', {'dataset_ids': 'foo|baz'})
    assert ret == {
        'foo': {iceprod.server.states.JOB_STATUS_START: 2},
        'baz': {'suspended': 1},
    }

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/job_counts/dataset_status')
    assert exc_info.value.response.status_code == 400


async def test_rest_jobs_counts_dataset_status_user_auth(server):
    client = server(roles=['user'], groups=['users'])

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/job_counts/dataset_status', {'dataset_ids': 'notmine'})
    assert exc_info.value.response.status_code == 403


async def _create_filter_jobs(client, dataset_id='foo', num_jobs=5):
    """Create jobs with job_index 0..num_jobs-1"""
    jobs = {}
    for job_index in range(num_jobs):
        data = {'dataset_id': dataset_id, 'job_index': job_index}
        ret = await client.request('POST', '/jobs', data)
        jobs[ret['result']] = data
    return jobs


async def test_rest_jobs_dataset_get_job_index_range(server):
    client = server(roles=['system'])
    jobs = await _create_filter_jobs(client)

    def expected(fn):
        return {j for j, d in jobs.items() if fn(d['job_index'])}

    ret = await client.request('GET', '/datasets/foo/jobs', {'job_index_min': 3})
    assert set(ret) == expected(lambda i: i >= 3)

    ret = await client.request('GET', '/datasets/foo/jobs', {'job_index_max': 1})
    assert set(ret) == expected(lambda i: i <= 1)

    ret = await client.request('GET', '/datasets/foo/jobs', {'job_index_min': 1, 'job_index_max': 3, 'keys': 'job_index'})
    assert set(ret) == expected(lambda i: 1 <= i <= 3)

    ret = await client.request('GET', '/datasets/foo/jobs', {'job_index': 0})
    assert set(ret) == expected(lambda i: i == 0)


async def test_rest_jobs_dataset_get_job_index_range_errors(server):
    client = server(roles=['system'])
    await _create_filter_jobs(client, num_jobs=1)

    for args in ({'job_index_min': 'a'}, {'job_index_max': '1.5'},
                 {'job_index': 0, 'job_index_min': 0}):
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            await client.request('GET', '/datasets/foo/jobs', args)
        assert exc_info.value.response.status_code == 400, args


async def test_rest_jobs_dataset_get_job_ids(server):
    client = server(roles=['system'])
    jobs = await _create_filter_jobs(client)
    job_ids = list(jobs)

    ret = await client.request('GET', '/datasets/foo/jobs', {'job_ids': '|'.join(job_ids[:2])})
    assert set(ret) == set(job_ids[:2])

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/datasets/foo/jobs', {'job_ids': '|'.join(f'j{i}' for i in range(101))})
    assert exc_info.value.response.status_code == 400


async def test_rest_jobs_dataset_get_filters_user_auth(server):
    client = server(roles=['user'], groups=['users'])

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/datasets/notmine/jobs', {'job_index_min': 0})
    assert exc_info.value.response.status_code == 403
