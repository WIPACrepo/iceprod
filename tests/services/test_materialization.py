import re
from unittest.mock import AsyncMock, MagicMock

import pytest
import requests
from rest_tools.client import RestClient

from iceprod.services.actions.materialization.materialize import Materialize


async def test_materialization_server_request(server):
    client = server(roles=['system'])
    args = {
        'dataset_id': 'd123',
    }
    ret = await client.request('POST', '/actions/materialization', args)
    assert 'result' in ret

    ret = await client.request('GET', f'/actions/materialization/{ret["result"]}')
    assert ret['status'] == 'queued'


async def test_materialization_server_request_dataset(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})

    # test system auth
    client = server(roles=['system'])
    ret = await client.request('POST', '/actions/materialization/datasets/d123', {})
    mat_id = ret['result']
    ret = await client.request('POST', '/actions/materialization/datasets/d123', {})
    mat_id2 = ret['result']

    assert mat_id == mat_id2

    # test user auth
    client = server(roles=['user'])
    ret = await client.request('POST', '/actions/materialization/datasets/d123', {})
    mat_id = ret['result']
    ret = await client.request('POST', '/actions/materialization/datasets/d123', {})
    mat_id2 = ret['result']

    assert mat_id == mat_id2

    ret = await client.request('GET', '/actions/materialization/datasets/d123')
    assert ret['status'] == 'queued'
    assert ret['id'] == mat_id

    # test no auth
    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('GET', '/actions/materialization/datasets/d123')
    assert exc_info.value.response.status_code == 403



def test_materialize_init():
    Materialize(MagicMock())

async def test_materialize_run_none(requests_mock):
    rc = RestClient('http://test.iceprod')
    m = Materialize(rc)
    m.buffer_job = AsyncMock(return_value=1)

    requests_mock.get('http://test.iceprod/dataset_summaries/status', json={})

    ret = await m.run_once(num=1)
    assert ret is True
    assert m.buffer_job.call_count == 0

async def test_materialize_run_one_job(requests_mock):
    rc = RestClient('http://test.iceprod')
    m = Materialize(rc)
    m.buffer_job = AsyncMock(return_value=1)

    requests_mock.get('http://test.iceprod/dataset_summaries/status', json={
        'processing': ['did123']
    })

    requests_mock.get('http://test.iceprod/datasets/did123', json={
        'dataset_id': 'did123',
        'dataset': 123,
        'status': 'processing',
        'tasks_per_job': 1,
        'jobs_submitted': 10,
        'tasks_submitted': 10,
        'debug': False,
    })

    requests_mock.get('http://test.iceprod/datasets/did123/job_counts/status', json={})
    requests_mock.get('http://test.iceprod/datasets/did123/task_counts/status', json={})
    requests_mock.get('http://test.iceprod/datasets/did123/jobs', json={})

    await m.run_once(num=1)

    m.buffer_job.assert_called_once()

async def test_materialize_buffer_job_no_depends(requests_mock):
    rc = RestClient('http://test.iceprod')
    m = Materialize(rc)
    config = {
        'tasks': [
            {
                'name': 'foo',
            }
        ],
        'options': {}
    }
    m.get_config = AsyncMock(return_value=config)
    m.prio = MagicMock()
    m.prio.get_task_prio = AsyncMock(return_value=1.)

    dataset = {
        'dataset_id': 'did123',
        'dataset': 123,
        'status': 'processing',
        'tasks_per_job': 1,
        'jobs_submitted': 10,
        'tasks_submitted': 10,
        'debug': False,
    }

    requests_mock.post('http://test.iceprod/jobs', json={'result': 'j123'})
    requests_mock.post('http://test.iceprod/tasks', json={'result': 't123'})
    requests_mock.patch('http://test.iceprod/tasks/t123', json={})

    ret = await m.buffer_job(dataset, 0)

    assert ret == 1


async def test_materialize_buffer_job_incomplete(monkeypatch, requests_mock):
    rc = RestClient('http://test.iceprod')
    m = Materialize(rc)
    config = {
        'tasks': [
            {
                'name': 'foo',
            },
            {
                'name': 'bar',
            },
            {
                'name': 'baz',
            }
        ],
        'options': {}
    }
    m.get_config = AsyncMock(return_value=config)
    prio_mock = MagicMock()
    monkeypatch.setattr('iceprod.services.actions.materialization.materialize.Priority', prio_mock)
    prio_mock.return_value.get_task_prio = AsyncMock(return_value=1.)

    requests_mock.get('http://test.iceprod/datasets/did123', json={
        'dataset_id': 'did123',
        'dataset': 123,
        'status': 'processing',
        'tasks_per_job': 3,
        'jobs_submitted': 10,
        'tasks_submitted': 10,
        'debug': False,
    })
    
    requests_mock.get('http://test.iceprod/datasets/did123/job_counts/status', json={
        'processing': 1,
    })
    requests_mock.get('http://test.iceprod/datasets/did123/task_counts/status', json={
        'idle': 1,
    })
    requests_mock.get('http://test.iceprod/datasets/did123/jobs', json={
        'j123': {
            'job_id': 'j123',
            'job_index': 0,
        },
    })
    requests_mock.get('http://test.iceprod/datasets/did123/tasks', json={
        't0': {
            'task_id': 't0',
            'job_id': 'j123',
            'task_index': 0,
        },
    })

    requests_mock.post('http://test.iceprod/jobs', json={'result': 'j2'})
    requests_mock.post('http://test.iceprod/tasks', json={'result': 't0'})
    requests_mock.patch('http://test.iceprod/tasks/t0', json={})

    ret = await m.run_once(only_dataset='did123', num=0)

    calls = [h for h in requests_mock.request_history if h.url == 'http://test.iceprod/tasks']
    assert len(calls) == 2


async def test_materialize_buffer_job_no_tasks(monkeypatch, requests_mock):
    rc = RestClient('http://test.iceprod')
    m = Materialize(rc)
    config = {
        'tasks': [
            {
                'name': 'foo',
            },
            {
                'name': 'bar',
            },
            {
                'name': 'baz',
            }
        ],
        'options': {}
    }
    m.get_config = AsyncMock(return_value=config)
    prio_mock = MagicMock()
    monkeypatch.setattr('iceprod.services.actions.materialization.materialize.Priority', prio_mock)
    prio_mock.return_value.get_task_prio = AsyncMock(return_value=1.)

    requests_mock.get('http://test.iceprod/datasets/did123', json={
        'dataset_id': 'did123',
        'dataset': 123,
        'status': 'processing',
        'tasks_per_job': 3,
        'jobs_submitted': 10,
        'tasks_submitted': 10,
        'debug': False,
    })
    
    requests_mock.get('http://test.iceprod/datasets/did123/job_counts/status', json={
        'processing': 1,
    })
    requests_mock.get('http://test.iceprod/datasets/did123/task_counts/status', json={
    })
    requests_mock.get('http://test.iceprod/datasets/did123/jobs', json={
        'j123': {
            'job_id': 'j123',
            'job_index': 0,
        },
    })
    requests_mock.get('http://test.iceprod/datasets/did123/tasks', json={
    })

    requests_mock.post('http://test.iceprod/jobs', json={'result': 'j2'})
    requests_mock.post('http://test.iceprod/tasks', json={'result': 't0'})
    requests_mock.patch('http://test.iceprod/tasks/t0', json={})

    ret = await m.run_once(only_dataset='did123', num=0)

    calls = [h for h in requests_mock.request_history if h.url == 'http://test.iceprod/tasks']
    assert len(calls) == 3
