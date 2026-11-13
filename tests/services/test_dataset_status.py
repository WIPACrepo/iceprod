from dataclasses import asdict
import re
from datetime import datetime
import logging
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from iceprod.common.mongo_queue import Message
from iceprod.services.actions.dataset_status import Action, Fields


@pytest.mark.parametrize('args,error', [
    ({
        'dataset_id': 'd1',
        'action': 'reset',
    }, False),
    ({
        'dataset_id': 'd1',
        'action': 'obsolete',
    }, False),
    ({
        'dataset_id': 'd1',
        'action': 'foo',
    }, 'invalid action'),
])
async def test_dataset_status_server_request(args, error, server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})

    client = server(roles=['user'], groups=['users'])

    if error:
        with pytest.raises(Exception, match=error):
            await client.request('POST', '/actions/dataset_status', args)
    else:
        ret = await client.request('POST', '/actions/dataset_status', args)
        assert 'result' in ret

        ret = await client.request('GET', f'/actions/dataset_status/{ret["result"]}')
        assert ret['status'] == 'queued'
        for k in args:
            assert ret['payload'][k] == args[k]
        assert ret['payload']['progress'] == 0


async def test_dataset_status_server_request_bad_auth(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})

    client = server(roles=['user'], groups=['users'])
    args = {
        'dataset_id': 'd1',
        'action': 'reset',
    }
    with pytest.raises(Exception, match='auth failed'):
        await client.request('POST', '/actions/dataset_status', args)


async def test_dataset_status_server_request_no_auth(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})

    client = server(roles=['system'], groups=[])
    args = {
        'dataset_id': 'd1',
        'action': 'reset',
    }
    await client.request('POST', '/actions/dataset_status', args)


@pytest.mark.parametrize('args', [
    {
        'dataset_id': 'd1',
        'action': 'reset',
    },
])
async def test_dataset_status_run(args):
    queue = AsyncMock()
    api_client = AsyncMock()
    cred_client = AsyncMock()
    submit = Action(queue, logging.getLogger('action'), api_client, cred_client)

    message = Message(
        payload=asdict(Fields(**args)),
        uuid=uuid4().hex,
        status='queued',
        priority=0,
        created_at=datetime.now(),
    )

    async def api(method: str, path: str, args: dict[str, Any] = {}):
        if method == 'POST' and path == f'/datasets/{message.payload["dataset_id"]}/task_actions/bulk_{message.payload["action"]}':
            return {}
        elif method == 'POST' and path == f'/datasets/{message.payload["dataset_id"]}/job_actions/bulk_{message.payload["action"]}':
            return {}
        elif method == 'PUT' and path == f'/datasets/{message.payload["dataset_id"]}/status':
            return {}
        elif method == 'GET' and path == f'/datasets/{message.payload["dataset_id"]}':
            return {
                'status': 'processing'
            }
        elif method == 'GET' and path == f'/datasets/{message.payload["dataset_id"]}/jobs':
            return {
                'j1': {'job_id': 'j1'},
                'j2': {'job_id': 'j2'},
            }
        else:
            raise Exception('invalid path')

    api_client.request.side_effect = api

    await submit.run(message)

    assert queue.update_payload.call_args[0][0] == message.uuid
    assert queue.update_payload.call_args[0][1] == {'progress': 100}


def make_api(dataset_id, dataset_status, calls):
    """Mock REST API for the dataset status action, recording calls"""
    async def api(method: str, path: str, args: dict[str, Any] = {}):
        calls.append((method, path, args))
        if method == 'POST' and path.startswith(f'/datasets/{dataset_id}/task_actions/'):
            return {}
        elif method == 'POST' and path.startswith(f'/datasets/{dataset_id}/job_actions/'):
            return {}
        elif method == 'PUT' and path == f'/datasets/{dataset_id}/status':
            return {}
        elif method == 'GET' and path == f'/datasets/{dataset_id}':
            return {
                'status': dataset_status,
                'jobs_submitted': 2,
                'username': 'user',
                'group': 'users',
            }
        elif method == 'GET' and path == f'/config/{dataset_id}':
            return {}
        elif method == 'GET' and path == f'/datasets/{dataset_id}/jobs':
            return {
                'j1': {'job_id': 'j1'},
                'j2': {'job_id': 'j2'},
            }
        else:
            raise Exception('invalid path')
    return api


def make_message(action):
    return Message(
        payload=asdict(Fields(dataset_id='d1', action=action)),
        uuid=uuid4().hex,
        status='queued',
        priority=0,
        created_at=datetime.now(),
    )


@pytest.mark.parametrize('dataset_status', ['processing', 'suspended', 'errors'])
async def test_dataset_status_run_obsolete(dataset_status):
    queue = AsyncMock()
    calls = []
    api_client = AsyncMock()
    api_client.request.side_effect = make_api('d1', dataset_status, calls)
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    message = make_message('obsolete')
    await action.run(message)

    assert ('PUT', '/datasets/d1/status', {'status': 'obsolete'}) in calls
    # active jobs and tasks are suspended
    assert ('POST', '/datasets/d1/task_actions/bulk_suspend', {'jobs': ['j1', 'j2']}) in calls or \
        ('POST', '/datasets/d1/task_actions/bulk_suspend', {'jobs': ['j2', 'j1']}) in calls
    assert any(c[:2] == ('POST', '/datasets/d1/job_actions/bulk_suspend') for c in calls)
    assert queue.update_payload.call_args[0] == (message.uuid, {'progress': 100})


async def test_dataset_status_run_obsolete_complete():
    queue = AsyncMock()
    calls = []
    api_client = AsyncMock()
    api_client.request.side_effect = make_api('d1', 'complete', calls)
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    message = make_message('obsolete')
    await action.run(message)

    assert ('PUT', '/datasets/d1/status', {'status': 'obsolete'}) in calls
    # nothing active in a complete dataset, so no bulk suspend
    assert not any(c[0] == 'POST' for c in calls)
    assert queue.update_payload.call_args[0] == (message.uuid, {'progress': 100})


async def test_dataset_status_run_obsolete_already():
    queue = AsyncMock()
    calls = []
    api_client = AsyncMock()
    api_client.request.side_effect = make_api('d1', 'obsolete', calls)
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    await action.run(make_message('obsolete'))

    # no status change, but stragglers are still suspended
    assert not any(c[0] == 'PUT' for c in calls)
    assert any(c[:2] == ('POST', '/datasets/d1/task_actions/bulk_suspend') for c in calls)


async def test_dataset_status_run_restore_obsolete():
    queue = AsyncMock()
    calls = []
    api_client = AsyncMock()
    api_client.request.side_effect = make_api('d1', 'obsolete', calls)
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    await action.run(make_message('suspend'))

    assert ('PUT', '/datasets/d1/status', {'status': 'suspended'}) in calls


@pytest.mark.parametrize('reset_action', ['reset', 'hard_reset'])
async def test_dataset_status_run_reset_obsolete(reset_action, monkeypatch):
    tokens_exist = AsyncMock(return_value=True)
    monkeypatch.setattr('iceprod.services.actions.dataset_status.TokenSubmitter.tokens_exist', tokens_exist)

    queue = AsyncMock()
    calls = []
    api_client = AsyncMock()
    api_client.request.side_effect = make_api('d1', 'obsolete', calls)
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    await action.run(make_message(reset_action))

    assert ('PUT', '/datasets/d1/status', {'status': 'processing'}) in calls
    assert any(c[:2] == ('POST', f'/datasets/d1/task_actions/bulk_{reset_action}') for c in calls)
