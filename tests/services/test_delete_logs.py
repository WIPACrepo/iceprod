from dataclasses import asdict
import re
from datetime import datetime
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import requests.exceptions

from iceprod.common.mongo_queue import Message
from iceprod.services.actions.delete_logs import Action, Fields


@pytest.mark.parametrize('args,error', [
    ({'dataset_id': 'd1'}, False),
    ({'dataset_id': 'd1', 'task_id': 't1'}, False),
    ({}, 'dataset_id'),
    ({'dataset_id': ''}, 'invalid dataset_id'),
    ({'dataset_id': 'd1', 'task_id': ''}, 'invalid task_id'),
    ({'dataset_id': 'd1', 'foo': 'bar'}, 'foo'),
])
async def test_delete_logs_server_request(args, error, server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})

    client = server(roles=['user'], groups=['users'])

    if error:
        with pytest.raises(Exception, match=error):
            await client.request('POST', '/actions/delete_logs', args)
    else:
        ret = await client.request('POST', '/actions/delete_logs', args)
        assert 'result' in ret

        ret = await client.request('GET', f'/actions/delete_logs/{ret["result"]}')
        assert ret['status'] == 'queued'
        for k in args:
            assert ret['payload'][k] == args[k]
        assert ret['payload']['progress'] == 0


async def test_delete_logs_server_request_dedupe(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)
    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})

    client = server(roles=['user'], groups=['users'])

    ret1 = await client.request('POST', '/actions/delete_logs', {'dataset_id': 'd1'})
    ret2 = await client.request('POST', '/actions/delete_logs', {'dataset_id': 'd1'})
    assert ret1['result'] == ret2['result']

    # different scope => different message
    ret3 = await client.request('POST', '/actions/delete_logs', {'dataset_id': 'd1', 'task_id': 't1'})
    assert ret3['result'] != ret1['result']


async def test_delete_logs_server_request_bad_auth(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})

    client = server(roles=['user'], groups=['users'])
    with pytest.raises(Exception, match='auth failed'):
        await client.request('POST', '/actions/delete_logs', {'dataset_id': 'd1'})


def _message(args):
    return Message(
        payload=asdict(Fields(**args)),
        uuid=uuid4().hex,
        status='queued',
        priority=0,
        created_at=datetime.now(),
    )


@pytest.mark.parametrize('args', [
    {'dataset_id': 'd1'},
    {'dataset_id': 'd1', 'task_id': 't1'},
])
async def test_delete_logs_run(args):
    queue = AsyncMock()
    api_client = AsyncMock()
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())
    action.BATCH_SIZE = 2

    message = _message(args)
    log_ids = ['l1', 'l2', 'l3', 'l4', 'l5']
    deleted = []

    async def api(method: str, path: str, args: dict[str, Any] = {}):
        if method == 'GET' and path == '/logs':
            assert args['dataset_id'] == message.payload['dataset_id']
            assert args['keys'] == 'log_id'
            if message.payload['task_id']:
                assert args['task_id'] == message.payload['task_id']
            else:
                assert 'task_id' not in args
            return {lid: {'log_id': lid} for lid in log_ids}
        elif method == 'DELETE' and path.startswith('/logs/'):
            deleted.append(path.rsplit('/', 1)[1])
            return {}
        raise Exception('invalid path')

    api_client.request.side_effect = api

    await action.run(message)

    assert sorted(deleted) == log_ids
    progress = [c[0][1]['progress'] for c in queue.update_payload.call_args_list]
    assert progress == [40, 80, 100]
    assert all(c[0][0] == message.uuid for c in queue.update_payload.call_args_list)


async def test_delete_logs_run_empty():
    queue = AsyncMock()
    api_client = AsyncMock()
    api_client.request.return_value = {}
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    await action.run(_message({'dataset_id': 'd1'}))

    api_client.request.assert_called_once()
    assert queue.update_payload.call_args[0][1] == {'progress': 100}


def _http_error(code):
    resp = MagicMock()
    resp.status_code = code
    return requests.exceptions.HTTPError(response=resp)


async def test_delete_logs_run_404_skipped():
    queue = AsyncMock()
    api_client = AsyncMock()
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    async def api(method: str, path: str, args: dict[str, Any] = {}):
        if method == 'GET':
            return {'l1': {'log_id': 'l1'}, 'l2': {'log_id': 'l2'}}
        elif path == '/logs/l1':
            raise _http_error(404)
        return {}

    api_client.request.side_effect = api

    await action.run(_message({'dataset_id': 'd1'}))
    assert queue.update_payload.call_args[0][1] == {'progress': 100}


async def test_delete_logs_run_error():
    queue = AsyncMock()
    api_client = AsyncMock()
    action = Action(queue, logging.getLogger('action'), api_client, AsyncMock())

    async def api(method: str, path: str, args: dict[str, Any] = {}):
        if method == 'GET':
            return {'l1': {'log_id': 'l1'}}
        raise _http_error(500)

    api_client.request.side_effect = api

    with pytest.raises(requests.exceptions.HTTPError):
        await action.run(_message({'dataset_id': 'd1'}))
    queue.update_payload.assert_not_called()
