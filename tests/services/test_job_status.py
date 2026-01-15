from dataclasses import asdict
import re
from datetime import datetime
import logging
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from iceprod.common.mongo_queue import Message
from iceprod.services.actions.job_status import Action, Fields


@pytest.mark.parametrize('args,error', [
    ({
        'dataset_id': 'd1',
        'action': 'reset',
    }, False),
    ({
        'dataset_id': 'd1',
        'action': 'hard_reset',
        'initial_status': 'suspended',
    }, False),
    ({
        'dataset_id': 'd1',
        'action': 'suspend',
        'job_ids': ['j1', 'j2'],
    }, False),
    ({
        'dataset_id': 'd1',
        'action': 'foo',
    }, 'invalid action'),
    ({
        'dataset_id': 'd1',
        'action': 'hard_reset',
        'initial_status': 'foo',
    }, 'invalid initial_status'),
    ({
        'dataset_id': 'd1',
        'action': 'reset',
        'initial_status': 'suspended',
        'job_ids': ['j1', 'j2'],
    }, 'cannot define both'),
])
async def test_job_status_server_request(args, error, server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})

    client = server(roles=['user'], groups=['users'])

    if error:
        with pytest.raises(Exception, match=error):
            await client.request('POST', '/actions/job_status', args)
    else:
        ret = await client.request('POST', '/actions/job_status', args)
        assert 'result' in ret

        ret = await client.request('GET', f'/actions/job_status/{ret["result"]}')
        assert ret['status'] == 'queued'
        for k in args:
            assert ret['payload'][k] == args[k]
        assert ret['payload']['progress'] == 0


async def test_job_status_server_request_bad_auth(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})

    client = server(roles=['user'], groups=['users'])
    args = {
        'dataset_id': 'd1',
        'action': 'reset',
    }
    with pytest.raises(Exception, match='auth failed'):
        await client.request('POST', '/actions/job_status', args)


async def test_job_status_server_request_no_auth(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})

    client = server(roles=['system'], groups=[])
    args = {
        'dataset_id': 'd1',
        'action': 'reset',
    }
    await client.request('POST', '/actions/job_status', args)


@pytest.mark.parametrize('args', [
    {
        'dataset_id': 'd1',
        'action': 'reset',
    },
    {
        'dataset_id': 'd1',
        'action': 'hard_reset',
        'initial_status': 'suspended',
    },
    {
        'dataset_id': 'd1',
        'action': 'suspend',
        'job_ids': ['j1', 'j2'],
    },
])
async def test_job_status_run(args):
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
