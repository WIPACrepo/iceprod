from copy import deepcopy
from datetime import datetime
import logging
import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from iceprod.common.mongo_queue import Message
from iceprod.core.config import Config
from iceprod.core.jsonUtil import json_encode
import iceprod.services.actions.submit
from iceprod.services.actions.edit_config import Action


@pytest.mark.parametrize('config,error', [
    ({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }],
        'version': 3.2,
    }, None),
    ({'tasks':[
        {
            'name': 'testing',
            'trays': []
        }
    ]}, 'Validation error'),
    ({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': '',
                    'data': []
                }]
            }]
        }],
        'version': 3.2,
    }, 'Validation error'),
    ({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': '',
                    'data': []
                }]
            }]
        }],
        'version': 3.1,
    }, None),
])
async def test_edit_config_server_request(config, error, server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)
    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})

    client = server(roles=['user'], groups=['users'])
    args = {
        'dataset_id': 'd1',
        'config': json_encode(config),
        'description': 'description',
    }
    if error:
        with pytest.raises(Exception, match=error):
            await client.request('POST', '/actions/edit_config', args)
    else:
        ret = await client.request('POST', '/actions/edit_config', args)
        assert 'result' in ret

        ret = await client.request('GET', f'/actions/edit_config/{ret["result"]}')
        assert ret['status'] == 'queued'


async def test_edit_config_server_request_bad_args(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)
    requests_mock.post('http://test.iceprod/auths', status_code=200, json={})
    
    client = server(roles=['user'], groups=['users'])
    args = {}
    with pytest.raises(Exception):
        await client.request('POST', '/actions/edit_config', args)


async def test_edit_config_server_request_bad_auth(server, requests_mock):
    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.register_uri('POST', re.compile('localhost'), real_http=True)

    requests_mock.post('http://test.iceprod/auths', status_code=403, json={})

    client = server(roles=['user'], groups=['users'])
    args = {
        'dataset_id': 'd1',
        'config': 'config',
        'description': 'description',
    }
    with pytest.raises(Exception, match='auth failed'):
        await client.request('POST', '/actions/edit_config', args)


@pytest.mark.parametrize('update,error', [
    ({'requirements': {
        'os': ['RHEL_9_x86_64'],
        'cpu': 2,
        'memory': 10.5,
        'time': 0.25,
    }}, None),
    ({'requirements': {
        'os': ['RHEL_9_x86_64'],
        'memory': '$(job)//100+2',
        'time': 0.25,
    }}, 'update requirements'),
    ({'name': 'bar'}, 'task names'),
    ({'depends': ['foo']}, 'task depends'),
])
async def test_edit_config_run(update, error, monkeypatch):
    config = {
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }],
            'data': [
                {
                    'remote': 'token:///data/sim/IceCube/2025/file.i3.zst',
                    'movement': 'input'
                }
            ]
        }],
        'version': 3.2,
    }

    monkeypatch.setattr(iceprod.services.actions.submit, 'TOKEN_PREFIXES', {
        'token://': 'https://iceprod.tokens',
    })
    token_submitter_mock = AsyncMock()
    monkeypatch.setattr(iceprod.services.actions.submit, 'TokenSubmitter', token_submitter_mock)

    queue = AsyncMock()
    api_client = AsyncMock()
    cred_client = AsyncMock()
    submit = Action(queue, logging.getLogger('action'), api_client, cred_client)

    description = 'Test dataset'

    d = Config(config)
    d.fill_defaults()
    d.validate()
    new_config = d.config
    old_config = deepcopy(new_config)
    new_config['tasks'][0].update(update)

    message = Message(
        payload={
            'dataset_id': 'd1',
            'config': json_encode(new_config),
            'description': description,
        },
        uuid=uuid4().hex,
        status='queued',
        priority=0,
        created_at=datetime.now(),
    )

    async def api(method: str, path: str, args: dict[str, Any] = {}):
        if method == 'POST' and path == f'/datasets/{message.payload["dataset_id"]}/task_actions/bulk_requirements/testing':
            return {}
        elif method == 'PUT' and path == f'/datasets/{message.payload["dataset_id"]}/description':
            return {}
        elif method == 'GET' and path == f'/datasets/{message.payload["dataset_id"]}':
            return {
                'dataset': 1,
                'dataset_id': message.payload["dataset_id"],
                'status': 'processing',
                'jobs_submitted': 10,
                'description': 'old description',
                'username': 'user',
                'group': 'users',
            }
        elif method == 'GET' and path == f'/config/{message.payload["dataset_id"]}':
            return old_config
        elif method == 'PUT' and path == f'/config/{message.payload["dataset_id"]}':
            return {}
        else:
            raise Exception('invalid path')

    api_client.request.side_effect = api
    token_submitter_mock.return_value.tokens_exist.return_value = True

    if error:
        with pytest.raises(Exception, match=error):
            await submit.run(message)
    else:
        await submit.run(message)

        api_calls = api_client.request.call_args_list
        assert len(api_calls) == 5
        assert api_calls[0][0][1] == '/datasets/d1'
        assert api_calls[1][0][1] == '/config/d1'
        assert api_calls[2][0][1] == '/datasets/d1/task_actions/bulk_requirements/testing'
        assert api_calls[3][0][1] == '/config/d1'
        assert api_calls[4][0][1] == '/datasets/d1/description'
