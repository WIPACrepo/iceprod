from datetime import datetime
import logging
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from iceprod.common.mongo_queue import Message
from iceprod.core.config import Config
from iceprod.core.jsonUtil import json_encode
import iceprod.services.actions.submit
from iceprod.services.actions.submit import Action, get_scope


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
        'version': 3.1,
    }, 'Validation error'),
])
async def test_submit_server_request(config, error, server):
    client = server(roles=['user'], groups=['users'])
    args = {
        'config': json_encode(config),
        'description': 'description',
        'jobs_submitted': 10,
    }
    if error:
        with pytest.raises(Exception, match=error):
            await client.request('POST', '/actions/submit', args)
    else:
        ret = await client.request('POST', '/actions/submit', args)
        assert 'result' in ret

        ret = await client.request('GET', f'/actions/submit/{ret["result"]}')
        assert ret['status'] == 'queued'


async def test_submit_server_request_bad_args(server):
    client = server(roles=['user'], groups=['users'])
    args = {}
    with pytest.raises(Exception):
        await client.request('POST', '/actions/submit', args)


@pytest.mark.parametrize('path,movement,scope', [
    ('/data/user/foo', 'input', 'storage.read:/data/user'),
    ('/data/user/foo', 'output', 'storage.modify:/data/user'),
    ('/data/user/foo', 'both', 'storage.modify:/data/user'),
    ('', 'input', 'storage.read:/'),
    ('/data/user/$(iter)/foo', 'input', 'storage.read:/data/user'),
    ('/data/user$/$(iter)/foo', 'input', 'storage.read:/data'),
    ('/data/user/foo/000000-000999/bar', 'input', 'storage.read:/data/user/foo'),
    ('/data/exp/IceCube/2025/filtered/PFFilt/0612/PFFilt_PhysicsFiltering_Run00141027_Subrun00000000_00000074.tar.bz', 'input', 'storage.read:/data/exp/IceCube/2025/filtered/PFFilt'),
    ('/data/exp/IceCube/2025/filtered/dev/off.7/0612/Run00141027_89/Offline_IC86.2025_data_Run00141027_Subrun00000000_00000073.i3.zst.sha512', 'input', 'storage.read:/data/exp/IceCube/2025/filtered/dev/off.7'),
])
def test_submit_scope(path, movement, scope):
    assert get_scope(path, movement) == scope


@pytest.mark.parametrize('config,scope', [
    ({
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
    }, 'storage.read:/data/sim/IceCube/2025'),
    ({
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
            ],
            'token_scopes': {
                'token://': 'storage.write:/foo/bar'
            }
        }],
        'version': 3.2,
    }, 'storage.read:/data/sim/IceCube/2025 storage.write:/foo/bar')
])
async def test_submit_run(config, scope, monkeypatch):
    monkeypatch.setattr(iceprod.services.actions.submit, 'TOKEN_PREFIXES', {
        'token://': 'https://iceprod.tokens',
    })

    queue = AsyncMock()
    api_client = AsyncMock()
    cred_client = AsyncMock()
    submit = Action(queue, logging.getLogger('action'), api_client, cred_client)

    description = 'Test dataset'

    d = Config(config)
    d.fill_defaults()
    d.validate()
    config_valid = d.config

    api_client.request.return_value = {'result': 'dataset'}
    cred_client.request.return_value = 'thetoken'

    await submit.run(Message(
        payload={
            'config': json_encode(config_valid),
            'description': description,
            'jobs_submitted': 10,
            'username': 'user',
            'group': 'users',
            'extra_submit_fields': '{"always_active":true}',
        },
        uuid=uuid4().hex,
        status='queued',
        priority=0,
        created_at=datetime.now(),
    ))

    api_calls = api_client.request.call_args_list
    assert len(api_calls) == 2
    assert api_calls[0][0][1] == '/datasets'
    assert api_calls[0][0][2] == {
        'description': description,
        'jobs_submitted': 10,
        'tasks_submitted': 10,
        'tasks_per_job': 1,
        'username': 'user',
        'group': 'users',
        'always_active': True,
    }
    assert api_calls[1][0][1] == '/config/dataset'

    cred_calls = cred_client.request.call_args_list
    assert len(cred_calls) == 2
    assert cred_calls[0][0][1] == '/create'
    assert cred_calls[0][0][2]['scope'] == scope
    assert cred_calls[1][0][1] == '/datasets/dataset/credentials'
