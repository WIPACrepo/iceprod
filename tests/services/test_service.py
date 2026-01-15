import pytest
import requests
from unittest.mock import AsyncMock, MagicMock

from iceprod.common.mongo_queue import AsyncMongoQueue
from iceprod.services.config import get_config
from iceprod.services.service import main
import iceprod.services.actions.submit

async def test_submit(monkeypatch, mongo_url, mongo_clear):
    monkeypatch.setenv('CI_TESTING', '1')
    action_mock = MagicMock()
    action_mock.return_value.run = AsyncMock(return_value=None)
    monkeypatch.setattr(iceprod.services.actions.submit, 'Action', action_mock)

    config = get_config()
    message_queue = AsyncMongoQueue(
        url=config.DB_URL,
        collection_name='services_queue',
        timeout=config.DB_TIMEOUT,
        write_concern=config.DB_WRITE_CONCERN
    )

    await main()
    assert action_mock.return_value.run.called == False

    await message_queue.push({
        'type': 'submit',
        'foo': 'bar'
    })

    await main()

    assert action_mock.return_value.run.called
    assert action_mock.return_value.run.call_args.args[0].payload == {'foo': 'bar'}

    await message_queue.close()