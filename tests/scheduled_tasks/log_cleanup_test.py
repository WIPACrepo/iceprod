"""
Test script for scheduled_tasks/log_cleanup
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from iceprod.scheduled_tasks import log_cleanup

logger = logging.getLogger('scheduled_tasks_log_cleanup_test')


async def test_200_run():
    rc = MagicMock()
    logs = {}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if method == 'GET' and url.startswith('/logs'):
            return logs
        elif method == 'DELETE' and url.startswith('/logs'):
            client.called = True
        else:
            raise Exception()
    client.called = False
    rc.request = client

    await log_cleanup.run(rc, debug=True)
    assert not client.called

    client.called = False
    logs['a'] = {'log_id':'a'}
    await log_cleanup.run(rc, debug=True)
    assert client.called

async def test_201_run():
    rc = MagicMock()
    rc.request = AsyncMock(side_effect=Exception())
    with pytest.raises(Exception):
        await log_cleanup.run(rc, debug=True)

    # check it normally hides the error
    await log_cleanup.run(rc, debug=False)
