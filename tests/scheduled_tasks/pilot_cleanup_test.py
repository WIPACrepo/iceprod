"""
Test script for scheduled_tasks/pilot_cleanup
"""

from datetime import datetime, timedelta
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from iceprod.scheduled_tasks import pilot_cleanup
from iceprod.server.util import datetime2str, nowstr

logger = logging.getLogger('scheduled_tasks_pilot_cleanup_test')


async def test_200_run():
    rc = MagicMock()
    pilots = {}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if method == 'GET' and url.startswith('/pilots'):
            return pilots
        elif method == 'DELETE' and url.startswith('/pilots'):
            client.called = True
        else:
            raise Exception()
    client.called = False
    rc.request = client

    await pilot_cleanup.run(rc, debug=True)
    assert not client.called

    pilots['a'] = {'pilot_id':'a', 'grid_queue_id':'blah', 'last_update':datetime2str(datetime.utcnow()-timedelta(days=30))}
    await pilot_cleanup.run(rc, debug=True)
    assert client.called

    client.called = False
    pilots['a'] = {'pilot_id':'a', 'grid_queue_id':'blah', 'last_update':nowstr()}
    await pilot_cleanup.run(rc, debug=True)
    assert not client.called


async def test_201_run():
    rc = MagicMock()
    rc.request = AsyncMock(side_effect=Exception())
    with pytest.raises(Exception):
        await pilot_cleanup.run(rc, debug=True)

    # check it normally hides the error
    await pilot_cleanup.run(rc, debug=False)
