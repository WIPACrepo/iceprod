"""
Test script for scheduled_tasks/pilot_monitor
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from iceprod.scheduled_tasks import pilot_monitor

logger = logging.getLogger('scheduled_tasks_pilot_monitor_test')


async def test_200_run():
    rc = MagicMock()
    pilots = {}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/pilots'):
            client.called = True
            return pilots
        else:
            raise Exception()
    client.called = False
    rc.request = client
    mon = MagicMock()

    await pilot_monitor.run(rc, mon, debug=True)
    assert client.called
    assert mon.gauge.called

    mon.gauge.reset_mock()
    pilots['a'] = {'tasks':[],'available':{'cpu':1,'time':10},'claimed':{}}
    await pilot_monitor.run(rc, mon, debug=True)
    assert mon.gauge.called


async def test_201_run():
    rc = MagicMock()
    rc.request = AsyncMock(side_effect=Exception())
    mon = MagicMock()
    with pytest.raises(Exception):
        await pilot_monitor.run(rc, mon, debug=True)

    # check it normally hides the error
    await pilot_monitor.run(rc, mon, debug=False)

