"""
Test script for scheduled_tasks/dataset_monitor
"""

from collections import defaultdict
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from iceprod.scheduled_tasks import dataset_monitor

logger = logging.getLogger('scheduled_tasks_dataset_monitor_test')


async def test_200_run():
    rc = MagicMock()
    pilots = {}
    jobs = {}
    tasks = defaultdict(dict)
    stats = {}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries/status'):
            return {'processing':['foo']}
        elif url.startswith('/datasets/foo/job_counts/status'):
            return jobs
        elif url.startswith('/datasets/foo/task_counts/name_status'):
            client.called = True
            return tasks
        elif url.startswith('/datasets/foo/task_stats'):
            return stats
        if url.startswith('/datasets/foo'):
            return {'dataset':123,'status':'processing','jobs_submitted':1,'tasks_submitted':1}
        else:
            raise Exception()
    client.called = False
    rc.request = client
    mon = MagicMock()

    await dataset_monitor.run(rc, mon, debug=True)
    assert client.called
    assert mon.gauge.called

    jobs['processing'] = 1
    mon.reset_mock()
    await dataset_monitor.run(rc, mon, debug=True)
    assert mon.gauge.called

    tasks['generate']['queued'] = 1
    mon.reset_mock()
    await dataset_monitor.run(rc, mon, debug=True)
    assert mon.gauge.called


async def test_201_run():
    rc = MagicMock()
    rc.request = AsyncMock(side_effect=Exception())
    mon = MagicMock()
    with pytest.raises(Exception):
        await dataset_monitor.run(rc, mon, debug=True)

    # check it normally hides the error
    await dataset_monitor.run(rc, mon, debug=False)
