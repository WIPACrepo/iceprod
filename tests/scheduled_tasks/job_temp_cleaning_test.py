"""
Test script for scheduled_tasks/job_temp_cleaning
"""

import logging
from datetime import datetime, timedelta, UTC
from unittest.mock import MagicMock, AsyncMock

import pytest
from iceprod.scheduled_tasks import job_temp_cleaning

logger = logging.getLogger('scheduled_tasks_job_temp_cleaning_test')


async def test_scheduled_tasks_job_temp_cleaning_run():
    rc = MagicMock()
    jobs = {}

    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/datasets?'):
            return {'0':{'dataset_id':'0', 'dataset':0}}
        elif url.startswith('/datasets/0/jobs'):
            return jobs
        else:
            raise Exception()
    rc.request = AsyncMock(side_effect=client)

    listdir = AsyncMock()
    rmtree = AsyncMock()

    path = '/foo'
    data = {'0': {'1': 1024}}

    # empty dir
    listdir.return_value = {}
    await job_temp_cleaning.run(rc, path, listdir, rmtree, debug=True)
    assert rc.request.await_count == 1
    listdir.assert_awaited()
    rmtree.assert_not_awaited()

    # dir with job, but no job in db
    rc.request = AsyncMock(side_effect=client)
    listdir = AsyncMock(return_value=data)
    await job_temp_cleaning.run(rc, path, listdir, rmtree, debug=True)
    listdir.assert_awaited()
    assert rc.request.await_count == 2
    rmtree.assert_not_awaited()

    # dir with completed job
    jobs['bar'] = {'job_index': 1, 'status': 'complete'}
    rc.request = AsyncMock(side_effect=client)
    listdir = AsyncMock(return_value=data)
    await job_temp_cleaning.run(rc, path, listdir, rmtree, debug=True)
    assert rc.request.await_count == 2
    listdir.assert_awaited()
    rmtree.assert_awaited_once_with(path+'/0/1')

    # dir with recent suspended job
    now = datetime.now(UTC)
    jobs['bar'] = {
        'job_index': 1,
        'status': 'suspended',
        'status_changed': now.strftime('%Y-%m-%dT%H:%M:%S.%f'),
    }
    rc.request = AsyncMock(side_effect=client)
    listdir = AsyncMock(return_value=data)
    rmtree.reset_mock()
    await job_temp_cleaning.run(rc, path, listdir, rmtree, debug=True)
    assert rc.request.await_count == 2
    listdir.assert_awaited()
    rmtree.assert_not_awaited()

    # dir with old suspended job
    now = datetime.now(UTC)
    jobs['bar'] = {
        'job_index': 1,
        'status': 'suspended',
        'status_changed': (now-timedelta(days=100)).strftime('%Y-%m-%dT%H:%M:%S'),
    }
    rc.request = AsyncMock(side_effect=client)
    listdir = AsyncMock(return_value=data)
    await job_temp_cleaning.run(rc, path, listdir, rmtree, debug=True)
    assert rc.request.await_count == 2
    listdir.assert_awaited()
    rmtree.assert_awaited_once_with(path+'/0/1')

    # error in rmtree
    rc.request = AsyncMock(side_effect=client)
    listdir = AsyncMock(return_value=data)
    rmtree = AsyncMock(side_effect=Exception('bad rmtree'))
    with pytest.raises(Exception, match='bad rmtree'):
        await job_temp_cleaning.run(rc, path, listdir, rmtree, debug=True)
    assert rc.request.await_count == 2
    listdir.assert_awaited()
    rmtree.assert_awaited_once_with(path+'/0/1')

    # error in listdir
    rc.request = AsyncMock(side_effect=client)
    listdir = AsyncMock(side_effect=Exception('bad listdir'))
    rmtree = AsyncMock()
    with pytest.raises(Exception, match='bad listdir'):
        await job_temp_cleaning.run(rc, path, listdir, rmtree, debug=True)
    assert rc.request.await_count == 0
    listdir.assert_awaited()
    rmtree.assert_not_awaited()
