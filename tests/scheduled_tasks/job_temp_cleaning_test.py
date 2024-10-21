"""
Test script for scheduled_tasks/job_temp_cleaning
"""

import logging
from datetime import datetime, timedelta, UTC
from unittest.mock import patch, MagicMock, AsyncMock
from concurrent.futures import ThreadPoolExecutor

import pytest
from iceprod.scheduled_tasks import job_temp_cleaning

logger = logging.getLogger('scheduled_tasks_job_temp_cleaning_test')


class FakeFile:
    def __init__(self, d, s=0):
        self.directory = True
        self.name = d
        self.size = s


@patch('iceprod.scheduled_tasks.job_temp_cleaning.GridFTP')
async def test_scheduled_tasks_job_temp_cleaning_gridftp_list(gridftp):
    executor = ThreadPoolExecutor(max_workers=2)

    # test empty dirs
    gridftp.list.return_value = []
    ret = await job_temp_cleaning.list_dataset_job_dirs_gridftp('', executor=executor)
    assert ret == {}

    # test dataset dir, but no jobs
    gridftp.list.side_effect = [[FakeFile('0')], []]
    ret = await job_temp_cleaning.list_dataset_job_dirs_gridftp('', executor=executor)
    assert ret == {}

    # test dataset dir and job
    gridftp.list.side_effect = [[FakeFile('0')], [FakeFile('1', 1024)]]
    ret = await job_temp_cleaning.list_dataset_job_dirs_gridftp('', executor=executor)
    assert ret == {'0': {'1': 1024}}

    # test dataset dir prefix
    gridftp.list.side_effect = [[FakeFile('1', 1024)]]
    ret = await job_temp_cleaning.list_dataset_job_dirs_gridftp('', prefix='0', executor=executor)
    assert ret == {'0': {'1': 1024}}


@patch('iceprod.scheduled_tasks.job_temp_cleaning.GridFTP')
async def test_scheduled_tasks_job_temp_cleaning_gridftp_rmtree(gridftp):
    executor = ThreadPoolExecutor(max_workers=2)

    await job_temp_cleaning.rmtree_gridftp('foo/0/1', executor=executor)
    assert gridftp.rmtree.called
    assert gridftp.rmtree.call_args[0][0] == 'foo/0/1'


async def test_scheduled_tasks_job_temp_cleaning_s3_list():
    s3 = AsyncMock(job_temp_cleaning.S3)

    data = {'0': {'1': 1024}}
    s3.list.return_value = data

    ret = await job_temp_cleaning.list_dataset_job_dirs_s3('/foo', s3_client=s3)
    assert ret == data
    s3.list.assert_awaited_once_with('/foo')

    s3.list.return_value = data['0']
    ret = await job_temp_cleaning.list_dataset_job_dirs_s3('/foo', prefix='0', s3_client=s3)
    assert ret == data
    s3.list.assert_awaited_with('/foo/0')


async def test_scheduled_tasks_job_temp_cleaning_s3_rmtree():
    s3 = AsyncMock(job_temp_cleaning.S3)
    await job_temp_cleaning.rmtree_s3('/foo', s3_client=s3)
    s3.rmtree.assert_awaited_with('/foo')


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
