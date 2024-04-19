"""
Test script for scheduled_tasks/job_completion
"""

import logging
from unittest.mock import MagicMock

import pytest
from iceprod.scheduled_tasks import job_completion

logger = logging.getLogger('scheduled_tasks_job_completion_test')


async def test_200_run():
    rc = MagicMock()
    dataset_summaries = {}
    job_summaries = {}
    tasks = {
        't1':{'task_id':'t1','task_index':1,'status':'processing'},
        't2':{'task_id':'t2','task_index':2,'status':'waiting'},
    }
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return dataset_summaries
        if url.startswith('/datasets/foo/job_summaries'):
            return job_summaries
        elif url.startswith('/datasets/foo/tasks'):
            return tasks
        elif url == '/datasets/foo/jobs/1/status' and method == 'PUT':
            client.called = True
            client.status = args['status']
            return {}
        else:
            raise Exception()
    client.called = False
    client.status = None
    rc.request = client

    await job_completion.run(rc, debug=True)
    assert not client.called

    dataset_summaries['processing'] = ['foo']
    await job_completion.run(rc, debug=True)
    assert not client.called

    job_summaries['processing'] = ['1']
    await job_completion.run(rc, debug=True)
    assert not client.called

    logger.info('test processing')
    await job_completion.run(rc, debug=True)
    assert not client.called

    logger.info('test errors')
    tasks['t1']['status'] = 'failed'
    tasks['t2']['status'] = 'failed'
    await job_completion.run(rc, debug=True)
    assert client.called
    assert client.status == 'errors'

    logger.info('test processing and suspended')
    tasks['t1']['status'] = 'processing'
    tasks['t2']['status'] = 'suspended'
    client.called = False
    client.status = None
    await job_completion.run(rc, debug=True)
    assert not client.called

    logger.info('test suspended')
    tasks['t1']['status'] = 'complete'
    tasks['t2']['status'] = 'suspended'
    await job_completion.run(rc, debug=True)
    assert client.called
    assert client.status == 'suspended'

    logger.info('test complete')
    tasks['t1']['status'] = 'complete'
    tasks['t2']['status'] = 'complete'
    client.called = False
    client.status = None
    await job_completion.run(rc, debug=True)
    assert client.called
    assert client.status == 'complete'


async def test_201_run():
    rc = MagicMock()
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return {'processing':['foo']}
        else:
            raise Exception()
    rc.request = client
    with pytest.raises(Exception):
        await job_completion.run(rc, debug=True)

    # check it normally hides the error
    await job_completion.run(rc, debug=False)
