"""
Test script for scheduled_tasks/dataset_completion
"""

import logging
from unittest.mock import MagicMock

import pytest
from iceprod.scheduled_tasks import dataset_completion

logger = logging.getLogger('scheduled_tasks_dataset_completion_test')


async def test_200_run():
    rc = MagicMock()
    job_counts = {}
    dataset_summaries = {'processing':['foo']}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return dataset_summaries
        elif url == '/config/foo':
            return {}
        elif url == '/datasets/foo':
            return {'jobs_submitted':2, 'tasks_submitted':2}
        elif url.startswith('/datasets/foo/job_counts'):
            return job_counts
        elif url == '/datasets/foo/status' and method == 'PUT':
            client.called = True
            client.status = args['status']
            return {}
        else:
            raise Exception()
    client.called = False
    client.status = None
    rc.request = client

    logger.info('test non-buffered')
    await dataset_completion.run(rc, debug=True)
    assert not client.called

    logger.info('test processing')
    job_counts['processing'] = 2
    await dataset_completion.run(rc, debug=True)
    assert not client.called

    logger.info('test errors')
    job_counts['errors'] = 2
    del job_counts['processing']
    await dataset_completion.run(rc, debug=True)
    assert client.called
    assert client.status == 'errors'

    logger.info('test processing and suspended')
    del job_counts['errors']
    job_counts['processing'] = 1
    job_counts['suspended'] = 1
    client.called = False
    client.status = None
    await dataset_completion.run(rc, debug=True)
    assert not client.called

    logger.info('test suspended')
    del job_counts['processing']
    job_counts['suspended'] = 1
    job_counts['complete'] = 1
    await dataset_completion.run(rc, debug=True)
    assert client.called
    assert client.status == 'suspended'

    logger.info('test complete')
    del job_counts['suspended']
    job_counts['complete'] = 2
    client.called = False
    client.status = None
    await dataset_completion.run(rc, debug=True)
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
        await dataset_completion.run(rc, debug=True)

    # check it normally hides the error
    await dataset_completion.run(rc, debug=False)



async def test_always_active():
    rc = MagicMock()
    dataset_summaries = {'processing':['foo']}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return dataset_summaries
        elif url == '/config/foo':
            return {}
        elif url == '/datasets/foo':
            client.called = True
            return {'jobs_submitted':2, 'tasks_submitted':2, 'always_active': True}
        else:
            raise Exception()
    rc.request = client
    client.called = False

    await dataset_completion.run(rc, debug=True)
    assert client.called
