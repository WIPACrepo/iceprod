"""
Test script for scheduled_tasks/reset_tasks
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from iceprod.scheduled_tasks import reset_tasks

logger = logging.getLogger('scheduled_tasks_reset_tasks_test')


async def test_200_run():
    rc = MagicMock()
    pilots = {}
    dataset_summaries = {'processing':['foo']}
    tasks = {}
    task = {}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return dataset_summaries
        elif url.startswith('/datasets/foo/task_summaries'):
            return tasks
        elif url == '/datasets/foo':
            return {'debug':False}
        elif url == '/tasks/bar':
            return task
        elif url == '/datasets/foo/tasks/bar/status' and method == 'PUT':
            client.called = True
            return {}
        else:
            raise Exception()
    client.called = False
    rc.request = client

    await reset_tasks.run(rc, debug=True)
    assert not client.called

    tasks['reset'] = ['bar']
    await reset_tasks.run(rc, debug=True)
    assert client.called

    client.called = False
    del dataset_summaries['processing']
    dataset_summaries['truncated'] = ['foo']
    await reset_tasks.run(rc, debug=True)
    assert client.called


async def test_201_run():
    rc = MagicMock()
    pilots = {'a':{}}
    # try tasks error
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return {'processing':['foo']}
        else:
            raise Exception()
    rc.request = client
    with pytest.raises(Exception):
        await reset_tasks.run(rc, debug=True)

    # check it normally hides the error
    await reset_tasks.run(rc, debug=False)

    # try dataset level error
    rc.request = AsyncMock(side_effect=Exception())
    with pytest.raises(Exception):
        await reset_tasks.run(rc, debug=True)

    # check it normally hides the error
    await reset_tasks.run(rc, debug=False)
