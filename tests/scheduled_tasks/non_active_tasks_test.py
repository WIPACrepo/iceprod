"""
Test script for scheduled_tasks/non_active_tasks
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from iceprod.scheduled_tasks import non_active_tasks

logger = logging.getLogger('scheduled_tasks_non_active_tasks_test')


async def test_200_run():
    rc = MagicMock()
    pilots = {}
    dataset_summaries = {'processing':['foo']}
    task = {'status':'processing','status_changed':'2000-01-01T00:00:00'}
    tasks = {}
    pilots = {}
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return dataset_summaries
        elif url.startswith('/pilots'):
            return pilots
        elif url.startswith('/datasets/foo/task_summaries'):
            return tasks
        elif url == '/datasets/foo/tasks/bar' and method == 'GET':
            return task
        elif url == '/datasets/foo/tasks/bar/status' and method == 'PUT':
            client.called = True
            return {}
        elif url.startswith('/logs'):
            return {}
        else:
            raise Exception()
    client.called = False
    rc.request = client

    await non_active_tasks.run(rc, debug=True)
    assert not client.called

    tasks['processing'] = ['bar']
    await non_active_tasks.run(rc, debug=True)
    assert client.called

    client.called = False
    del dataset_summaries['processing']
    dataset_summaries['truncated'] = ['foo']
    pilots['a'] = {'tasks':['bar']}
    await non_active_tasks.run(rc, debug=True)
    assert not client.called


async def test_201_run():
    rc = MagicMock()
    pilots = {'a':{}}
    # try tasks error
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url.startswith('/dataset_summaries'):
            return {'processing':['foo']}
        elif url.startswith('/pilots'):
            return pilots
        else:
            raise Exception()
    rc.request = client
    with pytest.raises(Exception):
        await non_active_tasks.run(rc, debug=True)

    # check it normally hides the error
    await non_active_tasks.run(rc, debug=False)

    rc.request = AsyncMock(side_effect=Exception())
    with pytest.raises(Exception):
        await non_active_tasks.run(rc, debug=True)

    # check it normally hides the error
    await non_active_tasks.run(rc, debug=False)
