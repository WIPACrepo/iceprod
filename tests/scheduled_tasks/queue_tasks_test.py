"""
Test script for scheduled_tasks/queue_tasks
"""

import logging
from unittest.mock import MagicMock

import pytest
from iceprod.scheduled_tasks import queue_tasks

logger = logging.getLogger('scheduled_tasks_queue_tasks_test')


async def test_200_run():
    rc = MagicMock()
    async def client(method, url, args=None):
        if url == '/datasets/foo':
            return {'priority':2}
        elif url == '/task_counts/status':
            return {'waiting':100,'queued':2}
        elif url == '/tasks':
            return {'tasks': [{'task_id': 'task1'}]}
        elif url == '/task_actions/bulk_status/queued' and method == 'POST':
            client.called = True
            return {}
        else:
            raise Exception()
    client.called = False
    rc.request = client
    await queue_tasks.run(rc, debug=True)
    assert client.called
    
    async def client(method, url, args=None):
        if url == '/task_counts/status':
            return {}
        elif url == '/task_actions/bulk_status/queued' and method == 'POST':
            client.called = True
            return {}
        else:
            raise Exception()
    client.called = False
    rc.request = client
    await queue_tasks.run(rc, debug=True)
    assert not client.called
    
    async def client(method, url, args=None):
        if url == '/datasets/foo':
            return {'priority':2}
        elif url.startswith('/task_counts/status'):
            return {}
        elif url == '/task_actions/bulk_status/queued' and method == 'POST':
            client.called = True
            return {}
        else:
            raise Exception()
    client.called = False
    rc.request = client
    await queue_tasks.run(rc, debug=True)
    assert not client.called

    async def client(method, url, args=None):
        if url == '/datasets/foo':
            return {'priority':2}
        elif url.startswith('/task_counts/status'):
            return {'waiting':100,'queued':100000}
        elif url == '/tasks':
            return {'tasks': []}
        elif url == '/task_actions/bulk_status/queued' and method == 'POST':
            client.called = True
            return {}
        else:
            raise Exception()
    client.called = False
    rc.request = client
    await queue_tasks.run(rc, debug=True)
    assert not client.called


async def test_300_run():
    rc = MagicMock()
    async def client(method, url, args=None):
        if url.startswith('/task_counts/status'):
            client.called = True
            return {'waiting':100,'queued':100000}
        else:
            raise Exception()
    client.called = False
    rc.request = client
    with pytest.raises(Exception):
        await queue_tasks.run(rc, debug=True)
    assert client.called

    # internally catch the error
    await queue_tasks.run(rc)
