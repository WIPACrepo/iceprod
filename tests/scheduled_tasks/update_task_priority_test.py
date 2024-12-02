"""
Test script for scheduled_tasks/update_task_priority
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from iceprod.scheduled_tasks import update_task_priority

logger = logging.getLogger('scheduled_tasks_update_task_priority_test')


async def test_200_run():
    rc = MagicMock()
    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        if url == '/tasks/bar':
            client.called = True
            return {}
        elif url == '/tasks':
            return {'tasks':[{'task_id':'bar','dataset_id':'foo'}]}
        elif url == '/datasets':
            return {'foo':{'dataset_id':'foo','start_date':'2024-01-01T01:00:00','username':'a','group':'g','tasks_submitted':200,'jobs_submitted':100,'priority':1.}}
        elif url == '/datasets/foo/tasks':
            return {'bar':{'task_id':'bar','dataset_id':'foo','task_index':0,'job_index':12}}
        elif url == '/datasets/foo/tasks/bar':
            return {'task_id':'bar','dataset_id':'foo','task_index':0,'job_index':12}
        elif url == '/users':
            return {'results':[{'username':'a','priority':0.5}]}
        elif url == '/groups':
            return {'results':[{'name':'g','priority':0.5}]}
        else:
            raise Exception()
    client.called = False
    rc.request = client

    await update_task_priority.run(rc, status=['idle', 'waiting'], debug=True)
    assert client.called


async def test_201_run():
    rc = MagicMock()
    pilots = {'a':{}}
    # try tasks error
    rc.request = AsyncMock(side_effect=Exception())

    # check it normally hides the error
    await update_task_priority.run(rc, status=['idle', 'waiting'], debug=False)

    with pytest.raises(Exception):
        await reset_tasks.run(rc, debug=True)

