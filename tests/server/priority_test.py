"""
Test script for priority
"""
import datetime
import time
import logging
from unittest.mock import MagicMock

import pytest
import iceprod.server
from iceprod.server import priority


logger = logging.getLogger('priority_test')


@pytest.fixture(autouse=True)
def set_time(monkeypatch):
    now = datetime.datetime(2024, 1, 1, 1, 10, 0, 0, datetime.UTC)
    mock = MagicMock()
    mock.now = MagicMock(return_value=now)
    monkeypatch.setattr(iceprod.server.priority, 'datetime', mock)
    tnow = time.mktime(now.utctimetuple())
    tmock = MagicMock(return_value=tnow)
    monkeypatch.setattr(time, 'time', tmock)
    yield now


def prio_setup():
    datasets = {
        'd0': {
            'dataset_id': 'd0',
            'jobs_submitted': 10,
            'tasks_submitted': 20,
            'priority': 1,
            'start_date': '2024-01-01T01:00:00',
            'group': 'users',
            'username': 'u_a',
            'tasks': {
                't0': {'task_id': 't0', 'task_index': 0, 'job_index': 0},
                't1': {'task_id': 't1', 'task_index': 1, 'job_index': 0},
            }
        },
        'd1': {
            'dataset_id': 'd1',
            'jobs_submitted': 10,
            'tasks_submitted': 20,
            'priority': 1,
            'start_date': '2024-01-01T01:00:00',
            'group': 'users',
            'username': 'u_b',
            'tasks': {
                't2': {'task_id': 't2', 'task_index': 0, 'job_index': 0},
                't3': {'task_id': 't3', 'task_index': 1, 'job_index': 0},
            }
        },
        'd2': {
            'dataset_id': 'd2',
            'jobs_submitted': 1000,
            'tasks_submitted': 20000,
            'priority': 1,
            'start_date': '2024-01-01T01:00:00',
            'group': 'simprod',
            'username': 'u_a',
            'tasks': {
                't4': {'task_id': 't4', 'task_index': 0, 'job_index': 400},
                't5': {'task_id': 't5', 'task_index': 1, 'job_index': 400},
            }
        },
        'd3': {
            'dataset_id': 'd3',
            'jobs_submitted': 1000,
            'tasks_submitted': 20000,
            'priority': 1,
            'start_date': '2023-10-01T01:00:00',
            'group': 'simprod',
            'username': 'u_a',
            'tasks': {
                't6': {'task_id': 't6', 'task_index': 0, 'job_index': 400},
                't7': {'task_id': 't7', 'task_index': 1, 'job_index': 400},
            }
        },
    }
    users = {
        'u_a': {'username': 'u_a', 'priority': 1.},
        'u_b': {'username': 'u_b', 'priority': .5},
    }

    p = priority.Priority(None)
    p.dataset_cache = datasets
    p.user_cache = users
    return p


async def test_10_get_dataset_prio():
    """Test get_dataset_prio"""
    p = prio_setup()
    prio1 = await p.get_dataset_prio('d0')
    prio2 = await p.get_dataset_prio('d1')
    assert prio2 < prio1

    prio3 = await p.get_dataset_prio('d2')
    assert prio3 < prio1

    prio4 = await p.get_dataset_prio('d3')
    assert prio4 > prio3


async def test_20_get_task_prio():
    """Test get_task_prio"""
    p = prio_setup()
    prio1 = await p.get_task_prio('d0', 't0')
    prio2 = await p.get_task_prio('d1', 't2')
    # equal at 1.0 because of boost
    assert prio2 == prio1

    prio3 = await p.get_task_prio('d2', 't4')
    prio4 = await p.get_task_prio('d2', 't5')
    assert prio3 < prio1 
    assert prio3 < prio4  # ordering of tasks in a job
