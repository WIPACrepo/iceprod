"""
Test script for scheduled_tasks/dataset_monitor
"""

from collections import defaultdict
import logging
from unittest.mock import AsyncMock, MagicMock

from prometheus_client import REGISTRY
import pytest
from iceprod.scheduled_tasks import dataset_monitor

logger = logging.getLogger('scheduled_tasks_dataset_monitor_test')


async def test_200_run():
    rc = MagicMock()
    pilots = {}
    jobs = {}
    tasks = defaultdict(dict)
    stats = {}
    resources = {}
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
        elif url.startswith('/datasets/foo/task_counts/resources'):
            return resources
        if url.startswith('/datasets/foo'):
            return {'dataset':123,'status':'processing','jobs_submitted':1,'tasks_submitted':1,'username':'alice','group':'simprod'}
        else:
            raise Exception()
    client.called = False
    rc.request = client

    await dataset_monitor.run(rc, debug=True)
    assert client.called

    jobs['processing'] = 1
    await dataset_monitor.run(rc, debug=True)
    assert REGISTRY.get_sample_value('iceprod_jobs', {'dataset': '123', 'username': 'alice', 'group': 'simprod', 'status': 'processing'}) == 1

    tasks['generate']['queued'] = 1
    resources['queued'] = {'count': 1, 'cpu': 2, 'gpu': 1, 'memory': 4.0}
    await dataset_monitor.run(rc, debug=True)
    assert REGISTRY.get_sample_value('iceprod_tasks', {'dataset': '123', 'taskname': 'generate', 'username': 'alice', 'group': 'simprod', 'status': 'queued'}) == 1
    owner = {'dataset': '123', 'username': 'alice', 'group': 'simprod'}
    assert REGISTRY.get_sample_value('iceprod_task_resources', {**owner, 'status': 'queued', 'resource': 'cpu'}) == 2
    assert REGISTRY.get_sample_value('iceprod_task_resources', {**owner, 'status': 'queued', 'resource': 'gpu'}) == 1
    assert REGISTRY.get_sample_value('iceprod_task_resources', {**owner, 'status': 'queued', 'resource': 'memory'}) == 4.0
    assert REGISTRY.get_sample_value('iceprod_task_resources', {**owner, 'status': 'processing', 'resource': 'cpu'}) == 0


async def test_201_run():
    rc = MagicMock()
    rc.request = AsyncMock(side_effect=Exception())
    with pytest.raises(Exception):
        await dataset_monitor.run(rc, debug=True)

    # check it normally hides the error
    await dataset_monitor.run(rc, debug=False)
