"""
Test script for scheduled_tasks/dataset_cleanup
"""

import logging
from unittest.mock import MagicMock

import pytest
from iceprod.scheduled_tasks import dataset_cleanup

logger = logging.getLogger('scheduled_tasks_dataset_cleanup_test')


@pytest.mark.parametrize('status', ['suspended', 'errors', 'obsolete'])
async def test_100_run_inactive_dataset(status):
    """Active jobs and tasks in inactive datasets are suspended"""
    calls = []

    async def client(method, url, args=None):
        logger.info('REST: %s, %s', method, url)
        calls.append((method, url, args))
        if url == '/dataset_summaries/status':
            return {status: ['foo'], 'complete': ['bar']}
        elif url == '/datasets/foo/job_summaries/status':
            return {'processing': ['j1'], 'complete': ['j2']}
        elif url == '/datasets/foo/task_summaries/status':
            return {'processing': ['t1'], 'queued': ['t2'], 'complete': ['t3'], 'failed': ['t4']}
        elif method == 'PUT' and url == '/datasets/foo/jobs/j1/status':
            return {}
        elif method == 'POST' and url == '/datasets/foo/task_actions/bulk_status/suspended':
            return {}
        else:
            raise Exception(f'unexpected {method} {url}')
    rc = MagicMock()
    rc.request = client

    await dataset_cleanup.run(rc, debug=True)

    assert ('PUT', '/datasets/foo/jobs/j1/status', {'status': 'suspended'}) in calls
    posts = [c for c in calls if c[0] == 'POST']
    assert len(posts) == 1
    assert sorted(posts[0][2]['tasks']) == ['t1', 't2']
    # complete datasets are left alone
    assert not any('/datasets/bar' in c[1] for c in calls)
