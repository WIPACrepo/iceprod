from unittest.mock import AsyncMock, MagicMock

from rest_tools.client import RestClient

from iceprod.materialization.materialize import Materialize

def test_materialize_init():
    Materialize(MagicMock())

async def test_materialize_run_one_job(requests_mock):
    rc = RestClient('http://test.iceprod')
    m = Materialize(rc)
    m.buffer_job = AsyncMock(return_value=1)

    requests_mock.get('http://test.iceprod/dataset_summaries/status', json={
        'processing': ['did123']
    })

    requests_mock.get('http://test.iceprod/datasets/did123', json={
        'dataset_id': 'did123',
        'dataset': 123,
        'status': 'processing',
        'tasks_per_job': 1,
        'jobs_submitted': 10,
        'tasks_submitted': 10,
        'debug': False,
    })

    requests_mock.get('http://test.iceprod/datasets/did123/job_counts/status', json={})
    requests_mock.get('http://test.iceprod/datasets/did123/task_counts/status', json={})
    requests_mock.get('http://test.iceprod/datasets/did123/jobs', json={})

    await m.run_once(num=1)

    m.buffer_job.assert_called_once()

async def test_materialize_buffer_job_no_depends(requests_mock):
    rc = RestClient('http://test.iceprod')
    m = Materialize(rc)
    config = {
        'tasks': [
            {
                'name': 'foo',
            }
        ],
        'options': {}
    }
    m.get_config = AsyncMock(return_value=config)
    m.prio = MagicMock()
    m.prio.get_task_prio = AsyncMock(return_value=1.)

    dataset = {
        'dataset_id': 'did123',
        'dataset': 123,
        'status': 'processing',
        'tasks_per_job': 1,
        'jobs_submitted': 10,
        'tasks_submitted': 10,
        'debug': False,
    }

    requests_mock.post('http://test.iceprod/jobs', json={'result': 'j123'})
    requests_mock.post('http://test.iceprod/tasks', json={'result': 't123'})
    requests_mock.patch('http://test.iceprod/tasks/t123', json={})

    ret = await m.buffer_job(dataset, 0)

    assert ret == 1
