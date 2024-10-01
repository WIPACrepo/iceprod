import pytest

from rest_tools.client import RestClient

from iceprod.scheduled_tasks import buffer_jobs_tasks


async def test_buffer_jobs_tasks_run(requests_mock):
    requests_mock.post('http://iceprod.test/', json={'result': 'matid'})
    requests_mock.get('http://iceprod.test/status/matid', json={'status': 'complete'})
    
    rc = RestClient('http://iceprod.test')
    await buffer_jobs_tasks.run(rc, run_once=True, delay=0)


async def test_buffer_jobs_tasks_run_dataset(requests_mock):
    requests_mock.post('http://iceprod.test/request/12345', json={'result': 'matid'})
    requests_mock.get('http://iceprod.test/status/matid', json={'status': 'complete'})
    
    rc = RestClient('http://iceprod.test')
    await buffer_jobs_tasks.run(rc, only_dataset='12345', run_once=True, delay=0)


async def test_buffer_jobs_tasks_run_fail(requests_mock):
    requests_mock.post('http://iceprod.test/', json={'result': 'matid'})
    requests_mock.get('http://iceprod.test/status/matid', json={'status': 'error'})
    
    rc = RestClient('http://iceprod.test')
    with pytest.raises(Exception) as exc_info:
        await buffer_jobs_tasks.run(rc, run_once=True, delay=0)

