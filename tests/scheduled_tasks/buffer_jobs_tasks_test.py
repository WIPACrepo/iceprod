import pytest

from rest_tools.client import RestClient

from iceprod.scheduled_tasks import buffer_jobs_tasks


async def test_buffer_jobs_tasks_run(requests_mock):
    requests_mock.get('http://iceprod.test/datasets', json={'123': {}, '456': {}})
    requests_mock.post('http://iceprod.test/actions/materialization', json={'result': 'matid'})
    requests_mock.get('http://iceprod.test/actions/materialization/matid', json={'status': 'complete'})
    
    rc = RestClient('http://iceprod.test')
    await buffer_jobs_tasks.run(rc, delay=0)


async def test_buffer_jobs_tasks_run_dataset(requests_mock):
    requests_mock.post('http://iceprod.test/actions/materialization', json={'result': 'matid'})
    requests_mock.get('http://iceprod.test/actions/materialization/matid', json={'status': 'complete'})
    
    rc = RestClient('http://iceprod.test')
    await buffer_jobs_tasks.run(rc, only_dataset='12345', delay=0)


async def test_buffer_jobs_tasks_run_fail(requests_mock):
    requests_mock.get('http://iceprod.test/datasets', json={'123': {}, '456': {}})
    requests_mock.post('http://iceprod.test/actions/materialization', json={'result': 'matid'})
    requests_mock.get('http://iceprod.test/actions/materialization/matid', json={'id': 'matid', 'status': 'error'})

    rc = RestClient('http://iceprod.test')
    with pytest.raises(Exception):
        await buffer_jobs_tasks.run(rc, debug=True, delay=0)
