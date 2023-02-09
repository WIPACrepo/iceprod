import pytest

from rest_tools.client import RestClient

from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import buffer_jobs_tasks


@pytest.fixture
def cfg(tmp_path):
    return {
        'queue':{
            'init_queue_interval':0.1,
            'submit_dir': str(tmp_path),
            '*':{'type':'Test1','description':'d'},
        },
        'master':{
            'url':False,
        },
        'materialization': {
            'url': 'http://iceprod.test',
        },
        'site_id':'abcd',
    }


def test_schedule_buffer_jobs_tasks(cfg, monkeypatch):
    monkeypatch.setenv('CI_TESTING', '1')
    s = schedule(cfg, None, None, None)
    buffer_jobs_tasks.buffer_jobs_tasks(s)


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

