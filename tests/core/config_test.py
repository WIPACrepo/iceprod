import pytest
from rest_tools.client import RestClient

from iceprod.core.config import Dataset, Job, Task
from iceprod.server.util import nowstr


def test_dataset_dataclasses():
    with pytest.raises(Exception):
        Dataset()

    d = Dataset('did123', 123, 'grp', 'usr', {})
    assert d.dataset_id == 'did123'
    assert d.dataset_num == 123
    assert d.group == 'grp'
    assert d.user == 'usr'
    assert d.config == {}


async def test_load_config(requests_mock):
    dataset_id = 'did123'
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    requests_mock.get(f'http://test.iceprod/datasets/{dataset_id}', json=dataset_data)
    config_data = {
        'my': 'config'
    }
    requests_mock.get(f'http://test.iceprod/config/{dataset_id}', json=config_data)

    r = RestClient('http://test.iceprod')
    d = await Dataset.load_from_api(dataset_id, r)

    
    assert d.dataset_id == dataset_id
    assert d.dataset_num == dataset_data['dataset']
    assert d.group == dataset_data['group']
    assert d.user == dataset_data['username']
    assert d.config == config_data


async def test_defaults():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    config_data = {}
    d = Dataset(dataset_data['dataset_id'], dataset_data['dataset'], dataset_data['group'], dataset_data['username'], config_data)
    d.fill_defaults()
    assert d.config['version'] == 3.1


async def test_validate_error():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    config_data = {
        'my': 'config'
    }
    d = Dataset(dataset_data['dataset_id'], dataset_data['dataset'], dataset_data['group'], dataset_data['username'], config_data)
    with pytest.raises(Exception):
        d.validate()


async def test_validate_valid():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    config_data = {
        'tasks': [{
            'name': 'first',
            'trays': [{
                'modules': [{}]
            }]
        }]
    }
    d = Dataset(dataset_data['dataset_id'], dataset_data['dataset'], dataset_data['group'], dataset_data['username'], config_data)
    d.fill_defaults()
    d.validate()


def test_job_dataclasses():
    with pytest.raises(Exception):
        Job()

    d = Dataset('did123', 123, 'grp', 'usr', {})
    j = Job(d, 'j123', 1, 'processing')

    assert j.dataset == d
    assert j.job_id == 'j123'
    assert j.job_index == 1
    assert j.status == 'processing'


def test_task_dataclasses():
    with pytest.raises(Exception):
        Task()

    d = Dataset('did123', 123, 'grp', 'usr', {})
    j = Job(d, 'j123', 1, 'processing')
    t = Task(d, j, 't123', 0, 'foo', [], {}, 'waiting', '', {})

    assert t.dataset == d
    assert t.job == j
    assert t.task_id == 't123'
    assert t.task_index == 0
    assert t.name == 'foo'
    assert t.depends == []
    assert t.requirements == {}
    assert t.status == 'waiting'
    assert t.site == ''
    assert t.stats == {}


def test_task_config():
    d = Dataset('did123', 123, 'grp', 'usr', {'tasks':[1,2,3]})
    j = Job(d, 'j123', 1, 'processing')
    t = Task(d, j, 't123', 0, 'foo', [], {}, 'waiting', '', {})

    assert t.get_task_config() == 1


async def test_task_load_from_api(requests_mock):
    dataset_id = 'did123'
    dataset_data = {
        'dataset': 123,
        'dataset_id': dataset_id,
        'group': 'g123',
        'username': 'u123',
    }
    requests_mock.get(f'http://test.iceprod/datasets/{dataset_id}', json=dataset_data)
    config_data = {
        'my': 'config'
    }
    requests_mock.get(f'http://test.iceprod/config/{dataset_id}', json=config_data)
    job_data = {
        'dataset_id': dataset_id,
        'job_id': 'j123',
        'job_index': 1,
        'status': 'processing',
    }
    requests_mock.get(f'http://test.iceprod/datasets/{dataset_id}/jobs/{job_data["job_id"]}', json=job_data)
    task_data = job_data | {
        'task_id': 't123',
        'task_index': 0,
        'name': 'foo',
        'depends': [],
        'requirements': {'cpu': 1},
        'status': 'waiting',
        'status_changed': nowstr(),
        'failures': 1,
        'evictions': 0,
        'walltime': 0.0,
        'walltime_err': 0.15,
        'walltime_err_n': 1,
        'site': 'CHTC',
    }
    requests_mock.get(f'http://test.iceprod/datasets/{dataset_id}/tasks/{task_data["task_id"]}', json=task_data)

    r = RestClient('http://test.iceprod')
    t = await Task.load_from_api(dataset_id, task_data['task_id'], r)

    assert t.dataset.dataset_id == dataset_id
    assert t.job.job_id == job_data['job_id']
    assert t.task_id == task_data['task_id']


async def test_task_load_stats(requests_mock):
    d = Dataset('did123', 123, 'grp', 'usr', {'tasks':[1,2,3]})
    j = Job(d, 'j123', 1, 'processing')
    t = Task(d, j, 't123', 0, 'foo', [], {}, 'waiting', '', {})

    stat_data = {
        'task_stat_id': 'ts123',
        'foo': 'bar',
    }
    requests_mock.get(
        f'http://test.iceprod/datasets/did123/tasks/t123/task_stats',
        json={stat_data['task_stat_id']: stat_data},
    )

    r = RestClient('http://test.iceprod')
    await t.load_stats_from_api(r)

    assert t.stats == stat_data
