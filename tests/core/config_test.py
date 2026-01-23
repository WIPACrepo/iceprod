import logging
import pytest
from rest_tools.client import RestClient

from iceprod.core.config import ConfigSchema, Config, Dataset, Job, Task
from iceprod.server.util import nowstr


def test_config_schema():
    assert ConfigSchema.list_versions() == [3.1, 3.2]

    assert ConfigSchema.schema()['title'] == 'IceProd Dataset Config'
    assert ConfigSchema.schema(3.2)['properties']['version']['default'] == 3.2

    assert ConfigSchema.data_defaults(3.2)['remote'] == ''

def test_config():
    config = {'tasks':[
        {
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }],
            'data': [
                {
                    'remote': 'token:///data/sim/IceCube/2025/file.i3.zst',
                    'movement': 'input'
                }
            ]
        }
    ]}
    c = Config(config)
    c.fill_defaults()
    c.validate()


def test_config_str_ver():
    config = {
        'version': '3.2',
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }
    ]}
    c = Config(config)
    c.fill_defaults()
    c.validate()


def test_config_invalid_data():
    config = {
        'version': 3.2,
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }],
            'data': [
                {
                    'type': 'job_temp',
                    'remote': 'token:///data/sim/IceCube/2025/file.i3.zst',
                    'movement': 'input'
                }
            ]
        }
    ]}
    c = Config(config)
    c.fill_defaults()
    with pytest.raises(Exception, match="remote should be empty"):
        c.validate()


def test_config_manual_scope():
    config = {
        'version': 3.2,
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }],
            'task_files': True,
        }
    ]}
    c = Config(config)
    c.fill_defaults()
    with pytest.raises(Exception, match="token_scopes are required"):
        c.validate()


def test_config_dups():
    config = {'tasks':[
        {
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }],
            'data': [
                {
                    'remote': 'token:///data/sim/IceCube/2025/file.i3.zst',
                    'movement': 'input'
                }
            ]
        },
        {
            'name': 'testing2',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }],
            'data': [
                {
                    'remote': 'token:///data/sim/IceCube/2025/file.i3.zst',
                    'movement': 'input'
                }
            ]
        }
    ]}
    c = Config(config)
    c.fill_defaults()
    c.validate()

    c.config['tasks'][0]['token_scopes']['foo'] = 'bar'
    assert 'foo' not in c.config['tasks'][1]['token_scopes']


def test_dataset_dataclasses():
    with pytest.raises(Exception):
        Dataset()

    d = Dataset('did123', 123, 1, 2, 3, 'processing', 0.5, 'grp', 'usr', False, config={})
    assert d.dataset_id == 'did123'
    assert d.dataset_num == 123
    assert d.jobs_submitted == 1
    assert d.tasks_submitted == 2
    assert d.tasks_per_job == 3
    assert d.status == 'processing'
    assert d.priority == 0.5
    assert d.group == 'grp'
    assert d.user == 'usr'
    assert d.debug is False
    assert d.config == {}


async def test_load_config(requests_mock):
    dataset_id = 'did123'
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'status': 'processing',
        'jobs_submitted': 1,
        'tasks_submitted': 1,
        'tasks_per_job': 1,
        'priority': 0.5,
        'group': 'g123',
        'username': 'u123',
        'debug': False
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
        'status': 'processing',
        'jobs_submitted': 1,
        'tasks_submitted': 1,
        'tasks_per_job': 1,
        'priority': 0.5,
        'group': 'g123',
        'username': 'u123',
        'debug': False
    }
    config_data = {}
    d = Dataset(
        dataset_data['dataset_id'],
        dataset_data['dataset'],
        dataset_data['jobs_submitted'],
        dataset_data['tasks_submitted'],
        dataset_data['tasks_per_job'],
        dataset_data['status'],
        dataset_data['priority'],
        dataset_data['group'],
        dataset_data['username'],
        dataset_data['debug'],
        config_data
    )
    d.fill_defaults()
    logging.info('after defaults: %r', d.config)
    assert d.config['version'] == 3.1
    assert d.config['options'] == {}
    assert d.config['steering'] == {'parameters': {}, 'batchsys': {}, 'data': []}

    
async def test_defaults_refs():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'status': 'processing',
        'jobs_submitted': 1,
        'tasks_submitted': 1,
        'tasks_per_job': 1,
        'priority': 0.5,
        'group': 'g123',
        'username': 'u123',
        'debug': False
    }
    config_data = {
        'steering': {
            'parameters': {'a': 'b'}
        },
        'tasks': [{
            'requirements': {},
            'data': [{
                'remote': 'http://test/file'
            }],
            'trays': [{
                'modules': [{}]
            }]
        }]
    }
    d = Dataset(
        dataset_data['dataset_id'],
        dataset_data['dataset'],
        dataset_data['jobs_submitted'],
        dataset_data['tasks_submitted'],
        dataset_data['tasks_per_job'],
        dataset_data['status'],
        dataset_data['priority'],
        dataset_data['group'],
        dataset_data['username'],
        dataset_data['debug'],
        config_data
    )
    d.fill_defaults()
    logging.info('after defaults: %r', d.config)
    assert d.config['tasks'][0]['requirements']['cpu'] == 1
    assert d.config['tasks'][0]['requirements']['memory'] == 1.0
    assert d.config['tasks'][0]['requirements']['disk'] == 1.0
    assert d.config['tasks'][0]['requirements']['time'] == 1.0
    assert d.config['tasks'][0]['data'][0]['local'] == ''
    assert d.config['tasks'][0]['data'][0]['type'] == 'permanent'
    assert d.config['tasks'][0]['data'][0]['movement'] == 'input'
    assert d.config['tasks'][0]['data'][0]['transfer'] is True
    assert d.config['tasks'][0]['trays'][0]['iterations'] == 1
    assert d.config['tasks'][0]['trays'][0]['modules'][0]['env_shell'] == ''
    assert d.config['tasks'][0]['trays'][0]['modules'][0]['env_clear'] is True

async def test_validate_error():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'status': 'processing',
        'jobs_submitted': 1,
        'tasks_submitted': 1,
        'tasks_per_job': 1,
        'priority': 0.5,
        'group': 'g123',
        'username': 'u123',
        'debug': False
    }
    config_data = {
        'my': 'config'
    }
    d = Dataset(
        dataset_data['dataset_id'],
        dataset_data['dataset'],
        dataset_data['jobs_submitted'],
        dataset_data['tasks_submitted'],
        dataset_data['tasks_per_job'],
        dataset_data['status'],
        dataset_data['priority'],
        dataset_data['group'],
        dataset_data['username'],
        dataset_data['debug'],
        config_data
    )
    with pytest.raises(Exception):
        d.validate()


async def test_validate_valid():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'status': 'processing',
        'jobs_submitted': 1,
        'tasks_submitted': 1,
        'tasks_per_job': 1,
        'priority': 0.5,
        'group': 'g123',
        'username': 'u123',
        'debug': False
    }
    config_data = {
        'tasks': [{
            'name': 'first',
            'trays': [{
                'modules': [{}]
            }]
        }]
    }
    d = Dataset(
        dataset_data['dataset_id'],
        dataset_data['dataset'],
        dataset_data['jobs_submitted'],
        dataset_data['tasks_submitted'],
        dataset_data['tasks_per_job'],
        dataset_data['status'],
        dataset_data['priority'],
        dataset_data['group'],
        dataset_data['username'],
        dataset_data['debug'],
        config_data
    )
    d.fill_defaults()
    d.validate()


def test_job_dataclasses():
    with pytest.raises(Exception):
        Job()

    d = Dataset('did123', 123, 2, 1, 1, 'processing', 0.5, 'grp', 'usr', False, {})
    j = Job(d, 'j123', 1, 'processing')

    assert j.dataset == d
    assert j.job_id == 'j123'
    assert j.job_index == 1
    assert j.status == 'processing'


def test_task_dataclasses():
    with pytest.raises(Exception):
        Task()

    d = Dataset('did123', 123, 2, 1, 1, 'processing', 0.5, 'grp', 'usr', False, {})
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
    assert not t.task_files
    assert not t.instance_id


def test_task_config():
    d = Dataset('did123', 123, 2, 1, 1, 'processing', 0.5, 'grp', 'usr', False, {'tasks':[1,2,3]})
    j = Job(d, 'j123', 1, 'processing')
    t = Task(d, j, 't123', 0, 'foo', [], {}, 'waiting', '', {})

    assert t.get_task_config() == 1


async def test_task_load_from_api(requests_mock):
    dataset_id = 'did123'
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'status': 'processing',
        'jobs_submitted': 1,
        'tasks_submitted': 1,
        'tasks_per_job': 1,
        'priority': 0.5,
        'group': 'g123',
        'username': 'u123',
        'debug': False
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
    config = {'tasks':[
        {
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }
    ]}
    d = Dataset('did123', 123, 2, 1, 1, 'processing', 0.5, 'grp', 'usr', False, config)
    d.fill_defaults()
    d.validate()
    j = Job(d, 'j123', 1, 'processing')
    t = Task(d, j, 't123', 0, 'foo', [], {}, 'waiting', '', {})

    stat_data = {
        'task_stat_id': 'ts123',
        'foo': 'bar',
    }
    requests_mock.get(
        'http://test.iceprod/datasets/did123/tasks/t123/task_stats',
        json={stat_data['task_stat_id']: stat_data},
    )

    r = RestClient('http://test.iceprod')
    await t.load_stats_from_api(r)

    assert t.stats == stat_data


async def test_task_load_task_files(requests_mock):
    config = {'tasks':[
        {
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }],
            'task_files': True
        }
    ]}
    d = Dataset('did123', 123, 2, 1, 1, 'processing', 0.5, 'grp', 'usr', False, config)
    d.fill_defaults()
    d.validate()
    j = Job(d, 'j123', 1, 'processing')
    t = Task(d, j, 't123', 0, 'foo', [], {}, 'waiting', '', {})

    data = [{
        'local': 'foo.txt',
        'remote': 'https://foo.bar.baz/foo.txt',
    }]
    requests_mock.get(
        'http://test.iceprod/datasets/did123/files/t123',
        json={'files': data},
    )

    r = RestClient('http://test.iceprod')
    await t.load_task_files_from_api(r)

    assert t.task_files
    assert len(t.task_files) == len(data)
    assert t.task_files[0]['local'] == data[0]['local']
    assert t.task_files[0]['remote'] == data[0]['remote']
    assert t.task_files[0]['type'] == 'permanent'
    assert t.task_files[0]['movement'] == 'input'
