import asyncio
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock

import pytest
import requests.exceptions

from iceprod.core.resources import Resources
from iceprod.core.config import Dataset, Job, Task
import iceprod.server.config
import iceprod.server.grid


def test_grid_init():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    assert 'gpu' not in g.site_requirements

def test_grid_init_gpu():
    override = ['queue.type=test', 'queue.site=grid-gpu']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    assert 'gpu' in g.site_requirements

def test_grid_init_exclusive():
    override = ['queue.type=test', 'queue.site=grid', 'queue.exclusive=true']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    assert 'requirements.site' in g.site_query_params
    assert g.site_query_params['requirements.site'] == 'grid'

async def test_grid_run():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    with pytest.raises(NotImplementedError):
        await g.run()


async def test_grid_dataset_lookup(monkeypatch):
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    dataset_mock = AsyncMock()
    d = dataset_mock.load_from_api.return_value
    d.fill_defaults = MagicMock()
    d.validate = MagicMock()
    monkeypatch.setattr(iceprod.server.grid, 'Dataset', dataset_mock)
    ret = await g.dataset_lookup('12345')
    assert ret == d
    assert dataset_mock.load_from_api.call_count == 1

    # test cache miss
    ret = await g.dataset_lookup('6789')
    assert ret == d
    assert dataset_mock.load_from_api.call_count == 2

    # test cache hit
    ret = await g.dataset_lookup('12345')
    assert ret == d
    assert dataset_mock.load_from_api.call_count == 2


async def test_grid_get_tasks_to_queue():
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    NUM_TASKS = 2
    rc.request = AsyncMock()
    g._convert_to_task = AsyncMock()
    g._get_resources = MagicMock()

    tasks = await g.get_tasks_to_queue(NUM_TASKS)

    assert len(tasks) == NUM_TASKS
    assert rc.request.call_count == NUM_TASKS
    assert g._convert_to_task.call_count == NUM_TASKS
    assert g._get_resources.call_count == NUM_TASKS


async def test_grid_submit_none():
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    NUM_TASKS = 2
    response = MagicMock()
    response.status_code = 404
    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError(response=response))
    g._convert_to_task = AsyncMock()
    g._get_resources = MagicMock()

    tasks = await g.get_tasks_to_queue(NUM_TASKS)

    assert len(tasks) == 0
    assert rc.request.call_count == 1
    assert g._convert_to_task.call_count == 0
    assert g._get_resources.call_count == 0


async def test_grid_convert_to_task():
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    TASK = MagicMock()
    TASK.load_task_files_from_api = AsyncMock()
    DATASET = MagicMock()
    g.dataset_lookup = AsyncMock(return_value=DATASET)

    ret = await g._convert_to_task(TASK)

    assert ret.dataset == DATASET


    
def test_grid_get_resources(i3prod_path):
    d = Dataset(
        dataset_id='1234',
        dataset_num=1234,
        jobs_submitted=10,
        tasks_submitted=10,
        tasks_per_job=1,
        status='processing',
        priority=.5,
        group='group',
        user='user',
        debug=False,
        config={},
    )
    j = Job(dataset=d, job_id='5678', job_index=3, status='processing')
    t = Task(
        dataset=d,
        job=j,
        task_id='91011',
        task_index=0,
        name='generate',
        depends=[],
        requirements={},
        status='queued',
        site='site',
        stats={},
    )

    r = iceprod.server.grid.BaseGrid._get_resources(t)
    assert list(r.keys()) == list(Resources.defaults.keys())

    t.requirements['cpu'] = 2
    r = iceprod.server.grid.BaseGrid._get_resources(t)
    assert r['cpu'] == 2

    t.requirements['gpu'] = 2
    r = iceprod.server.grid.BaseGrid._get_resources(t)
    assert r['gpu'] == 2

    t.requirements['os'] = 'RHEL_7_x86_64'
    r = iceprod.server.grid.BaseGrid._get_resources(t)
    assert r['os'] == ['RHEL_7_x86_64']

    t.requirements['os'] = ['RHEL_7_x86_64', 'RHEL_8_x86_64']
    r = iceprod.server.grid.BaseGrid._get_resources(t)
    assert r['os'] == ['RHEL_7_x86_64', 'RHEL_8_x86_64']


@dataclass(kw_only=True, slots=True)
class GT(iceprod.server.grid.GridTask):
    dataset_id: str
    task_id: str
    instance_id: str | None = None


async def test_grid_upload_log():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    task = GT(dataset_id='ddd', task_id='ttt')
    name = 'name'
    data = 'the log\ndata'
    await g._upload_log(task, name=name, data=data)

    assert rc.request.call_count == 1

    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError())
    await g._upload_log(task, name=name, data=data)


async def test_grid_upload_stats():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    task = GT(dataset_id='ddd', task_id='ttt')
    stats = {}
    await g._upload_stats(task, stats=stats)

    assert rc.request.call_count == 1
    
    stats = {
        'site': 'Foo'
    }
    await g._upload_stats(task, stats=stats)

    assert rc.request.call_args.args[-1]['site'] == 'Foo'

    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError())
    await g._upload_stats(task, stats=stats)


async def test_grid_task_idle():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    task = GT(dataset_id='ddd', task_id='ttt', instance_id='iii')
    await g.task_idle(task)

    assert rc.request.call_count == 1

    response = MagicMock()
    response.status_code = 404
    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError(response=response))
    await g.task_idle(task)

    response.status_code = 500
    with pytest.raises(requests.exceptions.HTTPError):
        await g.task_idle(task)


async def test_grid_task_processing():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    task = GT(dataset_id='ddd', task_id='ttt', instance_id='iii')
    await g.task_processing(task)

    assert rc.request.call_count == 1

    response = MagicMock()
    response.status_code = 404
    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError(response=response))
    await g.task_processing(task)

    response.status_code = 500
    with pytest.raises(requests.exceptions.HTTPError):
        await g.task_processing(task)


async def test_grid_task_reset():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    task = GT(dataset_id='ddd', task_id='ttt', instance_id='iii')
    await g.task_reset(task)

    assert rc.request.call_count == 1

    response = MagicMock()
    response.status_code = 404
    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError(response=response))
    await g.task_reset(task, reason='reason')

    assert rc.request.call_args.args[-1]['reason'] == 'reason'

    response.status_code = 500
    with pytest.raises(requests.exceptions.HTTPError):
        await g.task_reset(task)


async def test_grid_task_failure(i3prod_path):
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    task = GT(dataset_id='ddd', task_id='ttt', instance_id='iii')
    await g.task_failure(task)

    assert rc.request.call_count == 1

    response = MagicMock()
    response.status_code = 404
    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError(response=response))
    await g.task_failure(task)

    response.status_code = 500
    with pytest.raises(requests.exceptions.HTTPError):
        await g.task_failure(task)

    rc.request = AsyncMock()
    await g.task_failure(task, reason='reason')
    assert rc.request.call_count == 2
    assert rc.request.call_args_list[0].args[-1]['reason'] == 'reason'

    rc.request = AsyncMock()
    stats = {'resources': {'cpu': 1}}
    await g.task_failure(task, stats=stats)
    assert rc.request.call_count == 2

    rc.request = AsyncMock()
    outfile = i3prod_path / 'outfile'
    outfile.write_text('out data')
    await g.task_failure(task, stdout=outfile)
    assert rc.request.call_count == 2

    rc.request = AsyncMock()
    outfile = i3prod_path / 'outfile'
    outfile.write_text('out data')
    errfile = i3prod_path / 'errfile'
    errfile.write_text('err message')
    await g.task_failure(task, stdout=outfile, stderr=errfile, reason='reason')
    assert rc.request.call_count == 4


async def test_grid_task_success(i3prod_path):
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    task = GT(dataset_id='ddd', task_id='ttt', instance_id='iii')
    await g.task_success(task)

    assert rc.request.call_count == 1

    response = MagicMock()
    response.status_code = 404
    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError(response=response))
    await g.task_success(task)

    response.status_code = 500
    with pytest.raises(requests.exceptions.HTTPError):
        await g.task_success(task)

    rc.request = AsyncMock()
    stats = {'resources': {'cpu': 1, 'time': 4.3}, 'site': 'MySite'}
    await g.task_success(task, stats=stats)
    assert rc.request.call_count == 2

    rc.request = AsyncMock()
    outfile = i3prod_path / 'outfile'
    outfile.write_text('out data')
    await g.task_success(task, stdout=outfile)
    assert rc.request.call_count == 2

    rc.request = AsyncMock()
    outfile = i3prod_path / 'outfile'
    outfile.write_text('out data')
    errfile = i3prod_path / 'errfile'
    errfile.write_text('err message')
    await g.task_success(task, stdout=outfile, stderr=errfile)
    assert rc.request.call_count == 3


