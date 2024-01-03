import asyncio
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
    iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)


def test_grid_get_submit_dir():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    assert g.get_submit_dir() == g.submit_dir


async def test_grid_run():
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    g.active_jobs.load = AsyncMock()
    g.submit = AsyncMock()
    g.active_jobs.wait = AsyncMock()
    g.active_jobs.check = AsyncMock(side_effect=Exception('halt run'))

    with pytest.raises(Exception, match='halt run'):
        await g.run()

    assert g.active_jobs.load.called
    assert g.submit.called
    assert g.active_jobs.wait.called
    assert g.active_jobs.check.called


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
    ret = await g._dataset_lookup('12345')
    assert ret == d
    assert dataset_mock.load_from_api.call_count == 1

    # test cache miss
    ret = await g._dataset_lookup('6789')
    assert ret == d
    assert dataset_mock.load_from_api.call_count == 2

    # test cache hit
    ret = await g._dataset_lookup('12345')
    assert ret == d
    assert dataset_mock.load_from_api.call_count == 2


async def test_grid_submit():
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    NUM_TASKS = 2
    g.get_queue_num = MagicMock(return_value=NUM_TASKS)
    rc.request = AsyncMock()
    job = iceprod.server.grid.BaseGridJob(task=MagicMock())
    g.convert_task_to_job = AsyncMock(return_value=job)
    g._get_resources = MagicMock(return_value={"cpu":1})
    g.active_jobs.jobs.submit = AsyncMock()

    await g.submit()

    assert g.get_queue_num.called
    assert rc.request.call_count == NUM_TASKS
    assert g.convert_task_to_job.call_count == NUM_TASKS
    assert g._get_resources.call_count == NUM_TASKS
    assert g.active_jobs.jobs.submit.call_count == 1
    assert g.active_jobs.jobs.submit.call_args == (([job,job],),)


async def test_grid_submit_none():
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    NUM_TASKS = 2
    g.get_queue_num = MagicMock(return_value=NUM_TASKS)
    response = MagicMock()
    response.status_code = 404
    rc.request = AsyncMock(side_effect=requests.exceptions.HTTPError(response=response))
    job = iceprod.server.grid.BaseGridJob(task=MagicMock())
    g.convert_task_to_job = AsyncMock(return_value=job)
    g._get_resources = MagicMock(return_value={"cpu":1})
    g.active_jobs.jobs.submit = AsyncMock()

    await g.submit()

    assert g.get_queue_num.called
    assert rc.request.call_count == 1
    assert g.convert_task_to_job.call_count == 0
    assert g._get_resources.call_count == 0
    assert g.active_jobs.jobs.submit.call_count == 0


async def test_grid_convert_task_to_job():
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    TASK = MagicMock()
    DATASET = MagicMock()
    g._dataset_lookup = AsyncMock(return_value=DATASET)
    g.create_submit_dir = AsyncMock()

    ret = await g.convert_task_to_job(TASK)

    assert ret.task.dataset == DATASET
    assert g.create_submit_dir.called


async def test_grid_create_submit_dir(i3prod_path):
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    submit_dir = i3prod_path / 'submit'

    job = iceprod.server.grid.BaseGridJob(task=MagicMock())
    await g.create_submit_dir(job)

    assert job.submit_dir is not None
    assert job.submit_dir.is_relative_to(submit_dir)

    
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


async def test_grid_get_queue_num(i3prod_path):
    override = ['queue.type=test', 'queue.max_total_tasks_on_queue=10', 'queue.max_idle_tasks_on_queue=5', 'queue.max_tasks_per_submit=3']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    n = g.get_queue_num()
    assert n == 3

    JobStatus = iceprod.server.grid.JobStatus
    BaseGridJob = iceprod.server.grid.BaseGridJob
    g.active_jobs.jobs.jobs['1'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    g.active_jobs.jobs.jobs['2'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    g.active_jobs.jobs.jobs['3'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 2
    
    g.active_jobs.jobs.jobs['4'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    g.active_jobs.jobs.jobs['5'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 0

    g.active_jobs.jobs.jobs['1'] = BaseGridJob(task=MagicMock(), status=JobStatus.RUNNING)
    g.active_jobs.jobs.jobs['2'] = BaseGridJob(task=MagicMock(), status=JobStatus.RUNNING)
    g.active_jobs.jobs.jobs['3'] = BaseGridJob(task=MagicMock(), status=JobStatus.RUNNING)
    g.active_jobs.jobs.jobs['4'] = BaseGridJob(task=MagicMock(), status=JobStatus.RUNNING)
    g.active_jobs.jobs.jobs['5'] = BaseGridJob(task=MagicMock(), status=JobStatus.RUNNING)
    g.active_jobs.jobs.jobs['6'] = BaseGridJob(task=MagicMock(), status=JobStatus.RUNNING)
    n = g.get_queue_num()
    assert n == 3

    g.active_jobs.jobs.jobs['7'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    g.active_jobs.jobs.jobs['8'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    g.active_jobs.jobs.jobs['9'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 1

    g.active_jobs.jobs.jobs['10'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 0

    g.active_jobs.jobs.jobs['11'] = BaseGridJob(task=MagicMock(), status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 0
