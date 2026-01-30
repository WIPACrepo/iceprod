from collections import Counter
from dataclasses import dataclass
import logging
from pprint import pprint
from unittest.mock import MagicMock, AsyncMock

from prometheus_client import REGISTRY
import pytest
import requests.exceptions

from iceprod.util import VERSION_STRING
from iceprod.core.resources import Resources
from iceprod.core.config import Dataset, Job, Task
import iceprod.server.config
import iceprod.server.grid


def test_grid_init():
    override = ['queue.type=test', 'queue.site=bar']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    assert 'gpu' not in g.site_requirements

    pprint(list(REGISTRY.collect()))
    metric = REGISTRY.get_sample_value('iceprod_info', {
        'name': 'bar',
        'type': 'grid',
        'queue_type': 'test',
        'version': VERSION_STRING,
        'exclusive': 'False',
    })
    assert metric == 1

def test_grid_init_gpu():
    override = ['queue.type=test', 'queue.site=grid-gpu']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    assert 'gpu' in g.site_requirements

    pprint(list(REGISTRY.collect()))
    metric = REGISTRY.get_sample_value('iceprod_info', {
        'name': 'grid-gpu',
        'type': 'grid',
        'queue_type': 'test',
        'version': VERSION_STRING,
        'exclusive': 'False',
    })
    assert metric == 1

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


@pytest.mark.parametrize('num_tasks,queue,avail,priorities,out_tasks', [
    (10, {}, {'d1': 50}, {'d1': 0.5}, {'d1': 10}),
    (10, {'d1': 100}, {'d1': 5}, {'d1': 0.5}, {'d1': 5}),
    (10, {}, {'d1': 2, 'd2': 3}, {'d1': 0.5, 'd2': 0.5}, {'d1': 2, 'd2': 3}),
    (10, {'d1': 100, 'd2': 80}, {'d1': 10, 'd2': 10, 'd3': 2}, {'d1': 0.5, 'd2': 0.5, 'd3': 0.5}, {'d2': 8, 'd3': 2}),
    (10, {'d1': 100, 'd2': 80}, {'d1': 10, 'd2': 10, 'd3': 20}, {'d1': 0.5, 'd2': 0.5, 'd3': 0.5}, {'d3': 10}),
    (10, {'d1': 10, 'd2': 21}, {'d1': 20, 'd2': 20}, {'d1': 0.25, 'd2': 0.5}, {'d1': 10}),
    (10, {'d1': 10, 'd2': 19}, {'d1': 20, 'd2': 20}, {'d1': 0.25, 'd2': 0.5}, {'d2': 10}),
])
async def test_grid_get_tasks_to_queue(num_tasks, queue, avail, priorities, out_tasks):
    override = ['queue.type=test', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    async def rest_queue(method, path, args):
        num = args.get('num', 1)
        dataset_exclude = args.get('dataset_deprio', [])
        ret = []
        while len(ret) < num:
            for d in avail:
                if avail[d] > 0 and d not in dataset_exclude:
                    avail[d] -= 1
                    ret.append({'dataset_id': d})
                    break
            else:
                if dataset_exclude:
                    dataset_exclude.pop()
                else:
                    break
        if ret:
            return ret
        r = MagicMock()
        r.status_code = 404
        logging.info('404!')
        raise requests.exceptions.HTTPError(response=r)
    
    def convert(val):
        ret = MagicMock()
        ret.dataset.dataset_id = val['dataset_id']
        return ret

    rc.request = AsyncMock(side_effect=rest_queue)
    g._convert_to_task = AsyncMock(side_effect=convert)
    g._get_resources = MagicMock()
    g._get_priority_object = AsyncMock()
    g._get_priority_object.return_value.get_dataset_prio = AsyncMock(side_effect=lambda d: priorities[d])
    g.queue_dataset_status = MagicMock(return_value={iceprod.server.grid.GridStatus.QUEUED: queue})

    tasks = await g.get_tasks_to_queue(num_tasks)

    out_count = sum(out_tasks.values())

    assert len(tasks) == out_count
    assert Counter(t.dataset.dataset_id for t in tasks) == out_tasks
    assert g._convert_to_task.call_count == out_count
    assert g._get_resources.call_count == out_count


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
    g._get_priority_object = AsyncMock()
    g._get_priority_object.return_value.get_dataset_prio = AsyncMock(return_value=0.5)
    g.queue_dataset_status = MagicMock(return_value={})

    tasks = await g.get_tasks_to_queue(NUM_TASKS)

    assert len(tasks) == 0
    assert rc.request.call_count == 1
    assert g._convert_to_task.call_count == 0
    assert g._get_resources.call_count == 0


async def test_grid_convert_to_task(monkeypatch):
    override = ['queue.type=test', 'queue.check_time=0', 'queue.site_temp=http://foo.bar']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=None)

    TASK = MagicMock()
    DATASET = MagicMock()
    DATASET.config = {'options':{}, 'version': 3.2}
    g.dataset_lookup = AsyncMock(return_value=DATASET)
    rc.request.return_value = {'files': []}

    ret = await g._convert_to_task(TASK)

    assert ret.dataset.config['options']['site_temp'] == 'http://foo.bar'


async def test_grid_get_dataset_credentials():
    override = ['queue.type=test', 'queue.check_time=0', 'queue.site_temp=http://foo.bar', 'oauth_condor_client_id=client']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    rc.request = AsyncMock()
    cred_client = MagicMock()
    cred_client.request = AsyncMock(return_value=['creds'])
    g = iceprod.server.grid.BaseGrid(cfg=cfg, rest_client=rc, cred_client=cred_client)

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
        config={'tasks':[{'task_scopes':{}}]},
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

    ret = await g._get_dataset_credentials(t)
    assert ret == ['creds']
    assert cred_client.request.call_count == 1
    assert cred_client.request.call_args == [('GET', '/datasets/1234/tasks/generate/exchange', {'client_id': 'client'}),]

    cred_client.request = AsyncMock(return_value=['creds2'])
    ret = await g._get_dataset_credentials(t)
    assert ret == ['creds']
    assert cred_client.request.call_count == 0

    
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
    assert r['os'] == ['RHEL_7_x86_64']  # type: ignore

    t.requirements['os'] = ['RHEL_7_x86_64', 'RHEL_8_x86_64']
    r = iceprod.server.grid.BaseGrid._get_resources(t)
    assert r['os'] == ['RHEL_7_x86_64', 'RHEL_8_x86_64']  # type: ignore


@dataclass(kw_only=True, slots=True)
class GT(iceprod.server.grid.GridTask):
    dataset_id: str  # type: ignore
    task_id: str  # type: ignore
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


