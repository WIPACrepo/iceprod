import asyncio
import logging
from pathlib import Path
import os
import shutil
import time
from unittest.mock import MagicMock, AsyncMock

import htcondor
import pytest
import requests.exceptions

from iceprod.core.resources import Resources
from iceprod.core.config import Dataset, Job, Task
from iceprod.core.exe import Data
import iceprod.server.config
import iceprod.server.grid
import iceprod.server.plugins.condor

htcondor.enable_debug()


@pytest.fixture
def schedd(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(htcondor, 'Schedd', mock)
    yield mock


def test_grid_init(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)


def test_CondorJobId():
    j1 = iceprod.server.plugins.condor.CondorJobId(cluster_id=0, proc_id=0)
    assert str(j1) == '0.0'


def test_CondorSubmit_init(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)
    set(sub.transfer_plugins.keys()) == {'gsiftp'}


def test_CondorSubmit_condor_os_reqs():
    ret = iceprod.server.plugins.condor.CondorSubmit.condor_os_reqs('RHEL_6_x86_64')
    assert 'OpSysAndVer' in ret
    assert 'OSGVO_OS_STRING' in ret


def test_CondorSubmit_condor_resource_reqs():
    task = Task(
        dataset=MagicMock(),
        job=MagicMock(),
        task_id='task',
        task_index=0,
        name='generate',
        depends=[],
        requirements={'cpu': 1},
        status='queued',
        site='site',
        stats={},
    )

    ret = iceprod.server.plugins.condor.CondorSubmit.condor_resource_reqs(task)
    assert ret['request_cpus'] == 1
    assert 'request_memory' not in ret
    assert 'request_gpus' not in ret
    assert 'requirements' not in ret

    task.requirements = {
        'gpu': 1,
    }
    ret = iceprod.server.plugins.condor.CondorSubmit.condor_resource_reqs(task)
    assert ret['request_gpus'] == 1

    task.requirements = {
        'os': ['RHEL_7_x86_64'],
    }
    ret = iceprod.server.plugins.condor.CondorSubmit.condor_resource_reqs(task)
    assert 'RHEL' in ret['requirements']


def test_CondorSubmit_condor_infiles(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    infiles = [
        Data(url='http://foo.test/foo', local='foo')
    ]

    ret = sub.condor_infiles(infiles)
    assert ret['transfer_input_files'] == ['http://foo.test/foo']
    assert 'PreCmd' not in ret


def test_CondorSubmit_condor_infiles_gsiftp(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    infiles = [
        Data(url='gsiftp://foo.test/foo', local='foo')
    ]

    with pytest.raises(RuntimeError, match='x509 proxy'):
        sub.condor_infiles(infiles)

    cfg['queue']['x509proxy'] = '/tmp/x509'

    ret = sub.condor_infiles(infiles)
    assert ret['transfer_input_files'] == ['/tmp/x509', 'gsiftp://foo.test/foo']
    assert 'PreCmd' not in ret


def test_CondorSubmit_condor_precmd(schedd, i3prod_path):
    logging.info('cfgfile: %r', os.path.exists(os.path.expandvars('$I3PROD/etc/iceprod_config.json')))
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    infiles = [
        Data(url='http://foo.test/foo', local='bar')
    ]

    logging.info('cfg: %r', cfg)

    ret = sub.condor_infiles(infiles)
    logging.info('ret: %r', ret)
    assert ret['transfer_input_files'][0] == 'http://foo.test/foo'
    assert 'PreCmd' in ret
    assert 'PreArguments' in ret
    assert 'bar' in ret['PreArguments']


def test_CondorSubmit_condor_outfiles(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    outfiles = [
        Data(url='http://foo.test/foo', local='bar')
    ]

    ret = sub.condor_outfiles(outfiles)
    assert ret['transfer_output_files'] == ['bar']
    assert ret['transfer_output_remaps'] == 'bar = http://foo.test/foo'


async def test_CondorSubmit_submit(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)
    sub.condor_schedd.submit = MagicMock()

    task = Task(
        dataset=MagicMock(),
        job=MagicMock(),
        task_id='task',
        task_index=0,
        name='generate',
        depends=[],
        requirements={'cpu': 1},
        status='queued',
        site='site',
        stats={},
    )

    jobs = [
        task
    ]

    jel = submit_dir / 'today' / 'condor.log'
    await sub.submit(jobs, jel=jel)

    assert (submit_dir / 'today' / 'task').is_dir()

    assert sub.condor_schedd.submit.call_count == 1


async def test_Grid_save_load_timestamp(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.last_event_timestamp = 12345.
    g.save_timestamp()

    g.last_event_timestamp = 0
    g.load_timestamp()
    assert g.last_event_timestamp == 12345.


async def test_Grid_run(schedd, i3prod_path):
    override = ['queue.type=htcondor', 'queue.submit_interval=0', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submit = AsyncMock()
    g.wait = AsyncMock()
    g.check = AsyncMock()

    await g.run(forever=False)
    assert g.submit.call_count == 1
    assert g.wait.call_count == 2
    assert g.check.call_count == 1


async def test_Grid_run_error(schedd, i3prod_path):
    override = ['queue.type=htcondor', 'queue.submit_interval=0', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submit = AsyncMock(side_effect=RuntimeError())
    g.wait = AsyncMock(side_effect=RuntimeError())
    g.check = AsyncMock(side_effect=RuntimeError())

    await g.run(forever=False)
    assert g.submit.call_count == 1
    assert g.wait.call_count == 2
    assert g.check.call_count == 1


async def test_Grid_submit(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.get_queue_num = MagicMock(return_value=5)
    tasks = [MagicMock(), MagicMock()]
    g.get_tasks_to_queue = AsyncMock(return_value=tasks)
    jel_path = i3prod_path/'today'/'jel.log'
    jel_path.parent.mkdir()
    g.get_current_JEL = MagicMock(return_value=jel_path)
    g.submitter.submit = AsyncMock()
    g.task_reset = AsyncMock()

    await g.submit()
    assert g.get_queue_num.call_count == 1
    assert g.get_tasks_to_queue.call_count == 1
    assert g.get_current_JEL.call_count == 1
    assert g.submitter.submit.call_count == 2
    assert g.task_reset.call_count == 0


async def test_Grid_submit_error(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.get_queue_num = MagicMock(return_value=5)
    tasks = [MagicMock(), MagicMock()]
    g.get_tasks_to_queue = AsyncMock(return_value=tasks)
    jel_path = i3prod_path/'today'/'jel.log'
    jel_path.parent.mkdir()
    g.get_current_JEL = MagicMock(return_value=jel_path)
    g.submitter.submit = AsyncMock(side_effect=RuntimeError())
    g.task_reset = AsyncMock()

    await g.submit()
    assert g.get_queue_num.call_count == 1
    assert g.get_tasks_to_queue.call_count == 1
    assert g.get_current_JEL.call_count == 1
    assert g.submitter.submit.call_count == 2
    assert g.task_reset.call_count == 2


async def test_Grid_get_queue_num(schedd, i3prod_path):
    override = ['queue.type=htcondor', 'queue.max_total_tasks_on_queue=10', 'queue.max_idle_tasks_on_queue=5', 'queue.max_tasks_per_submit=3']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    n = g.get_queue_num()
    assert n == 3

    JobStatus = iceprod.server.plugins.condor.JobStatus
    CondorJob = iceprod.server.plugins.condor.CondorJob
    CondorJobId = iceprod.server.plugins.condor.CondorJobId
    g.jobs[CondorJobId(cluster_id=1, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    g.jobs[CondorJobId(cluster_id=2, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    g.jobs[CondorJobId(cluster_id=3, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 2

    g.jobs[CondorJobId(cluster_id=4, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    g.jobs[CondorJobId(cluster_id=5, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 0

    g.jobs[CondorJobId(cluster_id=1, proc_id=0)] = CondorJob(status=JobStatus.RUNNING)
    g.jobs[CondorJobId(cluster_id=2, proc_id=0)] = CondorJob(status=JobStatus.RUNNING)
    g.jobs[CondorJobId(cluster_id=3, proc_id=0)] = CondorJob(status=JobStatus.RUNNING)
    g.jobs[CondorJobId(cluster_id=4, proc_id=0)] = CondorJob(status=JobStatus.RUNNING)
    g.jobs[CondorJobId(cluster_id=5, proc_id=0)] = CondorJob(status=JobStatus.RUNNING)
    g.jobs[CondorJobId(cluster_id=6, proc_id=0)] = CondorJob(status=JobStatus.RUNNING)
    g.jobs[CondorJobId(cluster_id=7, proc_id=0)] = CondorJob(status=JobStatus.RUNNING)
    n = g.get_queue_num()
    assert n == 3

    g.jobs[CondorJobId(cluster_id=8, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    g.jobs[CondorJobId(cluster_id=9, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 1

    g.jobs[CondorJobId(cluster_id=10, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 0

    g.jobs[CondorJobId(cluster_id=11, proc_id=0)] = CondorJob(status=JobStatus.IDLE)
    n = g.get_queue_num()
    assert n == 0


async def test_Grid_get_current_JEL(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.get_current_JEL()

    assert len(g.jels) == 1


async def test_Grid_wait_no_events(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    await g.wait(timeout=0)


async def test_Grid_wait_JEL(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.task_idle = AsyncMock()
    g.task_processing = AsyncMock()
    g.task_reset = AsyncMock()
    g.finish = AsyncMock()

    jel_path = g.get_current_JEL()
    TEST_JEL = Path(__file__).parent / 'condor_test_logfile'
    shutil.copy(TEST_JEL, jel_path)

    # 0 = success, transfer output
    # 1 = success, input and output
    # 2 = success, chirp to ulog
    # 3 = failure
    # 4 = hold, memory exceeded
    # 5 = success, job evicted
    # 6 = condor_rm

    await g.wait(timeout=0)

    assert int(g.last_event_timestamp) == 1710261003
    assert len(g.jobs) == 7

    JobStatus = iceprod.server.plugins.condor.JobStatus
    CondorJobId = iceprod.server.plugins.condor.CondorJobId
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].dataset_id == '4ksd8'
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].task_id == 'lnk3f'
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].submit_dir == Path('/scratch/dschultz')
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].status == JobStatus.COMPLETED
    
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=1)].status == JobStatus.COMPLETED
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=2)].status == JobStatus.COMPLETED
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=3)].status == JobStatus.FAILED
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=4)].status == JobStatus.FAILED
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=5)].status == JobStatus.COMPLETED
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=6)].status == JobStatus.FAILED

    assert g.task_idle.call_count == 1
    assert g.task_processing.call_count == 7
    assert g.task_reset.call_count == 0
    assert g.finish.call_count == 6


async def test_Grid_wait_JEL_finish(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.task_idle = AsyncMock()
    g.task_processing = AsyncMock()
    g.task_reset = AsyncMock()
    g.task_failure = AsyncMock()
    g.task_success = AsyncMock()

    jel_path = g.get_current_JEL()
    TEST_JEL = Path(__file__).parent / 'condor_test_logfile'
    shutil.copy(TEST_JEL, jel_path)

    await g.wait(timeout=0)

    assert int(g.last_event_timestamp) == 1710261003
    assert len(g.jobs) == 1

    CondorJobId = iceprod.server.plugins.condor.CondorJobId
    assert list(g.jobs.keys()) == [CondorJobId(cluster_id=110828038, proc_id=4)]

    assert g.task_idle.call_count == 1
    assert g.task_processing.call_count == 7
    assert g.task_reset.call_count == 0
    assert g.task_failure.call_count == 2
    assert g.task_success.call_count == 4


async def test_Grid_wait_JEL_exception(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.task_idle = AsyncMock(side_effect=RuntimeError)
    g.task_processing = AsyncMock(side_effect=RuntimeError)
    g.task_reset = AsyncMock(side_effect=RuntimeError)
    g.task_failure = AsyncMock(side_effect=RuntimeError)
    g.task_success = AsyncMock(side_effect=RuntimeError)

    jel_path = g.get_current_JEL()
    TEST_JEL = Path(__file__).parent / 'condor_test_logfile'
    shutil.copy(TEST_JEL, jel_path)

    await g.wait(timeout=0)

    assert int(g.last_event_timestamp) == 1710261003
    assert len(g.jobs) == 1

    JobStatus = iceprod.server.plugins.condor.JobStatus
    CondorJobId = iceprod.server.plugins.condor.CondorJobId
    assert list(g.jobs.keys()) == [CondorJobId(cluster_id=110828038, proc_id=4)]

    assert g.task_idle.call_count == 1
    assert g.task_processing.call_count == 7
    assert g.task_reset.call_count == 0
    assert g.task_failure.call_count == 2
    assert g.task_success.call_count == 4


async def test_Grid_wait_JEL_reprocess(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.task_idle = AsyncMock()
    g.task_processing = AsyncMock()
    g.task_reset = AsyncMock()
    g.finish = AsyncMock()

    g.last_event_timestamp = 1710261004

    jel_path = g.get_current_JEL()
    TEST_JEL = Path(__file__).parent / 'condor_test_logfile'
    shutil.copy(TEST_JEL, jel_path)

    await g.wait(timeout=0)

    assert int(g.last_event_timestamp) == 1710261004
    assert len(g.jobs) == 0


async def test_Grid_check_empty(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submitter.remove = MagicMock()

    await g.check()

    dirs = {x.name: list(x.iterdir()) for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')}
    assert dirs == {}


async def test_Grid_check_delete_day(schedd, i3prod_path):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submitter.remove = MagicMock()

    jel = g.get_current_JEL()
    p = jel.parent
    t = time.time() - 60
    os.utime(p, (t, t))
    logging.info('set time to %d', t)
    
    assert g.jels != {}

    await g.check()

    dirs = {x.name: [x for x in x.iterdir() if x.is_dir()] for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')}
    assert dirs == {}
    assert g.jels == {}


async def test_Grid_check_old(schedd, i3prod_path):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submitter.remove = MagicMock()

    jel = g.get_current_JEL()
    daydir = jel.parent
    p = daydir / 'olddir'
    p.mkdir()
    t = time.time() - 60
    os.utime(p, (t, t))
    logging.info('set time to %d', t)

    await g.check()

    dirs = {x.name: [x for x in x.iterdir() if x.is_dir()] for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')}
    assert dirs == {daydir.name: []}


async def test_Grid_check_oldjob(schedd, i3prod_path):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submitter.remove = MagicMock()

    jel = g.get_current_JEL()
    daydir = jel.parent
    p = daydir / 'olddir'
    p.mkdir()
    t = time.time() - 25
    os.utime(p, (t, t))
    logging.info('set time to %d', t)
    
    JobStatus = iceprod.server.plugins.condor.JobStatus
    CondorJob = iceprod.server.plugins.condor.CondorJob
    CondorJobId = iceprod.server.plugins.condor.CondorJobId
    g.jobs[CondorJobId(cluster_id=1, proc_id=0)] = CondorJob(status=JobStatus.IDLE, submit_dir=p)

    await g.check()

    dirs = {x.name: [x for x in x.iterdir() if x.is_dir()] for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')}
    assert dirs == {daydir.name: [p]}
    assert g.submitter.remove.call_count == 1

