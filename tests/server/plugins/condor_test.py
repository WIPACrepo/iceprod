from collections import Counter
import datetime
import json
import logging
from pathlib import Path
import os
import shutil
import time
from unittest.mock import MagicMock, AsyncMock

import htcondor2 as htcondor
import pytest

from iceprod.core.config import Job, Task
from iceprod.core.exe import Data, Transfer
import iceprod.server.config
import iceprod.server.grid
from iceprod.server.util import datetime2str
import iceprod.server.plugins.condor
from iceprod.server.plugins.condor import CondorJob, CondorJobId, JobStatus

htcondor.enable_debug()


@pytest.fixture
def schedd(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(htcondor, 'Schedd', mock)
    yield mock


@pytest.fixture
def credd(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(htcondor, 'Credd', mock)
    yield mock


@pytest.fixture
def set_time(monkeypatch):
    now = datetime.datetime(2024, 10, 10, 10, 50, 0, 0, datetime.UTC)
    mock = MagicMock()
    mock.now = MagicMock(return_value=now)
    monkeypatch.setattr(iceprod.server.plugins.condor, 'datetime', mock)
    tnow = time.mktime(now.utctimetuple())
    tmock = MagicMock(return_value=tnow)
    monkeypatch.setattr(time, 'time', tmock)
    yield now


def test_grid_init(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)


def test_CondorJobId():
    j1 = CondorJobId(cluster_id=0, proc_id=0)
    assert str(j1) == '0.0'


def test_CondorSubmit_init(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)
    assert set(sub.transfer_plugins.keys()) == {'gsiftp', 'iceprod-plugin'}


def test_CondorSubmit_condor_os_container():
    ret = iceprod.server.plugins.condor.CondorSubmit.condor_os_container('RHEL_6_x86_64')
    assert 'el6' in ret


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


def test_CondorSubmit_condor_infiles(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    infiles = [
        Data(url='http://foo.test/foo', local='foo', transfer=Transfer.TRUE)
    ]

    ret = sub.condor_infiles(infiles, {})
    assert ret['transfer_input_files'] == ['iceprod-plugin://true-http://foo.test/foo']
    assert 'PreCmd' not in ret


def test_CondorSubmit_condor_infiles_maybe(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    infiles = [
        Data(url='http://foo.test/foo', local='foo', transfer=Transfer.MAYBE)
    ]

    ret = sub.condor_infiles(infiles, {})
    assert ret['transfer_input_files'] == ['iceprod-plugin://maybe-http://foo.test/foo']
    assert 'PreCmd' not in ret


def test_CondorSubmit_condor_infiles_gsiftp(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    infiles = [
        Data(url='gsiftp://foo.test/foo', local='foo', transfer=Transfer.TRUE)
    ]

    with pytest.raises(RuntimeError, match='x509 proxy'):
        sub.condor_infiles(infiles, {})

    cfg['queue']['x509proxy'] = '/tmp/x509'

    ret = sub.condor_infiles(infiles, {})
    assert ret['transfer_input_files'] == ['/tmp/x509', 'iceprod-plugin://true-gsiftp://foo.test/foo']
    assert 'PreCmd' not in ret


# skip this because we're using the iceprod transfer plugin
@pytest.mark.skip
def test_CondorSubmit_condor_precmd(schedd, i3prod_path):
    logging.info('cfgfile: %r', os.path.exists(os.path.expandvars('$I3PROD/etc/iceprod_config.json')))
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    infiles = [
        Data(url='http://foo.test/foo', local='bar', transfer=Transfer.TRUE)
    ]

    logging.info('cfg: %r', cfg)

    ret = sub.condor_infiles(infiles, {})
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
        Data(url='http://foo.test/foo', local='bar', transfer=Transfer.TRUE)
    ]

    ret = sub.condor_outfiles(outfiles, {})
    assert ret['transfer_output_files'] == ['bar']
    assert ret['transfer_output_remaps'] == 'bar = http://foo.test/foo'


def test_CondorSubmit_condor_outfiles_maybe(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    outfiles = [
        Data(url='http://foo.test/foo', local='bar', transfer=Transfer.MAYBE)
    ]

    ret = sub.condor_outfiles(outfiles, {})
    assert ret['transfer_output_files'] == ['bar']
    assert ret['transfer_output_remaps'] == 'bar = iceprod-plugin://maybe-http://foo.test/foo'


def test_CondorSubmit_condor_oauth_transform(schedd):
    override = ['queue.type=condor', 'oauth_services.token://=ttt', 'oauth_services.token2=t2']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    handle = 'handle'
    scopes = {
        'token://': 'storage.read:/ storage.write:/data'
    }
    services = sub.condor_oauth_url_transform(handle, scopes)
    assert services == {'token://': 'ttt.handle+token://'}

    scopes = {
        'token://': 'storage.read:/ storage.write:/data',
        'token2': 'storage.read:/'
    }
    services = sub.condor_oauth_url_transform(handle, scopes)
    assert services == {'token://': 'ttt.handle+token://', 'token2': 't2.handle+token2'}

    scopes = {
        'token3': 'storage.read:/'
    }
    with pytest.raises(RuntimeError, match='unknown token scope url prefix'):
        sub.condor_oauth_url_transform(handle, scopes)


def test_CondorSubmit_condor_oauth_scratch(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    cfg['queue']['x509proxy'] = '/tmp/x509'
    cfg['oauth_services'] = {
        'token://foo.bar': 'token'
    }
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    cred_dir.mkdir(parents=True)
    cred_path = cred_dir / 'scratch'
    exp = time.time() + 100
    cred = {"type":"oauth","refresh_token":"refresh","access_token":"access","expiration":exp}
    with open(cred_path, 'w') as f:
        json.dump([cred], f)

    dataset = MagicMock()
    dataset.dataset_id = 'dataset'
    dataset.dataset_num = 0
    dataset.config = {
        'options': {
            'site_temp': 'token://foo.bar/scratch',
        },
        'steering': {
            'parameters': {},
        },
        'tasks': [{
            'name': 'generate',
            'batchsys': {},
            'requirements': {},
            'token_scopes': {},
            'trays': [{
                'iterations': 1,
                'modules': [{
                    'name': 'foo',
                    'src': 'foo.py',
                    'args': '',
                    'env_shell': None,
                    'env_clear': True,
                    'configs': None,
                }]
            }],
            'data': [{
                'movement': 'output',
                'local': 'foo.tgz',
                'remote': '',
                'type': 'job_temp',
                'transfer': True,
            }]
        }]
    }
    dataset.fill_defaults()
    dataset.validate()
    logging.info('config: %r', dataset.config)
    job = Job(
        dataset=dataset,
        job_id='job',
        job_index=1,
        status='processing',
    )
    task = Task(
        dataset=dataset,
        job=job,
        task_id='task',
        task_index=0,
        name='generate',
        depends=[],
        requirements={'cpu': 1},
        status='queued',
        site='site',
        stats={},
    )

    ret = sub.condor_oauth_scratch(task)
    assert ret
    prefix, ret_cred = ret
    assert prefix == 'token://foo.bar'
    assert ret_cred == cred


def test_CondorSubmit_add_oauth_tokens(schedd, credd):
    override = ['queue.type=condor', 'queue.site_temp=http://foo.bar']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    cfg['queue']['x509proxy'] = '/tmp/x509'
    cfg['oauth_services'] = {
        'token://foo.bar': 'token'
    }
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)

    transforms = {
        'token://foo.bar': 'token.datasettaskname+token://foo.bar'
    }
    tokens = [
        {
            'url': 'http://issuer.bar',
            'type': 'oauth',
            'transfer_prefix': 'token://foo.bar',
            'access_token': 'access',
            'refresh_token': 'refresh',
            'scope': 'storage.modify:/baz',
            'expiration': time.time() + 10.4,
        }
    ]

    add_cred = credd.return_value.add_user_service_cred = MagicMock()

    sub.add_oauth_tokens(transforms, tokens)

    logging.info('check_call_args: %r', add_cred.call_args_list)
    assert add_cred.call_count == 2
    assert add_cred.call_args_list[0].kwargs['refresh'] == False
    assert json.loads(add_cred.call_args_list[0].kwargs['credential']) == {
        "access_token": "access",
        "token_type": "bearer",
        "expires_in": 10,
        "expires_at": tokens[0]["expiration"],
        "scope": tokens[0]["scope"].split(),
    }
    assert add_cred.call_args_list[1].kwargs['refresh'] == True
    assert json.loads(add_cred.call_args_list[1].kwargs['credential']) == {
        "refresh_token": "refresh",
        "scopes": tokens[0]["scope"],
    }


async def test_CondorSubmit_submit(schedd):
    override = ['queue.type=condor', 'queue.site_temp=pelican://foo.bar/scratch']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    cfg['oauth_services'] = {
        'osdf:///': 'token',
        'pelican://foo.bar': 'pelican',
    }
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))
    cred_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['credentials_dir'])))

    cred_dir.mkdir(parents=True)
    cred_path = cred_dir / 'scratch'
    exp = time.time() + 100
    cred = {"type":"oauth","refresh_token":"refresh","access_token":"access","expiration":exp}
    with open(cred_path, 'w') as f:
        json.dump([cred], f)

    sub = iceprod.server.plugins.condor.CondorSubmit(cfg=cfg, submit_dir=submit_dir, credentials_dir=cred_dir)
    sub.condor_schedd.submit = MagicMock()
    sub._restart_schedd = MagicMock()
    sub.add_oauth_tokens = MagicMock()

    dataset = MagicMock()
    dataset.dataset_id = 'dataset'
    dataset.dataset_num = 0
    dataset.config = {
        'options': {
            'site_temp': 'pelican://foo.bar/scratch',
        },
        'steering': {
            'parameters': {},
        },
        'tasks': [{
            'name': 'generate',
            'batchsys': {},
            'requirements': {},
            'token_scopes': {
                'osdf:///': 'storage.modify:/baz',
            },
            'trays': [{
                'iterations': 1,
                'modules': [{
                    'name': 'foo',
                    'src': 'foo.py',
                    'args': '',
                    'env_shell': None,
                    'env_clear': True,
                    'configs': None,
                }]
            }],
            'data': [{
                'movement': 'output',
                'local': 'foo.tgz',
                'remote': '',
                'type': 'job_temp',
                'transfer': True,
            }, {
                'movement': 'input',
                'type': 'permanent',
                'local': '',
                'remote': 'gsiftp://foo.bar/baz',
                'transfer': 'maybe',
            }, {
                'movement': 'output',
                'type': 'permanent',
                'local': '',
                'remote': 'osdf:///baz',
                'transfer': 'maybe',
            }]
        }]
    }
    job = Job(
        dataset=dataset,
        job_id='job',
        job_index=1,
        status='processing',
    )
    tokens = [
        {
            'url': 'http://issuer.bar',
            'transfer_prefix': 'osdf:///',
            'access_token': 'access',
            'refresh_token': 'refresh',
        }
    ]
    task = Task(
        dataset=dataset,
        job=job,
        task_id='task',
        task_index=0,
        name='generate',
        depends=[],
        requirements={'cpu': 1},
        status='queued',
        site='site',
        stats={},
        oauth_tokens=tokens,
    )

    jobs = [
        task
    ]

    jel = submit_dir / 'today' / 'condor.log'
    await sub.submit(jobs, jel=jel)

    assert (submit_dir / 'today' / 'task').is_dir()

    assert sub.condor_schedd.submit.call_count == 1
    itemdata = list(sub.condor_schedd.submit.call_args.kwargs['itemdata'])[0]
    logging.info('itemdata: %r', itemdata)
    assert itemdata['infiles'].strip('"') == ''
    assert itemdata['outfiles'].strip('"') == ''
    assert itemdata['outremaps'].strip('"') == ''

    assert sub.add_oauth_tokens.call_count == 2
    assert sub.add_oauth_tokens.call_args_list[1].args == ({'osdf:///': 'token.datasetgenerate+osdf:///'}, tokens)
    assert sub.add_oauth_tokens.call_args_list[0].args == ({'pelican://foo.bar/scratch': 'pelican.scratch+pelican://foo.bar/scratch'}, [cred])

    submitfile = sub.condor_schedd.submit.call_args.args[0]
    logging.info('submitfile: %s', submitfile)
    assert submitfile['My.OAuthServicesNeeded'] == '"token*datasetgenerate pelican*scratch"'

    exe = open(submit_dir / 'today' / 'task' / 'task_runner.sh').read()
    logging.info('exe: %s', exe)
    assert 'object put foo.tgz pelican://foo.bar/scratch/0/1/foo.tgz' in exe
    assert 'if [ -f baz ]' in exe
    assert 'object put baz osdf:///baz' in exe

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
    assert g.check.call_count == 2


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
    assert g.check.call_count == 2


def test_Grid_queue_dataset_status(schedd, i3prod_path):
    override = ['queue.type=htcondor', 'queue.submit_interval=0', 'queue.check_time=0']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submit = AsyncMock(side_effect=RuntimeError())
    g.wait = AsyncMock(side_effect=RuntimeError())
    g.check = AsyncMock(side_effect=RuntimeError())

    ret = g.queue_dataset_status()
    assert not ret

    g.jobs[CondorJobId(cluster_id=1, proc_id=0)] = CondorJob(status=JobStatus.IDLE, dataset_id='d1')
    ret = g.queue_dataset_status()
    assert ret == {iceprod.server.grid.GridStatus.QUEUED: {'d1': 1}}

    g.jobs[CondorJobId(cluster_id=2, proc_id=0)] = CondorJob(status=JobStatus.RUNNING, dataset_id='d1')
    g.jobs[CondorJobId(cluster_id=2, proc_id=1)] = CondorJob(status=JobStatus.RUNNING, dataset_id='d1')
    ret = g.queue_dataset_status()
    assert ret == {iceprod.server.grid.GridStatus.QUEUED: {'d1': 1},
                   iceprod.server.grid.GridStatus.PROCESSING: {'d1': 2}}

    g.jobs[CondorJobId(cluster_id=3, proc_id=0)] = CondorJob(status=JobStatus.COMPLETED, dataset_id='d1')
    ret = g.queue_dataset_status()
    assert ret == {iceprod.server.grid.GridStatus.QUEUED: {'d1': 1},
                   iceprod.server.grid.GridStatus.PROCESSING: {'d1': 2}}

    g.jobs[CondorJobId(cluster_id=4, proc_id=0)] = CondorJob(status=JobStatus.RUNNING, dataset_id='d2')
    ret = g.queue_dataset_status()
    assert ret == {iceprod.server.grid.GridStatus.QUEUED: {'d1': 1},
                   iceprod.server.grid.GridStatus.PROCESSING: {'d1': 2, 'd2': 1}}


async def test_Grid_submit(schedd, i3prod_path):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)
    g.get_scratch_credentials = AsyncMock()

    g.get_queue_num = MagicMock(return_value=5)
    tasks = [MagicMock(), MagicMock()]
    g.get_tasks_to_queue = AsyncMock(return_value=tasks)
    jel_path = i3prod_path/'today'/'jel.log'
    jel_path.parent.mkdir()
    g.get_current_JEL = MagicMock(return_value=jel_path)
    g.submitter.submit = AsyncMock(return_value={})
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
    g.get_scratch_credentials = AsyncMock()

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


async def test_Grid_get_current_JEL(schedd, i3prod_path, set_time):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    ret = g.get_current_JEL()
    assert set_time.strftime('%Y-%m-%d') in str(ret) 

    assert len(g.jels) == 1


async def test_Grid_wait_no_events(schedd, i3prod_path, set_time):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    await g.wait(timeout=0)


async def test_Grid_wait_JEL(schedd, i3prod_path, set_time):
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

    #assert len(g.jobs) == 7

    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].dataset_id == '4ksd8'
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].task_id == 'lnk3f'
    assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].submit_dir == Path('/scratch/dschultz')
    #assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=0)].status == JobStatus.COMPLETED
    
    #assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=1)].status == JobStatus.COMPLETED
    #assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=2)].status == JobStatus.COMPLETED
    #assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=3)].status == JobStatus.FAILED
    #assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=4)].status == JobStatus.FAILED
    #assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=5)].status == JobStatus.COMPLETED
    #assert g.jobs[CondorJobId(cluster_id=110828038, proc_id=6)].status == JobStatus.FAILED

    assert g.task_idle.call_count == 1
    assert g.task_processing.call_count == 7
    assert g.task_reset.call_count == 0
    #assert g.finish.call_count == 6


async def test_Grid_wait_JEL_finish(schedd, i3prod_path, set_time):
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

    #assert len(g.jobs) == 1
    assert CondorJobId(cluster_id=110828038, proc_id=4) in g.jobs

    assert g.task_idle.call_count == 1
    assert g.task_processing.call_count == 7
    assert g.task_reset.call_count == 0
    #assert g.task_failure.call_count == 2
    #assert g.task_success.call_count == 4


async def test_Grid_wait_JEL_exception(schedd, i3prod_path, set_time):
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

    #assert len(g.jobs) == 1
    assert CondorJobId(cluster_id=110828038, proc_id=4) in g.jobs

    assert g.task_idle.call_count == 1
    assert g.task_processing.call_count == 7
    assert g.task_reset.call_count == 0
    #assert g.task_failure.call_count == 2
    #assert g.task_success.call_count == 4


async def test_Grid_wait_JEL_reprocess(schedd, i3prod_path, set_time):
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


async def test_Grid_check_empty(schedd, i3prod_path, set_time):
    override = ['queue.type=htcondor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submitter.get_jobs = MagicMock(return_value={})
    g.submitter.get_history = MagicMock(return_value={})
    g.submitter.remove = MagicMock()
    g.get_tasks_on_queue = AsyncMock(return_value=[])

    await g.check()

    dirs = {x.name: list(x.iterdir()) for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]')}
    assert dirs == {}


async def test_Grid_check_delete_day(schedd, i3prod_path, set_time):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submitter.get_jobs = MagicMock(return_value={})
    g.submitter.get_history = MagicMock(return_value={})
    g.submitter.remove = MagicMock()
    g.get_tasks_on_queue = AsyncMock(return_value=[])

    jel = g.get_current_JEL()
    p = jel.parent
    t = time.mktime(set_time.utctimetuple())
    
    assert g.jels != {}

    await g.check()

    dirs = {x.name: [x for x in x.iterdir() if x.is_dir()] for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]')}
    assert dirs == {}
    assert g.jels == {}


async def test_Grid_check_old_delete(schedd, i3prod_path, set_time):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    g.submitter.get_jobs = MagicMock(return_value={})
    g.submitter.get_history = MagicMock(return_value={})
    g.submitter.remove = MagicMock()
    g.get_tasks_on_queue = AsyncMock(return_value=[])

    jel = g.get_current_JEL()
    daydir = jel.parent
    p = daydir / 'olddir'
    p.mkdir()
    t = time.mktime(set_time.utctimetuple()) - 35  # must be older than all times added together
    os.utime(p, (t, t))
    logging.info('set time to %d', t)

    await g.check()

    dirs = {x.name: [x for x in x.iterdir() if x.is_dir()] for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]')}
    assert dirs == {daydir.name: []}


async def test_Grid_check_oldjob_remove(schedd, i3prod_path, set_time):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    jobs = {}
    g.submitter.get_jobs = MagicMock(return_value=jobs)
    g.submitter.get_history = MagicMock(return_value={})
    g.submitter.remove = MagicMock()
    g.get_tasks_on_queue = AsyncMock(return_value=[])

    jel = g.get_current_JEL()
    daydir = jel.parent
    p = daydir / 'olddir'
    p.mkdir()
    t = time.mktime(set_time.utctimetuple()) - 25  # must be older than queued + processing time
    os.utime(p, (t, t))
    logging.info('set time to %d', t)

    jobs[CondorJobId(cluster_id=1, proc_id=0)] = CondorJob(status=JobStatus.IDLE, submit_dir=p, task_id=p.name)

    await g.check()

    dirs = {x.name: [x for x in x.iterdir() if x.is_dir()] for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]')}
    assert dirs == {daydir.name: [p]}
    assert g.submitter.remove.call_count == 1


async def test_Grid_check_oldjob_delete(schedd, i3prod_path, set_time):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    jobs = {}
    g.submitter.get_jobs = MagicMock(return_value=jobs)
    g.submitter.get_history = MagicMock(return_value={})
    g.submitter.remove = MagicMock()
    g.get_tasks_on_queue = AsyncMock(return_value=[])

    jel = g.get_current_JEL()
    daydir = jel.parent
    p = daydir / 'olddir'
    p.mkdir()
    t = time.mktime(set_time.utctimetuple()) - 15  # must be older than suspend time
    os.utime(p, (t, t))
    logging.info('set time to %d', t)

    await g.check()

    dirs = {x.name: [x for x in x.iterdir() if x.is_dir()] for x in g.submit_dir.glob('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]')}
    assert dirs == {daydir.name: []}
    assert g.submitter.remove.call_count == 0


@pytest.mark.parametrize('jel_jobs,queue_jobs,hist_jobs,remove_calls,finish_calls', [
    ({(1,0): JobStatus.IDLE},
     {(1,0): JobStatus.IDLE},
     {},
     0, 0
    ),
    ({(1,0): JobStatus.IDLE},
     {},
     {(1,0): JobStatus.COMPLETED},
     0, 1
    ),
    ({(1,0): JobStatus.IDLE},
     {(1,0): JobStatus.COMPLETED},
     {(1,0): JobStatus.COMPLETED},
     0, 1
    ),
    ({(1,0): JobStatus.IDLE},
     {(1,0): JobStatus.FAILED},
     {},
     1, 0
    ),
    ({},
     {(1,0): JobStatus.IDLE},
     {},
     0, 0
    ),
    ({},
     {(1,0): JobStatus.RUNNING},
     {},
     0, 0
    ),
    ({},
     {(1,0): JobStatus.FAILED},
     {},
     1, 0
    ),
    ({},
     {(1,0): JobStatus.COMPLETED},
     {},
     0, 0
    ),
    ({},
     {},
     {(1,0): JobStatus.COMPLETED},
     0, 1
    ),
    ({},
     {},
     {(1,0): JobStatus.FAILED},
     0, 1
    ),
])
async def test_Grid_check_queue_jel_mismatch(schedd, i3prod_path, set_time, jel_jobs, queue_jobs, hist_jobs, remove_calls, finish_calls):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    qjobs = {}
    hjobs = {}
    g.submitter.get_jobs = MagicMock(return_value=qjobs)
    g.submitter.get_history = MagicMock()
    g.submitter.remove = MagicMock()
    g.finish = AsyncMock()
    g.get_tasks_on_queue = AsyncMock(return_value=[])

    jel = g.get_current_JEL()
    daydir = jel.parent
    def mkdir(name):
        p = daydir / name
        if p.exists():
            return p
        p.mkdir()
        return p

    for (c,p), s in jel_jobs.items():
        g.jobs[CondorJobId(cluster_id=c, proc_id=p)] = CondorJob(status=s, submit_dir=mkdir(f'{c}.{p}'))
    for (c,p), s in queue_jobs.items():
        qjobs[CondorJobId(cluster_id=c, proc_id=p)] = CondorJob(status=s, submit_dir=mkdir(f'{c}.{p}'))
    for (c,p), s in hist_jobs.items():
        hjobs[CondorJobId(cluster_id=c, proc_id=p)] = CondorJob(status=s, submit_dir=mkdir(f'{c}.{p}'))

    g.submitter.get_history.return_value = hjobs.items()
    await g.check()

    assert g.submitter.remove.call_count == remove_calls
    assert g.finish.call_count == finish_calls


@pytest.mark.parametrize('queue_jobs,hist_jobs,iceprod_tasks,reset_calls', [
    ({(1,0): ("dataset", "task", "instance")},
     {},
     [("dataset", "task", "instance", 0)],
     0,
    ),
    ({},
     {(1,0): ("dataset", "task", "instance")},
     [("dataset", "task", "instance", 1)],
     0,
    ),
    ({},
     {},
     [("dataset", "task", "instance", 1)],
     0,
    ),
    ({},
     {},
     [("dataset", "task", "instance", 600)],
     1,
    ),
])
async def test_Grid_check_queue_iceprod_mismatch(schedd, i3prod_path, set_time, queue_jobs, hist_jobs, iceprod_tasks, reset_calls):
    override = ['queue.type=htcondor', 'queue.max_task_queued_time=10', 'queue.max_task_processing_time=10', 'queue.suspend_submit_dir_time=10']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    rc = MagicMock()
    g = iceprod.server.plugins.condor.Grid(cfg=cfg, rest_client=rc, cred_client=None)

    qjobs = {}
    hjobs = {}
    g.submitter.get_jobs = MagicMock(return_value=qjobs)
    g.submitter.get_history = MagicMock()
    g.submitter.remove = MagicMock()
    g.finish = AsyncMock()
    g.task_reset = AsyncMock()
    itasks = []
    g.get_tasks_on_queue = AsyncMock(return_value=itasks)

    jel = g.get_current_JEL()
    daydir = jel.parent
    def mkdir(name):
        p = daydir / name
        if p.exists():
            return p
        p.mkdir()
        return p

    for (c,p), (d_id, t_id, i_id) in queue_jobs.items():
        qjobs[CondorJobId(cluster_id=c, proc_id=p)] = CondorJob(dataset_id=d_id, task_id=t_id, instance_id=i_id, submit_dir=mkdir(f'{c}.{p}'))
    for (c,p), (d_id, t_id, i_id) in hist_jobs.items():
        hjobs[CondorJobId(cluster_id=c, proc_id=p)] = CondorJob(dataset_id=d_id, task_id=t_id, instance_id=i_id, submit_dir=mkdir(f'{c}.{p}'))
    for (d_id, t_id, i_id, secs) in iceprod_tasks:
        t = set_time - datetime.timedelta(seconds=secs)
        itasks.append({'dataset_id': d_id, 'task_id': t_id, 'instance_id': i_id, 'status_changed': datetime2str(t)})

    g.submitter.get_history.return_value = hjobs.items()
    await g.check()

    assert g.task_reset.call_count == reset_calls


async def test_reset_task(schedd, i3prod_path, set_time):
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

    # normal failure
    jobid = CondorJobId(cluster_id=1, proc_id=0)
    g.jobs[jobid] = CondorJob(status=JobStatus.IDLE, submit_dir=p)

    g.task_success = AsyncMock()
    g.task_reset = AsyncMock()
    g.task_failure = AsyncMock()

    await g.finish(jobid, success=False)
    
    assert g.task_success.call_count == 0
    assert g.task_reset.call_count == 0
    assert g.task_failure.call_count == 1

    # success
    g.jobs[jobid] = CondorJob(status=JobStatus.IDLE, submit_dir=p)

    g.task_success = AsyncMock()
    g.task_reset = AsyncMock()
    g.task_failure = AsyncMock()

    await g.finish(jobid, success=True)
    
    assert g.task_success.call_count == 1
    assert g.task_reset.call_count == 0
    assert g.task_failure.call_count == 0

    # reset
    g.jobs[jobid] = CondorJob(status=JobStatus.IDLE, submit_dir=p)

    g.task_success = AsyncMock()
    g.task_reset = AsyncMock()
    g.task_failure = AsyncMock()

    await g.finish(jobid, success=False, reason=iceprod.server.plugins.condor.RESET_CONDOR_REASONS[0])

    assert g.task_success.call_count == 0
    assert g.task_reset.call_count == 1
    assert g.task_failure.call_count == 0

    # reset stderr
    g.jobs[jobid] = CondorJob(status=JobStatus.IDLE, submit_dir=p)

    g.task_success = AsyncMock()
    g.task_reset = AsyncMock()
    g.task_failure = AsyncMock()

    (p / 'condor.err').open('w').write(iceprod.server.plugins.condor.RESET_STDERR_REASONS[0])

    await g.finish(jobid, success=False)

    assert g.task_success.call_count == 0
    assert g.task_reset.call_count == 1
    assert g.task_failure.call_count == 0
