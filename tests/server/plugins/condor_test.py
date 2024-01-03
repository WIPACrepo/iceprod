import asyncio
import logging
from pathlib import Path
import os
from unittest.mock import MagicMock, AsyncMock

import htcondor
import pytest
import requests.exceptions

from iceprod.core.resources import Resources
from iceprod.core.config import Dataset, Job, Task
import iceprod.server.config
import iceprod.server.grid
import iceprod.server.plugins.condor


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


def test_CondorJob():
    j1 = iceprod.server.plugins.condor.CondorJob(task=MagicMock(), raw_status=htcondor.JobEventType.SUBMIT, cluster_id=0, proc_id=0)
    j2 = iceprod.server.plugins.condor.CondorJob(task=MagicMock(), raw_status=htcondor.JobEventType.SUBMIT, cluster_id=0, proc_id=0)
    assert j1 == j2

    j3 = iceprod.server.plugins.condor.CondorJob(task=MagicMock(), raw_status=htcondor.JobEventType.SUBMIT, cluster_id=0, proc_id=3)
    assert j1 < j3

    j4 = iceprod.server.plugins.condor.CondorJob(task=MagicMock(), raw_status=htcondor.JobEventType.SUBMIT, cluster_id=1, proc_id=0)
    assert j4 > j1


def test_CondorJobActions_init(schedd):
    override = ['queue.type=condor']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)
    rc = MagicMock()
    submit_dir = Path(os.path.expanduser(os.path.expandvars(cfg['queue']['submit_dir'])))

    cj = iceprod.server.plugins.condor.CondorJobActions(site='site', rest_client=rc, submit_dir=submit_dir, cfg=cfg)
    set(cj.transfer_plugins.keys()) == {'gsiftp'}


def test_CondorJobActions_condor_os_reqs():
    ret = iceprod.server.plugins.condor.CondorJobActions.condor_os_reqs('RHEL_6_x86_64')
    assert 'OpSysAndVer' in ret
    assert 'OSGVO_OS_STRING' in ret


def test_CondorJobActions_condor_resource_reqs():
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

    ret = iceprod.server.plugins.condor.CondorJobActions.condor_resource_reqs(task)
    assert ret['request_cpus'] == 1
    assert ret['request_memory'] == 1000
    assert 'request_gpus' not in ret
    assert 'requirements' not in ret
    
    task.requirements = {
        'gpu': 1,
    }
    ret = iceprod.server.plugins.condor.CondorJobActions.condor_resource_reqs(task)
    assert ret['request_gpus'] == 1
    
    
    task.requirements = {
        'os': ['RHEL_7_x86_64'],
    }
    ret = iceprod.server.plugins.condor.CondorJobActions.condor_resource_reqs(task)
    assert 'RHEL' in ret['requirements']

    