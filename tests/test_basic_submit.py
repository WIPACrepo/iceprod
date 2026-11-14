import importlib.util
import json
import logging
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


# import basic_submit.py
def import_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(module)  # type: ignore
    return module

basic_submit = import_from_path('basic_submit', Path(__file__).parent.parent / 'bin' / 'basic_submit.py')


async def test_one_job(tmp_path, monkeypatch):
    monkeypatch.setattr(basic_submit, 'DATA_PREFIX', str(tmp_path))
    monkeypatch.setattr(basic_submit, 'ACTIONS_SLEEP', 0.001)

    files = tmp_path / 'files.txt'
    infile1 = tmp_path / 'infile1'
    infile1.touch()
    infile2 = tmp_path / 'infile2'
    infile2.touch()
    outfile = tmp_path / 'outfile1'
    files.write_text(f"""
{infile1} {infile2} {outfile}
""")

    rpc_files = []
    async def rpc_mock(method, path, kwargs=None):
        match path:
            case '/actions/submit':
                return {'result': 'submitid'}
            case '/actions/submit/submitid':
                return {'status': 'complete', 'payload': {'dataset_id': 'did'}}
            case '/actions/materialization':
                return {'result': 'mid'}
            case '/actions/materialization/mid':
                return {'status': 'complete'}
            case '/datasets/did/file_actions/bulk':
                rpc_files.extend(kwargs['files'])
                return {'count': len(kwargs['files'])}
            case '/datasets/did/status':
                return {}
            case '/datasets/did':
                return {'dataset': 12345}
        raise Exception('bad api call')

    rpc = MagicMock()
    rpc.request = AsyncMock(side_effect=rpc_mock)

    args = {
        'env_shell': '',
        'script': '/cvmfs/foo.py',
        'args': ['a', 'b'],
        'files': str(files),
        'request_memory': 1,
        'request_cpus': 1,
        'request_gpus': 0,
        'description': '',
        'group': 'users',
        'scope': [],
    }
    await basic_submit.run(rpc, args)

    assert rpc.request.called
    assert len(rpc_files) == 3
    assert rpc_files[0]['filename'] == f'osdf:///icecube/wipac{infile1}'
    assert rpc_files[1]['filename'] == f'osdf:///icecube/wipac{infile2}'
    assert rpc_files[2]['filename'] == f'osdf:///icecube/wipac{outfile}'

    config = json.loads(rpc.request.call_args_list[0].args[2]['config'])
    logging.info('config: %r', config)
    assert config['tasks'][0]['token_scopes'] == {
        'osdf:///icecube/wipac': f'storage.modify:{infile1.parent} storage.read:{outfile.parent}'
    }


async def test_many_jobs(tmp_path, monkeypatch):
    monkeypatch.setattr(basic_submit, 'DATA_PREFIX', str(tmp_path))
    monkeypatch.setattr(basic_submit, 'ACTIONS_SLEEP', 0.001)
    fail = MagicMock(side_effect=Exception())
    monkeypatch.setattr(basic_submit, 'fail', fail)

    files = tmp_path / 'files.txt'
    with files.open('w') as f:
        for i in range(100):
            dirname = tmp_path / str(i)
            dirname.mkdir()
            infile1 = dirname / 'infile1'
            infile1.touch()
            infile2 = dirname / 'infile2'
            infile2.touch()
            outfile = dirname / 'outfile1'
            print(f'{infile1} {infile2} {outfile}', file=f)

    rpc_files = []
    async def rpc_mock(method, path, kwargs=None):
        match path:
            case '/actions/submit':
                return {'result': 'submitid'}
            case '/actions/submit/submitid':
                return {'status': 'complete', 'payload': {'dataset_id': 'did'}}
            case '/actions/materialization':
                return {'result': 'mid'}
            case '/actions/materialization/mid':
                return {'status': 'complete'}
            case '/datasets/did/file_actions/bulk':
                rpc_files.extend(kwargs['files'])
                return {'count': len(kwargs['files'])}
            case '/datasets/did/status':
                return {}
            case '/datasets/did':
                return {'dataset': 12345}
        raise Exception('bad api call')

    rpc = MagicMock()
    rpc.request = AsyncMock(side_effect=rpc_mock)

    args = {
        'env_shell': '',
        'script': '/cvmfs/foo.py',
        'args': ['a', 'b'],
        'files': str(files),
        'request_memory': 1,
        'request_cpus': 1,
        'request_gpus': 0,
        'description': '',
        'group': 'users',
        'scope': [],
    }
    with pytest.raises(Exception):
        await basic_submit.run(rpc, args)

    assert not rpc.request.called
    assert fail.called
    assert fail.call_args.args[0].startswith('too many token scopes')

    args['scope'] = [f'storage.modify:{tmp_path}', f'storage.read:{tmp_path}']
    await basic_submit.run(rpc, args)

    assert rpc.request.called
    assert len(rpc_files) == 300

    config = json.loads(rpc.request.call_args_list[0].args[2]['config'])
    logging.info('config: %r', config)
    assert config['tasks'][0]['token_scopes'] == {
        'osdf:///icecube/wipac': f'storage.modify:{tmp_path} storage.read:{tmp_path}'
    }


async def test_bulk_chunks(tmp_path, monkeypatch):
    monkeypatch.setattr(basic_submit, 'DATA_PREFIX', str(tmp_path))
    monkeypatch.setattr(basic_submit, 'ACTIONS_SLEEP', 0.001)
    monkeypatch.setattr(basic_submit, 'BULK_FILES_CHUNK', 4)

    # 3 jobs x 3 files = 9 files -> chunks of 4, 4, 1
    files = tmp_path / 'files.txt'
    with files.open('w') as f:
        for i in range(3):
            dirname = tmp_path / str(i)
            dirname.mkdir()
            for name in ('infile1', 'infile2'):
                (dirname / name).touch()
            print(f'{dirname}/infile1 {dirname}/infile2 {dirname}/outfile1', file=f)

    chunks = []
    async def rpc_mock(method, path, kwargs=None):
        match path:
            case '/actions/submit':
                return {'result': 'submitid'}
            case '/actions/submit/submitid':
                return {'status': 'complete', 'payload': {'dataset_id': 'did'}}
            case '/actions/materialization':
                return {'result': 'mid'}
            case '/actions/materialization/mid':
                return {'status': 'complete'}
            case '/datasets/did/file_actions/bulk':
                assert method == 'POST'
                chunks.append(kwargs['files'])
                return {'count': len(kwargs['files'])}
            case '/datasets/did/status':
                return {}
            case '/datasets/did':
                return {'dataset': 12345}
        raise Exception('bad api call')

    rpc = MagicMock()
    rpc.request = AsyncMock(side_effect=rpc_mock)

    args = {
        'env_shell': '',
        'script': '/cvmfs/foo.py',
        'args': [],
        'files': str(files),
        'request_memory': 1,
        'request_cpus': 1,
        'request_gpus': 0,
        'description': '',
        'group': 'users',
        'scope': [],
    }
    await basic_submit.run(rpc, args)

    assert [len(c) for c in chunks] == [4, 4, 1]
    all_files = [f for c in chunks for f in c]
    assert [f['job_index'] for f in all_files] == [0, 0, 0, 1, 1, 1, 2, 2, 2]
    assert [f['movement'] for f in all_files] == ['input', 'input', 'output'] * 3
    assert all(f['task_name'] == 'BasicSubmit' for f in all_files)
    assert all_files[2]['filename'] == f'osdf:///icecube/wipac{tmp_path}/0/outfile1'
