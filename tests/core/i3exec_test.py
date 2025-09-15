"""
Test script for i3exec
"""
import argparse
import json
import logging

import iceprod.core.exe
import iceprod.core.i3exec


logger = logging.getLogger('i3exe_test')


async def test_run(tmp_path, mocker):
    config = {
        'tasks': [{
            'name': 'foo',
            'trays': [{
                'modules': [{
                    'env_clear': False,
                    'src': 'foo.py'
                }]
            }],
        }]
    }
    configfile = tmp_path / 'config.json'
    with open(configfile, 'w') as f:
        json.dump(config, f)

    args = argparse.Namespace(
        dataset_id=None,
        task_id=None,
        config=str(configfile),
        task='foo',
        dataset_num=1,
        jobs_submitted=1,
        job_index=0,
        dry_run=0,
    )

    conv = mocker.patch('iceprod.core.exe.WriteToScript.convert', return_value='foo')
    sub = mocker.patch('subprocess.run')
    await iceprod.core.i3exec.run(args)

    conv.assert_called_once()
    sub.assert_called_once_with(['foo'], check=True)

    

