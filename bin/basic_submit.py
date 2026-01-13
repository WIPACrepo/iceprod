#!/usr/bin/env python
"""
IceProd Basic Submit
====================

Processes a script over jobs with input and output files.

Details
-------

Files are expected to be either full URLs or paths on the UW-Madison IceCube
file system.

Script arguments will be passed as a string. They can use these built-in macros:

* $(input) = The input file list, space-separated.
* $(output) = The output file.
* $(dataset) = The dataset_id in numerical form.
* $(job) = The job index within the dataset.

Example
-------

An example submission::

    ./basic_submit.py --env_shell '/cvmfs/icecube.opensciencegrid.org/py3-v4.1.1/icetray-env combo/stable' my_script.py '--foo=bar $(input) $(output)' job_files.txt

This will execute `my_script.py` from the local directory, while in the
combo/stable environment.  If the first line of `job_files.txt` contains::

    /data/user/XXX/gcdfile.i3.gz /data/user/XXX/infile_01.i3.gz /data/user/XXX/outfile_01.i3.gz

Then the first job will look like this::

    my_script.py --foo=bar gcdfile.i3.gz infile_01.i3.gz outfile_01.i3.gz

With data transfer of the script and files happening automatically.

Args:
    script (str): url or file path to script
    args (str): arguments to script
    files (str): filename with input and output files (one line per job)
    description: general description for dataset
    env_shell: an environment shell to execute before the script starts
    request_memory (float): request memory in GB (default: 1 GB)
    request_cpus (int): request CPUs (default: 1)
    request_gpus (int): request GPUs (default: 0)
    token (str): IceProd user token (https://iceprod2.icecube.wisc.edu/profile)
"""

import json
import os
import sys
import argparse
import logging
import asyncio
import time
from typing import Any

from rest_tools.client import SavedDeviceGrantAuth


logger = logging.getLogger('basic_submit')

def fail(message):
    print(message)
    sys.exit(1)

async def run(rpc, args):
    def modify_path(src):
        if src.startswith('/cvmfs'):
            return src
        if os.path.exists(src):
            if not src.startswith('/data'):
                raise Exception('path must be in /data')
            return 'gsiftp://gridftp.icecube.wisc.edu' + os.path.abspath(src)
        elif not (src.startswith('http://') or src.startswith('https://') or src.startswith('gsiftp://')):
            raise Exception('unknown path: '+src)
        return src

    # check script location
    args['script'] = modify_path(args['script'])

    # check input and output files
    jobfiles = []
    with open(args['files']) as f:
        for line in f:
            files = [x.strip() for x in line.split() if x.strip()]
            if not files:
                continue
            outfiles = ['gsiftp://gridftp.icecube.wisc.edu' + os.path.abspath(files[-1])]
            jobfiles.append([modify_path(x) for x in files[:-1]]+outfiles)

    # make dataset config
    config = {
      "parent_id":0,
      "version":3.1,
      "options":{},
      "steering":{
        "parameters":{},
        "batchsys":{},
        "data":[]
      },
      "tasks":[
        {
          "depends":[],
          "batchsys":{},
          "trays":[
            {
              "iterations":1,
              "modules":[
                {
                  "running_class":"",
                  "src":args['script'],
                  "args":args['args'],
                  "env_shell":args['env_shell'],
                  "env_clear":True,
                  "name":"",
                  "data":[],
                  "parameters":{}
                }
              ],
              "name":"",
              "data":[],
              "parameters":{}
            }
          ],
          "requirements":{
            "memory": args['request_memory'],
            "cpu": args['request_cpus'],
            "gpu": args['request_gpus']
          },
          "name":"BasicSubmit",
          "task_files":True,
          "data":[],
          "parameters":{}
        }
      ],
      "difplus":None,
      "description":args['description'],
      "categories":[]
    }

    # create the dataset
    print('submitting the config')
    rpc_args = {
        'config': json.dumps(config),
        'description': args['description'],
        'jobs_submitted': len(jobfiles),
        'group': args['group'],
        'extra_submit_fields': '{"status": "suspended"}',
    }
    dataset_id = ''
    try:
        ret = await rpc.request('POST', '/actions/submit', rpc_args)
        submit_id = ret['result']
        while True:
            await asyncio.sleep(10)
            print('.', end='')
            ret = await rpc.request('GET', f'/actions/submit/{submit_id}')
            if ret['status'] == 'complete':
                dataset_id = ret['payload']['dataset_id']
                break
            elif ret['status'] == 'error':
                fail('Submit failed:'+ret.get('error_message',''))
    except Exception:
        logger.warning('failed to create dataset', exc_info=True)
        fail('Failed to create dataset')
    print('Submit complete. Dataset id =', dataset_id)

    try:
        # materialize tasks
        try:
            ret = await rpc.request('POST', '/actions/materialization', {'dataste_id': dataset_id, 'num': len(jobfiles)})
        except Exception:
            logger.warning(f'materialization request for dataset {dataset_id} failed', exc_info=True)
            fail('Creation of jobs failed')
        materialization_id = ret['result']  # type: ignore

        logger.info(f'waiting for materialization request')
        while True:
            await asyncio.sleep(10)
            ret = await rpc.request('GET', f'/actions/materialization/{materialization_id}')
            if ret['status'] == 'complete':
                logger.info(f'materialization request complete')
                break
            elif ret['status'] == 'error':
                logger.warning(f'materialization request failed')
                fail('Creation of jobs failed')
            print('.', end='', flush=True)

        # upload file paths
        for i,files in enumerate(jobfiles):
            if i%100 == 0:
                print('.', end='', flush=True)
            try:
                for f in files[:-1]:
                    rpc_args = {
                        'filename': f,
                        'movement': 'input',
                        'job_index': i,
                        'task_name': 'BasicSubmit',
                    }
                    await rpc.request('POST', f'/datasets/{dataset_id}/files', rpc_args)
                rpc_args = {
                    'filename': files[-1],
                    'movement': 'output',
                    'job_index': i,
                    'task_name': 'BasicSubmit',
                }
                await rpc.request('POST', f'/datasets/{dataset_id}/files', rpc_args)
            except Exception:
                logger.warning(f'failed to upload file for job_index {i}', exc_info=True)
                fail('Failed to upload an input or output file path')

        # set dataset to processing
        rpc_args = {
            'status': 'processing'
        }
        try:
            await rpc.request('PUT', f'/datasets/{dataset_id}/status', rpc_args)
        except Exception:
            logger.warning(f'failed to set dataset {dataset_id} to processing', exc_info=True)
            fail('Failed to set dataset to processing')
    except Exception:
        rpc_args = {
            'status': 'failed'
        }
        try:
            await rpc.request('PUT', f'/datasets/{dataset_id}/status', rpc_args)
        except Exception:
            pass
        logger.warning(f'failed to start dataset', exc_info=True)
        fail('failed to start dataset')

    # get dataset num
    try:
        ret = await rpc.request('GET', f'/datasets/{dataset_id}', {'keys': 'dataset'})
        dataset_num = ret['dataset']
    except Exception:
        logger.warning(f'failed to get dataset info for dataset {dataset_id}', exc_info=True)
        fail('Failed to get final dataset info')

    print(f'Dataset {dataset_num} is running.')  # type: ignore
    print(f'Check status at https://iceprod2.icecube.wisc.edu/dataset/{dataset_num}')

def main():
    parser = argparse.ArgumentParser(description='job completion')
    parser.add_argument('script', help='url or file path to script')
    parser.add_argument('args', help='arguments to script')
    parser.add_argument('files', help='filename with input and output files (one line per job)')
    parser.add_argument('--description', default='', help='general description for dataset')
    parser.add_argument('--group', default='users', choices=['users', 'simprod', 'filtering'],
                        help='group to run under (default: users)')
    parser.add_argument('--env_shell', default='', help='environment shell (icetray env_shell.sh syntax)')
    parser.add_argument('--request_memory', type=float, default=1.0,
                        help='request memory in GB (default: 1 GB)')
    parser.add_argument('--request_cpus', type=int, default=1,
                        help='request CPUs (default: 1)')
    parser.add_argument('--request_gpus', type=int, default=0,
                        help='request GPUs (default: 0)')
    parser.add_argument('--log_level', default='warning', help='log level (defaut: WARN)')

    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=getattr(logging, args['log_level'].upper()))

    rpc_kwargs: dict[str, Any] = {
        'token_url': 'https://keycloak.icecube.wisc.edu/auth/realms/IceCube',
        'filename': os.path.abspath(os.path.expandvars('$HOME/.iceprod-auth')),
        'client_id': 'iceprod-public',
    }
    rpc = SavedDeviceGrantAuth('https://api.iceprod.icecube.aq', **rpc_kwargs)

    asyncio.run(run(rpc, args))

if __name__ == '__main__':
    main()
