"""
Handle input and output files for IceProd.

This module can be called standalone with:

.. code-block:: bash

    python -m iceprod.core.io -f CONFIG_FILE -d DIRECTORY {input,output}
"""

import os
import asyncio
import logging

from iceprod.core.serialization import serialize_json
from iceprod.core.exe import Config, SetupEnv

logger = logging.getLogger('io')

def get_current_task(config):
    name = config['options']['task']
    for task in config['tasks']:
        if task['name'] == name:
            return task
    raise Exception('cannot find task in config')

async def process(config):
    task = get_current_task(config)
    cfg = Config(config=config, logger=logger)
    env_opts = cfg.parseObject(config['options'], {})
    async with SetupEnv(cfg, config['steering'], {'options':env_opts}, logger=logger) as env:
        async with SetupEnv(cfg, task, env, logger=logger) as env2:
            for tray in task['trays']:
                for i in range(tray['iterations']):
                    cfg.config['options']['iter'] = i
                    async with SetupEnv(cfg, tray, env2, logger=logger) as env3:
                        for module in tray['modules']:
                            async with SetupEnv(cfg, module, env3, logger=logger) as env:
                                pass

async def handle_input(config):
    """Handle downloading all input files for specified task"""
    # first, filter out uploaded files
    def process_obj(obj):
        if 'data' in obj:
            new_data = []
            for data in obj['data']:
                if data['movement'] in ('input','both'):
                    new_data.append(data)
            obj['data'] = new_data
    process_obj(config['steering'])
    for task in config['tasks']:
        process_obj(task)
        for tray in task['trays']:
            process_obj(tray)
            for module in tray['modules']:
                process_obj(module)

    # now do the download
    await process(config)

async def handle_output(config):
    """Handle uploading all output files for specified task"""
    # first, filter out downloaded files
    def process_obj(obj):
        if 'data' in obj:
            new_data = []
            for data in obj['data']:
                if data['movement'] in ('output','both'):
                    new_data.append(data)
            obj['data'] = new_data
        if 'resources' in obj:
            obj['resources'] = []
        if 'classes' in obj:
            obj['classes'] = []
    process_obj(config['steering'])
    for task in config['tasks']:
        process_obj(task)
        for tray in task['trays']:
            process_obj(tray)
            for module in tray['modules']:
                process_obj(module)

    # now do the upload
    await process(config)

def setup_directory(config, directory):
    """Set up directory in config"""
    os.chdir(directory)

    # make sure some basic options are set
    if 'resource_url' not in config['options']:
        config['options']['resource_url'] = 'http://prod-exe.icecube.wisc.edu/'
    if 'offline' not in config['options']:
        config['options']['offline'] = True
    if 'offline_transfer' not in config['options']:
        config['options']['offline_transfer'] = True
    if 'data_url' not in config['options']:
        config['options']['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
    if 'svn_repository' not in config['options']:
        config['options']['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
    if 'site_temp' not in config['options']:
        config['options']['site_temp'] = 'gsiftp://gridftp-scratch.icecube.wisc.edu/local/simprod/'
    if 'dataset_temp' not in config['options']:
        config['options']['dataset_temp'] = os.path.join(config['options']['site_temp'],'$(dataset)')
    if 'job_temp' not in config['options']:
        config['options']['job_temp'] = os.path.join(config['options']['dataset_temp'],'$(job)')
    if 'subprocess_dir' not in config['options']:
        config['options']['subprocess_dir'] = directory
    if 'task_temp' not in config['options']:
        config['options']['task_temp'] = os.path.join(config['options']['subprocess_dir'],'task_temp')
    if 'tray_temp' not in config['options']:
        config['options']['tray_temp'] = os.path.join(config['options']['subprocess_dir'],'tray_temp')
    if 'local_temp' not in config['options']:
        config['options']['local_temp'] = os.path.join(config['options']['subprocess_dir'],'local_temp')

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Handle IceProd I/O')
    parser.add_argument('-f','--config_file',help='IceProd json config file')
    parser.add_argument('-d','--directory',help='Directory to read from / write to')
    parser.add_argument('transfer', choices=['input','output'], help='type of transfer')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    config = serialize_json.load(args.config_file)

    setup_directory(config, args.directory)
    if args.transfer == 'input':
        fut = handle_input(config)
    elif args.transfer == 'output':
        fut = handle_output(config)
    else:
        raise Exception('bad transfer type')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(fut)

if __name__ == '__main__':
    main()
