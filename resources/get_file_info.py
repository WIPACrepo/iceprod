#!/usr/bin/env python

import os
import subprocess
import argparse
import json
import glob
import logging
import asyncio
from datetime import datetime
from pprint import pprint

from rest_tools.client import RestClient
from iceprod.core.serialization import dict_to_dataclasses
from iceprod.core.parser import ExpParser

logger = logging.getLogger()

async def run(rest_client, filename, dataset_num=None, job_index=None):
    if not dataset_num:
        # get dataset
        parts = filename.split('/')
        for p in parts[:-1]:
            try:
                dataset_num = int(p)
                if 20000 < dataset_num < 30000:
                    break
            except Exception:
                continue
        else:
            raise Exception('could not determine dataset number')
        logger.info(f'dataset num: {dataset_num}')

    # get dataset_id
    datasets = await rest_client.request('GET', '/datasets?keys=dataset_id|dataset|jobs_submitted')
    for dataset_id in datasets:
        if datasets[dataset_id]['dataset'] == dataset_num:
            jobs_submitted = datasets[dataset_id]['jobs_submitted']
            break
    else:
        raise Exception(f'dataset num {dataset_num} not found')
    logger.info(f'dataset_id: {dataset_id}')

    # get config
    config_url = f'https://iceprod2.icecube.wisc.edu/config?dataset_id={dataset_id}'
    logger.info(f'config_url: {config_url}')
    config = await rest_client.request('GET', f'/config/{dataset_id}')
    config = dict_to_dataclasses(config)

    # get output file patterns
    parser = ExpParser()
    files = []
    for task in config['tasks']:
        for d in task['data']:
            if d['type'] in ('permanent','site_temp') and d['movement'] in ('output','both'):
                files.append({'url': d['remote'], 'iters': 1, 'task': task['name']})
        for tray in task['trays']:
            for d in tray['data']:
                if d['type'] in ('permanent','site_temp') and d['movement'] in ('output','both'):
                    files.append({'url': d['remote'], 'iters': tray['iterations'], 'task': task['name']})
            for module in tray['modules']:
                for d in module['data']:
                    if d['type'] in ('permanent','site_temp') and d['movement'] in ('output','both'):
                        files.append({'url': d['remote'], 'iters': tray['iterations'], 'task': task['name']})

    async def success():
        ret = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks', {'job_index': config['options']['job'], 'keys': 'name|task_id|job_id|task_index'})
        for task in ret.values():
            if task['name'] == config['options']['task']:
                break
        else:
            raise Exception('cannot get task info')
        data = {
            'dataset': dataset_num,
            'dataset_id': dataset_id,
            'job': config['options']['job'],
            'job_id': task['job_id'],
            'task': task['name'],
            'task_id': task['task_id'],
            'config': config_url,
        }
        return data

    env = {'parameters': config['steering']['parameters']}
    config['options'].update({
        'dataset': dataset_num,
        'dataset_id': dataset_id,
        'jobs_submitted': jobs_submitted,
    })
    if job_index:
        job_search = [job_index]
    else:
        job_search = range(jobs_submitted)
    for f in reversed(files):
        logger.info(f'searching task {f["task"]}')
        config['options']['task'] = f['task']
        for j in job_search:
            config['options']['job'] = j
            for i in range(f['iters']):
                config['options']['iter'] = i
                url = parser.parse(f['url'], config, env)
                if '//' not in url:
                    path = url
                else:
                    path = '/'+url.split('//',1)[1].split('/',1)[1]
                logger.info(f'checking path {path}')
                if path == filename:
                    logger.info(f'success on job_index: {j}, iter: {i}')
                    return await success()

    raise Exception('no path match found')

def main():
    parser = argparse.ArgumentParser(description='get IceProd info for an output file')
    parser.add_argument('-t', '--token',help='auth token')
    parser.add_argument('--log_level', default='info', help='log level')
    parser.add_argument('--dataset', type=int, help='dataset num')
    parser.add_argument('--job', type=int, help='job index')
    parser.add_argument('file',help='file')
    
    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=getattr(logging, args['log_level'].upper()))
    
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    ret = asyncio.run(run(rpc, args['file'], dataset_num=args['dataset'], job_index=args['job']))
    pprint(ret)

if __name__ == '__main__':
    main()

