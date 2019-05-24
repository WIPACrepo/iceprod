#!/usr/bin/env python

import os
import subprocess
import argparse
import json
import glob
import logging
import asyncio
from collections import defaultdict

from rest_tools.client import RestClient

async def get_stats(args):
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    datasets = await rpc.request('GET', '/datasets', {'keys': 'dataset_id|dataset'})
    dataset_id = None
    for d in datasets:
        if datasets[d]['dataset'] == args['dataset']:
            dataset_id = d
            break
    else:
        raise Exception('bad dataset num')
    dataset = await rpc.request('GET', f'/datasets/{dataset_id}')
    jobs = await rpc.request('GET', f'/datasets/{dataset_id}/jobs', {'keys': 'job_id|job_index'})

    params = {
        'status': 'complete',
        'keys': 'task_id|name|job_id', 
    }
    tasks = await rpc.request('GET', f'/datasets/{dataset_id}/tasks', params)
    if not tasks:
        raise Exception('no tasks found')

    dataset_stats = defaultdict(dict)
    try:
        futures = []
        for task_id in sorted(tasks, key=lambda tid: jobs[tasks[tid]['job_id']]['job_index']):
            futures.append(asyncio.create_task(rpc.request('GET', f'/datasets/{dataset_id}/tasks/{task_id}/task_stats')))

        print(len(futures))
        for f in asyncio.as_completed(futures):
            raw_stats = await f
            stats = [raw_stats[k] for k in raw_stats if 'error_summary' not in raw_stats[k]['stats']][-1]
            if stats:
                try:
                    resources = stats['stats']['resources']
                    task_id = stats['task_id']
                except KeyError:
                    continue
                try:
                    filesize = sum(f['size'] for f in stats['stats']['task_stats']['upload'])
                    resources['filesize'] = filesize
                except KeyError:
                    resources['filesize'] = -1

                name = tasks[task_id]['name']
                job_index = jobs[tasks[task_id]['job_id']]['job_index']
                print(job_index, name, resources)
                dataset_stats[name][job_index] = resources
    except KeyboardInterrupt:
        print('stopping...')

    with open(args['output'], 'w') as f:
        json.dump(dataset_stats, f)

def main():
    parser = argparse.ArgumentParser(description='manually run IceProd i3exec')
    parser.add_argument('-t', '--token',help='auth token')
    parser.add_argument('-d','--dataset',type=int,help='dataset number')
    parser.add_argument('-o','--output',type=str,help='output json file')
    
    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=logging.WARNING)

    asyncio.run(get_stats(args))

if __name__ == '__main__':
    main()
