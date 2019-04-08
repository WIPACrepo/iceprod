#!/usr/bin/env python

import os
import subprocess
import argparse
import json
import glob
import logging

from rest_tools.client import RestClient

def cleanup():
    for file in glob.glob('*.i3*'):
        os.remove(file)
    for file in glob.glob('iceprod_*'):
        os.remove(file)
    try:
        os.remove('summary.json')
    except Exception:
        pass
    try:
        os.remove('rng.state')
    except Exception:
        pass

def run(token, config, jobs_submitted, job, task):
    cleanup()
    subprocess.check_call(['python', '-m', 'iceprod.core.i3exec', '-p', token,
                           '-u', 'https://iceprod2-api.icecube.wisc.edu',
                           '--jobs_submitted', f'{jobs_submitted}', '-f', config,
                           '--job', f'{job}', '--task', f'{task}'])
    cleanup()

def write_config(config, filename, dataset_id, dataset, task_id):
    data = config.copy()
    data['dataset'] = dataset
    data['options'] = {
        'dataset_id': dataset_id,
        'dataset': dataset,
        'task_id': task_id,
    }
    with open(filename, 'w') as f:
        json.dump(data, f)

def main():
    parser = argparse.ArgumentParser(description='manually run IceProd i3exec')
    parser.add_argument('-t', '--token',help='auth token')
    parser.add_argument('-d','--dataset',type=int,help='dataset number')
    parser.add_argument('-j','--job',type=int,help='job number (optional)')
    
    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=logging.DEBUG)
    
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])
    
    datasets = rpc.request_seq('GET', '/datasets', {'keys': 'dataset_id|dataset'})
    dataset_id = None
    for d in datasets:
        if datasets[d]['dataset'] == args['dataset']:
            dataset_id = d
            break
    else:
        raise Exception('bad dataset num')
    dataset = rpc.request_seq('GET', f'/datasets/{dataset_id}')
    config = rpc.request_seq('GET', f'/config/{dataset_id}')

    jobs = rpc.request_seq('GET', f'/datasets/{dataset_id}/jobs', {'status': 'processing|errors'})
    if args['job']:
        jobs = {j:jobs[j] for j in jobs if jobs[j]['job_index'] == args['job']}
    if not jobs:
        raise Exception('no jobs found')

    for job_id in jobs:
        tasks = rpc.request_seq('GET', f'/datasets/{dataset_id}/tasks',
                                {'job_id': job_id, 'keys': 'task_id|task_index|name|depends',
                                 'status': 'waiting|queued|reset|failed'})
        for task_id in sorted(tasks, key=lambda t:tasks[t]['task_index']):
            print(f'processing {dataset["dataset"]} {jobs[job_id]["job_index"]} {tasks[task_id]["name"]}')
            write_config(config, 'config.json', dataset_id, args['dataset'], task_id)
            run(token=args['token'], config='config.json',
                jobs_submitted=dataset['jobs_submitted'],
                job=jobs[job_id]['job_index'],
                task=tasks[task_id]['name'])

if __name__ == '__main__':
    main()
