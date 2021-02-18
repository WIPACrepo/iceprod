#!/usr/bin/env python

import os
import subprocess
import argparse
import json
import glob
import tempfile
import shutil
import logging
from contextlib import contextmanager

from rest_tools.client import RestClient

def cleanup():
    for file in glob.glob('iceprod_*'):
        os.remove(file)
    try:
        shutil.rmtree('local_temp')
    except Exception:
        pass

def run(token, config, jobs_submitted, job, task, clean=True, debug=False):
    cleanup()
    cmd = ['python', '-m', 'iceprod.core.i3exec', '-p', token,
           '-u', 'https://iceprod2-api.icecube.wisc.edu',
           '--jobs_submitted', f'{jobs_submitted}', '-f', config,
           '--job', f'{job}', '--task', f'{task}']
    if debug:
        cmd += ['--debug']
    subprocess.check_call(cmd)
    if clean:
        cleanup()

@contextmanager
def make_pilot(rpc):
    resources = {'cpu': 1, 'gpu': 0, 'memory': 4, 'disk': 1000, 'time': 24}
    pilot = {'resources': resources,
             'resources_available': resources,
             'resources_claimed': resources,
             'tasks': [],
             'queue_host': 'manual_run',
             'queue_version': 'unknown',
             'version': 'unknown',
    }
    ret = rpc.request_seq('POST', '/pilots', pilot)
    pilot_id = ret['result']

    def set_task(task_id):
        data = {'tasks': [task_id]}
        rpc.request_seq('PATCH', f'/pilots/{pilot_id}', data)

    try:
        yield set_task
    finally:
        rpc.request_seq('DELETE', f'/pilots/{pilot_id}')

def write_config(config, filename, dataset_id, dataset, task_id, subprocess_dir=os.getcwd()):
    data = config.copy()
    data['dataset'] = dataset
    data['options'] = {
        'dataset_id': dataset_id,
        'dataset': dataset,
        'task_id': task_id,
        'subprocess_dir': subprocess_dir,
    }
    with open(filename, 'w') as f:
        json.dump(data, f)

def main():
    parser = argparse.ArgumentParser(description='manually run IceProd i3exec')
    parser.add_argument('-t', '--token', help='auth token')
    parser.add_argument('-d','--dataset', type=int, help='dataset number')
    parser.add_argument('-j','--job', type=int, help='job number (optional)')
    parser.add_argument('--run-failed-jobs', action='store_true', help='also run failed jobs')
    parser.add_argument('--no-clean', dest='clean', action='store_false', help='do not clean up after job')
    parser.add_argument('--log-level', default='DEBUG', choices=['ERROR','WARNING','INFO','DEBUG'], help='log level')
    parser.add_argument('--ignore-error', action='store_true', help='keep going if a job fails')
    
    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=args['log_level'])

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
    if args['job'] is not None:
        jobs = {j:jobs[j] for j in jobs if jobs[j]['job_index'] == args['job']}
    if not jobs:
        raise Exception('no jobs found')

    with make_pilot(rpc) as pilot:
        for job_id in jobs:
            status = 'waiting|queued|reset'
            if args['run_failed_jobs']:
                status += '|failed'
            tasks = rpc.request_seq('GET', f'/datasets/{dataset_id}/tasks',
                                    {'job_id': job_id, 'keys': 'task_id|task_index|name|depends',
                                     'status': status})
            if not tasks:
                logging.warning(f'no tasks available for {dataset["dataset"]} {jobs[job_id]["job_index"]}')
            for task_id in sorted(tasks, key=lambda t:tasks[t]['task_index']):
                ret = rpc.request_seq('GET', f'/datasets/{dataset_id}/tasks/{task_id}')
                if ret['status'] not in status.split('|'):
                    continue
                print(f'processing {dataset["dataset"]} {jobs[job_id]["job_index"]} {tasks[task_id]["name"]}')

                tmpdir = tempfile.mkdtemp(suffix='.{}'.format(task_id), dir=os.getcwd())
                try:
                    write_config(config, 'config.json', dataset_id, args['dataset'], task_id, subprocess_dir=tmpdir)
                    pilot(task_id)
                    run(token=args['token'], config='config.json',
                        jobs_submitted=dataset['jobs_submitted'],
                        job=jobs[job_id]['job_index'],
                        task=tasks[task_id]['name'],
                        clean=args['clean'],
                        debug=args['log_level'] == 'DEBUG')
                except subprocess.SubprocessError:
                    if args['ignore_error']:
                        logging.warn("error in subprocess", exc_info=True)
                        continue
                    raise
                finally:
                    shutil.rmtree(tmpdir)

    try:
        os.remove('config.json')
    except Exception:
        pass

if __name__ == '__main__':
    main()
