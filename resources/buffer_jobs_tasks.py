#!/usr/bin/env python

import os
import subprocess
import argparse
import json
import glob
import logging
import asyncio

from rest_tools.client import RestClient

from iceprod.core.parser import ExpParser
from iceprod.core.resources import Resources
from iceprod.server.scheduled_tasks.buffer_jobs_tasks import get_reqs, get_depends

logger = logging.getLogger()

async def run(rest_client, only_dataset=None, num=10, debug=True):
    datasets = await rest_client.request('GET', '/dataset_summaries/status')
    if 'processing' in datasets:
        for dataset_id in datasets['processing']:
            if only_dataset and dataset_id != only_dataset:
                continue
            try:
                logging.warning('buffering dataset %s', dataset_id)
                dataset = await rest_client.request('GET', '/datasets/{}'.format(dataset_id))
                tasks = await rest_client.request('GET', '/datasets/{}/task_counts/status'.format(dataset_id))
                if 'waiting' not in tasks or tasks['waiting'] < num:
                    # buffer for this dataset
                    jobs = await rest_client.request('GET', '/datasets/{}/jobs'.format(dataset_id))
                    jobs_to_buffer = min(num, dataset['jobs_submitted'] - len(jobs))
                    if jobs_to_buffer > 0:
                        config = await rest_client.request('GET', '/config/{}'.format(dataset_id))
                        parser = ExpParser()
                        task_names = [task['name'] if task['name'] else str(i) for i,task in enumerate(config['tasks'])]
                        job_index = max(jobs[i]['job_index'] for i in jobs)+1 if jobs else 0
                        for i in range(jobs_to_buffer):
                            # buffer job
                            args = {'dataset_id': dataset_id, 'job_index': job_index}
                            job_id = await rest_client.request('POST', '/jobs', args)
                            # buffer tasks
                            task_ids = []
                            for task_index,name in enumerate(task_names):
                                depends = await get_depends(rest_client, config, job_index,
                                                            task_index, task_ids)
                                config['options']['job'] = job_index
                                config['options']['task'] = task_index
                                config['options']['dataset'] = dataset['dataset']
                                config['options']['jobs_submitted'] = dataset['jobs_submitted']
                                config['options']['tasks_submitted'] = dataset['tasks_submitted']
                                config['options']['debug'] = dataset['debug']
                                args = {
                                    'dataset_id': dataset_id,
                                    'job_id': job_id['result'],
                                    'task_index': task_index,
                                    'name': name,
                                    'depends': depends,
                                    'requirements': get_reqs(config, task_index, parser),
                                }
                                task_id = await rest_client.request('POST', '/tasks', args)
                                task_ids.append(task_id['result'])
                            job_index += 1
            except Exception:
                logger.error('error buffering dataset %s', dataset_id, exc_info=True)
                if debug:
                    raise

def main():
    parser = argparse.ArgumentParser(description='clean up processing tasks not in pilots')
    parser.add_argument('-t', '--token', help='auth token')
    parser.add_argument('-d','--dataset', help='dataset id')
    parser.add_argument('-n','--num', type=int, default=20000,
                        help='max number of jobs to buffer')
    parser.add_argument('--debug', action='store_true', help='debug')
    
    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=logging.DEBUG if args['debug'] else logging.INFO)
    
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    asyncio.run(run(rpc, args['dataset'], num=args['num'], debug=args['debug']))

if __name__ == '__main__':
    main()

