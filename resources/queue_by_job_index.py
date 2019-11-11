#!/usr/bin/env python

import os
import subprocess
import argparse
import json
import glob
import logging
import asyncio
from datetime import datetime

from rest_tools.client import RestClient

logger = logging.getLogger()

async def run(rest_client, dataset_id, job_start, job_stop, debug=True):
    async def update(job_id):
        logger.info('deleting job %s', job_id)
        await rest_client.request('PUT', f'/datasets/{dataset_id}/jobs/{job_id}/status', {'status':'suspended'})
        tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks', {'job_id':job_id, 'keys':'task_id'})
        for t in tasks:
            await rest_client.request('PUT', f'/datasets/{dataset_id}/tasks/{t}/status', {'status':'suspended'})
    try:
        jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/jobs?keys=job_id|job_index')
        tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks?status=waiting&keys=task_id|job_id')
        for task_id in tasks:
            index = jobs[tasks[task_id]['job_id']]['job_index']
            if job_start <= index <= job_stop:
                await rest_client.request('PUT', f'/datasets/{dataset_id}/tasks/{task_id}/status', {'status':'queued'})

    except Exception:
        logger.error('error queueing tasks', exc_info=True)
        logger.info('%r',task_id)
        if debug:
            raise

def main():
    parser = argparse.ArgumentParser(description='clean up processing tasks not in pilots')
    parser.add_argument('-t', '--token',help='auth token')
    parser.add_argument('-d', '--dataset',help='dataset_id')
    parser.add_argument('job_start',type=int,help='job index start')
    parser.add_argument('job_stop',type=int,help='job index stop')
    
    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=logging.DEBUG)
    
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    asyncio.run(run(rpc, args['dataset'], args['job_start'], args['job_stop']))

if __name__ == '__main__':
    main()

