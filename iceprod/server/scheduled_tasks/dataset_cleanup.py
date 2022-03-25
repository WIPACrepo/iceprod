"""
Suspend any jobs and tasks from datasets that are suspended / errors.

Initial delay: rand(15 minutes)
Periodic delay: 60 minutes
"""

import logging
import random
import time

from tornado.ioloop import IOLoop

logger = logging.getLogger('dataset_cleanup')

def dataset_cleanup(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(10,60*10), run, module.rest_client)

async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()
    datasets = await rest_client.request('GET', '/dataset_summaries/status')
    dataset_ids = []
    if 'suspended' in datasets:
        dataset_ids.extend(list(datasets['suspended']))
    if 'errors' in datasets:
        dataset_ids.extend(list(datasets['errors']))
    for dataset_id in dataset_ids:
        try:
            jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/job_summaries/status')
            if 'processing' in jobs:
                for job_id in jobs['processing']:
                    args = {'status': 'suspended'}
                    await rest_client.request('PUT', f'/datasets/{dataset_id}/jobs/{job_id}/status', args)

            tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/task_summaries/status')
            task_ids = []
            for status in tasks:
                if status not in ('complete','suspended','failed'):
                    task_ids.extend(tasks[status])
            while task_ids:
                tids = task_ids[:10000]
                task_ids = task_ids[10000:]
                args = {'tasks': tids}
                await rest_client.request('POST', f'/datasets/{dataset_id}/task_actions/bulk_status/suspended', args)

        except Exception:
            logger.error('error cleaning a job/task in dataset %s', dataset_id, exc_info=True)
            if debug:
                raise

    # clean up tasks in suspended/errors jobs in processing datasets
    dataset_ids = []
    if 'processing' in datasets:
        dataset_ids.extend(list(datasets['processing']))
    for dataset_id in dataset_ids:
        try:
            jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/job_summaries/status')
            job_ids = set()
            if 'suspended' in jobs:
                job_ids.update(jobs['suspended'])
            if 'errors' in jobs:
                job_ids.update(jobs['errors'])

            args = {
                'keys': 'task_id|job_id|status'
            }
            tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args)
            task_ids = []
            for tasks in tasks.values():
                if tasks['status'] not in ('complete','suspended') and tasks['job_id'] in job_ids:
                    task_ids.append(tasks['task_id'])
            while task_ids:
                tids = task_ids[:10000]
                task_ids = task_ids[10000:]
                args = {'tasks': tids}
                await rest_client.request('POST', f'/datasets/{dataset_id}/task_actions/bulk_status/suspended', args)

        except Exception:
            logger.error('error cleaning a task in dataset %s', dataset_id, exc_info=True)
            if debug:
                raise

    # run again after 60 minute delay
    stop_time = time.time()
    delay = max(60*60 - (stop_time-start_time), 60*10)
    IOLoop.current().call_later(delay, run, rest_client)


def main():
    import argparse
    import asyncio
    import os
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    parser.add_argument('-t', '--token', default=os.environ.get('ICEPROD_TOKEN', None), help='auth token')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()
    args = vars(args)

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args['log_level'].upper()))

    from rest_tools.client import RestClient
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    asyncio.run(run(rpc, debug=args['debug']))

if __name__ == '__main__':
    main()
