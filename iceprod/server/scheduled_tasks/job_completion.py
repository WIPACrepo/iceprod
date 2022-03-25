"""
Mark jobs as completed.

Check tasks in a job, and when all are complete mark the job as complete.

Initial delay: rand(15 minutes)
Periodic delay: 60 minutes
"""

import logging
import random
import time

from tornado.ioloop import IOLoop

logger = logging.getLogger('job_completion')

def job_completion(module):
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
    if 'processing' in datasets:
        dataset_ids.extend(list(datasets['processing']))
    if 'truncated' in datasets:
        dataset_ids.extend(list(datasets['truncated']))
    for dataset_id in dataset_ids:
        try:
            jobs = await rest_client.request('GET', '/datasets/{}/job_summaries/status'.format(dataset_id))
            if 'processing' not in jobs:
                continue
            for job_id in jobs['processing']:
                tasks = await rest_client.request('GET', '/datasets/{}/tasks?keys=task_id|task_index|status&job_id={}'.format(dataset_id,job_id))
                task_statuses = []
                for task_id in sorted(tasks, key=lambda t:tasks[t]['task_index']):
                    task_statuses.append(tasks[task_id]['status'])
                if set(task_statuses) == set(['complete']):
                    logger.info('dataset %s job %s status -> complete', dataset_id, job_id)
                    args = {'status':'complete'}
                    await rest_client.request('PUT', '/datasets/{}/jobs/{}/status'.format(dataset_id,job_id), args)
                for status in task_statuses:
                    if status not in ('idle','waiting','queued','processing','reset','complete'):
                        if status == 'failed':
                            logger.info('dataset %s job %s status -> errors', dataset_id, job_id)
                            args = {'status':'errors'}
                        else:
                            logger.info('dataset %s job %s status -> suspended', dataset_id, job_id)
                            args = {'status':'suspended'}
                        await rest_client.request('PUT', '/datasets/{}/jobs/{}/status'.format(dataset_id,job_id), args)
                        break
        except Exception:
            logger.error('error completing a job in dataset %s', dataset_id, exc_info=True)
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
