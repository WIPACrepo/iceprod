"""
Mark datasets as completed.

Check jobs in a dataset, and when all are complete mark the dataset
as complete.

Initial delay: rand(15 minutes)
Periodic delay: 60 minutes
"""

import logging
import random
import time

from tornado.ioloop import IOLoop

logger = logging.getLogger('dataset_completion')

def dataset_completion(module):
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
        dataset_ids.extend(datasets['processing'])
    if 'truncated' in datasets:
        dataset_ids.extend(datasets['truncated'])
    for dataset_id in dataset_ids:
        try:
            dataset = await rest_client.request('GET', '/datasets/{}'.format(dataset_id))
            jobs = await rest_client.request('GET', '/datasets/{}/job_counts/status'.format(dataset_id))
            if sum(jobs[s] for s in jobs) >= dataset['jobs_submitted']:
                # jobs are all buffered / materialized
                job_statuses = set(jobs)
                if job_statuses == set(['complete']):
                    logger.info('dataset %s status -> complete', dataset_id)
                    args = {'status':'complete'}
                    await rest_client.request('PUT', '/datasets/{}/status'.format(dataset_id), args)
                elif 'processing' not in job_statuses:
                    if 'errors' in job_statuses:
                        logger.info('dataset %s status -> errors', dataset_id)
                        args = {'status':'errors'}
                    else:
                        logger.info('dataset %s status -> suspended', dataset_id)
                        args = {'status':'suspended'}
                    await rest_client.request('PUT', '/datasets/{}/status'.format(dataset_id), args)
        except Exception:
            logger.error('error completing dataset %s', dataset_id, exc_info=True)
            if debug:
                raise

    # run again after 60 minute delay
    stop_time = time.time()
    delay = max(60*60 - (stop_time-start_time), 60*10)
    IOLoop.current().call_later(delay, run, rest_client)
