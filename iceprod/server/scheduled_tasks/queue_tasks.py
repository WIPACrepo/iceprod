"""
Queue tasks.

Move task statuses from waiting to queued, for a certain number of
tasks.  Also uses dataset priority for ordering.

Initial delay: rand(1 minute)
Periodic delay: 5 minutes
"""

import logging
import random
import time

from tornado.ioloop import IOLoop

from iceprod.server import GlobalID

logger = logging.getLogger('dataset_monitor')

def queue_tasks(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(5,60), run, module.rest_client)

async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()
    try:
        num_tasks_waiting = 0
        num_tasks_queued = 0
        dataset_prios = {}
        datasets = await rest_client.request('GET', '/dataset_summaries/status')
        if 'processing' in datasets:
            for dataset_id in datasets['processing']:
                tasks = await rest_client.request('GET', '/datasets/{}/task_counts/status'.format(dataset_id))
                if 'waiting' in tasks:
                    num_tasks_waiting += tasks['waiting']
                if 'queued' in tasks:
                    num_tasks_queued += tasks['queued']
                dataset = await rest_client.request('GET', '/datasets/{}'.format(dataset_id))
                dataset_prios[dataset_id] = dataset['priority']

        if num_tasks_waiting > 0:
            tasks_to_queue = 10000 - num_tasks_queued
            if tasks_to_queue > 0:
                args = {
                    'num_tasks': tasks_to_queue,
                    'dataset_prio': dataset_prios,
                }
                await rest_client.request('POST', '/task_actions/queue', args)
    except Exception:
        logger.error('error monitoring datasets', exc_info=True)
        if debug:
            raise

    # run again after 5 minute delay
    stop_time = time.time()
    delay = max(60*5 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client)
