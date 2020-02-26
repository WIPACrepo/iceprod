"""
Queue tasks.

Move task statuses from waiting to queued, for a certain number of
tasks.  Also uses priority for ordering.

Initial delay: rand(1 minute)
Periodic delay: 5 minutes
"""

import logging
import random
import time
from collections import defaultdict

from tornado.ioloop import IOLoop

from iceprod.server import GlobalID

logger = logging.getLogger('queue_tasks')

NTASKS = 250000
NTASKS_PER_CYCLE = 1000

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
        tasks = await rest_client.request('GET', '/task_counts/status')
        if 'waiting' in tasks:
            num_tasks_waiting = tasks['waiting']
        if 'queued' in tasks:
            num_tasks_queued = tasks['queued']
        tasks_to_queue = min(num_tasks_waiting, NTASKS - num_tasks_queued, NTASKS_PER_CYCLE)
        logger.info(f'num tasks waiting: {num_tasks_waiting}')
        logger.info(f'num tasks queued: {num_tasks_queued}')
        logger.info(f'tasks to queue: {tasks_to_queue}')

        if tasks_to_queue > 0:
            args = {
                'status': 'waiting',
                'keys': 'task_id|depends',
                'sort': 'priority=-1',
            }
            ret = await rest_client.request('GET', '/tasks', args)
            queue_tasks = []
            for task in ret['tasks']:
                for dep in task['depends']:
                    ret = await rest_client.request('GET', f'/tasks/{dep}')
                    if ret['status'] != 'complete':
                        logger.debug('dependency not met for task %s', task['task_id'])
                        break
                else:
                    queue_tasks.append(task['task_id'])
                    if len(queue_tasks) >= tasks_to_queue:
                        break

            logger.info('queueing %d tasks', len(queue_tasks))
            if queue_tasks:
                args = {'tasks': queue_tasks}
                await rest_client.request('POST', '/task_actions/bulk_status/queued', args)

        # ~ while tasks_to_queue > 0:
            # ~ num = min(tasks_to_queue, 10)
            # ~ tasks_to_queue -= num

            # ~ ret = await rest_client.request('POST', '/task_actions/queue', {'num_tasks': num})
            # ~ logger.info(f'num queued: {ret["queued"]}')
            # ~ if ret['queued'] < num:
                # ~ break

    except Exception:
        logger.error('error queueing tasks', exc_info=True)
        if debug:
            raise

    # run again after 5 minute delay
    stop_time = time.time()
    delay = max(60*5 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client)
