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
import asyncio
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
        logger.warning(f'num tasks waiting: {num_tasks_waiting}')
        logger.warning(f'num tasks queued: {num_tasks_queued}')
        logger.warning(f'tasks to queue: {tasks_to_queue}')

        if tasks_to_queue > 0:
            args = {
                'status': 'waiting',
                'keys': 'task_id|depends',
                'sort': 'priority=-1',
                'limit': 5*tasks_to_queue,
            }
            ret = await rest_client.request('GET', '/tasks', args)
            queue_tasks = []
            deps_futures = set()
            async def check_deps(task):
                for dep in task['depends']:
                    ret = await rest_client.request('GET', f'/tasks/{dep}')
                    if ret['status'] != 'complete':
                        logger.info('dependency not met for task %s', task['task_id'])
                        await rest_client.request('PATCH', f'/tasks/{task["task_id"]}', {'priority': 0})
                        return None
                return task
            for task in ret['tasks']:
                if not task['depends']:
                    logger.info('queueing task %s', task['task_id'])
                    queue_tasks.append(task['task_id'])
                else:
                    fut = asyncio.create_task(check_deps(task))
                    deps_futures.add(fut)
                    if len(deps_futures) >= 20:
                        done, pending = await asyncio.wait(deps_futures, return_when=asyncio.FIRST_COMPLETED)
                        for fut in done:
                            task = await fut
                            if task and len(queue_tasks) < tasks_to_queue:
                                logger.info('queueing task %s', task['task_id'])
                                queue_tasks.append(task['task_id'])
                        deps_futures = pending
                if len(queue_tasks) >= tasks_to_queue:
                    break

            for fut in asyncio.as_completed(deps_futures):
                task = await fut
                if task and len(queue_tasks) < tasks_to_queue:
                    logger.info('queueing task %s', task['task_id'])
                    queue_tasks.append(task['task_id'])

            logger.warning('queueing %d tasks', len(queue_tasks))
            if queue_tasks:
                args = {'tasks': queue_tasks}
                await rest_client.request('POST', '/task_actions/bulk_status/queued', args)

    except Exception:
        logger.error('error queueing tasks', exc_info=True)
        if debug:
            raise

    # run again after 5 minute delay
    stop_time = time.time()
    delay = max(60*5 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client)
