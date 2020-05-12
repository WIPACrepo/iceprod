"""
Update task priority.

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
from iceprod.server.priority import Priority

logger = logging.getLogger('update_task_priority')

def update_task_priority(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(60,600), run, module.rest_client)

async def run(rest_client, dataset_id=None, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        dataset_id (str): (optional) dataset id to update
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()
    prio = Priority(rest_client)
    try:
        args = {
            'status': 'waiting|queued|processing|reset',
            'keys': 'task_id|dataset_id',
        }
        if dataset_id:
            ret = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args)
            tasks = ret.values()
        else:
            url = f'/datasets/{dataset_id}/tasks'
            ret = await rest_client.request('GET', '/tasks', args)
            tasks = ret['tasks']

        futures = set()
        for task in tasks:
            if len(futures) >= 20:
                done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
                futures = pending
            p = await prio.get_task_prio(task['dataset_id'], task['task_id'])
            logger.info('updating priority for %s.%s = %.4f', task['dataset_id'], task['task_id'], p)
            t = asyncio.create_task(rest_client.request('PATCH', f'/tasks/{task["task_id"]}', {'priority': p}))
            futures.add(t)
        while futures:
            done, pending = await asyncio.wait(futures)
            futures = pending

    except Exception:
        logger.error('error updating task priority', exc_info=True)
        if debug:
            raise

    # run again after 12 hour delay
    stop_time = time.time()
    delay = max(3600*12 - (stop_time-start_time), 600)
    IOLoop.current().call_later(delay, run, rest_client)
