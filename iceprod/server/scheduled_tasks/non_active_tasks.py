"""
Reset tasks that are not active (in a pilot).

Check all the tasks that are processing, and compare with the
tasks in pilots.  Reset the difference.

Initial delay: rand(5 minutes)
Periodic delay: 10 minutes
"""

import logging
import random
import time
import asyncio

from tornado.ioloop import IOLoop

logger = logging.getLogger('non_active_tasks')

def non_active_tasks(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(60,60*5), run, module.rest_client)

async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()

    try:
        datasets = await rest_client.request('GET', '/dataset_summaries/status')
        dataset_ids = []
        if 'processing' in datasets:
            dataset_ids.extend(datasets['processing'])
        if 'truncated' in datasets:
            dataset_ids.extend(datasets['truncated'])
        pilots = await rest_client.request('GET', '/pilots')
        task_ids_in_pilots = set()
        for p in pilots.values():
            if 'tasks' in p and p['tasks']:
                task_ids_in_pilots.update(p['tasks'])
        dataset_tasks = {}
        for dataset_id in dataset_ids:
            dataset_tasks[dataset_id] = await rest_client.request('GET', '/datasets/{}/task_summaries/status'.format(dataset_id))

        async def reset(dataset_id, task_id):
            await rest_client.request('PUT', f'/datasets/{dataset_id}/tasks/{task_id}/status', args)
            data = {
                'name': 'stdlog',
                'task_id': task_id,
                'dataset_id': dataset_id,
                'data': 'task status = processing, but not found in any pilot',
            }
            await rest_client.request('POST', '/logs', data)
            data.update({'name':'stdout', 'data': ''})
            await rest_client.request('POST', '/logs', data)
            data.update({'name':'stderr', 'data': ''})
            await rest_client.request('POST', '/logs', data)

        awaitables = set()
        for dataset_id in dataset_ids:
            tasks = dataset_tasks[dataset_id]
            if 'processing' in tasks:
                reset_tasks = set(tasks['processing'])-task_ids_in_pilots
                if reset_tasks:
                    logger.info('dataset %s reset tasks: %s', dataset_id, reset_tasks)
                    args = {'status':'reset'}
                    for t in reset_tasks:
                        awaitables.add(reset(dataset_id,t))
        
        for fut in asyncio.as_completed(awaitables):
            try:
                await fut
            except Exception:
                logger.error('error resetting non-active tasks', exc_info=True)
                if debug:
                    raise
    except Exception:
        logger.error('error resetting non-active tasks', exc_info=True)
        if debug:
            raise

    # run again after 60 minute delay
    stop_time = time.time()
    delay = max(60*10 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client)
