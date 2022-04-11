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
            'keys': 'task_id|depends|dataset_id',
        }
        if dataset_id:
            ret = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args)
            tasks = ret.values()
        else:
            url = f'/datasets/{dataset_id}/tasks'
            ret = await rest_client.request('GET', '/tasks', args)
            tasks = ret['tasks']

        async def check_deps(task):
            dep_futures = []
            for dep in task.get('depends', []):
                t = asyncio.create_task(rest_client.request('GET', f'/tasks/{dep}'))
                dep_futures.append(t)
            for ret in await asyncio.gather(*dep_futures):
                if ret['status'] != 'complete':
                    logger.info('dependency not met for task %s', task['task_id'])
                    return None
            return task

        # check dependencies
        futures = set()
        tasks2 = []
        for task in tasks:
            if len(futures) >= 20:
                done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
                for t in done:
                    ret = await t
                    if ret:
                        tasks2.append(ret)
                futures = pending
            t = asyncio.create_task(check_deps(task))
            futures.add(t)
        while futures:
            done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                ret = await t
                if ret:
                    tasks2.append(ret)
            futures = pending
        logger.warning(f'len(tasks) = {len(tasks)}')
        logger.warning(f'len(tasks2) = {len(tasks2)}')

        # update priorities
        futures = set()
        for task in tasks2:
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

    # run again after 4 hour delay
    stop_time = time.time()
    delay = max(3600*4 - (stop_time-start_time), 600)
    IOLoop.current().call_later(delay, run, rest_client)


def main():
    import argparse
    import os
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    parser.add_argument('-t', '--token', default=os.environ.get('ICEPROD_TOKEN', None), help='auth token')
    parser.add_argument('-d', '--dataset', type=str, default=None, help='dataset id (optional)')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()
    args = vars(args)

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args['log_level'].upper()))

    from rest_tools.client import RestClient
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    import asyncio
    asyncio.run(run(rpc, dataset_id=args['dataset'], debug=args['debug']))

if __name__ == '__main__':
    main()
