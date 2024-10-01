"""
Queue tasks.

Move task statuses from idle to waiting, for a certain number of
tasks.  Also uses priority for ordering.
"""

import argparse
import asyncio
import logging
import os

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

logger = logging.getLogger('queue_tasks')

NTASKS = 250000
NTASKS_PER_CYCLE = 1000


async def run(rest_client, ntasks=NTASKS, ntasks_per_cycle=NTASKS_PER_CYCLE, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    try:
        num_tasks_waiting = 0
        num_tasks_queued = 0
        tasks = await rest_client.request('GET', '/task_counts/status')
        if 'idle' in tasks:
            num_tasks_waiting = tasks['idle']
        if 'waiting' in tasks:
            num_tasks_queued = tasks['waiting']
        tasks_to_queue = min(num_tasks_waiting, ntasks - num_tasks_queued, ntasks_per_cycle)
        logger.warning(f'num tasks idle: {num_tasks_waiting}')
        logger.warning(f'num tasks waiting: {num_tasks_queued}')
        logger.warning(f'tasks to waiting: {tasks_to_queue}')

        if tasks_to_queue > 0:
            args = {
                'status': 'idle',
                'keys': 'task_id|depends',
                'sort': 'priority=-1',
                'limit': 5*tasks_to_queue,
            }
            ret = await rest_client.request('GET', '/tasks', args)
            queue_tasks = []
            tasks_queue_pending = 0
            deps_futures = set()

            async def check_deps(task):
                for dep in task.get('depends', []):
                    ret = await rest_client.request('GET', f'/tasks/{dep}')
                    if ret['status'] != 'complete':
                        logger.info('dependency not met for task %s', task['task_id'])
                        await rest_client.request('PATCH', f'/tasks/{task["task_id"]}', {'priority': 0})
                        return None
                return task
            for task in ret['tasks']:
                tasks_queue_pending += 1
                if not task.get('depends', None):
                    logger.info('queueing task %s', task['task_id'])
                    queue_tasks.append(task['task_id'])
                else:
                    fut = asyncio.create_task(check_deps(task))
                    deps_futures.add(fut)
                    if len(deps_futures) >= 20:
                        done, pending = await asyncio.wait(deps_futures, return_when=asyncio.FIRST_COMPLETED)
                        for fut in done:
                            task = await fut
                            if task:
                                logger.info('queueing task %s', task['task_id'])
                                queue_tasks.append(task['task_id'])
                            else:
                                tasks_queue_pending -= 1
                        deps_futures = pending
                while tasks_queue_pending >= tasks_to_queue and deps_futures:
                    done, pending = await asyncio.wait(deps_futures, return_when=asyncio.FIRST_COMPLETED)
                    for fut in done:
                        task = await fut
                        if task:
                            logger.info('queueing task %s', task['task_id'])
                            queue_tasks.append(task['task_id'])
                        else:
                            tasks_queue_pending -= 1
                    deps_futures = pending
                if tasks_queue_pending >= tasks_to_queue:
                    break

            if deps_futures:
                for fut in asyncio.as_completed(deps_futures):
                    task = await fut
                    if task:
                        logger.info('queueing task %s', task['task_id'])
                        queue_tasks.append(task['task_id'])

            logger.warning('queueing %d tasks', len(queue_tasks))
            count = 0
            while queue_tasks:
                task_ids = queue_tasks[:100]
                queue_tasks = queue_tasks[100:]
                args = {'task_ids': task_ids}
                ret = await rest_client.request('POST', '/task_actions/waiting', args)
                count += ret['waiting']
            logger.warning('queued %d tasks', count)

    except Exception:
        logger.error('error queueing tasks', exc_info=True)
        if debug:
            raise


def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('--ntasks', type=int, default=os.environ.get('NTASKS', NTASKS),
                        help='number of tasks to keep queued')
    parser.add_argument('--ntasks_per_cycle', type=int, default=os.environ.get('NTASKS_PER_CYCLE', NTASKS_PER_CYCLE),
                        help='number of tasks to queue per cycle')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    asyncio.run(run(rest_client, ntasks=args.ntasks, ntasks_per_cycle=args.ntasks_per_cycle, debug=args.debug))


if __name__ == '__main__':
    main()
