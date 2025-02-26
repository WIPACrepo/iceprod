"""
Queue tasks.

Move task statuses from idle to waiting, for a certain number of
tasks.  Also uses priority for ordering.
"""

import argparse
import asyncio
import logging

from wipac_dev_tools import from_environment, strtobool

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

logger = logging.getLogger('queue_tasks')

default_config = {
    'NTASKS': 250000,
    'NTASKS_PER_CYCLE': 1000,
    'TASKS_GET_FACTOR': 5,
}


async def run(rest_client, config, dataset_id='', gpus=None, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        config (dict): config dict
        dataset_id (str): dataset to queue
        gpus (bool): run on gpu tasks, cpu tasks, or both
        debug (bool): debug flag to propagate exceptions
    """
    try:
        num_tasks_idle = 0
        num_tasks_waiting = 0
        if dataset_id:
            route = f'/datasets/{dataset_id}/task_counts/status'
        else:
            route = '/task_counts/status'
        args = {'status': 'idle|waiting'}
        if gpus is not None:
            args['gpu'] = gpus
        tasks = await rest_client.request('GET', route, args)
        if 'idle' in tasks:
            num_tasks_idle = tasks['idle']
        if 'waiting' in tasks:
            num_tasks_waiting = tasks['waiting']
        tasks_to_queue = min(num_tasks_idle, config['NTASKS'] - num_tasks_waiting, config['NTASKS_PER_CYCLE'])
        logger.warning(f'num tasks idle: {num_tasks_idle}')
        logger.warning(f'num tasks waiting: {num_tasks_waiting}')
        logger.warning(f'tasks to waiting: {tasks_to_queue}')

        if tasks_to_queue > 0:
            if dataset_id:
                route = f'/datasets/{dataset_id}/tasks'
                args = {
                    'status': 'idle',
                    'keys': 'task_id|depends|requirements.gpu',
                }
            else:
                route = '/tasks'
                args = {
                    'status': 'idle',
                    'keys': 'task_id|depends|requirements.gpu',
                    'sort': 'priority=-1',
                    'limit': config['TASKS_GET_FACTOR'] * tasks_to_queue,
                }
            ret = await rest_client.request('GET', route, args)
            idle_tasks = ret.values() if dataset_id else ret['tasks']
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
            for task in idle_tasks:
                if gpus is not None:
                    task_gpus = task.get('requirements', {}).get('gpu', 0)
                    if (gpus and task_gpus <= 0) or (not gpus and task_gpus > 0):
                        continue
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
    config = from_environment(default_config)

    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('--dataset-id', help='dataset id')
    parser.add_argument('--gpus', default=None, type=strtobool, help='whether to select only gpu or non-gpu tasks')
    parser.add_argument('--ntasks', type=int, default=config['NTASKS'],
                        help='number of tasks to keep queued')
    parser.add_argument('--ntasks_per_cycle', type=int, default=config['NTASKS_PER_CYCLE'],
                        help='number of tasks to queue per cycle')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()
    config.update(vars(args))

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    asyncio.run(run(rest_client, dataset_id=args.dataset_id, gpus=args.gpus, config=config, debug=args.debug))


if __name__ == '__main__':
    main()
