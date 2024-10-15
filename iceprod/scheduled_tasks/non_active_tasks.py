"""
Reset tasks that are not active (in a pilot).

Check all the tasks that are processing, and compare with the
tasks in pilots.  Reset the difference.
"""

import argparse
import asyncio
from datetime import datetime
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.server.util import str2datetime

logger = logging.getLogger('non_active_tasks')


async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    try:
        datasets = await rest_client.request('GET', '/dataset_summaries/status')
        dataset_ids = []
        if 'processing' in datasets:
            dataset_ids.extend(datasets['processing'])
        if 'truncated' in datasets:
            dataset_ids.extend(datasets['truncated'])
        pilots = await rest_client.request('GET', '/pilots', {'keys': 'pilot_id|tasks'})
        task_ids_in_pilots = set()
        for p in pilots.values():
            if 'tasks' in p and p['tasks']:
                task_ids_in_pilots.update(p['tasks'])
        dataset_tasks = {}
        for dataset_id in dataset_ids:
            dataset_tasks[dataset_id] = await rest_client.request('GET', '/datasets/{}/task_summaries/status'.format(dataset_id))

        async def reset(dataset_id, task_id):
            args = {'status':'reset'}
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

        async def update_pilot(pilot_id, tasks):
            args = {'tasks': tasks}
            await rest_client.request('PATCH', '/pilots/{}'.format(pilot_id), args)

        async def delete_pilot(pilot_id):
            await rest_client.request('DELETE', '/pilots/{}'.format(pilot_id))

        awaitables = set()
        reset_pilots = set()
        for dataset_id in dataset_ids:
            tasks = dataset_tasks[dataset_id]
            if 'processing' in tasks:
                reset_tasks = set(tasks['processing'])-task_ids_in_pilots
                for task_id in reset_tasks:
                    args = {'keys': 'status|status_changed'}
                    task = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks/{task_id}', args)
                    # check status, and that we haven't just changed status
                    if task['status'] == 'processing' and (datetime.utcnow()-str2datetime(task['status_changed'])).total_seconds() > 600:
                        logger.info('dataset %s reset task %s', dataset_id, task_id)
                        awaitables.add(reset(dataset_id,task_id))

            for k in tasks.keys():
                if k in ('reset', 'waiting', 'failed', 'suspended'):
                    for task_id in task_ids_in_pilots.intersection(tasks[k]):
                        args = {'keys': 'status|status_changed'}
                        task = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks/{task_id}', args)
                        # check status, and that we haven't just changed status
                        if task['status'] in ('reset', 'waiting', 'failed', 'suspended') and (datetime.utcnow()-str2datetime(task['status_changed'])).total_seconds() > 600:
                            reset_pilots.add(task_id)

        for p in pilots.values():
            if 'tasks' in p:
                updated_tasks = set(p['tasks']) - reset_pilots
                if not updated_tasks:
                    awaitables.add(delete_pilot(p['pilot_id']))
                elif updated_tasks != set(p['tasks']):
                    awaitables.add(delete_pilot(p['pilot_id'], updated_tasks))

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


def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    asyncio.run(run(rest_client, debug=args.debug))


if __name__ == '__main__':
    main()
