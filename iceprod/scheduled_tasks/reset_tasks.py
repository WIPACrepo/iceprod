"""
Move tasks from reset to waiting/failed/suspended.
"""

import argparse
import asyncio
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

logger = logging.getLogger('reset_tasks')


async def reset_dataset(dataset_id, rest_client=None, debug=False):
    try:
        dataset = await rest_client.request('GET', '/datasets/{}'.format(dataset_id))
        tasks = await rest_client.request('GET', '/datasets/{}/task_summaries/status'.format(dataset_id))
        if 'reset' in tasks:
            logger.info('dataset %s reset tasks: %s', dataset_id, tasks['reset'])
            for task_id in tasks['reset']:
                try:
                    task = await rest_client.request('GET', '/tasks/{}'.format(task_id))
                    status = 'waiting'
                    if dataset['debug']:
                        status = 'suspended'
                    elif 'failures' in task and task['failures'] > 10:
                        status = 'failed'
                    args = {'status': status}
                    await rest_client.request('PUT', '/datasets/{}/tasks/{}/status'.format(dataset_id,task_id), args)
                except Exception:
                    logger.error('error resetting task %s', task_id, exc_info=True)
                    if debug:
                        raise
    except Exception:
        logger.error('error resetting tasks in dataset %s', dataset_id, exc_info=True)
        if debug:
            raise


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
        awaitables = set()
        for dataset_id in dataset_ids:
            fut = asyncio.create_task(reset_dataset(dataset_id, rest_client=rest_client, debug=debug))
            awaitables.add(fut)
            if len(awaitables) >= 20:
                done, pending = await asyncio.wait(awaitables, return_when=asyncio.FIRST_COMPLETED)
                for fut in done:
                    await fut
                awaitables = pending
        for fut in asyncio.as_completed(awaitables):
            await fut
    except Exception:
        logger.error('error resetting non-active tasks', exc_info=True)
        if debug:
            raise


def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('--dataset-id', default=None, help='dataset id to reset')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    if args.dataset_id:
        asyncio.run(reset_dataset(args.dataset_id, rest_client, debug=args.debug))
    else:
        asyncio.run(run(rest_client, debug=args.debug))


if __name__ == '__main__':
    main()
