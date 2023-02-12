"""
Suspend any jobs and tasks from datasets that are suspended / errors.
"""

import argparse
import asyncio
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

logger = logging.getLogger('dataset_cleanup')


async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    datasets = await rest_client.request('GET', '/dataset_summaries/status')
    dataset_ids = []
    if 'suspended' in datasets:
        dataset_ids.extend(list(datasets['suspended']))
    if 'errors' in datasets:
        dataset_ids.extend(list(datasets['errors']))
    for dataset_id in dataset_ids:
        try:
            jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/job_summaries/status')
            if 'processing' in jobs:
                for job_id in jobs['processing']:
                    args = {'status': 'suspended'}
                    await rest_client.request('PUT', f'/datasets/{dataset_id}/jobs/{job_id}/status', args)

            tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/task_summaries/status')
            task_ids = []
            for status in tasks:
                if status not in ('complete','suspended','failed'):
                    task_ids.extend(tasks[status])
            while task_ids:
                tids = task_ids[:10000]
                task_ids = task_ids[10000:]
                args = {'tasks': tids}
                await rest_client.request('POST', f'/datasets/{dataset_id}/task_actions/bulk_status/suspended', args)

        except Exception:
            logger.error('error cleaning a job/task in dataset %s', dataset_id, exc_info=True)
            if debug:
                raise

    # clean up tasks in suspended/errors jobs in processing datasets
    dataset_ids = []
    if 'processing' in datasets:
        dataset_ids.extend(list(datasets['processing']))
    for dataset_id in dataset_ids:
        try:
            jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/job_summaries/status')
            job_ids = set()
            if 'suspended' in jobs:
                job_ids.update(jobs['suspended'])
            if 'errors' in jobs:
                job_ids.update(jobs['errors'])

            args = {
                'keys': 'task_id|job_id|status'
            }
            tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args)
            task_ids = []
            for tasks in tasks.values():
                if tasks['status'] not in ('complete','suspended') and tasks['job_id'] in job_ids:
                    task_ids.append(tasks['task_id'])
            while task_ids:
                tids = task_ids[:10000]
                task_ids = task_ids[10000:]
                args = {'tasks': tids}
                await rest_client.request('POST', f'/datasets/{dataset_id}/task_actions/bulk_status/suspended', args)

        except Exception:
            logger.error('error cleaning a task in dataset %s', dataset_id, exc_info=True)
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
