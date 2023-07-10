"""
Mark jobs as completed.

Check tasks in a job, and when all are complete mark the job as complete.
"""

import argparse
import asyncio
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

logger = logging.getLogger('job_completion')


async def run(rest_client, dataset_id=None, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        dataset_id (str): specific dataset to run on
        debug (bool): debug flag to propagate exceptions
    """
    if dataset_id:
        dataset_ids = [dataset_id]
    else:
        datasets = await rest_client.request('GET', '/dataset_summaries/status')
        dataset_ids = []
        if 'processing' in datasets:
            dataset_ids.extend(list(datasets['processing']))
        if 'truncated' in datasets:
            dataset_ids.extend(list(datasets['truncated']))
    for dataset_id in dataset_ids:
        try:
            jobs = await rest_client.request('GET', '/datasets/{}/job_summaries/status'.format(dataset_id))
            if 'processing' not in jobs:
                continue
            for job_id in jobs['processing']:
                tasks = await rest_client.request('GET', '/datasets/{}/tasks?keys=task_id|task_index|status&job_id={}'.format(dataset_id,job_id))
                task_statuses = []
                for task_id in sorted(tasks, key=lambda t:tasks[t]['task_index']):
                    task_statuses.append(tasks[task_id]['status'])
                if set(task_statuses) == set(['complete']):
                    logger.info('dataset %s job %s status -> complete', dataset_id, job_id)
                    args = {'status':'complete'}
                    await rest_client.request('PUT', '/datasets/{}/jobs/{}/status'.format(dataset_id,job_id), args)
                for status in task_statuses:
                    if status not in ('idle','waiting','queued','processing','reset','complete'):
                        if status == 'failed':
                            logger.info('dataset %s job %s status -> errors', dataset_id, job_id)
                            args = {'status':'errors'}
                        else:
                            logger.info('dataset %s job %s status -> suspended', dataset_id, job_id)
                            args = {'status':'suspended'}
                        await rest_client.request('PUT', '/datasets/{}/jobs/{}/status'.format(dataset_id,job_id), args)
                        break
        except Exception:
            logger.error('error completing a job in dataset %s', dataset_id, exc_info=True)
            if debug:
                raise


def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')
    parser.add_argument('--dataset-id', help='specific dataset to run on')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    asyncio.run(run(rest_client, dataset_id=args.dataset_id, debug=args.debug))


if __name__ == '__main__':
    main()
