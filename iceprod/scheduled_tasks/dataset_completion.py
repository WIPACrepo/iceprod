"""
Mark datasets as completed.

Check jobs in a dataset, and when all are complete mark the dataset
as complete.
"""

import argparse
import asyncio
import logging

import requests.exceptions

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

logger = logging.getLogger('dataset_completion')


async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    datasets = await rest_client.request('GET', '/dataset_summaries/status')
    dataset_ids = datasets.get('processing', [])
    for dataset_id in dataset_ids:
        # clean up misconfigured datasets that have no config
        try:
            try:
                await rest_client.request('GET', f'/config/{dataset_id}')
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.info('dataset %s status -> suspended', dataset_id)
                    args = {'status': 'suspended'}
                    await rest_client.request('PUT', f'/datasets/{dataset_id}/status', args)
                    break
        except Exception:
            logger.error('error checking dataset %s config', dataset_id, exc_info=True)
            if debug:
                raise

        # test if dataset is complete / failed
        try:
            dataset = await rest_client.request('GET', f'/datasets/{dataset_id}')
            jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/job_counts/status')
            if dataset.get('truncated', False) or sum(jobs[s] for s in jobs) >= dataset['jobs_submitted']:
                # jobs are all buffered / materialized
                job_statuses = set(jobs)
                if job_statuses == {'complete'}:
                    logger.info('dataset %s status -> complete', dataset_id)
                    args = {'status': 'complete'}
                    await rest_client.request('PUT', f'/datasets/{dataset_id}/status', args)
                elif 'processing' not in job_statuses:
                    if 'errors' in job_statuses:
                        logger.info('dataset %s status -> errors', dataset_id)
                        args = {'status': 'errors'}
                    else:
                        logger.info('dataset %s status -> suspended', dataset_id)
                        args = {'status': 'suspended'}
                    await rest_client.request('PUT', f'/datasets/{dataset_id}/status', args)
        except Exception:
            logger.error('error completing dataset %s', dataset_id, exc_info=True)
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
