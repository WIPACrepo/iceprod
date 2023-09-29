"""
Buffer jobs and tasks into the queue.

Also known as "late materialization" of jobs and tasks,
this method finds datasets that are not fully materialized and
buffers a few more jobs and tasks into existence so they can be queued.
"""

import argparse
import asyncio
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

logger = logging.getLogger('buffer_jobs_tasks')


async def run(rest_client, only_dataset=None, num=1000, run_once=False, delay=10, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        only_dataset (str): dataset_id if we should only buffer a single dataset
        num (int): max number of jobs per dataset to buffer
        run_once (bool): flag to only run once and stop
        delay (float): wait N seconds between result checks
        debug (bool): debug flag to propagate exceptions
    """
    try:
        logger.info('starting materialization request')
        if only_dataset:
            ret = await rest_client.request('POST', f'/request/{only_dataset}', {'num': num})
        else:
            ret = await rest_client.request('POST', '/', {'num': num})
        materialization_id = ret['result']
        logger.info(f'waiting for materialization request {materialization_id}')

        while True:
            await asyncio.sleep(delay)
            ret = await rest_client.request('GET', f'/status/{materialization_id}')
            if ret['status'] == 'complete':
                logger.info(f'materialization request {materialization_id} complete')
                break
            elif ret['status'] == 'error':
                logger.warning(f'materialization request {materialization_id} failed')
                if run_once:
                    raise Exception('materialization failed: %r', ret)
                break
    except Exception:
        logger.warning('materialization error', exc_info=True)
        if debug or run_once:
            raise


def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('-d', '--dataset', type=str, default=None, help='dataset id (optional)')
    parser.add_argument('-n', '--num', type=int, default=100, help='number of jobs per dataset to buffer')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    asyncio.run(run(rest_client, only_dataset=args.dataset, num=args.num, run_once=True, debug=args.debug))


if __name__ == '__main__':
    main()
