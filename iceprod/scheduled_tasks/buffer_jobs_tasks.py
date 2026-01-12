"""
Buffer jobs and tasks into the queue.

Also known as "late materialization" of jobs and tasks,
this method finds datasets that are not fully materialized and
buffers a few more jobs and tasks into existence so they can be queued.
"""

import argparse
import asyncio
import logging

from rest_tools.client import RestClient

from iceprod.client_auth import add_auth_to_argparse, create_rest_client


logger = logging.getLogger('buffer_jobs_tasks')

MAX_PARALLEL_REQUESTS = 100


async def check_requests(requests: list[str], *, debug: bool, rest_client: RestClient) -> list[str]:
    logging.info('check_requests starting with %d requests', len(requests))
    futures = set()
    for mat_id in requests:
        futures.add(asyncio.create_task(rest_client.request('GET', f'/actions/materialization/{mat_id}')))

    requests2 = []
    while futures:
        logging.info('check_requests waiting on %d requests', len(futures))
        done, futures = await asyncio.wait(futures, timeout=1)
        for fut in done:
            try:
                ret = await fut
            except Exception as e:
                logger.warning('materialization request could not be queried')
                if debug:
                    raise e
            else:
                if ret['status'] in ('queued', 'processing'):
                    requests2.append(ret['id'])
                elif ret['status'] == 'complete':
                    logger.info(f'materialization request {ret["id"]} complete')
                elif ret['status'] == 'error':
                    logger.warning(f'materialization request {ret["id"]} failed')
                    if debug:
                        raise Exception(f'materialization failed: {ret}')
                else:
                    logger.warning(f'materialization request {ret["id"]} has unknown status')
                    if debug:
                        raise Exception(f'materialization failed: {ret}')
    return requests2


async def run(rest_client: RestClient, only_dataset: None | str = None, num: int = 1000, delay: int = 5, debug: bool = False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        only_dataset (str): dataset_id if we should only buffer a single dataset
        num (int): max number of jobs per dataset to buffer
        delay (float): wait N seconds between result checks
        debug (bool): debug flag to propagate exceptions
    """
    try:
        logger.info('starting materialization request')
        if only_dataset:
            datasets = [only_dataset]
        else:
            ret = await rest_client.request('GET', '/datasets', {'status': 'processing', 'keys': 'dataset_id'})
            datasets = list(ret.keys())

        futures = set()
        requests = []
        for i,dataset_id in enumerate(datasets):
            logger.info('starting request for dataset %s', dataset_id)
            futures.add(asyncio.create_task(rest_client.request('POST', '/actions/materialization', {'dataset_id': dataset_id, 'num': num})))
            while len(futures) > 8:
                logger.info('waiting for futures')
                done, futures = await asyncio.wait(futures, timeout=1)
                for fut in done:
                    try:
                        ret = await fut
                        materialization_id = ret['result']
                        logger.info(f'waiting for materialization request {materialization_id}')
                        requests.append(materialization_id)
                    except Exception as e:
                        logger.error('error creating materialization request for dataset', exc_info=True)
                        if debug:
                            raise e

            logger.warning('progress: %d of %d complete', i+1-len(futures)-len(requests), len(datasets))
            logger.info('%d futures outstanding', len(futures))
            logger.info('%d requests outstanding', len(requests))
            while len(requests) > MAX_PARALLEL_REQUESTS:
                requests = await check_requests(requests, debug=debug, rest_client=rest_client)
                await asyncio.sleep(delay)

        while futures:
            logger.info('waiting for futures')
            done, futures = await asyncio.wait(futures, timeout=1)
            for fut in done:
                try:
                    ret = await fut
                    materialization_id = ret['result']
                    logger.info(f'waiting for materialization request {materialization_id}')
                    requests.append(materialization_id)
                except Exception as e:
                    logger.error('error creating materialization request for dataset', exc_info=True)
                    if debug:
                        raise e

        while requests:
            logger.info('progress: %d of %d complete', len(datasets)-len(futures)-len(requests), len(datasets))
            logger.info('%d futures outstanding', len(futures))
            logger.info('%d requests outstanding', len(requests))
            requests = await check_requests(requests, debug=debug, rest_client=rest_client)
            await asyncio.sleep(delay)

        logger.info('complete')

    except Exception:
        logger.warning('materialization error', exc_info=True)
        if debug:
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

    asyncio.run(run(rest_client, only_dataset=args.dataset, num=args.num, debug=args.debug))


if __name__ == '__main__':
    main()
