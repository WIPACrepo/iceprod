"""
Clean job temp directories.

Check job temp directories, and if the job is complete then delete it.
"""

import argparse
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, UTC
from functools import partial
import logging
import os
import shutil
from typing import Any
from wipac_dev_tools import from_environment

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.core.gridftp import GridFTP
from iceprod.server.util import str2datetime

logger = logging.getLogger('job_temp_cleaning')


listing = dict[str, list[str]]


def list_dataset_job_dirs_fs(path: str, *, prefix: str | None = None) -> listing:
    dataset_dirs = defaultdict(list)
    if prefix:
        path = os.path.join(path, prefix)
        dataset_dirs[prefix] = os.listdir(path)
    else:
        dirs = os.listdir(path)
        for d in dirs:
            p = os.path.join(path, d)
            if os.path.isdir(p):
                dataset_dirs[d] = os.listdir(p)
    return dataset_dirs


def filter_job_dirs(job_dirs: list[str], job_indexes: set[str], debug: bool = False):
    for j in job_dirs:
        if not j.isnumeric():
            if debug:
                logger.info('j is not numeric: %r', j)
                raise Exception('not numeric')
            continue
        if int(j) in job_indexes:
            yield j


async def clean_dataset_dir(dataset_num: str, dataset_id: str, job_dirs: list[str], temp_dir: str, rmtree: Any, debug: bool, rest_client: Any):
    logger.info('temp cleaning for dataset %r', dataset_num)
    logger.debug('job_dirs: %r', job_dirs)

    now = datetime.now(UTC)
    suspend_time = timedelta(days=90)

    try:
        jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/jobs', {'keys':'status|status_changed|job_index'})
    except Exception:
        logger.error('failed to get jobs for dataset %r', dataset_num, exc_info=True)
        if debug:
            raise
        return
    logger.debug('jobs: %r', jobs)

    job_indexes = set()
    for job in jobs.values():
        if job['status'] == 'complete' or (job['status'] != 'processing' and now - str2datetime(job['status_changed']) > suspend_time):
            job_indexes.add(job['job_index'])
    logger.debug('job_indexes: %r', job_indexes)

    futures = set()  # type: ignore
    for j in filter_job_dirs(job_dirs, job_indexes, debug=debug):
        while len(futures) >= 16:
            done, futures = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
            for f in done:
                try:
                    await f
                except Exception:
                    logger.warning('failed to clean site_temp', exc_info=True)
                    if debug:
                        raise

        dagtemp = os.path.join(temp_dir, dataset_num, j)
        logger.info('cleaning site_temp %r', dagtemp)
        ret = rmtree(dagtemp)
        if ret and asyncio.iscoroutine(ret):
            futures.add(asyncio.create_task(ret))

    if futures:
        for f in asyncio.as_completed(futures):
            await f


async def run(rest_client, temp_dir, list_dirs, rmtree, dataset=None, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        temp_dir (str): temporary directory to clean
        list_dirs (callable): list temporary directory
        rmtree (callable): delete temporary directory and contents
        dataset (int): dataset num to run over (optional)
        debug (bool): debug flag to propagate exceptions
    """
    try:
        # get all the job_indexes currently in tmp
        if dataset:
            dataset_dirs = list_dirs(temp_dir, prefix=str(dataset))
            if dataset_dirs and asyncio.iscoroutine(dataset_dirs):
                dataset_dirs = await dataset_dirs
        else:
            dataset_dirs = list_dirs(temp_dir)
            if dataset_dirs and asyncio.iscoroutine(dataset_dirs):
                dataset_dirs = await dataset_dirs
        logger.debug('dataset_dirs: %r', dataset_dirs)

        ret = await rest_client.request('GET', '/datasets?keys=dataset_id|dataset')
        logger.debug('datasets api raw: %r', ret)
        datasets = {}
        for d in ret:
            datasets[str(ret[d]['dataset'])] = ret[d]['dataset_id']
        logger.debug('datasets: %r', datasets)

        for d in dataset_dirs:
            if not isinstance(dataset_dirs[d], dict):
                continue
            await clean_dataset_dir(d, datasets[d], dataset_dirs[d], temp_dir, rmtree, debug=debug, rest_client=rest_client)

    except Exception:
        logger.error('error checking job temp', exc_info=True)
        if debug:
            raise


def main():
    default_config = {
        'SITE_TEMP': '/scratch',
    }
    config = from_environment(default_config)

    parser = argparse.ArgumentParser(description='job temp cleaning')
    add_auth_to_argparse(parser)
    parser.add_argument('-d', '--dataset', type=str, help='dataset num (optional)')
    parser.add_argument('--site-temp', default=config['SITE_TEMP'], help='site temp location')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    if os.path.exists(args.site_temp):
        logging.info('using local filesystem')
        listdir = list_dataset_job_dirs_fs
        rmtree = shutil.rmtree
    else:
        raise RuntimeError('unknown type of scratch')

    logging.info('temp dir: %r', args.site_temp)
    asyncio.run(run(rest_client, args.site_temp, list_dirs=listdir, rmtree=rmtree, dataset=args.dataset, debug=args.debug))


if __name__ == '__main__':
    main()
