"""
Clean job temp directories.

Check job temp directories, and if the job is complete then delete it.
"""

import argparse
import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
import logging
import os
from urllib.parse import urlparse
from wipac_dev_tools import from_environment

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.core.gridftp import GridFTP
from iceprod.s3 import S3
from iceprod.server.util import str2datetime

logger = logging.getLogger('job_temp_cleaning')


async def list_dataset_job_dirs_gridftp(path, prefix=None, executor=None):
    dataset_dirs = defaultdict(dict)

    async def list_job_dirs(d):
        jobs = await asyncio.wrap_future(executor.submit(partial(GridFTP.list, os.path.join(path, d), details=True)))
        for entry2 in jobs:
            if not entry2.directory:
                continue
            j = entry2.name
            dataset_dirs[d][j] = entry2.size

    if prefix:
        await list_job_dirs(prefix)
    else:
        dirs = await asyncio.wrap_future(executor.submit(partial(GridFTP.list, path, details=True)))
        for entry in dirs:
            if not entry.directory:
                continue
            try:
                await list_job_dirs(entry.name)
            except Exception:
                logger.warning('error listing %s', entry.name, exc_info=True)
    return dataset_dirs


async def rmtree_gridftp(path, executor=None):
    await asyncio.wrap_future(executor.submit(partial(GridFTP.rmtree, path)))


async def list_dataset_job_dirs_webdav(path, prefix=None, rest_client=None):
    dataset_dirs = defaultdict(dict)
    return dataset_dirs


async def rmtree_webdav(path, rest_client=None):
    pass


async def list_dataset_job_dirs_s3(path, prefix=None, s3_client=None):
    if prefix:
        path = os.path.join(path, prefix)
        ret = await s3_client.list(path)
        return {prefix: ret}
    else:
        return await s3_client.list(path)


async def rmtree_s3(path, s3_client=None):
    await s3_client.rmtree(path)


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
    suspend_time = timedelta(days=90)
    now = datetime.utcnow()

    try:
        # get all the job_indexes currently in tmp
        if dataset:
            dataset_dirs = await list_dirs(temp_dir, prefix=str(dataset))
        else:
            dataset_dirs = await list_dirs(temp_dir)
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
            logger.info('temp cleaning for dataset %r', d)
            job_dirs = dataset_dirs[d]
            logger.debug('job_dirs: %r', job_dirs)

            try:
                jobs = await rest_client.request('GET', f'/datasets/{datasets[d]}/jobs', {'keys':'status|status_changed|job_index'})
            except Exception:
                logger.error('failed to get jobs for dataset %r', d, exc_info=True)
                if debug:
                    raise
                continue
            logger.debug('jobs: %r', jobs)

            job_indexes = set()
            for job in jobs.values():
                if job['status'] == 'complete' or (job['status'] != 'processing' and now - str2datetime(job['status_changed']) > suspend_time):
                    job_indexes.add(job['job_index'])
            logger.debug('job_indexes: %r', job_indexes)

            futures = set()
            for j in job_dirs:
                if isinstance(job_dirs[j], dict):
                    continue
                if not j.isnumeric():
                    if debug:
                        logger.info('j is not numeric: %r', j)
                        raise Exception('not numeric')
                    continue
                if int(j) in job_indexes:
                    while len(futures) >= 16:
                        done, futures = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
                        for f in done:
                            try:
                                await f
                            except Exception:
                                logger.warning('failed to clean site_temp', exc_info=True)
                                if debug:
                                    raise

                    dagtemp = os.path.join(temp_dir, d, j)
                    logger.info('cleaning site_temp %r', dagtemp)
                    futures.add(asyncio.create_task(rmtree(dagtemp)))

            if futures:
                for f in asyncio.as_completed(futures):
                    try:
                        await f
                    except Exception:
                        logger.warning('failed to clean site_temp', exc_info=True)
                        if debug:
                            raise

    except Exception:
        logger.error('error checking job temp', exc_info=True)
        if debug:
            raise


def main():
    default_config = {
        'SITE_TEMP': 'gsiftp://gridftp-scratch.icecube.wisc.edu/mnt/tank/simprod',
        'S3_ACCESS_KEY': '',
        'S3_SECRET_KEY': '',
    }
    config = from_environment(default_config)

    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('-d', '--dataset', type=str, help='dataset num (optional)')
    parser.add_argument('--site-temp', default=config['SITE_TEMP'], help='site temp location')
    parser.add_argument('--s3-access-key', default=config['S3_ACCESS_KEY'], help='s3 access key')
    parser.add_argument('--s3-secret-key', default=config['S3_SECRET_KEY'], help='s3 secret key')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    if args.site_temp.startswith('gsiftp://'):
        logging.info('using GridFTP')
        pool = ThreadPoolExecutor()
        listdir = partial(list_dataset_job_dirs_gridftp, executor=pool)
        rmtree = partial(rmtree_gridftp, executor=pool)
    elif args.s3_access_key:
        logging.info('using S3')
        o = urlparse(args.site_temp)
        url = o.path.lstrip('/')
        if '/' in url:
            bucket, url = url.split('/', 1)
        else:
            bucket = url
            url = ''
        host = f'{o.scheme}://{o.netloc}'
        logging.info('host: %r, bucket: %r', host, bucket)
        args.site_temp = url
        s3client = S3(host, bucket=bucket, access_key=args.s3_access_key, secret_key=args.s3_secret_key)
        listdir = partial(list_dataset_job_dirs_s3, s3_client=s3client)
        rmtree = partial(rmtree_s3, s3_client=s3client)
    else:
        raise RuntimeError('unknown type of scratch')

    logging.info('temp dir: %r', args.site_temp)
    asyncio.run(run(rest_client, args.site_temp, list_dirs=listdir, rmtree=rmtree, dataset=args.dataset, debug=args.debug))


if __name__ == '__main__':
    main()
