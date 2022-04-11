"""
Clean job temp directories.

Check job temp directories, and if the job is complete then delete it.

Initial delay: rand(15 minutes)
Periodic delay: 60 minutes
"""

import os
import logging
import random
import time
from datetime import datetime, timedelta
from functools import partial
from asyncio import wrap_future

from tornado.ioloop import IOLoop

from iceprod.core.gridftp import GridFTP
from iceprod.server.util import str2datetime

logger = logging.getLogger('job_temp_cleaning')

def job_temp_cleaning(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(10,60*10), run, module.rest_client,
            module.cfg, module.executor)

async def run(rest_client, cfg, executor, dataset=None, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        cfg (:py:class:`iceprod.server.config.IceProdConfig`): iceprod config
        debug (bool): debug flag to propagate exceptions
    """
    if 'queue' not in cfg or 'site_temp' not in cfg['queue']:
        return

    start_time = time.time()

    suspend_time = timedelta(days=90)
    now = datetime.utcnow()

    try:
        # get all the job_indexes currently in tmp
        temp_dir = cfg['queue']['site_temp']
        dataset_dirs = await wrap_future(executor.submit(partial(GridFTP.list, temp_dir, details=True)))
        logger.debug('dataset_dirs: %r', dataset_dirs)
        ret = await rest_client.request('GET', '/datasets?keys=dataset_id|dataset')
        datasets = {}
        for d in ret:
            datasets[str(ret[d]['dataset'])] = ret[d]['dataset_id']
        for entry in dataset_dirs:
            if not entry.directory:
                continue
            d = entry.name
            if dataset and d != dataset:
                continue
            logger.info('temp cleaning for dataset %r', d)
            try:
                job_dirs = await wrap_future(executor.submit(partial(GridFTP.list, os.path.join(temp_dir, d), details=True)))
            except Exception:
                logger.error('failed to get job dirs for dataset %r', d, exc_info=True)
                if debug:
                    raise
                continue
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
                if job['status'] == 'complete' or (job['status'] != 'processing'
                    and now - str2datetime(job['status_changed']) > suspend_time):
                    job_indexes.add(job['job_index'])
            logger.debug('job_indexes: %r', job_indexes)
            for job in job_dirs:
                if not job.directory:
                    continue
                j = job.name
                if not j.isnumeric():
                    if debug:
                        logger.info('j is not numeric: %r', j)
                        raise Exception('not numeric')
                    continue
                if int(j) in job_indexes:
                    try:
                        dagtemp = os.path.join(temp_dir, d, j)
                        logger.info('cleaning site_temp %r', dagtemp)
                        await wrap_future(executor.submit(partial(GridFTP.rmtree, dagtemp)))
                    except Exception:
                        logger.warning('failed to clean site_temp', exc_info=True)
                        if debug:
                            raise

    except Exception:
        logger.error('error checking job temp', exc_info=True)
        if debug:
            raise

    # run again after 60 minute delay
    stop_time = time.time()
    delay = max(60*60 - (stop_time-start_time), 60*10)
    IOLoop.current().call_later(delay, run, rest_client, cfg, executor, debug)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    parser.add_argument('-t', '--token', default=os.environ.get('ICEPROD_TOKEN', None), help='auth token')
    parser.add_argument('-d', '--dataset', type=str, help='dataset num (optional)')
    parser.add_argument('--site-temp', default='gsiftp://gridftp-scratch.icecube.wisc.edu/mnt/tank/simprod', help='site temp location')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()
    args = vars(args)

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args['log_level'].upper()))

    from rest_tools.client import RestClient
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    from iceprod.server.config import IceProdConfig
    cfg = IceProdConfig()
    if 'queue' not in cfg:
        cfg['queue'] = {}
    if 'site_temp' not in cfg['queue']:
        cfg['queue']['site_temp'] = args['site_temp']

    from concurrent.futures import ThreadPoolExecutor
    pool = ThreadPoolExecutor()

    import asyncio
    asyncio.run(run(rpc, cfg, pool, dataset=args['dataset'], debug=args['debug']))

if __name__ == '__main__':
    main()
