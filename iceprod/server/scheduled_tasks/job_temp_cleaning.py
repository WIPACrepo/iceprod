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

async def run(rest_client, cfg, executor, debug=False):
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
        logger.info('dataset_dirs: %r', dataset_dirs)
        ret = await rest_client.request('GET', '/datasets?keys=dataset_id|dataset')
        datasets = {}
        for d in ret:
            datasets[str(d['dataset'])] = d['dataset_id']
        for entry in dataset_dirs:
            if not entry.directory:
                continue
            d = entry.name
            logger.info('temp cleaning for dataset %r', d)
            try:
                job_dirs = await wrap_future(executor.submit(partial(GridFTP.list, os.path.join(temp_dir, d))))
            except Exception:
                logger.error('failed to get job dirs for dataset %r', d, exc_info=True)
                continue
            logger.info('job_dirs: %r', job_dirs)
            jobs = await rest_client.request('GET', '/datasets/{}/jobs'.format(datasets[d]))
            logger.info('jobs: %r', jobs)
            job_indexes = set()
            for job in jobs.values():
                if job['status'] == 'complete' or (job['status'] != 'processing'
                    and now - get_date(job['status_changed']) > suspend_time):
                    job_indexes.add(job['job_index'])
            logger.info('job_indexes: %r', job_indexes)
            for job in job_dirs:
                j = job.name
                if not j.isnumeric():
                    continue
                if int(j) in job_indexes:
                    try:
                        dagtemp = os.path.join(temp_dir, d, j)
                        logger.info('cleaning site_temp %r', dagtemp)
                        await wrap_future(executor.submit(partial(GridFTP.rmtree, dagtemp)))
                    except Exception:
                        logger.warning('failed to clean site_temp', exc_info=True)

    except Exception:
        logger.error('error checking job temp', exc_info=True)
        if debug:
            raise

    # run again after 60 minute delay
    stop_time = time.time()
    delay = max(60*60 - (stop_time-start_time), 60*10)
    IOLoop.current().call_later(delay, run, rest_client)

def get_date(strdate):
    if '.' in strdate:
        return datetime.strptime(strdate, '%Y-%m-%dT%H:%M:%S.%f')
    else:
        return datetime.strptime(strdate, '%Y-%m-%dT%H:%M:%S')
