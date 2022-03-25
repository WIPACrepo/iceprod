"""
Buffer jobs and tasks into the queue.

Also known as "late materialization" of jobs and tasks,
this method finds datasets that are not fully materialized and
buffers a few more jobs and tasks into existence so they can be queued.

Initial delay: rand(10 minutes)
Periodic delay: 10 minutes
"""

import os
import logging
import random
import time
import asyncio

from tornado.ioloop import IOLoop
from rest_tools.client import RestClient

from iceprod.core.parser import ExpParser
from iceprod.core.resources import Resources
from iceprod.server.priority import Priority

logger = logging.getLogger('buffer_jobs_tasks')

def buffer_jobs_tasks(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # set up rest client
    if 'materialization' in module.cfg and 'url' in module.cfg['materialization']:
        url = module.cfg['materialization']['url']
    else:
        raise Exception('no materialization url')

    if ('rest_api' in module.cfg and 'url' in module.cfg['rest_api']
        and 'auth_key' in module.cfg['rest_api']):
        auth_key = module.cfg['rest_api']['auth_key']
    else:
        raise Exception('no auth key')
    rest_client = RestClient(url, auth_key)

    IOLoop.current().call_later(random.randint(10,60*10), run, rest_client)

async def run(rest_client, only_dataset=None, num=1000, run_once=False, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        only_dataset (str): dataset_id if we should only buffer a single dataset
        num (int): max number of jobs per dataset to buffer
        run_once (bool): flag to only run once and stop
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()

    try:
        logger.info('starting materialization request')
        if only_dataset:
            ret = await rest_client.request('POST', f'/request/{only_dataset}', {'num': num})
        else:
            ret = await rest_client.request('POST', '/', {'num': num})
        materialization_id = ret['result']
        logger.info(f'waiting for materialization request {materialization_id}')

        while True:
            await asyncio.sleep(10)
            ret = await rest_client.request('GET', f'/status/{materialization_id}')
            if ret['status'] == 'complete':
                logger.info(f'materialization request {materialization_id} complete')
                break
            elif ret['status'] == 'error':
                logger.warning(f'materialization request {materialization_id} failed')
                break
    except Exception as e:
        logger.warning('materialization error', exc_info=True)
        if debug or run_once:
            raise

    if not run_once:
        # run again after 10 minute delay
        stop_time = time.time()
        delay = max(60*10 - (stop_time-start_time), 60*5)
        IOLoop.current().call_later(delay, run, rest_client, only_dataset, num, run_once, debug)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    parser.add_argument('-t', '--token', default=os.environ.get('ICEPROD_TOKEN', None), help='auth token')
    parser.add_argument('-d', '--dataset', type=str, default=None, help='dataset id (optional)')
    parser.add_argument('-n', '--num', type=int, default=100, help='number of jobs per dataset to buffer')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()
    args = vars(args)

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args['log_level'].upper()))

    from rest_tools.client import RestClient
    rpc = RestClient('https://materialization.iceprod.icecube.aq', args['token'])

    import asyncio
    asyncio.run(run(rpc, only_dataset=args['dataset'], num=args['num'], run_once=True, debug=args['debug']))

if __name__ == '__main__':
    main()
