"""
Update pilots when tasks change state via a user.

Initial delay: rand(5 minutes)
Periodic delay: 5 minutes
"""

import logging
import random
import time
import asyncio
from datetime import datetime

from tornado.ioloop import IOLoop
import requests.exceptions

from iceprod.server.util import str2datetime

logger = logging.getLogger('removed_tasks')

def removed_tasks(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(60,60*5), run, module.rest_client)

async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()

    try:
        async def test_pilot(pilot):
            new_tasks = []
            for task_id in pilot['tasks']:
                ret = await rest_client.request('GET', f'/tasks/{task_id}')
                if ret['status'] == 'processing':
                    new_tasks.append(task_id)
            if new_tasks != pilot['tasks']:
                args = {'tasks': new_tasks}
                try:
                    await rest_client.request('PATCH', f'/pilots/{pilot["pilot_id"]}', args)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code != 404:
                        raise

        awaitables = set()
        pilots = await rest_client.request('GET', '/pilots?keys=pilot_id|tasks')
        for p in pilots.values():
            if 'tasks' in p and p['tasks']:
                awaitables.add(asyncio.create_task(test_pilot(p)))
                if len(awaitables) >= 40:
                    done,pending = await asyncio.wait(awaitables, return_when=asyncio.FIRST_COMPLETED)
                    awaitables = pending
                    for fut in done:
                        await fut
        for fut in asyncio.as_completed(awaitables):
            await fut

    except Exception:
        logger.error('error updating pilot for removed tasks', exc_info=True)
        if debug:
            raise

    # run again after 60 minute delay
    stop_time = time.time()
    delay = max(60*5 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client)


def main():
    import argparse
    import os
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    parser.add_argument('-t', '--token', default=os.environ.get('ICEPROD_TOKEN', None), help='auth token')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()
    args = vars(args)

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args['log_level'].upper()))

    from rest_tools.client import RestClient
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    asyncio.run(run(rpc, debug=args['debug']))

if __name__ == '__main__':
    main()
