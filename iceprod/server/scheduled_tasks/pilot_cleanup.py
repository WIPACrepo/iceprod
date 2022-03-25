"""
Clean up the pilots.

Initial delay: rand(60 minute)
Periodic delay: 60 minutes
"""

import logging
import random
import time
from datetime import datetime, timedelta

from tornado.ioloop import IOLoop

from iceprod.server.util import str2datetime

logger = logging.getLogger('pilot_monitor')

def pilot_cleanup(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(60,60*60), run,
                                module.rest_client)

async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()
    time_limit = datetime.utcnow() - timedelta(days=14)
    async def reset_pilot(pilot_id):
        await rest_client.request('DELETE', '/pilots/{}'.format(pilot_id))
    try:
        pilots = await rest_client.request('GET', '/pilots')
        for pilot in pilots.values():
            if 'last_update' not in pilot or str2datetime(pilot['last_update']) < time_limit:
                await reset_pilot(pilot['pilot_id'])

    except Exception:
        logger.error('error cleaning pilots', exc_info=True)
        if debug:
            raise

    # run again after 5 minute delay
    stop_time = time.time()
    delay = max(60*60 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client)


def main():
    import argparse
    import asyncio
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
