"""
Clean up the logs based on retention policy.

iceprod_log: 1 month
stderr: 1 year
stdout: 1 year

Initial delay: rand(60 minute)
Periodic delay: 12 hours
"""

import logging
import random
import time
from datetime import datetime, timedelta
import asyncio

from tornado.ioloop import IOLoop

from iceprod.server.util import datetime2str

logger = logging.getLogger('pilot_monitor')

def log_cleanup(module):
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
    async def delete_logs(name, days):
        time_limit = datetime.utcnow() - timedelta(days=days)
        args = {
            'to': datetime2str(time_limit),
            'name': name,
            'keys': 'log_id',
            'limit': 100,
        }
        logs = await rest_client.request('GET', '/logs', args)
        for log_id in logs:
            await rest_client.request('DELETE', '/logs/{}'.format(log_id))
        return len(logs)
    try:
        while (await delete_logs('stdlog', 31)) == 100:
            await asyncio.sleep(60)
        #while (await delete_logs('stderr', 365)) == 1000:
        #    await asyncio.sleep(60)
        #while (await delete_logs('stdout', 365)) == 1000:
        #    await asyncio.sleep(60)
    except Exception:
        logger.error('error cleaning logs', exc_info=True)
        if debug:
            raise

    # run again after 12 hours
    stop_time = time.time()
    delay = max(12*3600 - (stop_time-start_time), 3600)
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
