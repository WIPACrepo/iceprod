"""
Monitor the pilots.

Send monitoring data to graphite.

Initial delay: rand(1 minute)
Periodic delay: 5 minutes
"""

import logging
import random
import time
from collections import defaultdict,Counter

from tornado.ioloop import IOLoop

from iceprod.core.resources import Resources

logger = logging.getLogger('pilot_monitor')

def pilot_monitor(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(5,60), run,
                                module.rest_client, module.statsd)

async def run(rest_client, statsd, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        statsd (:py:class:`statsd.StatsClient`): statsd (graphite) client
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()
    try:
        pilots = await rest_client.request('GET', '/pilots')
        res = defaultdict(Counter)
        count = 0
        for pilot in pilots.values():
            for n in ('available','claimed'):
                n2 = 'resources_'+n
                if n2 in pilot:
                    for t in Resources.defaults:
                        if t in pilot[n2] and isinstance(pilot[n2][t], (int,float)) and pilot[n2][t] > 0:
                            res[n][t] += pilot[n2][t]
            count += len(pilot['tasks'])
        for n in res:
            for t in res[n]:
                statsd.gauge('pilot_resources.{}.{}'.format(n,t), int(res[n][t]))
                logger.info('pilot_resources.{}.{} = {}'.format(n,t,res[n][t]))
        statsd.gauge('pilot_count', count)
    except Exception:
        logger.error('error monitoring pilots', exc_info=True)
        if debug:
            raise

    # run again after 5 minute delay
    stop_time = time.time()
    delay = max(60*5 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client, statsd)


def main():
    import argparse
    import asyncio
    import os
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    parser.add_argument('-t', '--token', default=os.environ.get('ICEPROD_TOKEN', None), help='auth token')
    parser.add_argument('--statsd-address', default=os.environ.get('STATSD_ADDRESS', None), help='statsd address')
    parser.add_argument('--statsd-prefix', default=os.environ.get('STATSD_PREFIX', 'site'), help='statsd site prefix')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()
    args = vars(args)

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args['log_level'].upper()))

    from rest_tools.client import RestClient
    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    from iceprod.server.module import FakeStatsClient, StatsClientIgnoreErrors
    if not args['statsd_address']:
        statsd = FakeStatsClient()
    else:
        addr = args['statsd_address']
        port = 8125
        if ':' in addr:
            addr,port = addr.split(':')
            port = int(port)
        statsd = StatsClientIgnoreErrors(addr, port=port, prefix=args['statsd_prefix']+'.schedule')

    asyncio.run(run(rpc, statsd, debug=args['debug']))

if __name__ == '__main__':
    main()
