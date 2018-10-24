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
            try:
                for n in ('available','claimed'):
                    for t in Resources.defaults:
                        res[n][t] += int(pilot[n][t]) if t in pilot[n] and pilot[n][t] > 0 else 0
                count += len(pilot['tasks'])
            except Exception:
                logger.warning('error getting pilot resources', exc_info=True)
        for n in res:
            for t in res[n]:
                statsd.gauge('pilot_resources.{}.{}'.format(n,t), res[n][t])
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
