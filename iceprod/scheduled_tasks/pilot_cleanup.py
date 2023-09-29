"""
Clean up the pilots.
"""

import argparse
import asyncio
import logging
from datetime import datetime, timedelta

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.server.util import str2datetime

logger = logging.getLogger('pilot_monitor')


async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
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


def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    asyncio.run(run(rest_client, debug=args.debug))


if __name__ == '__main__':
    main()
