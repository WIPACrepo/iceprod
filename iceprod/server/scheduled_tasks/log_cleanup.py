"""
Clean up the logs based on retention policy.

iceprod_log: 1 month
stderr: 1 year
stdout: 1 year
"""

import argparse
import asyncio
from datetime import datetime, timedelta
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.server.util import datetime2str

logger = logging.getLogger('pilot_monitor')


async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
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
        # while (await delete_logs('stderr', 365)) == 1000:
        #     await asyncio.sleep(60)
        # while (await delete_logs('stdout', 365)) == 1000:
        #     await asyncio.sleep(60)
    except Exception:
        logger.error('error cleaning logs', exc_info=True)
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
