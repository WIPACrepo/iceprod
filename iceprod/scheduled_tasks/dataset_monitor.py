"""
Monitor the datasets.

Send monitoring data to graphite.
"""

import argparse
import asyncio
from collections import Counter
import logging
import os

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.server.module import FakeStatsClient, StatsClientIgnoreErrors
from iceprod.server import states

logger = logging.getLogger('dataset_monitor')


TASKS_IN_PARALLEL = 100


async def process_dataset(rest_client, statsd, dataset_id):
    """
    Process a single dataset.

    Returns:
        dict: future resources
    """
    future_resources = {'gpu': 0, 'cpu': 0}
    dataset = await rest_client.request('GET', f'/datasets/{dataset_id}')
    dataset_num = dataset['dataset']
    dataset_status = dataset['status']
    jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/job_counts/status')
    jobs_counter = Counter()
    for status in jobs:
        if dataset_status in ('suspended', 'errors') and status in states.job_prev_statuses('suspended'):
            jobs_counter['suspended'] = jobs[status]
        else:
            jobs_counter[status] = jobs[status]
    for status in states.JOB_STATUS:
        if status not in jobs_counter:
            jobs_counter[status] = 0
        statsd.gauge(f'datasets.{dataset_num}.jobs.{status}', jobs_counter[status])
    tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/task_counts/name_status')
    task_stats = await rest_client.request('GET', f'/datasets/{dataset_id}/task_stats')

    for name in tasks:
        tasks_counter = Counter()
        for status in tasks[name]:
            if dataset_status in ('suspended', 'errors') and status in states.task_prev_statuses('suspended'):
                tasks_counter['suspended'] += tasks[name][status]
            else:
                tasks_counter[status] = tasks[name][status]
        for status in states.TASK_STATUS:
            if status not in tasks_counter:
                tasks_counter[status] = 0
            statsd.gauge(f'datasets.{dataset_num}.tasks.{name}.{status}', tasks_counter[status])

            # now add to future resource prediction
            if status not in ('idle', 'failed', 'suspended', 'complete'):
                if name not in task_stats:
                    continue
                res = 'gpu' if task_stats[name]['gpu'] > 0 else 'cpu'
                future_resources[res] += tasks_counter[status]*task_stats[name]['avg_hrs']

    # add jobs not materialized to future resource prediction
    if dataset_status not in ('suspended', 'errors'):
        num_jobs_remaining = dataset['jobs_submitted'] - sum(jobs.values())
        for name in task_stats:
            res = 'gpu' if task_stats[name]['gpu'] > 0 else 'cpu'
            future_resources[res] += num_jobs_remaining*task_stats[name]['avg_hrs']

    return future_resources


async def run(rest_client, statsd, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        statsd (:py:class:`statsd.StatsClient`): statsd (graphite) client
        debug (bool): debug flag to propagate exceptions
    """
    try:
        future_resources = {'gpu': 0, 'cpu': 0}

        async def process_task(t):
            ret = await t
            if ret:
                for k in future_resources:
                    if k in ret:
                        future_resources[k] += ret[k]

        pending_tasks = set()
        datasets = await rest_client.request('GET', '/dataset_summaries/status')
        for status in datasets:
            for dataset_id in datasets[status]:
                while len(pending_tasks) >= TASKS_IN_PARALLEL:
                    done, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                    for t in done:
                        await process_task(t)
                pending_tasks.add(asyncio.create_task(process_dataset(rest_client, statsd, dataset_id)))

        while pending_tasks:
            done, pending_tasks = await asyncio.wait(pending_tasks)
            for t in done:
                await process_task(t)

        for res in future_resources:
            statsd.gauge(f'future_resources.{res}', int(future_resources[res]))

    except Exception:
        logger.error('error monitoring datasets', exc_info=True)
        if debug:
            raise


def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    add_auth_to_argparse(parser)
    parser.add_argument('--statsd-address', default=os.environ.get('STATSD_ADDRESS', None), help='statsd address')
    parser.add_argument('--statsd-prefix', default=os.environ.get('STATSD_PREFIX', 'site'), help='statsd site prefix')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('--debug', default=False, action='store_true', help='debug enabled')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=getattr(logging, args.log_level.upper()))

    rest_client = create_rest_client(args)

    if not args.statsd_address:
        statsd = FakeStatsClient()
    else:
        addr = args.statsd_address
        port = 8125
        if ':' in addr:
            addr,port = addr.split(':')
            port = int(port)
        statsd = StatsClientIgnoreErrors(addr, port=port, prefix=args.statsd_prefix+'.schedule')

    asyncio.run(run(rest_client, statsd, debug=args.debug))


if __name__ == '__main__':
    main()
