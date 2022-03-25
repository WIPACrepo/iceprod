"""
Monitor the datasets.

Send monitoring data to graphite.

Initial delay: rand(1 minute)
Periodic delay: 5 minutes
"""

import asyncio
import logging
import random
import time

from tornado.ioloop import IOLoop

from iceprod.server import GlobalID

logger = logging.getLogger('dataset_monitor')


TASKS_IN_PARALLEL = 100


def dataset_monitor(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(5,60), run,
                                module.rest_client, module.statsd)

async def process_dataset(rest_client, statsd, dataset_id):
    """
    Process a single dataset.

    Returns:
        dict: future resources
    """
    future_resources = {'gpu': 0,'cpu': 0}
    dataset = await rest_client.request('GET', f'/datasets/{dataset_id}')
    dataset_num = dataset['dataset']
    dataset_status = dataset['status']
    jobs = await rest_client.request('GET', f'/datasets/{dataset_id}/job_counts/status')
    jobs2 = {}
    for status in jobs:
        if dataset_status in ('suspended','errors') and status == 'processing':
            jobs2['suspended'] = jobs[status]
        else:
            jobs2[status] = jobs[status]
    jobs = jobs2
    for status in ('processing','failed','suspended','errors','complete'):
        if status not in jobs:
            jobs[status] = 0
        statsd.gauge(f'datasets.{dataset_num}.jobs.{status}', jobs[status])
    tasks = await rest_client.request('GET', f'/datasets/{dataset_id}/task_counts/name_status')
    task_stats = await rest_client.request('GET', f'/datasets/{dataset_id}/task_stats')

    for name in tasks:
        tasks2 = {}
        for status in tasks[name]:
            if dataset_status in ('suspended','errors') and status in ('waiting','queued','processing'):
                if 'suspended' not in tasks2:
                    tasks2['suspended'] = tasks[name][status]
                else:
                    tasks2['suspended'] += tasks[name][status]
                tasks2[status] = 0
            else:
                tasks2[status] = tasks[name][status]
        for status in ('idle','waiting','queued','processing','reset','failed','suspended','complete'):
            if status not in tasks2:
                tasks2[status] = 0
            statsd.gauge(f'datasets.{dataset_num}.tasks.{name}.{status}', tasks2[status])

            # now add to future resource prediction
            if status not in ('idle','failed','suspended','complete'):
                if name not in task_stats:
                    continue
                res = 'gpu' if task_stats[name]['gpu'] > 0 else 'cpu'
                future_resources[res] += tasks2[status]*task_stats[name]['avg_hrs']

    # add jobs not materialized to future resource prediction
    if dataset_status not in ('suspended','errors'):
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
    start_time = time.time()
    try:
        future_resources = {'gpu': 0,'cpu': 0}
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

    # run again after 60 minute delay
    stop_time = time.time()
    delay = max(60*5 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client, statsd)

def main():
    import argparse
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
