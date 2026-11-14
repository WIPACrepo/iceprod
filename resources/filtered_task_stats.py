#!/usr/bin/env python
"""
Fetch task stats for a filtered subset of tasks in a dataset.

The REST API cannot filter task stats by task name or job index directly,
so this is a two-step process:

1. Query `/datasets/{dataset_id}/tasks` with filters to get matching task ids.
2. Stream `/datasets/{dataset_id}/bulk/task_stats` for those task ids,
   in chunks of at most 100 ids per request.

Output is newline-delimited JSON, one task_stat entry per line.

Example:
    python resources/filtered_task_stats.py <dataset_id> --name ppc --job-index-max 99 --last
"""
import argparse
import json
import logging
import sys

from iceprod.client_auth import add_auth_to_argparse, create_rest_client

#: max task ids per bulk task_stats request (REST API limit)
CHUNK_SIZE = 100


def get_task_ids(client, dataset_id, *, status=None, name=None, job_index_min=None, job_index_max=None):
    """Get the task ids in a dataset that match the filters."""
    args = {'keys': 'task_id'}
    if status:
        args['status'] = status
    if name:
        args['name'] = name
    if job_index_min is not None:
        args['job_index_min'] = job_index_min
    if job_index_max is not None:
        args['job_index_max'] = job_index_max
    tasks = client.request_seq('GET', f'/datasets/{dataset_id}/tasks', args)
    return sorted(tasks)


def get_task_stats(client, dataset_id, task_ids, *, last=False, keys=None):
    """Yield task_stat entries for the given task ids, chunked to fit the API limit."""
    for i in range(0, len(task_ids), CHUNK_SIZE):
        args = {'task_ids': '|'.join(task_ids[i:i+CHUNK_SIZE])}
        if last:
            args['last'] = 'true'
        if keys:
            args['keys'] = keys
        # request_stream decodes each JSON line for us
        yield from client.request_stream('GET', f'/datasets/{dataset_id}/bulk/task_stats', args)


def main():
    parser = argparse.ArgumentParser(description='Fetch task stats for a filtered subset of tasks')
    parser.add_argument('dataset_id', help='dataset id (not the dataset number)')
    parser.add_argument('--status', help='| separated task statuses (e.g. "complete")')
    parser.add_argument('--name', help='| separated task names (e.g. "ppc")')
    parser.add_argument('--job-index-min', type=int, help='minimum job index (inclusive)')
    parser.add_argument('--job-index-max', type=int, help='maximum job index (inclusive)')
    parser.add_argument('--keys', help='| separated task_stat keys to return (e.g. "stats")')
    parser.add_argument('--last', action='store_true', help='only the latest stat per task')
    parser.add_argument('-o', '--output', default='-', help='output file (or "-" for stdout)')
    parser.add_argument('--log-level', default='INFO')
    add_auth_to_argparse(parser)
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    client = create_rest_client(args)

    task_ids = get_task_ids(
        client, args.dataset_id,
        status=args.status,
        name=args.name,
        job_index_min=args.job_index_min,
        job_index_max=args.job_index_max,
    )
    logging.info('found %d matching tasks', len(task_ids))

    out = sys.stdout if args.output == '-' else open(args.output, 'w')
    try:
        n = 0
        for stat in get_task_stats(client, args.dataset_id, task_ids, last=args.last, keys=args.keys):
            out.write(json.dumps(stat) + '\n')
            n += 1
        logging.info('wrote %d task stats', n)
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == '__main__':
    main()
