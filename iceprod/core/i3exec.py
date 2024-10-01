"""
The task runner.

Run it with `python -m iceprod.core.i3exec`.
"""

import argparse
import asyncio
import json
import logging
from pathlib import Path
import subprocess

import iceprod
from iceprod.client_auth import add_auth_to_argparse, create_rest_client
import iceprod.core.config
import iceprod.core.exe
import iceprod.core.logger


logger = logging.getLogger('i3exec')


async def run(args):
    if args.dataset_id:
        rc = create_rest_client(args)
        logger.info('Real dataset mode: dataset %s task %s', args.dataset_id, args.task_id)
        task = await iceprod.core.config.Task.load_from_api(args.dataset_id, args.task_id, rc)

    else:
        logger.info('Testing mode: dataset %d job %d task %s', args.dataset_num, args.job_index, args.task)
        with open(args.config) as f:
            cfg = json.load(f)
        task_names = [t['name'] for t in cfg['tasks']]

        d = iceprod.core.config.Dataset(
            dataset_id='datasetid',
            dataset_num=args.dataset_num,
            jobs_submitted=args.jobs_submitted,
            tasks_submitted=args.jobs_submitted*len(cfg['tasks']),
            tasks_per_job=len(cfg['tasks']),
            status='processing',
            priority=0,
            group='group',
            user='user',
            debug=True,
            config=cfg
        )
        task = iceprod.core.config.Task(
            dataset=d,
            job=iceprod.core.config.Job(d, '', args.job_index, 'processing'),
            task_id='taskid',
            task_index=task_names.index(args.task),
            name=args.task,
            depends=[],
            requirements={},
            status='processing',
            site='site',
            stats={}
        )

    task.dataset.fill_defaults()
    task.dataset.validate()

    ws = iceprod.core.exe.WriteToScript(task, workdir=Path.cwd(), logger=logger)
    scriptpath = await ws.convert()
    logger.info('running script %s', scriptpath)
    if not args.dry_run:
        subprocess.run([scriptpath], check=True)


async def main():
    parser = argparse.ArgumentParser(description='IceProd Core')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False, help='Dry run')
    add_auth_to_argparse(parser)

    real = parser.add_argument_group('Real Dataset', 'Download from IceProd server')
    real.add_argument('--dataset-id', help='IceProd dataset id')
    real.add_argument('--task-id', help='IceProd task id')

    testing = parser.add_argument_group('Testing')
    testing.add_argument('--config', help='Specify config file')
    testing.add_argument('--task', type=str, help='Name of the task to run')
    testing.add_argument('--dataset-num', type=int, default=1, help='Fake dataset number (optional)')
    testing.add_argument('--jobs-submitted', type=int, default=1, help='Total number of jobs in this dataset (optional)')
    testing.add_argument('--job-index', type=int, default=0, help='Fake job index (optional)')

    args = parser.parse_args()

    if args.dataset_id:
        if not args.task_id:
            parser.error('task-id is required')
    else:
        if not args.config:
            parser.error('config is required')
        if not args.task:
            parser.error('task is required')

    iceprod.core.logger.set_logger(loglevel=args.log_level)

    await run(args)


if __name__ == '__main__':
    asyncio.run(main())
