"""
The task runner.

Run it with `python -m iceprod.core.i3exec`.
"""

import argparse
import asyncio
from copy import copy
from inspect import iscoroutinefunction
import json
import logging
from pathlib import Path
import resource
import shutil
import subprocess
import os
import time
from typing import Callable

from iceprod.core.defaults import add_default_options

import iceprod
import iceprod.core.config
import iceprod.core.exe
import iceprod.core.logger
from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.server.plugins.test import Grid, TestTask


logger = logging.getLogger('i3exec')


def get_cpu_count() -> int:
    """Returns the total number of logical CPUs available to the process."""
    # os.sched_getaffinity accounts for CPU affinity limits (e.g., Docker/cgroups)
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 1


def get_free_memory() -> float:
    """Reads available memory directly from Linux /proc/meminfo."""
    with open("/proc/meminfo", "r") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                # Format: "MemAvailable:    16324012 kB"
                parts = line.split()
                kb = int(parts[1])
                return kb / (1024.0**2)  # Convert to GB
    return 0.0


def get_free_disk(path: str = ".") -> float:
    """Returns free space in GB for the file system containing the given directory."""
    usage = shutil.disk_usage(path)
    return usage.free / (1024**3)  # Convert bytes to GB


def gpus_info() -> list:
    """Checks for GPU hardware availability using native Linux interfaces."""
    try:
        res = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            check=True
        )
        return [line.strip() for line in res.stdout.strip().split("\n") if line.strip()]
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass
    return []


def get_dir_size_bytes(path: Path) -> int:
    """Recursively calculates total byte size of all files in a directory using pathlib."""
    total_bytes = 0
    try:
        for p in path.rglob("**"):
            if p.is_file() and not p.is_symlink():
                total_bytes += p.stat().st_size
    except PermissionError:
        pass
    return total_bytes


async def run_and_measure(
    cmd: list[str],
    work_dir: str | Path = 'workspace',
    stdout_file: str = 'stdout',
    stderr_file: str = 'stderr',
    update_function: Callable | None = None,
) -> dict:
    """Creates a working subdirectory using pathlib, executes a command inside it,
    redirects outputs, and tracks CPU, peak memory, and exact directory disk usage.
    """
    work_path = Path(work_dir)
    work_path.mkdir(parents=True, exist_ok=True)
    stdout_path = work_path / stdout_file
    stderr_path = work_path / stderr_file

    # Snapshot resource usages and directory size before running
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    disk_bytes_before = get_dir_size_bytes(work_path)
    start_time = time.perf_counter()

    with stdout_path.open('wb') as out_f, stderr_path.open('wb') as err_f:
        res = subprocess.Popen(
            cmd,
            cwd=work_path,
            stdout=out_f,
            stderr=err_f
        )
        while True:
            if res.poll() is not None:
                break

            time.sleep(300)
            if res.poll() is not None:
                break
            if update_function is not None:
                if iscoroutinefunction(update_function):
                    await update_function()
                else:
                    update_function()

    # Snapshot resource usages and directory size after completion
    elapsed_time = time.perf_counter() - start_time
    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    disk_bytes_after = get_dir_size_bytes(work_path)

    # Calculate CPU & Memory metrics
    user_cpu = usage_after.ru_utime - usage_before.ru_utime
    sys_cpu = usage_after.ru_stime - usage_before.ru_stime
    total_cpu_time = user_cpu + sys_cpu
    avg_cpu_percent = (total_cpu_time / elapsed_time) * 100 if elapsed_time > 0 else 0.0
    disk_delta_bytes = disk_bytes_after - disk_bytes_before

    return {
        'returncode': res.returncode,
        'time': elapsed_time,
        'cpu': avg_cpu_percent,
        'memory': usage_after.ru_maxrss / (1024**2),
        'disk': disk_delta_bytes / (1024**3),
    }


class ResourceError(Exception):
    pass


async def prod(args, task: iceprod.core.config.Task):
    logger.warning('Production Mode!')
    create_rest_client(args)

    if args.check_reqs:
        logger.info('checking task requirements')
        gpu_info = []
        for req,val in task.requirements.items():
            if req == 'cpu':
                if val > get_cpu_count():
                    raise ResourceError('not enough cpus: task requires %d', val)
            elif req == 'memory':
                if val > get_free_memory():
                    raise ResourceError('not enough free memory: task requires %f', val)
            elif req == 'disk':
                if val > get_free_disk():
                    raise ResourceError('not enough free disk: task requires %f', val)
            elif req == 'gpu':
                gpu_info = gpus_info()
                if val > len(gpu_info):
                    raise ResourceError('not enough gpus: task requires %d', val)

    logger.info('creating local grid')
    cred_args = copy(args)
    cred_args.rest_url = 'https://credentials.iceprod.icecube.aq'
    cred_rc = create_rest_client(cred_args)
    rest_rc = create_rest_client(args)
    grid_cfg = {
        'queue': {
            'site': 'local',
            'resources': {},
            'credentials_dir': 'credentials_dir',
            'submit_dir': 'workspace'
        },
        'oauth_condor_client_id': 'iceprod'
    }
    grid = Grid(grid_cfg, rest_client=rest_rc, cred_client=cred_rc)

    logger.info('getting Pelican tokens')
    cred_dir = grid.submit_dir / '.creds'
    if cred_dir.exists():
        shutil.rmtree(cred_dir)
    cred_dir.mkdir()
    os.environ['_CONDOR_CREDS'] = str(cred_dir)

    async def update_creds():
        credentials = []
        args = {'transfer_prefix': task.dataset.config['options']['site_temp']}
        ret = await cred_rc.request('GET', '/users/ice3simusr/credentials', args)
        logger.debug('ice3simusr scratch cred: %r', ret)
        credentials.extend(ret)
        ret = await cred_rc.request('GET', f'/datasets/{task.dataset.dataset_id}/credentials', {})
        logger.debug('dataset creds: %r', ret)
        credentials.extend(ret)
        for i,cred in enumerate(credentials):
            with open(cred_dir / f'{i}.use', 'w') as f:
                json.dump({
                    'access_token': cred.get('access_token', ''),
                    'token_type': 'bearer',
                    'expires_in': cred.get('expiration', 0) - time.time(),
                    'expires_at': cred.get('expiration', 0),
                    'scope': cred.get('scope', ''),
                }, f)
        logger.info('loaded %d Pelican tokens', len(credentials))
    await update_creds()

    logger.info('converting dataset/task to bash')
    ws = iceprod.core.exe.WriteToScript(task, workdir=grid.submit_dir, logger=logger)
    scriptpath = await ws.convert(transfer=True)

    logger.info('registering with IceProd')
    if task.status == 'complete':
        raise Exception('Cannot rerun a complete task')

    logger.info('running script: %s', scriptpath)
    await rest_rc.request('PATCH', f'/tasks/{task.task_id}', {'status': 'processing', 'site': 'local', 'instance_id': 'local'})

    ret = await run_and_measure([str(scriptpath)], work_dir=grid.submit_dir, update_function=update_creds)

    grid_task = TestTask(dataset_id=task.dataset.dataset_id, task_id=task.task_id, instance_id='local')
    returncode = ret.pop('returncode')

    if returncode != 0:
        logger.error('Task failed with return code %d', returncode)
        logger.error(f'stdout and stderr can be found at `{grid.submit_dir}/std[err|out]`')
        await grid.task_failure(
            grid_task,
            reason=f'Task failed with return code {returncode}',
            stats=ret,
            stdout=grid.submit_dir / 'stdout',
            stderr=grid.submit_dir / 'stderr'
        )
        raise subprocess.CalledProcessError(returncode, scriptpath)
    else:
        logger.info('Task success!')
        await grid.task_success(
            grid_task, 
            stats=ret, 
            stdout=grid.submit_dir / 'stdout', 
            stderr=grid.submit_dir / 'stderr'
        )


async def run(args):
    if args.dataset_id:
        rc = create_rest_client(args)
        logger.info('Real dataset mode: dataset %s task %s', args.dataset_id, args.task_id)
        task = await iceprod.core.config.Task.load_from_api(args.dataset_id, args.task_id, rc)
        await task.load_task_files_from_api(rc)

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

    if args.prod:
        add_default_options(task.dataset.config['options'])
        await prod(args, task)
    else:
        ws = iceprod.core.exe.WriteToScript(task, workdir=Path.cwd(), logger=logger)
        scriptpath = await ws.convert()
        if not args.dry_run:
            logger.info('running script %s', scriptpath)
            subprocess.run([scriptpath], check=True)


async def main():
    parser = argparse.ArgumentParser(description='IceProd Core')
    parser.add_argument('--log-level', default='info', help='log level')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False, help='Dry run')
    add_auth_to_argparse(parser)

    testing = parser.add_argument_group('Testing')
    testing.add_argument('--config', help='Specify config file')
    testing.add_argument('--task', type=str, help='Name of the task to run')
    testing.add_argument('--dataset-num', type=int, default=1, help='Fake dataset number (optional)')
    testing.add_argument('--jobs-submitted', type=int, default=1, help='Total number of jobs in this dataset (optional)')
    testing.add_argument('--job-index', type=int, default=0, help='Fake job index (optional)')

    real = parser.add_argument_group('Real Dataset', 'Download from IceProd server')
    real.add_argument('--dataset-id', help='IceProd dataset id')
    real.add_argument('--task-id', help='IceProd task id')

    prod = parser.add_argument_group('Production Mode', description='Must be used with a real dataset, and run as an admin user')
    prod.add_argument('--prod', default=False, action='store_true', help='Enable production mode')
    prod.add_argument('--ignore-reqs', dest='check_reqs', default=True, action='store_false', help='Ignore all task requirements')

    args = parser.parse_args()

    if args.dataset_id:
        if not args.task_id:
            parser.error('task-id is required')
    else:
        if not args.config:
            parser.error('config is required')
        if not args.task:
            parser.error('task is required')

    if args.prod and not (args.dataset_id or args.task_id):
        parser.error('Production mode requires dataset-id and task-id')
    if args.prod and args.dry_run:
        parser.error('Production mode and dry run conflict')

    iceprod.core.logger.set_logger(loglevel=args.log_level)

    await run(args)


if __name__ == '__main__':
    asyncio.run(main())
