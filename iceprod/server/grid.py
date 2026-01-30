"""
Interface for configuring and submitting jobs on a computing cluster.
Do not use this class directly. Instead use one of the batch plugins
that inherit from this class.
"""

import asyncio
from collections import Counter
from enum import StrEnum
import json
import os
import logging
from copy import deepcopy
from pathlib import Path
import socket
from typing import Any, Protocol

from asyncache import cached, cachedmethod  # type: ignore
from cachetools import TTLCache
from cachetools.func import ttl_cache
from prometheus_client import Info
import requests.exceptions
from wipac_dev_tools.prometheus_tools import GlobalLabels, AsyncPromWrapper, AsyncPromTimer

from iceprod.server.priority import Priority
from iceprod.util import VERSION_STRING
from iceprod.core import functions
from iceprod.core.config import Task, Job, Dataset
from iceprod.core.defaults import add_default_options
from iceprod.core.resources import Resources
from iceprod.common.prom_utils import HistogramBuckets
from iceprod.server.states import JOB_STATUS_START
from iceprod.server.util import nowstr


logger = logging.getLogger('grid')


@ttl_cache(ttl=3600)
def get_host():
    """Cache the host fqdn for 1 hour"""
    return socket.getfqdn()


class GridTask(Protocol):
    """Protocol for grid task dataclass"""
    dataset_id: str | None
    task_id: str | None
    instance_id: str | None


class GridStatus(StrEnum):
    QUEUED = 'queued'
    PROCESSING = 'processing'


class BaseGrid:
    """
    Interface for a generic job distribution system.
    Do not use this class directly.  Use one of the plugins.
    """

    def __init__(self, cfg, rest_client, cred_client):
        self.cfg = cfg
        self.rest_client = rest_client
        self.cred_client = cred_client

        queue_cfg = self.cfg['queue']

        # site name
        self.site = ''
        if 'site' in queue_cfg:
            self.site = queue_cfg['site']

        # task queue params
        self.site_requirements = queue_cfg['resources']
        self.site_query_params = {}
        if self.site:
            self.site_requirements['site'] = self.site
            if queue_cfg.get('exclusive', False):
                logger.info('site requirements: must match site name')
                self.site_query_params['requirements.site'] = self.site

            if 'gpu' in self.site.lower():
                logger.info('site requirements: GPU site!')
                self.site_requirements['gpu'] = 1

        # directories
        self.credentials_dir = Path(os.path.expanduser(os.path.expandvars(
            queue_cfg['credentials_dir'])))
        self.credentials_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

        self.submit_dir = Path(os.path.expanduser(os.path.expandvars(
            queue_cfg['submit_dir'])))
        self.submit_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

        # dataset lookup cache
        self.dataset_cache = TTLCache(maxsize=100, ttl=60)

        i = Info('iceprod', 'IceProd information')
        i.info({
            'name': str(self.site),
            'type': 'grid',
            'queue_type': queue_cfg.get('type',''),
            'version': VERSION_STRING,
            'exclusive': str(queue_cfg.get('exclusive', False)),
        })

        self.prometheus = GlobalLabels({
            'site': str(self.site),
            'type': queue_cfg.get('type',''),
            'gpu': str(self.site_requirements.get('gpu', 0)),
        })

    # Functions to override #

    async def run(self):
        """Override the `ActiveJobs` and `JobActions` batch submission / monitoring classes"""
        raise NotImplementedError()

    def queue_dataset_status(self) -> dict[GridStatus, Counter[str]]:
        """Get the current queue job counts by dataset and job status."""
        raise NotImplementedError()

    # Common functions #

    @cachedmethod(lambda self: self.dataset_cache)
    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_dataset_lookup', 'IceProd grid.dataset_lookup calls', buckets=HistogramBuckets.SECOND))
    async def dataset_lookup(self, dataset_id: str) -> Dataset:
        """
        Lookup a dataset fromn the REST API.

        Uses caching with TTL 60 seconds.

        Args:
            dataset_id: dataset id

        Returns:
            iceprod.core.config.Dataset object
        """
        ret = await Dataset.load_from_api(dataset_id, rest_client=self.rest_client)
        ret.fill_defaults()
        ret.validate()
        return ret

    @cached(TTLCache(10, 900), key=lambda _: 'self')
    async def _get_priority_object(self) -> Priority:
        p = Priority(rest_client=self.rest_client)
        await p._populate_dataset_cache()
        return p

    @AsyncPromWrapper(lambda self: self.prometheus.counter('iceprod_grid_queue_tasks', 'IceProd grid tasks queued', labels=['step'], finalize=False))
    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_tasks_to_queue', 'IceProd grid.tasks_to_queue calls'))
    async def get_tasks_to_queue(self, prom_counter, num: int) -> list[Task]:
        """
        Get new tasks to queue from the REST API.

        Args:
            num: number of tasks to retrieve

        Returns:
            list of iceprod.core.config.Task objects
        """
        # get current count of queued jobs (ignore processing)
        cur_jobs: Counter[str] = Counter(self.queue_dataset_status().get(GridStatus.QUEUED, {}))
        priorities = await self._get_priority_object()
        dataset_prios = {}
        for dataset_id in cur_jobs:
            dataset_prios[dataset_id] = await priorities.get_dataset_prio(dataset_id)

        # order the ignore list by number of queued jobs / dataset priority
        # this should slightly favor higher priority datasets, but allow lower priority datasets to come in
        ignore_datasets_list = sorted(cur_jobs, key=lambda x: cur_jobs[x]/dataset_prios[x] if dataset_prios.get(x,None) else 1000000, reverse=True)
        logger.info('ignore_dataset_list=%r', ignore_datasets_list)

        # get tasks to run from REST API, and convert to batch jobs
        try:
            ret = await self.rest_client.request('POST', '/task_actions/queue_many', {
                'num': num,
                'requirements': self.site_requirements,
                'dataset_deprio': ignore_datasets_list,
                'query_params': self.site_query_params,
            })
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return []
            raise

        futures = set()
        for row in ret:
            futures.add(asyncio.create_task(self._convert_to_task(row)))

        tasks_queued = len(ret)
        prom_counter.labels({'step': 'raw'}).inc(tasks_queued)
        logging.info('got %d tasks to queue', tasks_queued)

        tasks = []
        for f in asyncio.as_completed(futures):
            try:
                task = await f
            except Exception:  # already logged in function
                continue
            try:
                # add default resource requirements
                task.requirements = self._get_resources(task)
            except Exception:
                logger.warning('cannot get task resources for %s.%s', task.dataset.dataset_id, task.task_id, exc_info=True)
                continue
            try:
                # add oauth tokens
                task.oauth_tokens = await self._get_dataset_credentials(task)
            except Exception:
                logger.warning('cannot get oauth tokens for %s.%s', task.dataset.dataset_id, task.task_id, exc_info=True)
                continue
            tasks.append(task)

        tasks_queued2 = len(tasks)
        prom_counter.labels({'step': 'processed'}).inc(tasks_queued2)
        logging.info('got %d Task objects to queue', tasks_queued2)

        return tasks

    async def _convert_to_task(self, task):
        """Convert from basic task dict to a Task object"""
        try:
            d = deepcopy(await self.dataset_lookup(task['dataset_id']))
            # don't bother looking up the job status - trust that if we got a task, we're in processing
            j = Job(dataset=d, job_id=task['job_id'], job_index=task['job_index'], status=JOB_STATUS_START)
            t = Task(
                dataset=d,
                job=j,
                task_id=task['task_id'],
                task_index=task['task_index'],
                instance_id=task['instance_id'],
                name=task['name'],
                depends=task['depends'],
                requirements=task['requirements'],
                status=task['status'],
                site=self.site,
                stats={},
                task_files=[],
            )
            await t.load_task_files_from_api(self.rest_client)

            # load some config defaults
            config = t.dataset.config
            if (not config['options'].get('site_temp','')) and self.cfg['queue'].get('site_temp', ''):
                config['options']['site_temp'] = self.cfg['queue']['site_temp']
            add_default_options(config['options'])

        except Exception:
            logger.warning('Error converting task dict to task: %s.%s', task['dataset_id'], task['task_id'], exc_info=True)
            raise

        return t

    @cached(TTLCache(1024, 60), key=lambda _,t: (t.dataset.dataset_id, t.name))
    async def _get_dataset_credentials(self, task: Task) -> list[Any]:
        # todo: handle non-oauth credentials, like s3
        # todo: delayed get to see if we already have these tokens in condor
        ret = []
        if client_id := self.cfg['oauth_condor_client_id']:
            # do dataset creds, with downscoping
            for prefix, scope in task.get_task_config().get('token_scopes', {}).items():
                args = {'client_id': client_id, 'transfer_prefix': prefix, 'new_scope': scope}
                ret2 = await self.cred_client.request('GET', f'/datasets/{task.dataset.dataset_id}/exchange', args)
                ret.extend(ret2)
            logging.info('dataset %s creds with downscoping: %d', task.dataset.dataset_id, len(ret))
            # do dataset-task creds
            args = {'client_id': client_id}
            ret2 = await self.cred_client.request('GET', f'/datasets/{task.dataset.dataset_id}/tasks/{task.name}/exchange', args)
            logging.info('dataset %s task creds: %d', task.dataset.dataset_id, len(ret2))
            ret.extend(ret2)
        return ret

    @cached(TTLCache(10, 60), key=lambda _: 'self')
    async def get_scratch_credentials(self):
        if client_id := self.cfg['oauth_condor_client_id']:
            args = {'client_id': client_id, 'transfer_prefix': self.cfg['queue']['site_temp']}
            ret = await self.cred_client.request('GET', '/users/ice3simusr/exchange', args)
            if ret:
                with open(self.credentials_dir / 'scratch', 'w') as f:
                    json.dump(ret, f)

    @staticmethod
    def _get_resources(task):
        """
        Combine task resources with default resources.

        Args:
            task (Task): IceProd Task
        Returns:
            dict: resources
        """
        resource = deepcopy(Resources.defaults)
        for k in resource:
            if isinstance(resource[k], list):
                resource[k] = len(resource[k])  # type: ignore
        values = {}
        try:
            for k in task.requirements:
                if k in resource and task.requirements[k]:
                    try:
                        if isinstance(resource[k], int):
                            values[k] = int(task.requirements[k])
                        elif isinstance(resource[k], float):
                            values[k] = float(task.requirements[k])
                        else:
                            values[k] = task.requirements[k]
                    except Exception:
                        logger.warning('bad reqs value for task %r', task)
                elif k == 'os' and task.requirements[k]:
                    logger.debug('OS req: %s', task.requirements[k])
                    values['os'] = task.requirements[k] if isinstance(task.requirements[k], list) else [task.requirements[k]]
        except TypeError:
            logger.warning('task.requirements: %r', task.requirements)
            raise
        resource.update(values)
        return resource

    @AsyncPromWrapper(lambda self: self.prometheus.gauge('iceprod_grid_tasks', 'IceProd grid tasks IceProd thinks are on the queue'))
    async def get_tasks_on_queue(self, prom_gauge) -> list:
        """
        Get all tasks that are "assigned" to this queue.

        Returns:
            list of tasks
        """
        args = {
            'status': 'queued|processing',
            'site': self.site,
            'keys': 'dataset_id|task_id|instance_id|status|status_changed',
        }
        try:
            tasks = await self.rest_client.request('GET', '/tasks', args)
            ret = tasks['tasks']
            prom_gauge.set(len(ret))
            return ret
        except requests.exceptions.HTTPError:
            logger.warning('cannot get tasks on queue', exc_info=True)
            return []

    # Task Actions #

    async def _upload_log(self, task: GridTask, name: str, data: str):
        """
        Upload a log to the IceProd API.

        Args:
            task: IceProd task info
            name: log name
            data: log text data
        """
        args = {
            'dataset_id': task.dataset_id,
            'task_id': task.task_id,
            'name': name,
            'data': data,
        }
        try:
            await self.rest_client.request('POST', '/logs', args)
        except requests.exceptions.HTTPError:
            logger.warning('cannot upload log', exc_info=True)

    async def _upload_stats(self, task: GridTask, stats: dict):
        """
        Upload task statistics to the IceProd API.

        Args:
            task: IceProd task info
            stats: stats dict
        """
        stats_cp = stats.copy()

        hostname = functions.gethostname()
        domain = '.'.join(hostname.split('.')[-2:])
        site = self.site
        if 'site' in stats_cp:
            site = stats_cp.pop('site', '')
        args = {
            'dataset_id': task.dataset_id,
            'hostname': hostname,
            'domain': domain,
            'site': site,
            'resources': stats_cp.pop('resources', {}),
            'task_stats': stats_cp,
            'time': nowstr(),
        }
        try:
            await self.rest_client.request('POST', f'/tasks/{task.task_id}/task_stats', args)
        except requests.exceptions.HTTPError:
            logger.warning('cannot upload stats', exc_info=True)

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_task_idle', 'IceProd grid.task_idle calls', buckets=HistogramBuckets.MINUTE))
    async def task_idle(self, task: GridTask):
        """
        Tell IceProd API a task is now idle on the queue (put back to "queue" status).

        Args:
            task: IceProd task info
        """
        if not task.task_id or not task.instance_id:
            raise RuntimeError("Either task_id or instance_id is empty")

        args = {
            'status': 'queued',
            'instance_id': task.instance_id,
        }
        try:
            await self.rest_client.request('PUT', f'/tasks/{task.task_id}/status', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_task_processing', 'IceProd grid.task_processing calls', buckets=HistogramBuckets.MINUTE))
    async def task_processing(self, task: GridTask, site: str | None = None):
        """
        Tell IceProd API a task is now processing (put in the "processing" status).

        Args:
            task: IceProd task info
            site: computing site the task is running at
        """
        if not task.task_id or not task.instance_id:
            raise RuntimeError("Either task_id or instance_id is empty")

        args = {
            'instance_id': task.instance_id,
        }
        if site:
            args['site'] = site
        try:
            await self.rest_client.request('POST', f'/tasks/{task.task_id}/task_actions/processing', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_task_reset', 'IceProd grid.task_reset calls', buckets=HistogramBuckets.MINUTE))
    async def task_reset(self, task: GridTask, reason: str | None = None, stats: dict | None = None):
        """
        Tell IceProd API a task should be reset back to the "waiting" status.

        Args:
            task: IceProd task info
            stats: task resource statistics
            reason: A reason for failure
        """
        if not task.task_id or not task.instance_id:
            raise RuntimeError("Either task_id or instance_id is empty")

        args = {
            'instance_id': task.instance_id,
        }
        if stats:
            args['resources'] = stats.get('resources', {})
            if site := stats.get('site'):
                args['site'] = site
        if reason:
            args['reason'] = reason
        try:
            await self.rest_client.request('POST', f'/tasks/{task.task_id}/task_actions/reset', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        else:
            if reason:
                await self._upload_log(task, 'stdlog', reason)

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_task_failure', 'IceProd grid.task_failure calls', buckets=HistogramBuckets.MINUTE))
    async def task_failure(self, task: GridTask, reason: str | None = None, stats: dict | None = None, stdout: Path | None = None, stderr: Path | None = None):
        """
        Tell IceProd API a task should be put in the "failed" status.

        Stats format:
            {
                resources: {cpu: 1, gpu: 1, ...},
                ...
            }

        Args:
            task: IceProd task info
            reason: A reason for failure
            stats: task resource statistics
            stdout: path to stdout file
            stderr: path to stderr file
        """
        if not task.task_id or not task.instance_id:
            raise RuntimeError("Either task_id or instance_id is empty")

        args: dict[str, Any] = {
            'instance_id': task.instance_id,
        }
        if reason:
            args['reason'] = reason
        if stats:
            args['resources'] = stats.get('resources', {})
            if site := stats.get('site'):
                args['site'] = site
        try:
            await self.rest_client.request('POST', f'/tasks/{task.task_id}/task_actions/failed', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        else:
            if stats:
                await self._upload_stats(task, stats)
            if stdout and stdout.exists():
                await self._upload_log(task, 'stdout', stdout.read_text())
            if stderr and stderr.exists():
                await self._upload_log(task, 'stderr', stderr.read_text())
            if reason:
                await self._upload_log(task, 'stdlog', reason)

    @AsyncPromTimer(lambda self: self.prometheus.histogram('iceprod_grid_task_success', 'IceProd grid.task_success calls', buckets=HistogramBuckets.MINUTE))
    async def task_success(self, task: GridTask, stats: dict | None = None, stdout: Path | None = None, stderr: Path | None = None):
        """
        Tell IceProd API a task was successfully completed.

        Args:
            task: IceProd task info
            stats: task resource statistics
            stdout: path to stdout file
            stderr: path to stderr file
        """
        if not task.task_id or not task.instance_id:
            raise RuntimeError("Either task_id or instance_id is empty")

        site = self.site
        if stats and 'site' in stats:
            site = stats['site']
        args = {
            'site': site,
            'instance_id': task.instance_id,
        }
        if stats:
            resources = stats.get('resources', {})
            if site := stats.get('site'):
                args['site'] = site
            if 'time' in resources:
                args['time_used'] = resources['time']*3600.

        try:
            await self.rest_client.request('POST', f'/tasks/{task.task_id}/task_actions/complete', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise
        else:
            if stats:
                await self._upload_stats(task, stats)
            if stdout and stdout.exists():
                await self._upload_log(task, 'stdout', stdout.read_text())
            if stderr and stderr.exists():
                await self._upload_log(task, 'stderr', stderr.read_text())
