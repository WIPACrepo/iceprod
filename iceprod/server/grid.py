"""
Interface for configuring and submitting jobs on a computing cluster.
Do not use this class directly. Instead use one of the batch plugins
that inherit from this class.
"""

import asyncio
import os
import logging
from copy import deepcopy
from pathlib import Path
import socket
from typing import Any, Protocol

from asyncache import cachedmethod  # type: ignore
from cachetools import TTLCache
from cachetools.func import ttl_cache
import requests.exceptions

from iceprod.core import functions
from iceprod.core.config import Task, Job, Dataset
from iceprod.core.defaults import add_default_options
from iceprod.core.resources import Resources
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
        self.site = None
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

    # Functions to override #

    async def run(self):
        """Override the `ActiveJobs` and `JobActions` batch submission / monitoring classes"""
        raise NotImplementedError()

    # Commong functions #

    @cachedmethod(lambda self: self.dataset_cache)
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

    async def get_tasks_to_queue(self, num: int) -> list[Task]:
        """
        Get new tasks to queue from the REST API.

        Args:
            num: number of tasks to retrieve

        Returns:
            list of iceprod.core.config.Task objects
        """
        # get tasks to run from REST API, and convert to batch jobs
        args = {
            'requirements': self.site_requirements,
            'query_params': self.site_query_params,
        }
        futures = set()
        tasks_queued = 0
        for tasks_queued in range(num):
            try:
                ret = await self.rest_client.request('POST', '/task_actions/queue', args)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    break
                raise
            else:
                futures.add(asyncio.create_task(self._convert_to_task(ret)))
        logging.info('got %d tasks to queue', tasks_queued)

        tasks = []
        for f in asyncio.as_completed(futures):
            task = await f
            # add default resource requirements
            task.requirements = self._get_resources(task)
            tasks.append(task)

        return tasks

    async def _convert_to_task(self, task):
        """Convert from basic task dict to a Task object"""
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

        return t

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
            if isinstance(resource[k],list):
                resource[k] = len(resource[k])
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

    async def task_processing(self, task: GridTask):
        """
        Tell IceProd API a task is now processing (put in the "processing" status).

        Args:
            task: IceProd task info
        """
        if not task.task_id or not task.instance_id:
            raise RuntimeError("Either task_id or instance_id is empty")

        args = {
            'instance_id': task.instance_id,
        }
        try:
            await self.rest_client.request('POST', f'/tasks/{task.task_id}/task_actions/processing', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise

    async def task_reset(self, task: GridTask, reason: str | None = None):
        """
        Tell IceProd API a task should be reset back to the "waiting" status.

        Args:
            task: IceProd task info
            reason: A reason for failure
        """
        if not task.task_id or not task.instance_id:
            raise RuntimeError("Either task_id or instance_id is empty")

        args = {
            'instance_id': task.instance_id,
        }
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


'''
    @run_on_executor
    def _delete_dirs(self, dirs):
        # delete dirs that need deleting
        for t in dirs:
            if not t.startswith(self.submit_dir):
                # some security against nefarious things
                raise Exception('directory %s not in submit_dir %s'%(t, self.submit_dir))
            try:
                logger.info('deleting submit_dir %s', t)
                functions.removedirs(t)
            except Exception:
                logger.warning('could not delete submit dir %s', t, exc_info=True)
                continue


    @ttl_cache(ttl=600)
    def get_token(self):
        return self.cred_client.make_access_token()

    @cached(TTLCache(1024, 60))
    async def get_user_credentials(self, username):
        ret = await self.cred_client.request('GET', f'/users/{username}/credentials')
        return ret

    @cached(TTLCache(1024, 60))
    async def get_group_credentials(self, group):
        ret = await self.cred_client.request('GET', f'/groups/{group}/credentials')
        return ret

    async def customize_task_config(self, task_cfg, job_cfg=None, dataset=None):
        """Transforms for the task config"""
        logger.info('customize_task_config for %s', job_cfg.get('options', {}).get('task_id', 'unknown'))

        # first expand site temp urls
        def expand_remote(cfg):
            new_data = []
            for d in cfg.get('data', []):
                if not d['remote']:
                    try:
                        remote_base = d.storage_location(job_cfg)
                        logger.info('expanding remote for %r', d['local'])
                        d['remote'] = os.path.join(remote_base, d['local'])
                    except Exception:
                        # ignore failed expansions, as these are likely local temp paths
                        pass
                new_data.append(d)
            cfg['data'] = new_data
        expand_remote(task_cfg)
        for tray in task_cfg['trays']:
            expand_remote(tray)
            for module in tray['modules']:
                expand_remote(module)
        logger.info('task_cfg: %r', task_cfg)

        # now apply S3 and token credentials
        creds = self.cfg.get('creds', {})
        if dataset['group'] == 'users':
            ret = await self.get_user_credentials(dataset['username'])
        else:
            ret = await self.get_group_credentials(dataset['group'])
        creds.update(ret)

        s3_creds = {url: creds.pop(url) for url in list(creds) if creds[url]['type'] == 's3'}
        if s3_creds:
            # if we have any s3 credentials, try presigning urls
            logger.info('testing job for s3 credentials')
            try:
                queued_time = timedelta(seconds=self.queue_cfg['max_task_queued_time'])
            except Exception:
                queued_time = timedelta(seconds=86400*2)
            try:
                processing_time = timedelta(seconds=self.queue_cfg['max_task_processing_time'])
            except Exception:
                processing_time = timedelta(seconds=86400*2)
            expiration = (queued_time + processing_time).total_seconds()
            logger.info(f's3 cred expire time: {expiration}')

            def presign_s3(cfg):
                new_data = []
                for d in cfg.get('data', []):
                    for url in s3_creds:
                        if d['remote'].startswith(url):
                            logger.info('found data for cred: %s', url)
                            path = d['remote'][len(url):].lstrip('/')
                            bucket = None
                            if '/' in path:
                                bucket, key = path.split('/', 1)
                            if (not bucket) or bucket not in s3_creds[url]['buckets']:
                                key = path
                                bucket = urlparse(url).hostname.split('.', 1)[0]
                                if bucket not in s3_creds[url]['buckets']:
                                    raise RuntimeError('bad s3 bucket')

                            while '//' in key:
                                key = key.replace('//', '/')
                            while key.startswith('/'):
                                key = key[1:]

                            s = S3(url, s3_creds[url]['access_key'], s3_creds[url]['secret_key'], bucket=bucket)
                            logger.info(f'S3 url={url} bucket={bucket} key={key}')
                            if d['movement'] == 'input':
                                d['remote'] = s.get_presigned(key, expiration=expiration)
                                new_data.append(d)
                            elif d['movement'] == 'output':
                                d['remote'] = s.put_presigned(key, expiration=expiration)
                            elif d['movement'] == 'both':
                                d['movement'] = 'input'
                                d['remote'] = s.get_presigned(key, expiration=expiration)
                                new_data.append(d.copy())
                                d['movement'] = 'output'
                                d['remote'] = s.put_presigned(key, expiration=expiration)
                            else:
                                raise RuntimeError('unknown s3 data movement')
                            new_data.append(d)
                            break
                    else:
                        new_data.append(d)
                cfg['data'] = new_data

            presign_s3(task_cfg)
            for tray in task_cfg['trays']:
                presign_s3(tray)
                for module in tray['modules']:
                    presign_s3(module)

        oauth_creds = {url: creds.pop(url) for url in list(creds) if creds[url]['type'] == 'oauth'}
        if oauth_creds:
            # if we have token-based credentials, add them to the config
            logger.info('testing job for oauth credentials')
            cred_keys = set()

            def get_creds(cfg):
                for d in cfg.get('data', []):
                    for url in oauth_creds:
                        if d['remote'].startswith(url):
                            logger.info('found data for cred: %s', url)
                            cred_keys.add(url)
                            break

            get_creds(task_cfg)
            for tray in task_cfg['trays']:
                get_creds(tray)
                for module in tray['modules']:
                    get_creds(module)

            file_creds = {}
            for url in cred_keys:
                cred_name = hashlib.sha1(oauth_creds[url]['access_token'].encode('utf-8')).hexdigest()
                path = os.path.join(self.credentials_dir, cred_name)
                if not os.path.exists(path):
                    with open(path, 'w') as f:
                        f.write(oauth_creds[url]['access_token'])
                file_creds[url] = cred_name
            job_cfg['options']['credentials'] = file_creds

    async def setup_submit_directory(self,task):
        """Set up submit directory"""
        # create directory for task
        submit_dir = self.submit_dir
        task_dir = os.path.join(submit_dir,task['task_id']+'_'+str(random.randint(0,1000000)))
        while os.path.exists(task_dir):
            task_dir = os.path.join(submit_dir,task['task_id']+'_'+str(random.randint(0,1000000)))
        task_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(task_dir)))
        os.makedirs(task_dir)
        task['submit_dir'] = task_dir

        # symlink or copy the .sh file
        src = get_pkg_binary('iceprod', 'loader.sh')
        dest = os.path.join(task_dir, 'loader.sh')
        try:
            os.symlink(src, dest)
        except Exception:
            try:
                functions.copy(src, dest)
            except Exception:
                logger.error('Error creating symlink or copy of .sh file: %s',dest,exc_info=True)
                raise

        # get passkey
        # expiration = self.queue_cfg['max_task_queued_time']
        # expiration += self.queue_cfg['max_task_processing_time']
        # expiration += self.queue_cfg['max_task_reset_time']
        # TODO: take expiration into account
        passkey = self.get_token()

        # write cfg
        cfg, filelist = self.write_cfg(task)

        # create submit file
        try:
            await asyncio.ensure_future(self.generate_submit_file(
                task,
                cfg=cfg,
                passkey=passkey,
                filelist=filelist
            ))
        except Exception:
            logger.error('Error generating submit file',exc_info=True)
            raise

    def write_cfg(self, task):
        """Write the config file for a task-like object"""
        filename = os.path.join(task['submit_dir'],'task.cfg')
        filelist = [filename]

        config = self.create_config(task)
        if creds := config['options'].get('credentials', {}):
            cred_dir = os.path.join(task['submit_dir'], CRED_SUBMIT_DIR)
            os.mkdir(cred_dir)
            for name in creds.values():
                src = os.path.join(self.credentials_dir, name)
                dest = os.path.join(cred_dir, name)
                os.symlink(src, dest)
            filelist.append(cred_dir)
        if 'system' in self.cfg and 'remote_cacert' in self.cfg['system']:
            config['options']['ssl'] = {}
            config['options']['ssl']['cacert'] = os.path.basename(self.cfg['system']['remote_cacert'])
            src = self.cfg['system']['remote_cacert']
            dest = os.path.join(task['submit_dir'],config['options']['ssl']['cacert'])
            try:
                os.symlink(src,dest)
            except Exception:
                try:
                    functions.copy(src,dest)
                except Exception:
                    logger.error('Error creating symlink or copy of remote_cacert',
                                 exc_info=True)
                    raise
            filelist.append(dest)
        if 'x509proxy' in self.cfg['queue'] and self.cfg['queue']['x509proxy']:
            config['options']['x509'] = os.path.basename(self.cfg['queue']['x509proxy'])
            src = self.cfg['queue']['x509proxy']
            logger.info('submit_dir %r  x509 %r', task['submit_dir'], config['options']['x509'])
            dest = os.path.join(task['submit_dir'],config['options']['x509'])
            try:
                os.symlink(src,dest)
            except Exception:
                try:
                    functions.copy(src,dest)
                except Exception:
                    logger.error('Error creating symlink or copy of x509 proxy',
                                 exc_info=True)
                    raise
            filelist.append(dest)
        if 'extra_file_tranfers' in self.cfg['queue'] and self.cfg['queue']['extra_file_tranfers']:
            for f in self.cfg['queue']['extra_file_tranfers']:
                logger.info('submit_dir %r  extra_files %r', task['submit_dir'], f)
                dest = os.path.join(task['submit_dir'],os.path.basename(f))
                try:
                    os.symlink(os.path.abspath(f),dest)
                except Exception:
                    try:
                        functions.copy(f,dest)
                    except Exception:
                        logger.error('Error creating symlink or copy of extra file %s',
                                     f, exc_info=True)
                        raise
                filelist.append(dest)
        if 'data_movement_stats' in self.cfg['queue'] and self.cfg['queue']['data_movement_stats']:
            config['options']['data_movement_stats'] = self.cfg['queue']['data_movement_stats']
        if 'upload_checksum' in self.cfg['queue']:
            config['options']['upload_checksum'] = self.cfg['queue']['upload_checksum']

        if 'reqs' in task:
            # add resources
            config['options']['resources'] = {}
            for r in task['reqs']:
                config['options']['resources'][r] = task['reqs'][r]

        # write to file
        serialization.serialize_json.dump(config, filename)

        c = iceprod.core.exe.Config(config)
        config = c.parseObject(config, {})

        return (config, filelist)

    # not async: called from executor
    def get_submit_args(self,task,cfg=None,passkey=None):
        """Get the submit arguments to start the loader script."""
        # get website address
        if ('rest_api' in self.cfg and self.cfg['rest_api'] and
                'url' in self.cfg['rest_api'] and self.cfg['rest_api']['url']):
            web_address = self.cfg['rest_api']['url']
        else:
            raise Exception('no web address for rest calls')

        args = []
        if 'software_dir' in self.queue_cfg and self.queue_cfg['software_dir']:
            args.append('-s {}'.format(self.queue_cfg['software_dir']))
        if 'iceprod_dir' in self.queue_cfg and self.queue_cfg['iceprod_dir']:
            args.append('-e {}'.format(self.queue_cfg['iceprod_dir']))
        if 'x509proxy' in self.cfg['queue'] and self.cfg['queue']['x509proxy']:
            args.append('-x {}'.format(os.path.basename(self.cfg['queue']['x509proxy'])))
        if ('download' in self.cfg and 'http_proxy' in self.cfg['download']
                and self.cfg['download']['http_proxy']):
            args.apend('-c {}'.format(self.cfg['download']['http_proxy']))
        args.append('--url {}'.format(web_address))
        if passkey:
            args.append('--passkey {}'.format(passkey))
        if cfg:
            args.append('--cfgfile task.cfg')
        if 'debug' in task and task['debug']:
            args.append('--debug')
        return args
'''
