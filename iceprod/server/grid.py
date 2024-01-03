"""
Interface for configuring and submitting jobs on a computing cluster.
Do not use this class directly. Instead use one of the batch plugins
that inherit from this class.
"""

import asyncio
import os
import random
import logging
import time
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime,timedelta
import enum
import hashlib
from pathlib import Path
import socket
import stat
from typing import Optional
from urllib.parse import urlparse

from asyncache import cached, cachedmethod
from cachetools import TTLCache
from cachetools.func import ttl_cache
from rest_tools.client import RestClient
import requests.exceptions
from tornado.concurrent import run_on_executor



from iceprod.core.config import Task

import iceprod
#import iceprod.core.exe
from iceprod.core.defaults import add_default_options
from iceprod.core.config import Task, Job, Dataset
#from iceprod.core import functions
#from iceprod.core import serialization
from iceprod.core.exe import WriteToScript
from iceprod.core.resources import Resources, group_hasher
from iceprod.s3 import S3
#from iceprod.server import get_pkg_binary
from iceprod.server.states import JOB_STATUS_START


logger = logging.getLogger('grid')


@ttl_cache(ttl=3600)
def get_host():
    """Cache the host fqdn for 1 hour"""
    return socket.getfqdn()


@enum.unique
class JobStatus(enum.Enum):
    IDLE = enum.auto()
    TRANSFERRING_INPUT = enum.auto()
    RUNNING = enum.auto()
    TRANSFERRING_OUTPUT = enum.auto()
    FAILED = enum.auto()
    COMPLETED = enum.auto()


@dataclass(kw_only=True, slots=True)
class BaseGridJob:
    """
    Interface for a grid job.
    Should be subclassed with a batch system id.
    """
    task: Task
    executable: str = ''
    infiles: list = field(default_factory=list)
    outfiles: list = field(default_factory=list)
    submit_dir: Optional[Path] = None
    status: JobStatus = JobStatus.IDLE


class GridJobActions:
    """
    Class holding job states and actions.

    Override for batch systems as needed, and call `super()` functions.

    Args:
        site: grid site name
        rest_client: IceProd API REST client
    """
    def __init__(self, site: str, rest_client: RestClient):
        self.jobs = {}
        self.site = site
        self.rest_client = rest_client

    async def submit(self, jobs: list[BaseGridJob]):
        """
        Submit multiple jobs to the batch system.

        Assumes that the resource requirements are identical.
        """
        raise NotImplementedError()

    async def job_update(self, job: BaseGridJob):
        """
        Send updated info from the batch system to the IceProd API.

        Must handle dup calls.
        """
        raise NotImplementedError()

    async def finish(self, job: BaseGridJob):
        """
        Run cleanup actions after a batch job completes.

        Must handle dup calls.
        """
        raise NotImplementedError()

    def get_job_counts(self):
        """
        Get an aggregated count of jobs per state.
        """
        ret = {s: 0 for s in JobStatus}
        for job in self.jobs.values():
            ret[job.status] += 1
        return ret


class BaseActiveJobs:
    """
    Interface for active job counting.
    Do not use this class directly.  Use one of the plugins.
    """
    def __init__(self, jobs: GridJobActions, submit_dir: Path):
        self.jobs = jobs
        self.submit_dir = submit_dir

    async def load(self):
        """
        Load currently active jobs.

        Returns:
            iterator of completed jobs
        """
        raise NotImplementedError()

    async def wait(self, timeout):
        """
        Wait for jobs to complete.

        Args:
            timeout: wait up to N seconds

        Returns:
            iterator of completed jobs
        """
        raise NotImplementedError()

    async def check(self):
        """
        Do any checks necessary that the active job tracking is correct.

        Returns:
            iterator of completed jobs
        """
        raise NotImplementedError()


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
                self.site_query_params['requirements.site'] = self.site

        # directories
        self.credentials_dir = Path(os.path.expanduser(os.path.expandvars(
            queue_cfg['credentials_dir'])))
        self.credentials_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

        self.submit_dir = Path(os.path.expanduser(os.path.expandvars(
            queue_cfg['submit_dir'])))
        self.submit_dir.mkdir(mode=0o700, parents=True, exist_ok=True)

        # dataset lookup cache
        self.dataset_cache = TTLCache(maxsize=100, ttl=60)
        
        # set up active jobs
        self.active_jobs = self.get_active_jobs()

    # Functions to override #

    def get_active_jobs(self):
        """Override the `ActiveJobs` and `JobActions` batch submission / monitoring classes"""
        return BaseActiveJobs(GridJobActions(site=self.site, rest_client=self.rest_client), submit_dir=self.submit_dir)

    def get_submit_dir(self):
        """Allow dynamically modifying the submit dir"""
        return self.submit_dir

    # Private functions #

    async def run(self):
        await self.active_jobs.load()

        check_time = time.monotonic()
        while True:
            await self.submit()
            await self.active_jobs.wait(timeout=300)

            now = time.monotonic()
            if now - check_time >= self.cfg['queue']['check_time']:
                await self.active_jobs.check()

    @cachedmethod(lambda self: self.dataset_cache)
    async def _dataset_lookup(self, dataset_id):
        ret = await Dataset.load_from_api(dataset_id, rest_client=self.rest_client)
        ret.fill_defaults()
        ret.validate()
        return ret

    async def submit(self):
        num_to_submit = self.get_queue_num()
        logger.info("Attempting to submit %d tasks", num_to_submit)

        # get tasks to run from REST API, and convert to batch jobs
        args = {
            'requirements': self.site_requirements,
            'query_params': self.site_query_params,
        }
        futures = set()
        for tasks_queued in range(num_to_submit):
            try:
                ret = await self.rest_client.request('GET', '/task_actions/queue', args)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    break
                raise
            else:
                futures.add(asyncio.create_task(self.convert_task_to_job(ret)))
        logging.info('got %d tasks to queue', tasks_queued)

        jobs = []
        while futures:
            done, futures = await asyncio.wait(futures)
            for f in done:
                job = await f
                jobs.append(job)

        if not jobs:
            return

        # add default resource requirements
        for job in jobs:
            job.task.requirements = self._get_resources(job.task)

        # submit to batch system
        await self.active_jobs.jobs.submit(jobs)

    async def convert_task_to_job(self, task):
        d = await self._dataset_lookup(task['dataset_id'])
        # don't bother looking up the job status - trust that if we got a task, we're in processing
        j = Job(dataset=d, job_id=task['job_id'], job_index=task['job_index'], status=JOB_STATUS_START)
        t = Task(
            dataset=d,
            job=j,
            task_id=task['task_id'],
            task_index=task['task_index'],
            name=task['name'],
            depends=task['depends'],
            requirements=task['requirements'],
            status=task['status'],
            site=self.site,
            stats={},
        )
        job = BaseGridJob(task=t)
        await self.create_submit_dir(job)
        return job

    async def create_submit_dir(self, job: BaseGridJob):
        """
        Create the submit dir and fill in the task
        """
        submit_dir = self.get_submit_dir()
        path = submit_dir / job.task.task_id
        i = 1
        while path.exists():
            path = submit_dir / '{job.task.task_id}_{i}'
            i += 1
        path.mkdir(parents=True)
        job.submit_dir = path

        s = WriteToScript(task=job.task, workdir=path)
        job.executable = await s.convert()
        job.infiles = s.infiles
        job.outfiles = s.outfiles

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

    def get_queue_num(self):
        """Determine how many tasks to queue."""
        counts = self.active_jobs.jobs.get_job_counts()
        idle_jobs = counts[JobStatus.IDLE]
        processing_jobs = counts[JobStatus.RUNNING] + counts[JobStatus.TRANSFERRING_INPUT] + counts[JobStatus.TRANSFERRING_OUTPUT]
        queue_tot_max = self.cfg['queue']['max_total_tasks_on_queue'] - idle_jobs - processing_jobs
        queue_idle_max = self.cfg['queue']['max_idle_tasks_on_queue'] - idle_jobs
        queue_interval_max = self.cfg['queue']['max_tasks_per_submit']
        queue_num = max(0, min(queue_tot_max, queue_idle_max, queue_interval_max))
        return queue_num









    # @run_on_executor
    # def _delete_dirs(self, dirs):
        # # delete dirs that need deleting
        # for t in dirs:
            # if not t.startswith(self.submit_dir):
                # # some security against nefarious things
                # raise Exception('directory %s not in submit_dir %s'%(t, self.submit_dir))
            # try:
                # logger.info('deleting submit_dir %s', t)
                # functions.removedirs(t)
            # except Exception:
                # logger.warning('could not delete submit dir %s', t, exc_info=True)
                # continue


    # @ttl_cache(ttl=600)
    # def get_token(self):
        # return self.cred_client.make_access_token()

    # @cached(TTLCache(1024, 60))
    # async def get_user_credentials(self, username):
        # ret = await self.cred_client.request('GET', f'/users/{username}/credentials')
        # return ret

    # @cached(TTLCache(1024, 60))
    # async def get_group_credentials(self, group):
        # ret = await self.cred_client.request('GET', f'/groups/{group}/credentials')
        # return ret

    # async def customize_task_config(self, task_cfg, job_cfg=None, dataset=None):
        # """Transforms for the task config"""
        # logger.info('customize_task_config for %s', job_cfg.get('options', {}).get('task_id', 'unknown'))

        # # first expand site temp urls
        # def expand_remote(cfg):
            # new_data = []
            # for d in cfg.get('data', []):
                # if not d['remote']:
                    # try:
                        # remote_base = d.storage_location(job_cfg)
                        # logger.info('expanding remote for %r', d['local'])
                        # d['remote'] = os.path.join(remote_base, d['local'])
                    # except Exception:
                        # # ignore failed expansions, as these are likely local temp paths
                        # pass
                # new_data.append(d)
            # cfg['data'] = new_data
        # expand_remote(task_cfg)
        # for tray in task_cfg['trays']:
            # expand_remote(tray)
            # for module in tray['modules']:
                # expand_remote(module)
        # logger.info('task_cfg: %r', task_cfg)

        # # now apply S3 and token credentials
        # creds = self.cfg.get('creds', {})
        # if dataset['group'] == 'users':
            # ret = await self.get_user_credentials(dataset['username'])
        # else:
            # ret = await self.get_group_credentials(dataset['group'])
        # creds.update(ret)

        # s3_creds = {url: creds.pop(url) for url in list(creds) if creds[url]['type'] == 's3'}
        # if s3_creds:
            # # if we have any s3 credentials, try presigning urls
            # logger.info('testing job for s3 credentials')
            # try:
                # queued_time = timedelta(seconds=self.queue_cfg['max_task_queued_time'])
            # except Exception:
                # queued_time = timedelta(seconds=86400*2)
            # try:
                # processing_time = timedelta(seconds=self.queue_cfg['max_task_processing_time'])
            # except Exception:
                # processing_time = timedelta(seconds=86400*2)
            # expiration = (queued_time + processing_time).total_seconds()
            # logger.info(f's3 cred expire time: {expiration}')

            # def presign_s3(cfg):
                # new_data = []
                # for d in cfg.get('data', []):
                    # for url in s3_creds:
                        # if d['remote'].startswith(url):
                            # logger.info('found data for cred: %s', url)
                            # path = d['remote'][len(url):].lstrip('/')
                            # bucket = None
                            # if '/' in path:
                                # bucket, key = path.split('/', 1)
                            # if (not bucket) or bucket not in s3_creds[url]['buckets']:
                                # key = path
                                # bucket = urlparse(url).hostname.split('.', 1)[0]
                                # if bucket not in s3_creds[url]['buckets']:
                                    # raise RuntimeError('bad s3 bucket')

                            # while '//' in key:
                                # key = key.replace('//', '/')
                            # while key.startswith('/'):
                                # key = key[1:]

                            # s = S3(url, s3_creds[url]['access_key'], s3_creds[url]['secret_key'], bucket=bucket)
                            # logger.info(f'S3 url={url} bucket={bucket} key={key}')
                            # if d['movement'] == 'input':
                                # d['remote'] = s.get_presigned(key, expiration=expiration)
                                # new_data.append(d)
                            # elif d['movement'] == 'output':
                                # d['remote'] = s.put_presigned(key, expiration=expiration)
                            # elif d['movement'] == 'both':
                                # d['movement'] = 'input'
                                # d['remote'] = s.get_presigned(key, expiration=expiration)
                                # new_data.append(d.copy())
                                # d['movement'] = 'output'
                                # d['remote'] = s.put_presigned(key, expiration=expiration)
                            # else:
                                # raise RuntimeError('unknown s3 data movement')
                            # new_data.append(d)
                            # break
                    # else:
                        # new_data.append(d)
                # cfg['data'] = new_data

            # presign_s3(task_cfg)
            # for tray in task_cfg['trays']:
                # presign_s3(tray)
                # for module in tray['modules']:
                    # presign_s3(module)

        # oauth_creds = {url: creds.pop(url) for url in list(creds) if creds[url]['type'] == 'oauth'}
        # if oauth_creds:
            # # if we have token-based credentials, add them to the config
            # logger.info('testing job for oauth credentials')
            # cred_keys = set()

            # def get_creds(cfg):
                # for d in cfg.get('data', []):
                    # for url in oauth_creds:
                        # if d['remote'].startswith(url):
                            # logger.info('found data for cred: %s', url)
                            # cred_keys.add(url)
                            # break

            # get_creds(task_cfg)
            # for tray in task_cfg['trays']:
                # get_creds(tray)
                # for module in tray['modules']:
                    # get_creds(module)

            # file_creds = {}
            # for url in cred_keys:
                # cred_name = hashlib.sha1(oauth_creds[url]['access_token'].encode('utf-8')).hexdigest()
                # path = os.path.join(self.credentials_dir, cred_name)
                # if not os.path.exists(path):
                    # with open(path, 'w') as f:
                        # f.write(oauth_creds[url]['access_token'])
                # file_creds[url] = cred_name
            # job_cfg['options']['credentials'] = file_creds

    # async def setup_submit_directory(self,task):
        # """Set up submit directory"""
        # # create directory for task
        # submit_dir = self.submit_dir
        # task_dir = os.path.join(submit_dir,task['task_id']+'_'+str(random.randint(0,1000000)))
        # while os.path.exists(task_dir):
            # task_dir = os.path.join(submit_dir,task['task_id']+'_'+str(random.randint(0,1000000)))
        # task_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(task_dir)))
        # os.makedirs(task_dir)
        # task['submit_dir'] = task_dir

        # # symlink or copy the .sh file
        # src = get_pkg_binary('iceprod', 'loader.sh')
        # dest = os.path.join(task_dir, 'loader.sh')
        # try:
            # os.symlink(src, dest)
        # except Exception:
            # try:
                # functions.copy(src, dest)
            # except Exception:
                # logger.error('Error creating symlink or copy of .sh file: %s',dest,exc_info=True)
                # raise

        # # get passkey
        # # expiration = self.queue_cfg['max_task_queued_time']
        # # expiration += self.queue_cfg['max_task_processing_time']
        # # expiration += self.queue_cfg['max_task_reset_time']
        # # TODO: take expiration into account
        # passkey = self.get_token()

        # # write cfg
        # cfg, filelist = self.write_cfg(task)

        # # create submit file
        # try:
            # await asyncio.ensure_future(self.generate_submit_file(
                # task,
                # cfg=cfg,
                # passkey=passkey,
                # filelist=filelist
            # ))
        # except Exception:
            # logger.error('Error generating submit file',exc_info=True)
            # raise

    # def create_config(self, task):
        # if 'config' in task and task['config']:
            # config = serialization.dict_to_dataclasses(task['config'])
        # else:
            # config = dataclasses.Job()

        # # add server options
        # config['options']['task_id'] = task['task_id']
        # config['options']['task'] = task['name']
        # if 'job' in task:
            # config['options']['job'] = task['job']
        # if 'jobs_submitted' in task:
            # config['options']['jobs_submitted'] = task['jobs_submitted']
        # if 'dataset_id' in task:
            # config['options']['dataset_id'] = task['dataset_id']
        # if 'dataset' in task:
            # config['options']['dataset'] = task['dataset']
        # config['options']['stillrunninginterval'] = self.queue_cfg['ping_interval']
        # config['options']['debug'] = task['debug']
        # config['options']['upload'] = 'logging'
        # config['options']['gridspec'] = self.gridspec
        # if (not config['options'].get('site_temp','')) and 'site_temp' in self.cfg['queue']:
            # config['options']['site_temp'] = self.cfg['queue']['site_temp']
        # if ('download' in self.cfg and 'http_username' in self.cfg['download']
                # and self.cfg['download']['http_username']):
            # config['options']['username'] = self.cfg['download']['http_username']
        # if ('download' in self.cfg and 'http_password' in self.cfg['download']
                # and self.cfg['download']['http_password']):
            # config['options']['password'] = self.cfg['download']['http_password']

        # add_default_options(config['options'])

        # return config

    # def write_cfg(self, task):
        # """Write the config file for a task-like object"""
        # filename = os.path.join(task['submit_dir'],'task.cfg')
        # filelist = [filename]

        # config = self.create_config(task)
        # if creds := config['options'].get('credentials', {}):
            # cred_dir = os.path.join(task['submit_dir'], CRED_SUBMIT_DIR)
            # os.mkdir(cred_dir)
            # for name in creds.values():
                # src = os.path.join(self.credentials_dir, name)
                # dest = os.path.join(cred_dir, name)
                # os.symlink(src, dest)
            # filelist.append(cred_dir)
        # if 'system' in self.cfg and 'remote_cacert' in self.cfg['system']:
            # config['options']['ssl'] = {}
            # config['options']['ssl']['cacert'] = os.path.basename(self.cfg['system']['remote_cacert'])
            # src = self.cfg['system']['remote_cacert']
            # dest = os.path.join(task['submit_dir'],config['options']['ssl']['cacert'])
            # try:
                # os.symlink(src,dest)
            # except Exception:
                # try:
                    # functions.copy(src,dest)
                # except Exception:
                    # logger.error('Error creating symlink or copy of remote_cacert',
                                 # exc_info=True)
                    # raise
            # filelist.append(dest)
        # if 'x509proxy' in self.cfg['queue'] and self.cfg['queue']['x509proxy']:
            # config['options']['x509'] = os.path.basename(self.cfg['queue']['x509proxy'])
            # src = self.cfg['queue']['x509proxy']
            # logger.info('submit_dir %r  x509 %r', task['submit_dir'], config['options']['x509'])
            # dest = os.path.join(task['submit_dir'],config['options']['x509'])
            # try:
                # os.symlink(src,dest)
            # except Exception:
                # try:
                    # functions.copy(src,dest)
                # except Exception:
                    # logger.error('Error creating symlink or copy of x509 proxy',
                                 # exc_info=True)
                    # raise
            # filelist.append(dest)
        # if 'extra_file_tranfers' in self.cfg['queue'] and self.cfg['queue']['extra_file_tranfers']:
            # for f in self.cfg['queue']['extra_file_tranfers']:
                # logger.info('submit_dir %r  extra_files %r', task['submit_dir'], f)
                # dest = os.path.join(task['submit_dir'],os.path.basename(f))
                # try:
                    # os.symlink(os.path.abspath(f),dest)
                # except Exception:
                    # try:
                        # functions.copy(f,dest)
                    # except Exception:
                        # logger.error('Error creating symlink or copy of extra file %s',
                                     # f, exc_info=True)
                        # raise
                # filelist.append(dest)
        # if 'data_movement_stats' in self.cfg['queue'] and self.cfg['queue']['data_movement_stats']:
            # config['options']['data_movement_stats'] = self.cfg['queue']['data_movement_stats']
        # if 'upload_checksum' in self.cfg['queue']:
            # config['options']['upload_checksum'] = self.cfg['queue']['upload_checksum']

        # if 'reqs' in task:
            # # add resources
            # config['options']['resources'] = {}
            # for r in task['reqs']:
                # config['options']['resources'][r] = task['reqs'][r]

        # # write to file
        # serialization.serialize_json.dump(config, filename)

        # c = iceprod.core.exe.Config(config)
        # config = c.parseObject(config, {})

        # return (config, filelist)

    # # not async: called from executor
    # def get_submit_args(self,task,cfg=None,passkey=None):
        # """Get the submit arguments to start the loader script."""
        # # get website address
        # if ('rest_api' in self.cfg and self.cfg['rest_api'] and
                # 'url' in self.cfg['rest_api'] and self.cfg['rest_api']['url']):
            # web_address = self.cfg['rest_api']['url']
        # else:
            # raise Exception('no web address for rest calls')

        # args = []
        # if 'software_dir' in self.queue_cfg and self.queue_cfg['software_dir']:
            # args.append('-s {}'.format(self.queue_cfg['software_dir']))
        # if 'iceprod_dir' in self.queue_cfg and self.queue_cfg['iceprod_dir']:
            # args.append('-e {}'.format(self.queue_cfg['iceprod_dir']))
        # if 'x509proxy' in self.cfg['queue'] and self.cfg['queue']['x509proxy']:
            # args.append('-x {}'.format(os.path.basename(self.cfg['queue']['x509proxy'])))
        # if ('download' in self.cfg and 'http_proxy' in self.cfg['download']
                # and self.cfg['download']['http_proxy']):
            # args.apend('-c {}'.format(self.cfg['download']['http_proxy']))
        # args.append('--url {}'.format(web_address))
        # if passkey:
            # args.append('--passkey {}'.format(passkey))
        # if cfg:
            # args.append('--cfgfile task.cfg')
        # if 'debug' in task and task['debug']:
            # args.append('--debug')
        # return args

