"""Functions to communicate with the server using JSONRPC"""

from __future__ import absolute_import, division, print_function

import sys
import os
import time
from copy import deepcopy
from functools import wraps
from datetime import datetime

import logging

from iceprod.core import constants
from iceprod.core import functions
from iceprod.core import dataclasses
from .serialization import dict_to_dataclasses
from .jsonUtil import json_compressor,json_decode
from .rest_client import Client


class ServerComms:
    """
    Setup JSONRPC communications with the IceProd server.

    Args:
        url (str): address to connect to
        passkey (str): passkey for authorization/authentication
        config (:py:class:`iceprod.server.exe.Config`): Config object
        **kwargs: passed to JSONRPC
    """
    def __init__(self, url, passkey, config, **kwargs):
        self.url = url
        self.cfg = config
        self.rest = Client(address=url,auth_key=passkey,**kwargs)

    async def download_task(self, gridspec, resources={}):
        """
        Download new task(s) from the server.

        Args:
            gridspec (str): gridspec the pilot was submitted from
            resources (dict): resources available in the pilot

        Returns:
            list: list of task configs
        """
        hostname = functions.gethostname()
        domain = '.'.join(hostname.split('.')[-2:])
        try:
            ifaces = functions.getInterfaces()
        except Exception:
            ifaces = None
        resources = deepcopy(resources)
        if 'gpu' in resources and isinstance(resources['gpu'],list):
            resources['gpu'] = len(resources['gpu'])
        os_type = os.environ['OS_ARCH'] if 'OS_ARCH' in os.environ else None
        if os_type:
            resources['os'] = os_type
        task = await self.rest.request('POST', '/task_actions/process',
                {'gridspec': gridspec,
                 'hostname': hostname, 
                 'domain': domain,
                 'ifaces': ifaces,
                 'requirements': resources,
                })
        if not task:
            return None

        # get config
        try:
            config = await self.rest.request('GET', '/config/{}'.format(task['dataset_id']))
            if not isinstance(config, dataclasses.Job):
                config = dict_to_dataclasses(config)
        except Exception:
            logging.warn('failed to get dataset config for dataset %s', task['dataset_id'])
            await self.task_kill(task['task_id'], dataset_id=task['dataset_id'],
                                 reason='failed to download dataset config')
            raise

        # fill in options
        if 'options' not in config:
            config['options'] = {}
        config['options']['task_id'] = task['task_id']
        config['options']['job_id'] = task['job_id']
        config['options']['dataset_id'] = task['dataset_id']
        config['options']['task'] = task['task_index']
        try:
            job = await self.rest.request('GET', '/jobs/{}'.format(task['job_id']))
            config['options']['job'] = job['job_index']
        except Exception:
            logging.warn('failed to get job %s', task['job_id'])
            await self.task_kill(task['task_id'], dataset_id=task['dataset_id'],
                                 reason='failed to download job')
            raise
        try:
            dataset = await self.rest.request('GET', '/datasets/{}'.format(task['dataset_id']))
            config['options']['dataset'] = dataset['dataset']
            config['options']['jobs_submitted'] = dataset['jobs_submitted']
            config['options']['tasks_submitted'] = dataset['tasks_submitted']
            config['options']['debug'] = dataset['debug']
        except Exception:
            logging.warn('failed to get dataset %s', task['dataset_id'])
            await self.task_kill(task['task_id'], dataset_id=task['dataset_id'],
                                 reason='failed to download dataset')
            raise
        return [config]

    async def processing(self, task_id):
        """
        Tell the server that we are processing this task.

        Only used for single task config, not for pilots.

        Args:
            task_id (str): task_id to mark as processing
        """
        await self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                              {'status': 'processing'})

    async def finish_task(self, task_id, dataset_id=None, stats={},
                          stat_filter=None, start_time=None, resources=None):
        """
        Finish a task.

        Args:
            task_id (str): task_id of task
            dataset_id (str): (optional) dataset_id of task
            stats (dict): (optional) task statistics
            stat_filter (iterable): (optional) stat filter by keywords
            start_time (float): (optional) task start time in unix seconds
            resources (dict): (optional) task resource usage
        """
        if stat_filter:
            # filter task stats
            stats = {k:stats[k] for k in stats if k in stat_filter}

        hostname = functions.gethostname()
        domain = '.'.join(hostname.split('.')[-2:])
        if start_time:
            t = time.time() - start_time
        else:
            t = None
        iceprod_stats = {
            'hostname': hostname,
            'domain': domain,
            'time_used': t,
            'task_stats': stats,
            'time': datetime.utcnow().isoformat(),
        }
        if resources:
            iceprod_stats['resources'] = resources
        if dataset_id:
            iceprod_stats['dataset_id'] = dataset_id

        await self.rest.request('POST', '/tasks/{}/task_stats'.format(task_id),
                              iceprod_stats)
        await self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                              {'status': 'complete'})

    async def still_running(self, task_id):
        """
        Check if the task should still be running according to the DB.

        Args:
            task_id (str): task_id of task
        """
        ret = await self.rest.request('GET', '/tasks/{}'.format(task_id))
        if (not ret) or 'status' not in ret or ret['status'] != 'processing':
            raise Exception('task should be stopped')

    async def task_error(self, task_id, dataset_id=None, stats={}, start_time=None, reason=None, resources=None):
        """
        Tell the server about the error experienced

        Args:
            task_id (str): task_id of task
            dataset_id (str): (optional) dataset_id of task
            stats (dict): (optional) task statistics
            start_time (float): (optional) task start time in unix seconds
            reason (str): (optional) one-line summary of error
            resources (dict): (optional) task resource usage
        """
        iceprod_stats = {}
        try:
            hostname = functions.gethostname()
            domain = '.'.join(hostname.split('.')[-2:])
            if start_time:
                t = time.time() - start_time
            else:
                t = None
            iceprod_stats = {
                'hostname': hostname,
                'domain': domain,
                'time_used': t,
                'task_stats': stats,
                'time': datetime.utcnow().isoformat(),
                'error_summary': reason if reason else '',
            }
            if resources:
                iceprod_stats['resources'] = resources
            if dataset_id:
                iceprod_stats['dataset_id'] = dataset_id
        except Exception:
            logging.warning('failed to collect error info', exc_info=True)

        try:
            await self.rest.request('POST', '/tasks/{}/task_stats'.format(task_id),
                                    iceprod_stats)
        except Exception:
            logging.warning('failed to post task_stats for %r', task_id)

        await self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                              {'status': 'reset'})

    async def task_kill(self, task_id, dataset_id=None, resources=None, reason=None, message=None):
        """
        Tell the server that we killed a task.

        Args:
            task_id (str): task_id of task
            dataset_id (str): (optional) dataset_id of task
            resources (dict): (optional) used resources
            reason (str): (optional) short summary for kill
            message (str): (optional) long message to replace log upload
        """
        if not reason:
            reason = 'killed'
        if not message:
            message = reason
        try:
            hostname = functions.gethostname()
            domain = '.'.join(hostname.split('.')[-2:])
            iceprod_stats = {
                'hostname': hostname,
                'domain': domain,
                'time': datetime.utcnow().isoformat(),
                'error_summary': reason if reason else '',
            }
            if resources:
                iceprod_stats['resources'] = resources
        except Exception:
            logging.warning('failed to collect error info', exc_info=True)
            iceprod_stats = {}
        try:
            await self.rest.request('POST', '/tasks/{}/task_stats'.format(task_id),
                                    iceprod_stats)
        except Exception:
            logging.warning('failed to post task_stats for %r', task_id)

        await self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                              {'status': 'reset'})

        data = {'name': 'stdlog', 'task_id': task_id}
        if dataset_id:
            data['dataset_id'] = dataset_id
        if message:
            data['data'] = message
        elif reason:
            data['data'] = reason
        else:
            data['data'] = 'task killed'
        await self.rest.request('POST', '/logs', data)
        data.update({'name':'stdout', 'data': ''})
        await self.rest.request('POST', '/logs', data)
        data.update({'name':'stderr', 'data': ''})
        await self.rest.request('POST', '/logs', data)

    async def _upload_logfile(self, name, filename, task_id=None, dataset_id=None):
        """Upload a log file"""
        data = {'name': name}
        if task_id:
            data['task_id'] = task_id
        if dataset_id:
            data['dataset_id'] = dataset_id
        try:
            with open(filename) as f:
                data['data'] = f.read()
        except Exception as e:
            data['data'] = str(e)
        await self.rest.request('POST', '/logs', data)

    async def uploadLog(self, **kwargs):
        """Upload log file"""
        logging.getLogger().handlers[0].flush()
        await self._upload_logfile('stdlog', os.path.abspath(constants['stdlog']), **kwargs)

    async def uploadErr(self, filename=None, **kwargs):
        """Upload stderr file"""
        sys.stderr.flush()
        if not filename:
            filename = os.path.abspath(constants['stderr'])
        await self._upload_logfile('stderr', filename, **kwargs)

    async def uploadOut(self, filename=None, **kwargs):
        """Upload stdout file"""
        sys.stdout.flush()
        if not filename:
            filename = os.path.abspath(constants['stdout'])
        await self._upload_logfile('stdout', filename, **kwargs)

    async def update_pilot(self, pilot_id, **kwargs):
        """
        Update the pilot table.

        Args:
            pilot_id (str): pilot id
            **kwargs: passed through to rpc function
        """
        await self.rest.request('PATCH', '/pilots/{}'.format(pilot_id), kwargs)


    # --- synchronous versions to be used from a signal handler
    # --- or other non-async code

    def task_kill_sync(self, task_id, dataset_id=None, resources=None, reason=None, message=None):
        """
        Tell the server that we killed a task (synchronous version).

        Args:
            task_id (str): task_id of task
            dataset_id (str): (optional) dataset_id of task
            resources (dict): (optional) used resources
            reason (str): (optional) short summary for kill
            message (str): (optional) long message to replace log upload
        """
        if not reason:
            reason = 'killed'
        if not message:
            message = reason
        try:
            hostname = functions.gethostname()
            domain = '.'.join(hostname.split('.')[-2:])
            iceprod_stats = {
                'hostname': hostname,
                'domain': domain,
                'time': datetime.utcnow().isoformat(),
                'error_summary': reason if reason else '',
            }
            if resources:
                iceprod_stats['resources'] = resources
        except Exception:
            logging.warning('failed to collect error info', exc_info=True)
            iceprod_stats = {}
        try:
            self.rest.request_seq('POST', '/tasks/{}/task_stats'.format(task_id),
                                  iceprod_stats)
        except Exception:
            logging.warning('failed to post task_stats for %r', task_id)

        self.rest.request_seq('PUT', '/tasks/{}/status'.format(task_id),
                               {'status': 'reset'})

        data = {'name': 'stdlog', 'task_id': task_id}
        if dataset_id:
            data['dataset_id'] = dataset_id
        if message:
            data['data'] = message
        elif reason:
            data['data'] = reason
        else:
            data['data'] = 'task killed'
        self.rest.request_seq('POST', '/logs', data)
        data.update({'name':'stdout', 'data': ''})
        self.rest.request_seq('POST', '/logs', data)
        data.update({'name':'stderr', 'data': ''})
        self.rest.request_seq('POST', '/logs', data)

    def update_pilot_sync(self, pilot_id, **kwargs):
        """
        Update the pilot table (synchronous version).

        Args:
            pilot_id (str): pilot id
            **kwargs: passed through to rpc function
        """
        self.rest.request_seq('PATCH', '/pilots/{}'.format(pilot_id), kwargs)
