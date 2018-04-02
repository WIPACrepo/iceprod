"""Functions to communicate with the server using JSONRPC"""

from __future__ import absolute_import, division, print_function

import sys
import os
import time
from copy import deepcopy
from functools import wraps
from datetime import datetime

import logging
logger = logging.getLogger('exe_json')

from iceprod.core import constants
from iceprod.core import functions
from iceprod.core import dataclasses
from .serialization import dict_to_dataclasses
from .jsonUtil import json_compressor,json_decode
from .rest_client import Client


def send_through_pilot(func):
    """
    Decorator to route communication through the pilot
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if 'task_id' not in self.cfg.config['options']:
            raise Exception('config["options"]["task_id"] not specified')
        if 'DBkill' in self.cfg.config['options'] and self.cfg.config['options']['DBkill']:
            raise Exception('DBKill')
        if 'message_queue' in self.cfg.config['options']:
            logger.info('send_through_pilot(%s)',func.__name__)
            send,recv = self.cfg.config['options']['message_queue']
            task_id = self.cfg.config['options']['task_id']
            # mq can't be pickled, so remove temporarily
            mq = self.cfg.config['options']['message_queue']
            del self.cfg.config['options']['message_queue']
            logger.info('config: %r', dict(self.cfg.config))
            logger.info('args: %r', args)
            logger.info('kwargs: %r', kwargs)
            try:
                send.put((task_id,func.__name__,self.cfg.config,args,kwargs))
                ret = recv.get()
                if ret:
                    if isinstance(ret, Exception):
                        raise ret
                    elif len(ret) == 2:
                        new_options, ret = ret
                        self.cfg.config['options'] = new_options
            finally:
                self.cfg.config['options']['message_queue'] = mq
            return ret
        else:
            return func(self, *args, **kwargs)
    return wrapper


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

    def download_task(self, gridspec, resources={}):
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
        task = self.rest.request('GET', '/task_actions/process',
                {'gridspec': gridspec,
                 'hostname': hostname, 
                 'domain': domain,
                 'ifaces': ifaces,
                 'requirements': resources,
                })
        if not task:
            return None
        if task and not isinstance(task,list):
            task = [task]
        # convert dict to Job
        ret = []
        for t in task:
            if not isinstance(t, dataclasses.Job):
                try:
                    ret.append(dict_to_dataclasses(t))
                except Exception:
                    logger.warning('not a Job: %r',t)
                    raise
            else:
                ret.append(t)
        return ret

    def processing(self, task_id):
        """
        Tell the server that we are processing this task.

        Only used for single task config, not for pilots.

        Args:
            task_id (str): task_id to mark as processing
        """
        self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                          {'status': 'processing'})

    @send_through_pilot
    def finish_task(self, stats={}, start_time=None, resources=None):
        """
        Finish a task.
        """
        if 'stats' in self.cfg.config['options']:
            # filter task stats
            stat_keys = set(json_decode(self.cfg.config['options']['stats']))
            stats = {k:stats[k] for k in stats if k in stat_keys}

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

        task_id = self.cfg.config['options']['task_id']
        self.rest.request('POST', '/tasks/{}/task_stats'.format(task_id),
                          iceprod_stats)
        self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                          {'status': 'complete'})

    @send_through_pilot
    def still_running(self):
        """Check if the task should still be running according to the DB"""
        task_id = self.cfg.config['options']['task_id']
        ret = self.rest.request('GET', '/tasks/{}'.format(task_id))
        if (not ret) or 'status' not in ret or ret['status'] != 'processing':
            self.cfg.config['options']['DBkill'] = True
            raise Exception('task should be stopped')

    @send_through_pilot
    def task_error(self, stats={}, start_time=None, reason=None, resources=None):
        """
        Tell the server about the error experienced

        Args:
            stats (dict): task statistics
            start_time (float): task start time in unix seconds
            reason (str): one-line summary of error
            resources (dict): task resource usage
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
        except Exception:
            logger.warning('failed to collect error info', exc_info=True)

        task_id = self.cfg.config['options']['task_id']
        self.rest.request('POST', '/tasks/{}/task_stats'.format(task_id),
                          iceprod_stats)
        self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                          {'status': 'reset'})

    def task_kill(self, task_id, resources=None, reason=None, message=None):
        """
        Tell the server that we killed a task

        Args:
            task_id (str): the task_id
            resources (dict): used resources
            reason (str): short summary for kill
            message (str): long message to replace log upload
        """
        try:
            hostname = functions.gethostname()
            domain = '.'.join(hostname.split('.')[-2:])
            iceprod_stats = {
                'hostname': hostname,
                'domain': domain,
                'task_stats': stats,
                'time': datetime.utcnow().isoformat(),
                'error_summary': reason if reason else '',
            }
            if resources:
                iceprod_stats['resources'] = resources
        except Exception:
            logger.warning('failed to collect error info', exc_info=True)
            error_info = None
        self.rest.request('POST', '/tasks/{}/task_stats'.format(task_id),
                          iceprod_stats)
        self.rest.request('PUT', '/tasks/{}/status'.format(task_id),
                          {'status': 'reset'})
        if message:
            data = {'name': 'stdlog', 'task_id': task_id}
            try:
                data['dataset_id'] = self.cfg.config['options']['dataset_id']
            except Exception:
                pass
            try:
                data = json_compressor.compress(message)
            except Exception as e:
                data['data'] = str(e)
            self.rest.request('POST', '/logs', data)
            data.update({'name':'stdout', 'data': ''})
            self.rest.request('POST', '/logs', data)
            data.update({'name':'stderr', 'data': ''})
            self.rest.request('POST', '/logs', data)

    @send_through_pilot
    def _upload_logfile(self, name, filename):
        """Upload a log file"""
        data = {'name': name}
        try:
            data['task_id'] = self.cfg.config['options']['task_id']
        except Exception:
            pass
        try:
            data['dataset_id'] = self.cfg.config['options']['dataset_id']
        except Exception:
            pass
        try:
            data['data'] = json_compressor.compress(open(filename,'rb').read())
        except Exception as e:
            data['data'] = str(e)
        self.rest.request('POST', '/logs', data)

    def uploadLog(self):
        """Upload log file"""
        logging.getLogger().handlers[0].flush()
        self._upload_logfile('stdlog', os.path.abspath(constants['stdlog']))

    def uploadErr(self):
        """Upload stderr file"""
        sys.stderr.flush()
        self._upload_logfile('stderr', os.path.abspath(constants['stderr']))

    def uploadOut(self):
        """Upload stdout file"""
        sys.stderr.flush()
        self._upload_logfile('stdout', os.path.abspath(constants['stdout']))

    def update_pilot(self, pilot_id, **kwargs):
        """
        Update the pilot table

        Args:
            pilot_id (str): pilot id
            **kwargs: passed through to rpc function
        """
        self.rest.request('PATCH', '/pilots/{}'.format(pilot_id), kwargs)
