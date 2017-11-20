"""Functions to communicate with the server using JSONRPC"""

from __future__ import absolute_import, division, print_function

import sys
import os
import time
from copy import deepcopy
from functools import wraps

import logging
logger = logging.getLogger('exe_json')

from iceprod.core import constants
from iceprod.core import functions
from iceprod.core import dataclasses
from iceprod.core.serialization import dict_to_dataclasses
from iceprod.core.jsonRPCclient import JSONRPC
from iceprod.core.jsonUtil import json_compressor,json_decode


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
        self.rpc = JSONRPC(address=url,passkey=passkey,**kwargs)

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
        task = self.rpc.new_task(gridspec=gridspec, hostname=hostname, 
                                 domain=domain, ifaces=ifaces,
                                 os=os_type,
                                 **resources)
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
        self.rpc.set_processing(task_id=task_id)

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
        stats = {'hostname': hostname, 'domain': domain,
                 'time_used': t, 'task_stats': stats}
        if resources:
            stats['resources'] = resources
        self.rpc.finish_task(task_id=self.cfg.config['options']['task_id'],
                             stats=stats)

    @send_through_pilot
    def still_running(self):
        """Check if the task should still be running according to the DB"""
        task_id = self.cfg.config['options']['task_id']
        ret = self.rpc.stillrunning(task_id=task_id)
        if not ret:
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
        try:
            hostname = functions.gethostname()
            domain = '.'.join(hostname.split('.')[-2:])
            error_info = {
                'hostname': hostname, 'domain': domain,
                'time_used': None,
                'error_summary': '',
                'task_stats': stats,
            }
            if start_time:
                error_info['time_used'] = time.time() - start_time
            if reason:
                error_info['error_summary'] = reason
            if resources:
                error_info['resources'] = resources
        except Exception:
            logger.warning('failed to collect error info', exc_info=True)
            error_info = None
        task_id = self.cfg.config['options']['task_id']
        self.rpc.task_error(task_id=task_id, error_info=error_info)

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
            error_info = {
                'hostname': hostname, 'domain': domain,
                'error_summary':'',
                'time_used':None,
            }
            if resources and 'time' in resources:
                error_info['time_used'] = resources['time']
            if resources:
                error_info['resources'] = resources
            if reason:
                error_info['error_summary'] = reason
        except Exception:
            logger.warning('failed to collect error info', exc_info=True)
            error_info = None
        if message:
            data = json_compressor.compress(message)
            self.rpc.upload_logfile(task=task_id,name='stdlog',data=data)
            self.rpc.upload_logfile(task=task_id,name='stdout',data='')
            self.rpc.upload_logfile(task=task_id,name='stderr',data='')
        self.rpc.task_error(task_id=task_id, error_info=error_info)

    @send_through_pilot
    def _upload_logfile(self, name, filename):
        """Upload a log file"""
        task_id = self.cfg.config['options']['task_id']
        data = json_compressor.compress(open(filename,'rb').read())
        ret = self.rpc.upload_logfile(task=task_id,name=name,data=data)

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
        self.rpc.update_pilot(pilot_id=pilot_id, **kwargs)
