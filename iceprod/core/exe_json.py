"""Functions to communicate with the server using JSONRPC"""

from __future__ import absolute_import, division, print_function

import sys
import os
import time
from copy import deepcopy

import logging
logger = logging.getLogger('exe_json')

from iceprod.core import constants
from iceprod.core import functions
from iceprod.core import dataclasses
from iceprod.core.serialization import dict_to_dataclasses
from iceprod.core.jsonRPCclient import JSONRPC
from iceprod.core.jsonUtil import json_compressor,json_decode


def setupjsonRPC(url, passkey, **kwargs):
    """Setup the JSONRPC communications"""
    JSONRPC.start(address=url,passkey=passkey,**kwargs)
    try:
        ret = JSONRPC.echo(value='e')
    except Exception as e:
        logger.error('error',exc_info=True)
        raise Exception('JSONRPC communcation did not start.  '
                        'url=%s and passkey=%s'%(url,passkey))
    else:
        if ret != 'e':
            raise Exception('JSONRPC communication error when starting - '
                            'echo failed (%r).  url=%s and passkey=%s'
                            %(ret,url,passkey))

def processing(task_id):
    """
    Tell the server that we are processing this task.

    Only used for single task config, not for pilots.
    """
    ret = JSONRPC.set_processing(task_id=task_id)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def downloadtask(gridspec, resources={}):
    """Download a new task from the server"""
    try:
        platform = os.environ['PLATFORM']
    except:
        platform = functions.platform()
    hostname = functions.gethostname()
    domain = '.'.join(hostname.split('.')[-2:])
    try:
        ifaces = functions.getInterfaces()
    except:
        ifaces = None
    resources = deepcopy(resources)
    if 'gpu' in resources and isinstance(resources['gpu'],list):
        resources['gpu'] = len(resources['gpu'])
    python_unicode = 'ucs4' if sys.maxunicode == 1114111 else 'ucs2'
    task = JSONRPC.new_task(gridspec=gridspec, platform=platform,
                            hostname=hostname, domain=domain, ifaces=ifaces,
                            python_unicode=python_unicode,
                            **resources)
    if isinstance(task,Exception):
        # an error occurred
        raise task
    if task and not isinstance(task, dataclasses.Job):
        try:
            task = dict_to_dataclasses(task)
        except Exception:
            logger.warn('not a Job: %r',task)
            raise
    return task

def finishtask(cfg, stats={}, start_time=None):
    """Finish a task"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"]["task_id"] not specified, '
                        'so cannot finish task')
    if 'DBkill' in cfg.config['options'] and cfg.config['options']['DBkill']:
        return # don't finish task on a DB kill
    if 'stats' in cfg.config['options']:
        # filter task stats
        stat_keys = set(json_decode(cfg.config['options']['stats']))
        stats = {k:stats[k] for k in stats if k in stat_keys}

    hostname = functions.gethostname()
    domain = '.'.join(hostname.split('.')[-2:])
    if start_time:
        t = time.time() - start_time
    else:
        t = None
    stats = {'hostname': hostname, 'domain': domain,
             'time_used': t, 'task_stats': stats}
    ret = JSONRPC.finish_task(task_id=cfg.config['options']['task_id'],
                              stats=stats)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def stillrunning(cfg):
    """Check if the task should still be running according to the DB"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot finish task')
    ret = JSONRPC.stillrunning(task_id=cfg.config['options']['task_id'])
    if isinstance(ret,Exception):
        # an error occurred
        raise ret
    if not ret:
        cfg.config['options']['DBkill'] = True
        raise Exception('task should be stopped')

def taskerror(cfg, start_time=None, reason=None):
    """
    Tell the server about the error experienced

    Args:
        cfg (:py:class:`iceprod.core.exe.Config`): the runner config
        start_time (float): job start time in unix seconds
        reason (str): one-line summary of error
    """
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send error')
    if 'DBkill' in cfg.config['options'] and cfg.config['options']['DBkill']:
        return # don't change status on a DB kill
    try:
        hostname = functions.gethostname()
        domain = '.'.join(hostname.split('.')[-2:])
        error_info = {
            'hostname': hostname, 'domain': domain,
            'time_used': None,
            'error_summary': '',
        }
        if start_time:
            error_info['time_used'] = time.time() - start_time
        if reason:
            error_info['error_summary'] = reason
    except Exception:
        logger.warn('failed to collect error info', exc_info=True)
        error_info = None
    ret = JSONRPC.task_error(task_id=cfg.config['options']['task_id'],
                             error_info=error_info)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def task_kill(task_id, resources=None, reason=None, message=None):
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
        logger.warn('failed to collect error info', exc_info=True)
        error_info = None
    if message:
        data = json_compressor.compress(message)
        ret = JSONRPC.upload_logfile(task=task_id,name='stdlog',data=data)
        if isinstance(ret,Exception):
            # an error occurred
            raise ret
        JSONRPC.upload_logfile(task=task_id,name='stdout',data='')
        JSONRPC.upload_logfile(task=task_id,name='stderr',data='')
    ret = JSONRPC.task_error(task_id=task_id, error_info=error_info)
    if isinstance(ret, Exception):
        # an error occurred
        raise ret

def _upload_logfile(cfg, task_id, name, filename):
    """Upload a log file"""
    if 'DBkill' in cfg.config['options'] and cfg.config['options']['DBkill']:
        return # don't upload logs on a DB kill
    data = json_compressor.compress(open(filename).read())
    ret = JSONRPC.upload_logfile(task=task_id,name=name,data=data)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def uploadLogging(cfg):
    """Upload all logging files"""
    uploadLog(cfg)
    uploadErr(cfg)
    uploadOut(cfg)

def uploadLog(cfg):
    """Upload log files"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send log')
    _upload_logfile(cfg, cfg.config['options']['task_id'],'stdlog',constants['stdlog'])

def uploadErr(cfg):
    """Upload error files"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send error')
    _upload_logfile(cfg, cfg.config['options']['task_id'],'stderr',constants['stderr'])

def uploadOut(cfg):
    """Upload out files"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send output')
    _upload_logfile(cfg, cfg.config['options']['task_id'],'stdout',constants['stdout'])

def update_pilot(pilot_id, **kwargs):
    """Update the pilot table"""
    ret = JSONRPC.update_pilot(pilot_id=pilot_id, **kwargs)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret
