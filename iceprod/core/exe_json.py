"""Functions to communicate with the server using JSONRPC"""

from __future__ import absolute_import, division, print_function

import sys
import os
import time

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
        logger.error('error: %r',e)
        raise Exception('JSONRPC communcation did not start.  '
                        'url=%s and passkey=%s'%(url,passkey))
    else:
        if ret != 'e':
            raise Exception('JSONRPC communication error when starting - '
                            'echo failed (%r).  url=%s and passkey=%s'
                            %(ret,url,passkey))

def downloadtask(gridspec):
    """Download a new task from the server"""
    try:
        platform = os.environ['PLATFORM']
    except:
        platform = functions.platform()
    hostname = functions.gethostname()
    ifaces = functions.getInterfaces()
    python_unicode = 'ucs4' if sys.maxunicode == 1114111 else 'ucs2'
    # TODO: add resources like GPUs, high memory, etc
    task = JSONRPC.new_task(gridspec=gridspec, platform=platform,
                            hostname=hostname, ifaces=ifaces,
                            python_unicode=python_unicode)
    if isinstance(task,Exception):
        # an error occurred
        raise task
    if not isinstance(task, dataclasses.Job):
        task = dict_to_dataclasses(task)
    return task

def processing(cfg):
    """Tell the server that we are processing this task"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot update status')
    ret = JSONRPC.set_processing(task=cfg.config['options']['task_id'])
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def finishtask(cfg, stats={}):
    """Finish a task"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"]["task_id"] not specified, '
                        'so cannot finish task')
    if 'DBkill' in cfg.config['options'] and cfg.config['options']['DBkill']:
        return # don't finish task on a DB kill
    outstats = stats
    if 'stats' in cfg.config['options']:
        # filter stats
        stat_keys = set(json_decode(cfg.config['options']['stats']))
        outstats = {k:stats[k] for k in stats if k in stat_keys}
    ret = JSONRPC.finish_task(task=cfg.config['options']['task_id'],stats=outstats)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def stillrunning(cfg):
    """Check if the task should still be running according to the DB"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot finish task')
    ret = JSONRPC.stillrunning(task=cfg.config['options']['task_id'])
    if isinstance(ret,Exception):
        # an error occurred
        raise ret
    if not ret:
        cfg.config['options']['DBkill'] = True
        raise Exception('task should be stopped')

def taskerror(cfg, start_time=None):
    """Tell the server about the error experienced"""
    if 'task_id' not in cfg.config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send error')
    if 'DBkill' in cfg.config['options'] and cfg.config['options']['DBkill']:
        return # don't change status on a DB kill
    try:
        hostname = functions.gethostname()
        if start_time:
            t = time.time() - start_time
        else:
            t = None
        if os.path.exists('stderr'):
            log = json_compressor.compress(open('stderr').read()[-1000:])
        else:
            log = ''
        error_info = {'hostname':hostname, 'time_used': t, 'error_summary':log}
    except Exception:
        logger.warn('failed to collect error info', exc_info=True)
        error_info = None
    ret = JSONRPC.task_error(task=cfg.config['options']['task_id'],
                             error_info=error_info)
    if isinstance(ret,Exception):
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
