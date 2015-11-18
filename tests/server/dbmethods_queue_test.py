"""
Test script for dbmethods.queue
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, cmp_dict

import logging
logger = logging.getLogger('dbmethods_test')

import os, sys, time
import shutil
import tempfile
import random
import stat
import StringIO
from itertools import izip
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import tornado.escape

from flexmock import flexmock

from iceprod.core import functions
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base,DB


class dbmethods_queue_test(dbmethods_base):
    @unittest_reporter
    def test_100_queue_get_site_id(self):
        """Test queue_get_site_id"""
        site_id = 'asdfasdfsdf'

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # normal site test
        tables = {
            'setting':[
                {'site_id':site_id},
            ],
        }

        cb.called = False
        self.mock.setup(tables)

        self._db.queue_get_site_id(callback=cb)

        if cb.called is False:
            raise Exception('normal site: callback not called')
        if cb.ret != site_id:
            raise Exception('normal site: callback ret != site_id')

        # site not in db
        self.mock.setup({'setting':[]})
        cb.called = False

        self._db.queue_get_site_id(callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback ret != Exception')

        # sql error
        self.mock.setup(tables)
        self.mock.failures = 1
        cb.called = False

        self._db.queue_get_site_id(callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_110_queue_get_active_tasks(self):
        """Test queue_get_active_tasks"""
        gridspec = 'klsjdfl.grid1'
        now = dbmethods.nowstr()

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
            ],
        }

        cb.called = False
        self.mock.setup(tables)

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        ret_should_be = {'queued':{'asdf':tables['task'][0]}}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('normal task: callback ret != task')

        # no tasks
        cb.called = False
        self.mock.setup({'task':[],'search':[]})

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != {}:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != {}')

        # several tasks
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'ertert', 'status':'processing', 'prev_status':'queued',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':1, 'depends': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                {'task_id':'gdf', 'job_id':'gew', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                {'task_id':'ertert', 'job_id':'asd', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'processing'},
            ],
        }
        cb.called = False
        self.mock.setup(tables2)

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('several tasks: callback not called')
        ret_should_be = {'queued':{'asdf':tables2['task'][0],
                                   'gdf':tables2['task'][1]},
                         'processing':{'ertert':tables2['task'][2]}
                        }
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks: callback ret != task task2 task3')

        # sql error
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_111_queue_set_task_status(self):
        """Test queue_set_task_status"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
            ],
        }
        cb.called = False
        task = 'asdf'
        status = 'waiting'
        self.mock.setup(tables)

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('single task: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single task: callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != status or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != status):
            logger.info('%r',end_tables)
            raise Exception('set status failed')

        # no task
        cb.called = False
        task = None
        status = 'waiting'
        self.mock.setup({'task':[],'search':[]})

        try:
            self._db.queue_set_task_status(task,status,callback=cb)
        except:
            pass
        else:
            raise Exception('no task: exception not raised')

        if cb.called is not False:
            raise Exception('no task: callback called')

        # multiple tasks (dict)
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'ertert', 'status':'processing', 'prev_status':'queued',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':1, 'depends': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'gdf', 'job_id':'gew', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'ertert', 'job_id':'asd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'processing'},
            ],
        }
        cb.called = False
        task = {'asdf':{},'gdf':{}}
        status = 'waiting'
        self.mock.setup(tables2)

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (dict): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (dict): callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != status or
            end_tables['task'][1]['status'] != status or
            end_tables['task'][2]['status'] != 'processing' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] != now or
            end_tables['search'][0]['task_status'] != status or
            end_tables['search'][1]['task_status'] != status or
            end_tables['search'][2]['task_status'] != 'processing'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (dict): set status failed')

        # multiple tasks (list)
        cb.called = False
        task = ['asdf','gdf']
        status = 'waiting'
        self.mock.setup(tables2)

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (list): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (list): callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != status or
            end_tables['task'][1]['status'] != status or
            end_tables['task'][2]['status'] != 'processing' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] != now or
            end_tables['search'][0]['task_status'] != status or
            end_tables['search'][1]['task_status'] != status or
            end_tables['search'][2]['task_status'] != 'processing'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (list): set status failed')

        # sql error
        cb.called = False
        task = 'asdf'
        status = 'waiting'
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_112_queue_reset_tasks(self):
        """Test queue_reset_tasks"""
        def cb(ret=None):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
            ],
        }
        cb.called = False
        reset = 'asdf'
        self.mock.setup(tables)

        self._db.queue_reset_tasks(reset,callback=cb)

        if cb.called is False:
            raise Exception('single task: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single task: callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'reset'):
            logger.info('%r',end_tables)
            raise Exception('reset failed')

        # single task with fail
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'ertert', 'status':'processing', 'prev_status':'queued',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':1, 'depends': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'gdf', 'job_id':'gew', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'ertert', 'job_id':'asd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'processing'},
            ],
        }
        cb.called = False
        reset = 'asdf'
        fail = 'gdf'
        self.mock.setup(tables2)

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('single task w/fail: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single task w/fail: callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][1]['status'] != 'failed' or
            end_tables['task'][2]['status'] != 'processing' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] != now or
            end_tables['search'][0]['task_status'] != 'reset' or
            end_tables['search'][1]['task_status'] != 'failed' or
            end_tables['search'][2]['task_status'] != 'processing'):
            logger.info('%r',end_tables)
            raise Exception('reset with fail failed')

        # single fail task
        cb.called = False
        fail = 'asdf'
        self.mock.setup(tables)

        self._db.queue_reset_tasks(fail=fail,callback=cb)

        if cb.called is False:
            raise Exception('single fail task: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single fail task: callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != 'failed' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'failed'):
            logger.info('%r',end_tables)
            raise Exception('single task w/fail')

        # multiple tasks (dict)
        cb.called = False
        reset = {'asdf':{},'gdf':{}}
        fail = {'ertert':{}}
        self.mock.setup(tables2)

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (dict): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (dict): callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][1]['status'] != 'reset' or
            end_tables['task'][2]['status'] != 'failed' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][2]['prev_status'] != 'processing' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'reset' or
            end_tables['search'][1]['task_status'] != 'reset' or
            end_tables['search'][2]['task_status'] != 'failed'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (dict): reset with fail failed')

        # multiple tasks (list)
        cb.called = False
        reset = ['asdf','gdf']
        fail = ['ertert']
        self.mock.setup(tables2)

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (list): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (list): callback ret == Exception')
        end_tables = self.mock.get(tables.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][1]['status'] != 'reset' or
            end_tables['task'][2]['status'] != 'failed' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][2]['prev_status'] != 'processing' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'reset' or
            end_tables['search'][1]['task_status'] != 'reset' or
            end_tables['search'][2]['task_status'] != 'failed'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (list): reset with fail failed')

        # sql error in reset
        cb.called = False
        reset = 'asdf'
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_reset_tasks(reset,callback=cb)

        if cb.called is False:
            raise Exception('sql error in reset: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error in reset: callback ret != Exception')

        # sql error in fail
        cb.called = False
        reset = None
        fail = 'asdf'
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('sql error in fail: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error in fail: callback ret != Exception')

    @unittest_reporter
    def test_113_queue_get_task(self):
        """Test queue_get_task"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
            ],
        }
        cb.called = False
        task_id = 'asdf'
        self.mock.setup(tables)

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        ret_should_be = tables['task'][0]
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('normal task: callback ret != task')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

        # no tasks
        cb.called = False
        self.mock.setup({'task':[]})

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != None:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != None')

        # no task_id
        cb.called = False
        task_id = None

        try:
            self._db.queue_get_task(task_id,callback=cb)
        except:
            pass
        else:
            raise Exception('no task_id: exception not raised')

        if cb.called is not False:
            raise Exception('no task_id: callback called, but not supposed to be')

        # several tasks
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'ertert', 'status':'processing', 'prev_status':'queued',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':1, 'depends': '',
                 'task_rel_id':None},
            ],
        }

        cb.called = False
        task_id = [t['task_id'] for t in tables2['task']]
        self.mock.setup(tables2)

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('several tasks: callback not called')
        ret_should_be = {t['task_id']:t for t in tables2['task']}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks: callback ret != task task2 task3')
        end_tables = self.mock.get(tables2.keys())
        if not cmp_dict(tables2,end_tables):
            raise Exception('several tasks: tables were modified')

        # sql error
        cb.called = False
        self.mock.setup({'task':[]})
        self.mock.failures = 1

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_114_queue_get_task_by_grid_queue_id(self):
        """Test queue_get_task_by_grid_queue_id"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
            ],
        }
        cb.called = False
        task_id = 'lkn'
        self.mock.setup(tables)

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        ret_should_be = tables['task'][0]
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('normal task: callback ret != task')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

        # no tasks
        cb.called = False
        self.mock.setup({'task':[]})

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != None:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != None')

        # no task_id
        cb.called = False
        task_id = None
        self.mock.setup(tables)

        try:
            self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)
        except:
            pass
        else:
            raise Exception('no task_id: exception not raised')

        if cb.called is not False:
            raise Exception('no task_id: callback called, but not supposed to be')

        # several tasks
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn2',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
                {'task_id':'ertert', 'status':'processing', 'prev_status':'queued',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn3',
                 'failures':0, 'evictions':1, 'depends': '',
                 'task_rel_id':None},
            ],
        }
        cb.called = False
        task_id = [t['grid_queue_id'] for t in tables2['task']]
        self.mock.setup(tables2)

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('several tasks: callback not called')
        ret_should_be = {t['task_id']:t for t in tables2['task']}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks: callback ret != task task2 task3')
        end_tables = self.mock.get(tables2.keys())
        if not cmp_dict(tables2,end_tables):
            raise Exception('tables were modified')

        # sql error
        cb.called = False
        self.mock.setup(tables2)
        self.mock.failures = 1

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_115_queue_set_submit_dir(self):
        """Test queue_set_submit_dir"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
            ],
        }
        cb.called = False
        task = 'asdf'
        submit_dir = '/test/submit'
        self.mock.setup(tables)

        self._db.queue_set_submit_dir(task,submit_dir,callback=cb)

        if cb.called is False:
            raise Exception('single task: callback not called')
        if cb.ret is not None:
            raise Exception('single task: callback ret != None')
        end_tables = self.mock.get(tables.keys())
        if end_tables['task'][0]['submit_dir'] != submit_dir:
            raise Exception('submit_dir not set')

        # no task
        cb.called = False
        task = None
        self.mock.setup(tables)

        try:
            self._db.queue_set_submit_dir(task,submit_dir,callback=cb)
        except:
            pass
        else:
            raise Exception('no task: exception not raised')

        if cb.called is not False:
            raise Exception('no task: callback called')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

        # sql error
        cb.called = False
        task = 'asdf'
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_set_submit_dir(task,submit_dir,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_119_queue_buffer_jobs_tasks(self):
        """Test queue_buffer_jobs_tasks"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        gridspec = 'msdfiner'
        now = dbmethods.nowstr()
        config_data = """
{"version":3,
 "parent_id":0,
 "tasks":[
    {"name":"task1",
     "trays":[
        {"name":"Corsika",
         "modules":[
            {"name":"generate_corsika",
             "class":"generators.CorsikaIC"
            }
        ]}
    ]}
]}
"""
        logger.info('config data: %s',config_data)
        tables = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
            ],
        }

        # single task
        cb.called = False
        self.mock.setup(tables)

        num = 10
        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 1d,1j,1t: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 1d,1j,1t: exception returned %s'%cb.ret)
        end_tables = self.mock.get(['task','task_rel','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 task')
        if 'task_rel' not in end_tables or len(end_tables['task_rel']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 task_rel')
        if 'search' not in end_tables or len(end_tables['search']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 search')
        if 'job' not in end_tables or len(end_tables['job']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 job')

        # check that it doesn't get resubmitted
        cb.called = False
        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer resubmit: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer resubmit: exception returned %s'%cb.ret)
        end_tables2 = self.mock.get(['task','task_rel','job','search'])
        if not cmp_dict(end_tables,end_tables2):
            logger.info('%r',end_tables)
            logger.info('%r',end_tables2)
            raise Exception('buffer resubmit: tables modified')

        # now try for multiple datasets
        tables2 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':2,
                 'trays':1, 'tasks_submitted':2,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
            ],
            'job':[],'task':[],'task_rel':[],'search':[],
        }
        cb.called = False
        self.mock.setup(tables2)

        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 1d,2j,1t: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 1d,2j,1t: exception returned %s'%cb.ret)
        end_tables = self.mock.get(['task','task_rel','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'task_rel' not in end_tables or len(end_tables['task_rel']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task_rel')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 job')
        return

        # now try for multiple gridspecs and datasets
        tables3 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
                {'dataset_id':'d2', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec+'a',
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
                {'dataset_id':'d2','config_data':config_data,'difplus_data':''},
            ],
            'job':[],'task':[],'task_rel':[],'search':[],
        }
        cb.called = False
        self.mock.setup(tables3)

        num = 10
        self._db.queue_buffer_jobs_tasks([gridspec,gridspec+'a'],num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 2d,1j,1t 2gs: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 2d,1j,1t 2gs: exception returned %s'%cb.ret)
        end_tables = self.mock.get(['task','task_rel','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'task_rel' not in end_tables or len(end_tables['task_rel']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task_rel')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 job')

        # now try with task names
        tables4 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':'{"task1":"'+gridspec+'"}',
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':2,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
            ],
            'job':[],'task':[],'task_rel':[],'search':[],
        }
        cb.called = False
        self.mock.setup(tables4)

        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 1d,1j,1t taskname: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 1d,1j,1t taskname: exception returned %s'%cb.ret)
        end_tables = self.mock.get(['task','task_rel','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'task_rel' not in end_tables or len(end_tables['task_rel']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task_rel')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 job')

        # now make some errors
        for i in range(1,10):
            cb.called = False
            self.mock.setup(tables4)
            self.mock.failures = i
            self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
            if cb.called is False:
                raise Exception('buffer 1d,1j,1t taskname: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('buffer 1d,1j,1t taskname: exception not returned %s'%cb.ret)
            end_tables = self.mock.get(['task','task_rel','job','search'])
            if end_tables:
                logger.info('%r',end_tables)
                raise Exception('tables have changed')

    @unittest_reporter
    def test_120_queue_get_queueing_datasets(self):
        """Test queue_get_queueing_datasets"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        gridspec = 'lksdf.grid1'
        now = dbmethods.nowstr()

        tables = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'idle'},
            ],
        }

        # single dataset
        cb.called = False
        self.mock.setup(tables)

        self._db.queue_get_queueing_datasets(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('single dataset: callback not called')
        expected = {'d1':tables['dataset'][0]}
        if cb.ret != expected:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',expected)
            raise Exception('single dataset: callback ret != dataset')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # no dataset
        cb.called = False
        self.mock.setup({'dataset':[],'search':[]})

        self._db.queue_get_queueing_datasets(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != {}:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no dataset: callback ret != {}')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # sql error
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_get_queueing_datasets(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

    @unittest_reporter
    def test_121_queue_get_queueing_tasks(self):
        """Test queue_get_queueing_tasks"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        gridspec = 'ggg.g1'
        now = dbmethods.nowstr()
        tables = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t2', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t3', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t4', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'task':[
                {'task_id':'t1', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':'tr1'},
                {'task_id':'t2', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':'tr2'},
                {'task_id':'t3', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':'tr3'},
                {'task_id':'t4', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':'tr4'},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','depends':None,'requirements':None},
                {'task_rel_id':'tr2','depends':None,'requirements':None},
                {'task_rel_id':'tr3','depends':None,'requirements':None},
                {'task_rel_id':'tr4','depends':None,'requirements':None},
            ],
        }

        # single dataset
        cb.called = False
        dataset_prios = {'d1':1}
        self.mock.setup(tables)

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,1,callback=cb)

        if cb.called is False:
            raise Exception('single dataset: callback not called')
        ret_should_be = {'t1':dict(tables['search'][0])}
        ret_should_be['t1']['debug'] = tables['dataset'][0]['debug']
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('single dataset: callback ret != task')

        # no tasks
        cb.called = False
        self.mock.setup({'search':[],'task':[]})

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,1,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        ret_should_be = {}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('no task: callback ret != {}')

        # no tasks sql error
        cb.called = False
        self.mock.setup({'search':[],'task':[]})
        self.mock.failures = 1

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,1,callback=cb)

        if cb.called is False:
            raise Exception('_db_read error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('_db_read error: callback ret != Exception')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # no dataset_prios
        cb.called = False
        self.mock.setup(tables)

        try:
            self._db.queue_get_queueing_tasks(None,gridspec,1,callback=cb)
        except:
            pass
        else:
            raise Exception('no dataset_prios: exception not raised')

        if cb.called is not False:
            raise Exception('no dataset_prios: callback called, but not supposed to be')

        # no callback
        cb.called = False
        self.mock.setup(tables)

        try:
            self._db.queue_get_queueing_tasks(dataset_prios,gridspec,1)
        except:
            pass
        else:
            raise Exception('no callback: exception not raised')

        if cb.called is not False:
            raise Exception('no callback: callback called, but not supposed to be')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # several tasks in same dataset
        cb.called = False
        self.mock.setup(tables)

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,3,callback=cb)

        if cb.called is False:
            raise Exception('several tasks in same dataset: callback not called')
        ret_should_be = {x['task_id']:dict(x) for x in tables['search'][:3]}
        for k in ret_should_be:
            ret_should_be[k]['debug'] = tables['dataset'][0]['debug']
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks in same dataset: callback ret != task task2 task3')

        # several tasks in diff dataset
        tables2 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
                {'dataset_id':'d2', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t2', 'job_id': 'j1', 'dataset_id': 'd2',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t3', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t4', 'job_id': 'j1', 'dataset_id': 'd2',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'task':tables['task'],
            'task_rel':tables['task_rel'],
        }
        cb.called = False
        dataset_prios = {'d1':1.1,'d2':1}
        self.mock.setup(tables2)

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,3,callback=cb)

        if cb.called is False:
            raise Exception('several tasks in diff dataset: callback not called')
        ret_should_be = {x['task_id']:dict(x) for x in tables2['search'] if x['task_id'] != 't4'}
        for k in ret_should_be:
            ret_should_be[k]['debug'] = tables['dataset'][0]['debug']
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks in diff dataset: callback ret != task task2 task3')

        # priority weighted towards one dataset
        cb.called = False
        dataset_prios = {'d1':.2,'d2':.8}
        self.mock.setup(tables2)

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,3,callback=cb)

        if cb.called is False:
            raise Exception('priority weighting dataset: callback not called')
        ret_should_be = {x['task_id']:dict(x) for x in tables2['search'] if x['task_id'] != 't3'}
        for k in ret_should_be:
            ret_should_be[k]['debug'] = tables['dataset'][0]['debug']
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('priority weighting dataset: callback ret != task2 task3 task4')

        # testing dependencies
        tables3 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'complete'},
                {'task_id':'t2', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'processing'},
                {'task_id':'t3', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t4', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t5', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t6', 'job_id': 'j2', 'dataset_id': 'd2',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t7', 'job_id': 'j3', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'task':[
                {'task_id':'t1', 'status':'complete', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':'tr1'},
                {'task_id':'t2', 'status':'processing', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': 't1',
                 'task_rel_id':'tr2'},
                {'task_id':'t3', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': 't1,t2',
                 'task_rel_id':'tr3'},
                {'task_id':'t4', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': 't25',
                 'task_rel_id':'tr4'},
                {'task_id':'t5', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': 't6',
                 'task_rel_id':'tr5'},
                {'task_id':'t7', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': 'unknown',
                 'task_rel_id':'tr1'},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','depends':None,'requirements':None},
                {'task_rel_id':'tr2','depends':'tr1','requirements':None},
                {'task_rel_id':'tr3','depends':'tr2','requirements':None},
                {'task_rel_id':'tr4','depends':'tr25','requirements':None},
                {'task_rel_id':'tr5','depends':'tr6','requirements':None},
            ],
        }
        cb.called = False
        dataset_prios = {'d1':1}
        self.mock.setup(tables3)

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,3,callback=cb)

        if cb.called is False:
            raise Exception('dependencies: callback not called')
        ret_should_be = {}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('dependencies: callback ret != {}')

        # testing resources
        tables4 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'trays':1, 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'parent_id':'sdf', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'task':[
                {'task_id':'t1', 'status':'waiting', 'prev_status':'idle',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'failures':0, 'evictions':0, 'depends': None,
                 'task_rel_id':'tr1'},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','depends':None,'requirements':'["cpu","gpu"]'},
            ],
        }
        cb.called = False
        dataset_prios = {'d1':1}
        self.mock.setup(tables4)
        resources = {'cpu':200,'gpu':10}

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,3,
                                          resources=resources,callback=cb)

        if cb.called is False:
            raise Exception('resources: callback not called')
        ret_should_be = {x['task_id']:dict(x) for x in tables4['search']}
        for k in ret_should_be:
            ret_should_be[k]['debug'] = tables['dataset'][0]['debug']
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('resources: callback ret')

        # resources no match
        cb.called = False
        self.mock.setup(tables4)
        resources = {}

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,3,
                                          resources=resources,callback=cb)

        if cb.called is False:
            raise Exception('no resources: callback not called')
        ret_should_be = {}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('no resources: callback ret != {}')

        # bad resource json
        tables4['task_rel'][0]['requirements'] = 'blah'
        cb.called = False
        self.mock.setup(tables4)
        resources = {'cpu':200}

        self._db.queue_get_queueing_tasks(dataset_prios,gridspec,3,
                                          resources=resources,callback=cb)

        if cb.called is False:
            raise Exception('resources bad json: callback not called')
        ret_should_be = {x['task_id']:dict(x) for x in tables4['search']}
        for k in ret_should_be:
            ret_should_be[k]['debug'] = tables['dataset'][0]['debug']
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('resources: callback ret')

    @unittest_reporter
    def test_130_queue_get_cfg_for_task(self):
        """Test queue_get_cfg_for_task"""
        def cb(ret):
            cb.called = True
            cb.ret = ret

        config_data = """
{"version":3,
 "parent_id":0,
 "tasks":[
    {"name":"task1",
     "trays":[
        {"name":"Corsika",
         "modules":[
            {"name":"generate_corsika",
             "class":"generators.CorsikaIC"
            }
        ]}
    ]}
]}
"""
        tables = {
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': 'grid', 'name': '1', 'task_status': 'waiting'},
            ],
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
            ],
        }

        cb.called = False
        self.mock.setup(tables)

        self._db.queue_get_cfg_for_task('t1',callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if cb.ret != config_data:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return config')

        # sql error
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_get_cfg_for_task('t1',callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.info('ret: %r',cb.ret)
            raise Exception('did not raise exception')

        # bad task
        self._db.queue_get_cfg_for_task('t32',callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.info('ret: %r',cb.ret)
            raise Exception('did not raise exception')

        # bad task_id
        try:
            self._db.queue_get_cfg_for_task(None,callback=cb)
        except Exception:
            pass
        else:
            raise Exception('did not raise exception')

    @unittest_reporter
    def test_131_queue_get_cfg_for_dataset(self):
        """Test queue_get_cfg_for_dataset"""
        def cb(ret):
            cb.called = True
            cb.ret = ret

        config_data = """
{"version":3,
 "parent_id":0,
 "tasks":[
    {"name":"task1",
     "trays":[
        {"name":"Corsika",
         "modules":[
            {"name":"generate_corsika",
             "class":"generators.CorsikaIC"
            }
        ]}
    ]}
]}
"""
        tables = {
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
            ],
        }

        cb.called = False
        self.mock.setup(tables)

        self._db.queue_get_cfg_for_dataset('d1',callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if cb.ret != config_data:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return config')

        # sql error
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.queue_get_cfg_for_dataset('d1',callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.info('ret: %r',cb.ret)
            raise Exception('did not raise exception')

        # bad dataset
        self._db.queue_get_cfg_for_dataset('d32',callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.info('ret: %r',cb.ret)
            raise Exception('did not raise exception')

        # bad dataset_id
        try:
            self._db.queue_get_cfg_for_dataset(None,callback=cb)
        except Exception:
            pass
        else:
            raise Exception('did not raise exception')

    @unittest_reporter
    def test_150_queue_set_site_queues(self):
        """Test queue_set_site_queues"""
        def cb(ret):
            cb.called = True
            cb.ret = ret

        now = dbmethods.nowstr()
        tables = {
            'site':[
                {'site_id':'s1','name':'n','institution':'inst',
                 'queues':'{}','auth_key':None,'website_url':'',
                 'version':'2','last_update':now,'admin_name':'',
                 'admin_email':''},
            ],
        }

        cb.called = False
        self.mock.setup({})
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}

        self._db.queue_set_site_queues('s0',queues,callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if cb.ret is not True:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return True')
        endtables = self.mock.get(['site'])
        if ((not endtables['site']) or
            endtables['site'][0]['site_id'] != 's0'):
            raise Exception('did not set site')
        expected = json_encode(queues)
        if endtables['site'][0]['queues'] != expected:
            logger.info('expected: %r',expected)
            logger.info('received: %r',endtables['site'][0]['queues'])
            raise Exception('did not set queues')

        # update no queue
        cb.called = False
        self.mock.setup(tables)
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}

        self._db.queue_set_site_queues('s1',queues,callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if cb.ret is not True:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return True')
        endtables = self.mock.get(['site'])
        if ((not endtables['site']) or
            endtables['site'][0]['site_id'] != 's1'):
            raise Exception('did not set site')
        expected = json_encode(queues)
        if endtables['site'][0]['queues'] != expected:
            logger.info('expected: %r',expected)
            logger.info('received: %r',endtables['site'][0]['queues'])
            raise Exception('did not set queues')

        # update existing
        tables2 = {
            'site':[
                {'site_id':'s1','name':'n','institution':'inst',
                 'queues':'{"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}','auth_key':None,'website_url':'',
                 'version':'2','last_update':now,'admin_name':'',
                 'admin_email':''},
            ],
        }
        cb.called = False
        self.mock.setup(tables2)
        queues = {"g1":{"type":"b","description":"desc","resources":{"disk":[20,10]}}}

        self._db.queue_set_site_queues('s1',queues,callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if cb.ret is not True:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return True')
        endtables = self.mock.get(['site'])
        if ((not endtables['site']) or
            endtables['site'][0]['site_id'] != 's1'):
            raise Exception('did not set site')
        expected = json_encode({"g1":{"type":"b","description":"desc","resources":{"mem":[20,10],"disk":[20,10]}}})
        if endtables['site'][0]['queues'] != expected:
            logger.info('expected: %r',expected)
            logger.info('received: %r',endtables['site'][0]['queues'])
            raise Exception('did not set queues')

        # bad queue db info
        tables3 = {
            'site':[
                {'site_id':'s0','name':'n','institution':'inst',
                 'queues':'garbage','auth_key':None,'website_url':'',
                 'version':'2','last_update':now,'admin_name':'',
                 'admin_email':''},
            ],
        }
        cb.called = False
        self.mock.setup(tables3)
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}

        self._db.queue_set_site_queues('s0',queues,callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return Exception')
        endtables = self.mock.get(['site'])
        if not cmp_dict(tables3,endtables):
            raise Exception('tables modified')

        # bad queue insert info
        cb.called = False
        self.mock.setup({'site':[]})
        queues = lambda a:a+1 # something that can't be json

        self._db.queue_set_site_queues('s0',queues,callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return Exception')
        endtables = self.mock.get(['site'])
        if not cmp_dict(tables3,endtables):
            raise Exception('tables modified')

        # sql error
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}
        for i in range(1,3):
            cb.called = False
            self.mock.setup({'site':[]})
            self.mock.failures = i
            logger.info('failure: %d',i)

            self._db.queue_set_site_queues('s0',queues,callback=cb)
            if cb.called is False:
                raise Exception('callback not called')
            if not isinstance(cb.ret,Exception):
                logger.info('ret: %r',cb.ret)
                raise Exception('did not raise exception')
            endtables = self.mock.get(['site'])
            if endtables['site']:
                raise Exception('tables modified')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_queue_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_queue_test))
    return suite
