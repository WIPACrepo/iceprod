"""
Test script for dbmethods.queue
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

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
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base,DB


class dbmethods_queue_test(dbmethods_base):
    @unittest_reporter
    def test_100_queue_get_site_id(self):
        """Test queue_get_site_id"""
        raise Exception('fixme')
        site_id = 'asdfasdfsdf'

        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # normal site test
        sql_read_task.ret = [[site_id]]

        self._db.queue_get_site_id(callback=cb)

        if cb.called is False:
            raise Exception('normal site: callback not called')
        if cb.ret != site_id:
            raise Exception('normal site: callback ret != site_id')

        # site not in db
        sql_read_task.ret = []
        cb.called = False

        self._db.queue_get_site_id(callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback ret != Exception')

        # sql error
        sql_read_task.ret = Exception('sql error')
        cb.called = False

        self._db.queue_get_site_id(callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_110_queue_get_active_tasks(self):
        """Test queue_get_active_tasks"""
        raise Exception('fixme')
        task = OrderedDict([('task_id','asdf'),
                ('status','queued'),
                ('prev_status','waiting'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',0),
                ('task_rel_id',None),
               ])
        task2 = OrderedDict([('task_id','gdf'),
                ('status','queued'),
                ('prev_status','waiting'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',0),
                ('task_rel_id',None),
               ])
        task3 = OrderedDict([('task_id','ertert'),
                ('status','processing'),
                ('prev_status','queued'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',1),
                ('task_rel_id',None),
               ])
        gridspec = 'klsjdfl.grid1'
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        cb.called = False
        sql_read_task.ret = [task.values()]

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        ret_should_be = {task['status']:{task['task_id']:task}}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('normal task: callback ret != task')
        if not sql_read_task.sql.startswith('select task.* from search join task on search.task_id = task.task_id '):
            raise Exception('normal task: sql incorrect')

        # no tasks
        cb.called = False
        sql_read_task.ret = []

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != {}:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != {}')
        if not sql_read_task.sql.startswith('select task.* from search join task on search.task_id = task.task_id '):
            raise Exception('no task: sql incorrect')

        # several tasks
        cb.called = False
        sql_read_task.ret = [task.values(),task2.values(),task3.values()]

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('several tasks: callback not called')
        ret_should_be = {task['status']:{task['task_id']:task,
                                         task2['task_id']:task2},
                         task3['status']:{task3['task_id']:task3}}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks: callback ret != task task2 task3')
        if not sql_read_task.sql.startswith('select task.* from search join task on search.task_id = task.task_id '):
            raise Exception('several tasks: sql incorrect')

        # sql error
        sql_read_task.ret = Exception('sql error')
        cb.called = False

        self._db.queue_get_active_tasks(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_111_queue_set_task_status(self):
        """Test queue_set_task_status"""
        raise Exception('fixme')
        def _db_write(conn,sql,bindings,*args):
            def w(s,b):
                _db_write.sql.append(s)
                _db_write.bindings.append(b)
                if b[0] in _db_write.task_ret:
                    return True
                else:
                    raise Exception('sql error')
            if isinstance(sql,basestring):
                return w(sql,bindings)
            elif isinstance(sql,Iterable):
                ret = None
                for s,b in izip(sql,bindings):
                    ret = w(s,b)
                return ret
        flexmock(DB).should_receive('_db_write').replace_with(_db_write)
        def non_blocking_task(cb):
            non_blocking_task.called = True
            cb()
        flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('waiting')
        cb.called = False
        task = 'asfsd'
        status = 'waiting'

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('single task: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single task: callback ret == Exception')
        if (len(_db_write.bindings) != 2 or
            _db_write.bindings[0] != (status,task) or
            _db_write.bindings[1][0] != status or
            _db_write.bindings[1][-1] != task):
            logger.info('sql bindings: %r',_db_write.bindings)
            raise Exception('single task: sql bindings != (status,task_id)')

        # no task
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('waiting')
        cb.called = False
        task = None
        status = 'waiting'

        try:
            self._db.queue_set_task_status(task,status,callback=cb)
        except:
            pass
        else:
            raise Exception('no task: exception not raised')

        if cb.called is not False:
            raise Exception('no task: callback called')

        # multiple tasks (dict)
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('waiting')
        cb.called = False
        task = OrderedDict([('asfsd',{}),('gsdf',{})])
        status = 'waiting'

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (dict): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (dict): callback ret == Exception')
        expected = [(status,'asfsd','gsdf'),(status,'asfsd','gsdf')]
        if (len(_db_write.bindings) != 2 or
            _db_write.bindings[0] != expected[0] or
            _db_write.bindings[1][0] != expected[1][0] or
            _db_write.bindings[1][2:] != expected[1][1:]):
            logger.info('sql bindings: %r',_db_write.bindings)
            logger.info('expected bindings: %r',expected)
            raise Exception('multiple tasks (dict): sql bindings incorrect')

        # multiple tasks (list)
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('waiting')
        cb.called = False
        task = ['asfsd','gsdf']
        status = 'waiting'

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (list): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (list): callback ret == Exception')
        expected = [(status,'asfsd','gsdf'),(status,'asfsd','gsdf')]
        if (len(_db_write.bindings) != 2 or
            _db_write.bindings[0] != expected[0] or
            _db_write.bindings[1][0] != expected[1][0] or
            _db_write.bindings[1][2:] != expected[1][1:]):
            logger.info('sql bindings: %r',_db_write.bindings)
            logger.info('expected bindings: %r',expected)
            raise Exception('multiple tasks (list): sql bindings incorrect')

        # sql error
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = {}
        cb.called = False
        task = 'asfsd'
        status = 'waiting'

        self._db.queue_set_task_status(task,status,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_112_queue_reset_tasks(self):
        """Test queue_reset_tasks"""
        raise Exception('fixme')
        def _db_write(conn,sql,bindings,*args):
            def w(s,b):
                _db_write.sql.append(s)
                _db_write.bindings.append(b)
                if b[0] in _db_write.task_ret:
                    return True
                else:
                    raise Exception('sql error')
            if isinstance(sql,basestring):
                return w(sql,bindings)
            elif isinstance(sql,Iterable):
                ret = None
                for s,b in izip(sql,bindings):
                    ret = w(s,b)
                return ret
        flexmock(DB).should_receive('_db_write').replace_with(_db_write)
        def non_blocking_task(cb):
            non_blocking_task.called = True
            cb()
        flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)

        def cb(ret=None):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset','failed')
        cb.called = False
        reset = 'asfsd'

        self._db.queue_reset_tasks(reset,callback=cb)

        if cb.called is False:
            raise Exception('single task: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single task: callback ret == Exception')
        expected = [('reset',reset),('reset',reset)]
        if (len(_db_write.bindings) != 2 or
            _db_write.bindings[0] != expected[0] or
            _db_write.bindings[1][0] != expected[1][0] or
            _db_write.bindings[1][2:] != expected[1][1:]):
            logger.info('sql bindings: %r',_db_write.bindings)
            raise Exception('single task: sql bindings != (reset,task_id)')

        # single task with fail
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset','failed')
        cb.called = False
        reset = 'asfsd'
        fail = 'sdfsdf'

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('single task w/fail: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single task w/fail: callback ret == Exception')
        expected = [('reset',reset),('reset',reset),
                    ('failed',fail),('failed',fail)]
        if (len(_db_write.bindings) != 4 or
            _db_write.bindings[0] != expected[0] or
            _db_write.bindings[1][0] != expected[1][0] or
            _db_write.bindings[1][2:] != expected[1][1:] or
            _db_write.bindings[2] != expected[2] or
            _db_write.bindings[3][0] != expected[3][0] or
            _db_write.bindings[3][2:] != expected[3][1:]):
            logger.info('sql bindings: %r',_db_write.bindings)
            logger.info('expected bindings: %r',expected)
            raise Exception('single task w/fail: sql bindings incorrect')

        # single fail task
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset','failed')
        cb.called = False
        fail = 'sdfsdf'

        self._db.queue_reset_tasks(fail=fail,callback=cb)

        if cb.called is False:
            raise Exception('single fail task: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('single fail task: callback ret == Exception')
        expected = [('failed',fail),('failed',fail)]
        if (len(_db_write.bindings) != 2 or
            _db_write.bindings[0] != expected[0] or
            _db_write.bindings[1][0] != expected[1][0] or
            _db_write.bindings[1][2:] != expected[1][1:]):
            logger.info('sql bindings: %r',_db_write.bindings)
            logger.info('expected bindings: %r',expected)
            raise Exception('single task w/fail: sql bindings incorrect')

        # multiple tasks (dict)
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset','failed')
        cb.called = False
        reset = OrderedDict([('asfsd',{}),('gsdf',{})])
        fail = OrderedDict([('asfsd',{}),('gsdf',{})])

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (dict): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (dict): callback ret == Exception')
        expected = [('reset','asfsd','gsdf'),('reset','asfsd','gsdf'),
                    ('failed','asfsd','gsdf'),('failed','asfsd','gsdf')]
        if (len(_db_write.bindings) != 4 or
            _db_write.bindings[0] != expected[0] or
            _db_write.bindings[1][0] != expected[1][0] or
            _db_write.bindings[1][2:] != expected[1][1:] or
            _db_write.bindings[2] != expected[2] or
            _db_write.bindings[3][0] != expected[3][0] or
            _db_write.bindings[3][2:] != expected[3][1:]):
            logger.info('sql bindings: %r',_db_write.bindings)
            logger.info('expected bindings: %r',expected)
            raise Exception('multiple tasks (dict): sql bindings incorrect')

        # multiple tasks (list)
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset','failed')
        cb.called = False
        reset = ['asfsd','gsdf']
        fail = ['asfsd','gsdf']

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('multiple tasks (list): callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('%r',cb.ret)
            raise Exception('multiple tasks (list): callback ret == Exception')
        expected = [('reset','asfsd','gsdf'),('reset','asfsd','gsdf'),
                    ('failed','asfsd','gsdf'),('failed','asfsd','gsdf')]
        if (len(_db_write.bindings) != 4 or
            _db_write.bindings[0] != expected[0] or
            _db_write.bindings[1][0] != expected[1][0] or
            _db_write.bindings[1][2:] != expected[1][1:] or
            _db_write.bindings[2] != expected[2] or
            _db_write.bindings[3][0] != expected[3][0] or
            _db_write.bindings[3][2:] != expected[3][1:]):
            logger.info('sql bindings: %r',_db_write.bindings)
            logger.info('expected bindings: %r',expected)
            raise Exception('multiple tasks (list): sql bindings incorrect')

        # sql error in reset
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('failed',)
        cb.called = False
        reset = 'asfsd'

        self._db.queue_reset_tasks(reset,callback=cb)

        if cb.called is False:
            raise Exception('sql error in reset: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error in reset: callback ret != Exception')

        # sql error in fail
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset',)
        cb.called = False
        reset = 'asfsd'
        fail = 'kljsdf'

        self._db.queue_reset_tasks(reset,fail,callback=cb)

        if cb.called is False:
            raise Exception('sql error in fail: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error in fail: callback ret != Exception')

    @unittest_reporter
    def test_113_queue_get_task(self):
        """Test queue_get_task"""
        raise Exception('fixme')
        task = OrderedDict([('task_id','asdf'),
                ('status','queued'),
                ('prev_status','waiting'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',0),
                ('task_rel_id',None),
               ])
        task2 = OrderedDict([('task_id','gdf'),
                ('status','queued'),
                ('prev_status','waiting'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',0),
                ('task_rel_id',None),
               ])
        task3 = OrderedDict([('task_id','ertert'),
                ('status','processing'),
                ('prev_status','queued'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',1),
                ('task_rel_id',None),
               ])
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        cb.called = False
        sql_read_task.ret = [task.values()]
        task_id = task['task_id']

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        ret_should_be = task
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('normal task: callback ret != task')
        if not sql_read_task.sql.startswith('select * from task where task_id ='):
            raise Exception('normal task: sql incorrect')

        # no tasks
        cb.called = False
        sql_read_task.ret = []
        task_id = task['task_id']

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != None:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != None')
        if not sql_read_task.sql.startswith('select * from task where task_id ='):
            raise Exception('no task: sql incorrect')

        # no tasks sql issue
        cb.called = False
        sql_read_task.ret = None
        task_id = task['task_id']

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != Exception')
        if not sql_read_task.sql.startswith('select * from task where task_id ='):
            raise Exception('no task: sql incorrect')

        # no task_id
        cb.called = False
        sql_read_task.ret = []
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
        cb.called = False
        sql_read_task.ret = [task.values(),task2.values(),task3.values()]
        task_id = [task['task_id'],task2['task_id'],task3['task_id']]

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('several tasks: callback not called')
        ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks: callback ret != task task2 task3')
        if not sql_read_task.sql.startswith('select * from task where task_id in'):
            raise Exception('several tasks: sql incorrect')

        # sql error
        cb.called = False
        sql_read_task.ret = Exception('sql error')
        task_id = task['task_id']

        self._db.queue_get_task(task_id,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_114_queue_get_task_by_grid_queue_id(self):
        """Test queue_get_task_by_grid_queue_id"""
        raise Exception('fixme')
        task = OrderedDict([('task_id','asdf'),
                ('status','queued'),
                ('prev_status','waiting'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',0),
                ('task_rel_id',None),
               ])
        task2 = OrderedDict([('task_id','gdf'),
                ('status','queued'),
                ('prev_status','waiting'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',0),
                ('task_rel_id',None),
               ])
        task3 = OrderedDict([('task_id','ertert'),
                ('status','processing'),
                ('prev_status','queued'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',self.test_dir),
                ('grid_queue_id','lkn'),
                ('failures',0),
                ('evictions',1),
                ('task_rel_id',None),
               ])
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        cb.called = False
        sql_read_task.ret = [task.values()]
        task_id = task['grid_queue_id']

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        ret_should_be = task
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('normal task: callback ret != task')
        if not sql_read_task.sql.startswith('select * from task where grid_queue_id ='):
            raise Exception('normal task: sql incorrect')

        # no tasks
        cb.called = False
        sql_read_task.ret = []
        task_id = task['grid_queue_id']

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != None:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != None')
        if not sql_read_task.sql.startswith('select * from task where grid_queue_id ='):
            raise Exception('no task: sql incorrect')

        # no tasks sql issue
        cb.called = False
        sql_read_task.ret = None
        task_id = task['grid_queue_id']

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no task: callback ret != Exception')
        if not sql_read_task.sql.startswith('select * from task where grid_queue_id ='):
            raise Exception('no task: sql incorrect')

        # no task_id
        cb.called = False
        sql_read_task.ret = []
        task_id = None

        try:
            self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)
        except:
            pass
        else:
            raise Exception('no task_id: exception not raised')

        if cb.called is not False:
            raise Exception('no task_id: callback called, but not supposed to be')

        # several tasks
        cb.called = False
        sql_read_task.ret = [task.values(),task2.values(),task3.values()]
        task_id = [task['grid_queue_id'],task2['grid_queue_id'],task3['grid_queue_id']]

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('several tasks: callback not called')
        ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks: callback ret != task task2 task3')
        if not sql_read_task.sql.startswith('select * from task where grid_queue_id in'):
            raise Exception('several tasks: sql incorrect')

        # sql error
        cb.called = False
        sql_read_task.ret = Exception('sql error')
        task_id = task['grid_queue_id']

        self._db.queue_get_task_by_grid_queue_id(task_id,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_115_queue_set_submit_dir(self):
        """Test queue_set_submit_dir"""
        raise Exception('fixme')
        def sql_write_task(sql,bindings,callback):
            sql_write_task.sql = sql
            sql_write_task.bindings = bindings
            callback(sql_write_task.ret)
        flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single task
        sql_write_task.ret = None
        cb.called = False
        task = 'asfsd'
        submit_dir = 'waiting'

        self._db.queue_set_submit_dir(task,submit_dir,callback=cb)

        if cb.called is False:
            raise Exception('single task: callback not called')
        if cb.ret is not None:
            raise Exception('single task: callback ret != None')
        if sql_write_task.bindings != (submit_dir,task):
            logger.info('sql bindings: %r',sql_write_task.bindings)
            raise Exception('single task: sql bindings != (status,status,task_id)')

        # no task
        sql_write_task.ret = None
        cb.called = False
        task = None
        submit_dir = 'waiting1'

        try:
            self._db.queue_set_submit_dir(task,submit_dir,callback=cb)
        except:
            pass
        else:
            raise Exception('no task: exception not raised')

        if cb.called is not False:
            raise Exception('no task: callback called')

        # sql error
        sql_write_task.ret = Exception('sql error')
        cb.called = False
        task = 'asfsd'
        submit_dir = 'waiting2'

        self._db.queue_set_submit_dir(task,submit_dir,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_119_queue_buffer_jobs_tasks(self):
        """Test queue_buffer_jobs_tasks"""
        raise Exception('fixme')
        def non_blocking_task(cb):
            non_blocking_task.called = True
            cb()
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            if bindings[0] in sql_read_task.task_ret:
                callback(sql_read_task.task_ret[bindings[0]])
            else:
                callback(Exception('sql error'))
        def _db_read(conn,sql,bindings,*args):
            _db_read.sql = sql
            _db_read.bindings = bindings
            if bindings[0] in _db_read.task_ret:
                return _db_read.task_ret[bindings[0]]
            else:
                raise Exception('sql error')
        def _db_write(conn,sql,bindings,*args):
            _db_write.sql.append(sql)
            _db_write.bindings.append(bindings)
            if _db_write.task_ret:
                return True
            else:
                raise Exception('sql error')
        def increment_id(table,conn=None):
            increment_id.table = table
            if table in increment_id.ret:
                return increment_id.ret[table]
            else:
                raise Exception('sql error')
        flexmock(DB).should_receive('_increment_id_helper').replace_with(increment_id)
        flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
        flexmock(DB).should_receive('_db_read').replace_with(_db_read)
        flexmock(DB).should_receive('_db_write').replace_with(_db_write)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        gridspec = 'msdfiner'
        now = datetime.utcnow()
        dataset = OrderedDict([('dataset_id','lknser834'),
                               ('name','test dataset'),
                               ('description','a simple test'),
                               ('gridspec',gridspec),
                               ('status','processing'),
                               ('username','user'),
                               ('institution','inst'),
                               ('submit_host','localhost'),
                               ('priority',0),
                               ('jobs_submitted',2),
                               ('trays',1),
                               ('tasks_submitted',2),
                               ('start_date',dbmethods.datetime2str(now)),
                               ('end_date',''),
                               ('temporary_storage',''),
                               ('global_storage',''),
                               ('parent_id','sdf'),
                               ('stat_keys','[]'),
                               ('categoryvalue_ids',''),
                               ('debug',True),
                              ])
        search = OrderedDict([('task_id','gdf'),
                ('job_id','3ns8'),
                ('dataset_id','lknser834'),
                ('gridspec','nsd89n3'),
                ('name','0'),
                ('task_status','queued'),
               ])
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

        # return values for first two callbacks
        sql_read_task.task_ret = {
            gridspec:
                [[dataset['dataset_id'],
                  dataset['status'],
                  dataset['gridspec'],
                  dataset['jobs_submitted'],
                  dataset['tasks_submitted']
                ]],
            dataset['dataset_id']:
                [[search['dataset_id'],
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ]]
        }
        # return values for blocking
        _db_read.task_ret = {
            dataset['dataset_id']:[[dataset['dataset_id'],config_data]]
        }
        increment_id.ret = {'job':'newjob',
                            'task':'newtask',
                           }
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = True
        cb.called = False

        num = 10
        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 1d,1j,1t: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 1d,1j,1t: exception returned %s'%cb.ret)

        # now try for multiple datasets
        # return values for first two callbacks
        sql_read_task.task_ret = {
            gridspec:
                [[dataset['dataset_id'],
                  dataset['status'],
                  dataset['gridspec'],
                  dataset['jobs_submitted'],
                  dataset['tasks_submitted']
                ],
                 [dataset['dataset_id']+'l',
                  dataset['status'],
                  dataset['gridspec'],
                  dataset['jobs_submitted'],
                  dataset['tasks_submitted']
                ]],
            dataset['dataset_id']:
                [[search['dataset_id'],
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ],
                 [search['dataset_id']+'l',
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ]],
            dataset['dataset_id']+'l':
                [[search['dataset_id'],
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ],
                 [search['dataset_id']+'l',
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ]]
        }
        # return values for blocking
        _db_read.task_ret = {
            dataset['dataset_id']:
                [[dataset['dataset_id'],config_data],
                 [dataset['dataset_id']+'l',config_data]],
            dataset['dataset_id']+'l':
                [[dataset['dataset_id'],config_data],
                 [dataset['dataset_id']+'l',config_data]]
        }
        increment_id.ret = {'job':'newjob',
                            'task':'newtask',
                           }
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = True
        cb.called = False

        num = 10
        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 2d,1j,1t: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 2d,1j,1t: exception returned %s'%cb.ret)


        # now try for multiple gridspecs and datasets
        # return values for first two callbacks
        sql_read_task.task_ret = {
            '%'+gridspec+'%':
                [[dataset['dataset_id'],
                  dataset['status'],
                  dataset['gridspec'],
                  dataset['jobs_submitted'],
                  dataset['tasks_submitted']
                ],
                 [dataset['dataset_id']+'l',
                  dataset['status'],
                  dataset['gridspec']+'a',
                  dataset['jobs_submitted'],
                  dataset['tasks_submitted']
                ]],
            dataset['dataset_id']:
                [[search['dataset_id'],
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ]],
            dataset['dataset_id']+'l':
                [[search['dataset_id'],
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ]]
        }
        # return values for blocking
        _db_read.task_ret = {
            dataset['dataset_id']:
                [[dataset['dataset_id'],config_data],
                 [dataset['dataset_id']+'l',config_data]],
            dataset['dataset_id']+'l':
                [[dataset['dataset_id'],config_data],
                 [dataset['dataset_id']+'l',config_data]]
        }
        increment_id.ret = {'job':'newjob',
                            'task':'newtask',
                           }
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = True
        cb.called = False

        num = 10
        self._db.queue_buffer_jobs_tasks([gridspec,gridspec+'a'],num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 2d,1j,1t 2gs: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 2d,1j,1t 2gs: exception returned %s'%cb.ret)
        if len(_db_write.sql) != 6:
            for s,b in zip(_db_write.sql,_db_write.bindings):
                logger.info('%s',s)
                logger.info('%r',b)
            raise Exception('buffer 2d,1j,1t 2gs: not enough jobs queued')

        # now try with task names
        gridspec = 'msdfiner'
        now = datetime.utcnow()
        dataset = OrderedDict([('dataset_id','lknser834'),
                               ('name','test dataset'),
                               ('description','a simple test'),
                               ('gridspec','{"task1":"'+gridspec+'"}'),
                               ('status','processing'),
                               ('username','user'),
                               ('institution','inst'),
                               ('submit_host','localhost'),
                               ('priority',0),
                               ('jobs_submitted',2),
                               ('trays',1),
                               ('tasks_submitted',2),
                               ('start_date',dbmethods.datetime2str(now)),
                               ('end_date',''),
                               ('temporary_storage',''),
                               ('global_storage',''),
                               ('parent_id','sdf'),
                               ('stat_keys','[]'),
                               ('categoryvalue_ids',''),
                               ('debug',True),
                              ])
        search = OrderedDict([('task_id','gdf'),
                ('job_id','3ns8'),
                ('dataset_id','lknser834'),
                ('gridspec','nsd89n3'),
                ('name','0'),
                ('task_status','queued'),
               ])

        # return values for first two callbacks
        sql_read_task.task_ret = {
            gridspec:
                [[dataset['dataset_id'],
                  dataset['status'],
                  dataset['gridspec'],
                  dataset['jobs_submitted'],
                  dataset['tasks_submitted']
                ]],
            dataset['dataset_id']:
                [[search['dataset_id'],
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ]]
        }
        # return values for blocking
        _db_read.task_ret = {
            dataset['dataset_id']:[[dataset['dataset_id'],config_data]]
        }
        increment_id.ret = {'job':'newjob',
                            'task':'newtask',
                           }
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = True
        cb.called = False

        num = 10
        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 1d,1j,1t taskname: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 1d,1j,1t taskname: exception returned %s'%cb.ret)


        # now try with task already buffered
        gridspec = 'msdfiner'
        now = datetime.utcnow()
        dataset = OrderedDict([('dataset_id','lknser834'),
                               ('name','test dataset'),
                               ('description','a simple test'),
                               ('gridspec','{"task1":"'+gridspec+'"}'),
                               ('status','processing'),
                               ('username','user'),
                               ('institution','inst'),
                               ('submit_host','localhost'),
                               ('priority',0),
                               ('jobs_submitted',2),
                               ('trays',1),
                               ('tasks_submitted',2),
                               ('start_date',dbmethods.datetime2str(now)),
                               ('end_date',''),
                               ('temporary_storage',''),
                               ('global_storage',''),
                               ('parent_id','sdf'),
                               ('stat_keys','[]'),
                               ('categoryvalue_ids',''),
                               ('debug',True),
                              ])
        search = OrderedDict([('task_id','gdf'),
                ('job_id','3ns8'),
                ('dataset_id','lknser834'),
                ('gridspec',gridspec),
                ('name','0'),
                ('task_status','waiting'),
               ])

        # return values for first two callbacks
        sql_read_task.task_ret = {
            gridspec:
                [[dataset['dataset_id'],
                  dataset['status'],
                  dataset['gridspec'],
                  dataset['jobs_submitted'],
                  dataset['tasks_submitted']
                ]],
            dataset['dataset_id']:
                [[search['dataset_id'],
                  search['job_id'],
                  search['task_id'],
                  search['gridspec'],
                  search['task_status'],
                ]]
        }
        # return values for blocking
        _db_read.task_ret = {
            dataset['dataset_id']:[[dataset['dataset_id'],config_data]]
        }
        increment_id.ret = {'job':'newjob',
                            'task':'newtask',
                           }
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = True
        cb.called = False

        num = 10
        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 1d,1j,1t buffered: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 1d,1j,1t buffered: exception returned %s'%cb.ret)


        # now try with buffer full
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = True
        cb.called = False

        num = 1
        self._db.queue_buffer_jobs_tasks(gridspec,num,callback=cb)
        if cb.called is False:
            raise Exception('buffer 1d,1j,1t buffer full: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('buffer 1d,1j,1t buffer full: exception returned %s'%cb.ret)

    @unittest_reporter
    def test_120_queue_get_queueing_datasets(self):
        """Test queue_get_queueing_datasets"""
        raise Exception('fixme')
        dataset_id = 'asdfasdf'
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        gridspec = 'lksdf.grid1'
        now = datetime.utcnow()
        dataset = OrderedDict([('dataset_id','lknser834'),
                               ('name','test dataset'),
                               ('description','a simple test'),
                               ('gridspec',gridspec),
                               ('status','processing'),
                               ('username','user'),
                               ('institution','inst'),
                               ('submit_host','localhost'),
                               ('priority',0),
                               ('jobs_submitted',2),
                               ('trays',1),
                               ('tasks_submitted',2),
                               ('start_date',dbmethods.datetime2str(now)),
                               ('end_date',''),
                               ('temporary_storage',''),
                               ('global_storage',''),
                               ('parent_id','sdf'),
                               ('stat_keys','[]'),
                               ('categoryvalue_ids',''),
                               ('debug',True),
                              ])

        # single dataset
        cb.called = False
        sql_read_task.ret = [dataset.values()]

        self._db.queue_get_queueing_datasets(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('single dataset: callback not called')
        expected = {dataset['dataset_id']:dataset}
        if cb.ret != expected:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',expected)
            raise Exception('single dataset: callback ret != task')
        if not sql_read_task.sql.startswith('select dataset.* from dataset '):
            raise Exception('single dataset: sql incorrect')

        # no dataset
        cb.called = False
        sql_read_task.ret = []
        gridspec = 'lksdf.grid1'

        self._db.queue_get_queueing_datasets(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        if cb.ret != {}:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no dataset: callback ret != {}')
        if not sql_read_task.sql.startswith('select dataset.* from dataset '):
            raise Exception('no dataset: sql incorrect')

        # sql error
        cb.called = False
        sql_read_task.ret = Exception('sql error')
        gridspec = 'lksdf.grid1'

        self._db.queue_get_queueing_datasets(gridspec,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_121_queue_get_queueing_tasks(self):
        """Test queue_get_queueing_tasks"""
        raise Exception('fixme')
        task = OrderedDict([
                ('task_id','asdf'),
                ('job_id','nsdf'),
                ('dataset_id','adnj'),
                ('gridspec','ggg.g1'),
                ('name','1'),
                ('task_status','waiting'),
                ('debug',True),
               ])
        task2 = OrderedDict([
                ('task_id','bgdf'),
                ('job_id','nsdf'),
                ('dataset_id','adnj'),
                ('gridspec','ggg.g1'),
                ('name','1'),
                ('task_status','waiting'),
                ('debug',False),
               ])
        task3 = OrderedDict([
                ('task_id','erte'),
                ('job_id','nsdf'),
                ('dataset_id','adnj'),
                ('gridspec','ggg.g1'),
                ('name','1'),
                ('task_status','waiting'),
                ('debug',False),
               ])
        task4 = OrderedDict([
                ('task_id','sdtr'),
                ('job_id','nsdf'),
                ('dataset_id','adnj'),
                ('gridspec','ggg.g1'),
                ('name','1'),
                ('task_status','waiting'),
                ('debug',True),
               ])

        def _db_read(conn,sql,bindings,*args):
            _db_read.sql.append(sql)
            _db_read.bindings.append(bindings)
            if bindings[0] in _db_read.task_ret:
                return _db_read.task_ret[bindings[0]]
            else:
                raise Exception('sql error')
        def non_blocking_task(cb):
            non_blocking_task.called = True
            cb()
        flexmock(DB).should_receive('_db_read').replace_with(_db_read)
        flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single dataset
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        _db_read.task_ret = {'adnj':[['adnj',task['task_id'],'','',task['task_status']]],
                             task['task_id']:[task.values()]}
        dataset_prios = {'adnj':1}

        self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)

        if cb.called is False:
            raise Exception('single dataset: callback not called')
        ret_should_be = {task['task_id']:task}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('single dataset: callback ret != task')

        # no tasks
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        _db_read.task_ret = {'adnj':[]}
        dataset_prios = {'adnj':1}

        self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)

        if cb.called is False:
            raise Exception('no task: callback not called')
        ret_should_be = {}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('no task: callback ret != {}')

        # no tasks sql error
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        _db_read.task_ret = {}
        dataset_prios = {'adnj':1}

        self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)

        if cb.called is False:
            raise Exception('_db_read error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('_db_read error: callback ret != Exception')

        # no dataset_prios
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        dataset_prios = None

        try:
            self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)
        except:
            pass
        else:
            raise Exception('no dataset_prios: exception not raised')

        if cb.called is not False:
            raise Exception('no dataset_prios: callback called, but not supposed to be')

        # no callback
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        _db_read.task_ret = {'adnj':[['adnj',task['task_id'],'','',task['task_status']]],
                             task['task_id']:[task.values()]}
        dataset_prios = {'adnj':1}

        try:
            self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',1)
        except:
            pass
        else:
            raise Exception('no callback: exception not raised')

        if cb.called is not False:
            raise Exception('no callback: callback called, but not supposed to be')

        # several tasks in same dataset
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        _db_read.task_ret = {'adnj':[['adnj',task['task_id'],'','',task['task_status']],
                                     ['adnj',task2['task_id'],'','',task2['task_status']],
                                     ['adnj',task3['task_id'],'','',task3['task_status']],
                                    ],
                             task['task_id']:[task.values(),task2.values(),task3.values()]}
        dataset_prios = {'adnj':1}

        self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',3,callback=cb)

        if cb.called is False:
            raise Exception('several tasks in same dataset: callback not called')
        ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks in same dataset: callback ret != task task2 task3')

        # several tasks in diff dataset
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        sql1 = [['adnj',task['task_id'],'','',task['task_status']],
                ['nksd',task2['task_id'],'','',task2['task_status']],
                ['nksd',task3['task_id'],'','',task3['task_status']],
               ]
        _db_read.task_ret = {'adnj':sql1,
                             'nksd':sql1,
                             task2['task_id']:[task.values(),task2.values(),task3.values()]}
        dataset_prios = {'adnj':.3,'nksd':.7}

        self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',3,callback=cb)

        if cb.called is False:
            raise Exception('several tasks in diff dataset: callback not called')
        ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
        if cb.ret != ret_should_be:
            logger.info('sql = %r, bindings = %r',_db_read.sql,_db_read.bindings)
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('several tasks in diff dataset: callback ret != task task2 task3')

        # priority weighted towards one dataset
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        sql1 = [['adnj',task['task_id'],'','',task['task_status']],
                ['nksd',task2['task_id'],'','',task2['task_status']],
                ['nksd',task3['task_id'],'','',task3['task_status']],
                ['nksd',task4['task_id'],'','',task4['task_status']],
               ]
        _db_read.task_ret = {'adnj':sql1,
                             'nksd':sql1,
                             task2['task_id']:[task2.values(),task3.values(),task4.values()]}
        dataset_prios = {'adnj':.2,'nksd':.8}

        self._db.queue_get_queueing_tasks(dataset_prios,'ggg.g1',3,callback=cb)

        if cb.called is False:
            raise Exception('priority weighting dataset: callback not called')
        ret_should_be = {task2['task_id']:task2,task3['task_id']:task3,task4['task_id']:task4}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('priority weighting dataset: callback ret != task2 task3 task4')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_queue_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_queue_test))
    return suite
