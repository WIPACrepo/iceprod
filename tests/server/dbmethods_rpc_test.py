"""
Test script for dbmethods.rpc
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


class dbmethods_rpc_test(dbmethods_base):

    @unittest_reporter
    def test_200_rpc_new_task(self):
        """Test rpc_new_task"""
        search = OrderedDict([('task_id','gdf'),
                ('job_id','3ns8'),
                ('dataset_id','sdj43'),
                ('gridspec','nsd89n3'),
                ('name','the_name'),
                ('job_status','processing'),
                ('task_status','queued'),
               ])
        task = OrderedDict([('task_id','gdf'),
                ('status','queued'),
                ('prev_status','waiting'),
                ('error_message',None),
                ('status_changed',datetime.now()),
                ('submit_dir',None),
                ('grid_queue_id',None),
                ('failures',0),
                ('evictions',0),
                ('depends',None),
               ])

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False
        
        
        now = dbmethods.nowstr()
        gridspec = 'nsd89n3'

        # single task
        tables = {
            'task':[
                    {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                    'error_message':None, 'status_changed':now,
                    'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                    'failures':0, 'evictions':0, 'task_rel_id':None},
                    ],
                    'search':[
                              {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                              'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                              ],
            'config': [{'dataset_id':'d1', 'config_data': 'somedata', 'difplus_data':'' }]
        }

        self.mock.setup(tables)


        # everything working
        cb.called = False

        self._db.rpc_new_task(gridspec=gridspec, platform='platform', hostname=self.hostname, ifaces=None, callback=cb)
        if cb.called is False:
            raise Exception('everything working: callback not called')

        ret_should_be = 'somedata'
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('everything working: callback ret != task')
        
        # no queued jobs
        cb.called = False

        self._db.rpc_new_task(gridspec=gridspec, platform='platform', hostname=self.hostname, ifaces=None, callback=cb)

        if cb.called is False:
            raise Exception('no queued jobs: callback not called')
        ret_should_be = None
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('no queued jobs: callback ret != task')


        # db errors
        self.mock.setup()
        for i in range(5):
            self.mock.failures = i + 1
            cb.called = False
            self._db.rpc_new_task(gridspec=gridspec, platform='platform', hostname=self.hostname, ifaces=None, callback=cb)
            if cb.called is False:
                raise Exception('db errors: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('db errors: callback ret != Exception')


    @unittest_reporter
    def test_201_rpc_finish_task(self):
        """Test rpc_finish_task"""
       

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False
        
        
        now = dbmethods.nowstr()
        gridspec = 'nsd89n3'
        task_id = 'asdf'
        tables = {
            'task':[
                    {'task_id':task_id, 'status':'queued', 'prev_status':'waiting',
                    'error_message':None, 'status_changed':now,
                    'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                    'failures':0, 'evictions':0, 'task_rel_id':None},
                    ],
            'search':[
                    {'task_id':task_id, 'job_id':'bfsd', 'dataset_id':'d1',
                    'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                    ],
            'config': [{'dataset_id':'d1', 'config_data': 'somedata', 'difplus_data':'' }],
            'task_stat': [{'task_stat_id': 0, 'task_id': task_id}],
            'dataset': [{'dataset_id':'d1', 'jobs_submitted': 2, 'tasks_submitted': 2}],
        }
        self.mock.setup(tables)

        

        # everything working
        cb.called = False
        

        stats = {'name1':123123,'name2':968343}
        self._db.rpc_finish_task(task_id,stats,callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')

        # distributed job
        '''
        cb.called = False
        _db_read.task_ret = {'task_stat_id,task_id':[],
                             'search.dataset_id,job_id,jobs_submitted,tasks_submitted':[['d','j',1,2]],
                             'task_id,task_status':[['task','complete']]}
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('complete','new_task_stat')

        self._db.rpc_finish_task('task',stats,callback=cb)

        if cb.called is False:
            raise Exception('distributed job: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('distributed job: callback ret is Exception')
        if _db_write.sql[-1].startswith('update job set status'):
            raise Exception('distributed job: wrongly updated job status')
        '''
        
        # db error
        for i in range(6):
            cb.called = False
            self.mock.setup()
            self.mock.failures = i + 1
            self._db.rpc_finish_task(task_id,stats,callback=cb)
            if cb.called is False:
                raise Exception('db error error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('db error error: callback ret != Exception')


        # update stats
        cb.called = False
        self.mock.setup()

        self._db.rpc_finish_task(task_id,stats,callback=cb)

        if cb.called is False:
            raise Exception('update stats: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('update stats: callback ret is Exception')


    @unittest_reporter
    def test_202_rpc_task_error(self):
        """Test rpc_task_error"""
        raise Exception('fixme')
        def non_blocking_task(cb):
            non_blocking_task.called = True
            cb()
        def _db_read(conn,sql,bindings,*args):
            _db_read.sql = sql
            _db_read.bindings = bindings
            if bindings[0] in _db_read.task_ret:
                return _db_read.task_ret[bindings[0]]
            else:
                raise Exception('sql error')
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
        flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
        flexmock(DB).should_receive('_db_read').replace_with(_db_read)
        flexmock(DB).should_receive('_db_write').replace_with(_db_write)
        flexmock(DB).should_receive('cfg').and_return({'queue':{'max_resets':10}})

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # everything working
        cb.called = False
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset',)
        _db_read.task_ret  = {'task':[['task',0]]}

        self._db.rpc_task_error('task',callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')

        # failure
        cb.called = False
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('failed',)
        _db_read.task_ret  = {'task':[['task',9]]}

        self._db.rpc_task_error('task',callback=cb)

        if cb.called is False:
            raise Exception('failure: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('failure: callback ret is Exception')

        # sql_read_task error
        cb.called = False
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset',)
        _db_read.task_ret  = {}

        self._db.rpc_task_error('task',callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql_read_task error: callback ret != Exception')

        # sql_read_task error2
        cb.called = False
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = ('reset',)
        _db_read.task_ret  = {'task':[]}

        self._db.rpc_task_error('task',callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error2: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql_read_task error2: callback ret != Exception')

        # sql_write_task error
        cb.called = False
        _db_write.sql = []
        _db_write.bindings = []
        _db_write.task_ret = {}
        _db_read.task_ret  = {'task':[['task',0]]}

        self._db.rpc_task_error('task',callback=cb)

        if cb.called is False:
            raise Exception('sql_write_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql_write_task error: callback ret != Exception')
        '''
    @unittest_reporter
    def test_203_rpc_upload_logfile(self):
        """Test rpc_upload_logfile"""
        raise Exception('fixme')
        def blocking_task(name,cb):
            blocking_task.called = True
            cb()
        def non_blocking_task(cb):
            non_blocking_task.called = True
            cb()
        def _db_read(conn,sql,bindings,*args):
            _db_read.sql = sql
            _db_read.bindings = bindings
            if bindings[0] in _db_read.task_ret:
                return _db_read.task_ret[bindings[0]]
            else:
                raise Exception('sql error')
        def _db_write(conn,sql,bindings,*args):
            _db_write.sql = sql
            _db_write.bindings = bindings
            if _db_write.task_ret is not None:
                return _db_write.task_ret
            else:
                raise Exception('sql error')
        def increment_id(table,conn=None):
            increment_id.table = table
            if table in increment_id.ret:
                return increment_id.ret[table]
            else:
                raise Exception('sql error')
        flexmock(DB).should_receive('blocking_task').replace_with(blocking_task)
        flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
        flexmock(DB).should_receive('_db_read').replace_with(_db_read)
        flexmock(DB).should_receive('_db_write').replace_with(_db_write)
        flexmock(DB).should_receive('_increment_id_helper').replace_with(increment_id)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # everything working
        cb.called = False
        _db_read.task_ret = {'task':[]}
        increment_id.ret = {'task_log':'new_task_log'}
        _db_write.task_ret = []

        name = 'logfile'
        data = 'thelogfiledata'
        self._db.rpc_upload_logfile('task',name,data,callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')

        # _db_read error
        cb.called = False
        _db_read.task_ret = None

        self._db.rpc_upload_logfile('task',name,data,callback=cb)

        if cb.called is False:
            raise Exception('_db_read error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('_db_read error: callback ret != Exception')

        # _db_write error
        cb.called = False
        _db_read.task_ret = {'task':[]}
        increment_id.ret = {'task_log':'new_task_log'}
        _db_write.task_ret = None

        self._db.rpc_upload_logfile('task',name,data,callback=cb)

        if cb.called is False:
            raise Exception('_db_write error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('_db_write error: callback ret != Exception')

        # update stats
        cb.called = False
        _db_read.task_ret = {'task':[['new_task_log','task']]}
        increment_id.ret = {}
        _db_write.task_ret = []

        self._db.rpc_upload_logfile('task',name,data,callback=cb)

        if cb.called is False:
            raise Exception('update stats: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('update stats: callback ret is Exception')

        # _db_write update error
        cb.called = False
        _db_read.task_ret = {'task':[['new_task_log','task']]}
        increment_id.ret = {}
        _db_write.task_ret = None

        self._db.rpc_upload_logfile('task',name,data,callback=cb)

        if cb.called is False:
            raise Exception('_db_write update error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('_db_write update error: callback ret != Exception')

    @unittest_reporter
    def test_204_rpc_stillrunning(self):
        """Test rpc_stillrunning"""
        raise Exception('fixme')
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            if bindings[0] in sql_read_task.task_ret:
                callback(sql_read_task.task_ret[bindings[0]])
            else:
                callback(Exception('sql error'))
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # processing
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','processing']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('processing: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('processing: callback ret is Exception')
        if cb.ret != True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('processing: callback ret != True')

        # queued
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','queued']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('queued: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('queued: callback ret is Exception')
        if cb.ret != True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('queued: callback ret != True')

        # reset
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','reset']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('reset: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('reset: callback ret is Exception')
        if cb.ret != False:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('reset: callback ret != False')

        # resume
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','resume']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('resume: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('resume: callback ret is Exception')
        if cb.ret != False:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('resume: callback ret != False')

        # suspended
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','suspended']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('suspended: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('suspended: callback ret is Exception')
        if cb.ret != False:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('suspended: callback ret != False')

        # failed
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','failed']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('failed: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('failed: callback ret is Exception')
        if cb.ret != False:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('failed: callback ret != False')

        # waiting
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','waiting']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('waiting: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('waiting: callback ret is Exception')
        if cb.ret != False:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('waiting: callback ret != False')

        # complete
        cb.called = False
        sql_read_task.task_ret = {'task':[['task','complete']]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('complete: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('complete: callback ret is Exception')
        if cb.ret != False:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('complete: callback ret != False')

        # sql error
        cb.called = False
        sql_read_task.task_ret = {}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql error: callback ret != Exception')

        # sql error2
        cb.called = False
        sql_read_task.task_ret = {'task':[]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('sql error2: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql error2: callback ret != Exception')

        # sql error3
        cb.called = False
        sql_read_task.task_ret = {'task':[[]]}

        self._db.rpc_stillrunning('task',callback=cb)

        if cb.called is False:
            raise Exception('sql error3: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql error3: callback ret != Exception')

    @unittest_reporter
    def test_121_rpc_queue_master(self):
        """Test rpc_queue_master"""
        raise Exception('fixme')
        now = datetime.utcnow()
        dataset = OrderedDict([('dataset_id','adnj'),
                               ('name','test dataset'),
                               ('description','a simple test'),
                               ('gridspec','ggg.g1'),
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
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        def misc_get_tables_for_task(task_ids,callback=None):
            callback(misc_get_tables_for_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
        flexmock(DB).should_receive('_db_read').replace_with(_db_read)
        flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
        (flexmock(self._db.subclasses[self._db.methods['misc_get_tables_for_task']])
            .should_receive('misc_get_tables_for_task')
            .replace_with(misc_get_tables_for_task))

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single dataset
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        sql_read_task.ret = [dataset.values()]
        _db_read.task_ret = {'adnj':[['adnj',task['task_id'],'','',task['task_status']]],
                             task['task_id']:[task.values()]}
        misc_get_tables_for_task.ret = {}

        self._db.rpc_queue_master('',callback=cb)

        if cb.called is False:
            raise Exception('single dataset: callback not called')
        ret_should_be = {}
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('single dataset: callback ret != task')

        # no tasks
        cb.called = False
        _db_read.sql = []
        _db_read.bindings = []
        _db_read.task_ret = {'adnj':[]}

        self._db.rpc_queue_master('',callback=cb)

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

        self._db.rpc_queue_master('',callback=cb)

        if cb.called is False:
            raise Exception('_db_read error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('_db_read error: callback ret != Exception')
'''

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_rpc_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_rpc_test))
    return suite
