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
from iceprod.core import functions
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods
from .dbmethods_test import dbmethods_base,DB


now = dbmethods.nowstr()
gridspec = 'nsd89n3'
task_id = 'asdf'
def get_tables(test_dir):
    tables = {
        'task':[
                {'task_id':task_id, 'status':'queued', 'prev_status':'waiting',
                 'error_message':None, 'status_changed':now,
                 'submit_dir':test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'task_rel_id':None},
                ],
        'search':[
                  {'task_id':task_id, 'job_id':'bfsd', 'dataset_id':'d1',
                   'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                 ],
        'config': [{'dataset_id':'d1', 'config_data': '{"name":"value"}', 'difplus_data':'' }],
        'task_stat': [{'task_stat_id': 0, 'task_id': task_id}],
        'dataset': [{'dataset_id':'d1', 'jobs_submitted': 2, 'tasks_submitted': 2}],
    }
    return tables


class dbmethods_rpc_test(dbmethods_base):

    @unittest_reporter
    def test_000_rpc_new_task(self):
        """Test rpc_new_task"""

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        tables = get_tables(self.test_dir)
        self.mock.setup(tables)

        # everything working
        cb.called = False

        self._db.rpc_new_task(gridspec=gridspec, platform='platform', hostname=self.hostname, ifaces=None, callback=cb)
        if cb.called is False:
            raise Exception('everything working: callback not called')

        ret_should_be = {'name':'value','options':{'task_id':task_id}}
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
    def test_001_rpc_finish_task(self):
        """Test rpc_finish_task"""


        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False


        tables = get_tables(self.test_dir)
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
    def test_002_rpc_task_error(self):
        """Test rpc_task_error"""

        if not 'queue' in self.mock.cfg: self.mock.cfg['queue'] = {}
        self.mock.cfg['queue']['max_resets'] = 10

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        tables = get_tables(self.test_dir)
        self.mock.setup(tables)

        # everything working
        cb.called = False

        self._db.rpc_task_error(task_id,callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')

        # error info
        cb.called = False

        self._db.rpc_task_error(task_id, error_info={'a':1}, callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')
        end_tables = self.mock.get(['task_stat'])['task_stat'][-1]
        if not end_tables:
            raise Exception('no stats')
        stat = json_decode(end_tables['stat'])
        if not stat.keys()[0].startswith('error_'):
            raise Exception('bad stat name')
        if stat.values()[0] != {'a':1}:
            raise Exception('bad stat value')

        # failure
        cb.called = False
        tables['task'][0]['failures'] = 9;

        self._db.rpc_task_error(task_id, callback=cb)

        if cb.called is False:
            raise Exception('failure: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('failure: callback ret is Exception')


        self.mock.setup()
        for i in range(1,4):
            cb.called = False
            self.mock.failures = i
            self._db.rpc_task_error(task_id,callback=cb)

            if cb.called is False:
                raise Exception('db error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('db error: callback ret != Exception')


    @unittest_reporter
    def test_003_rpc_upload_logfile(self):
        """Test rpc_upload_logfile"""

        tables = get_tables(self.test_dir)
        self.mock.setup(tables)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # everything working
        cb.called = False

        name = 'logfile'
        data = 'thelogfiledata'
        self._db.rpc_upload_logfile(task_id,name,data,callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')

        # update stats
        cb.called = False
        self._db.rpc_upload_logfile(task_id,name,data,callback=cb)

        if cb.called is False:
            raise Exception('update stats: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('update stats: callback ret is Exception')

        self.mock.setup()
        for i in range(3):
            cb.called = False
            self.mock.failures = i + 1
            self._db.rpc_upload_logfile(task_id,name,data,callback=cb)
            if cb.called is False:
                raise Exception('db error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('db error: callback ret != Exception')


    @unittest_reporter
    def test_004_rpc_stillrunning(self):
        """Test rpc_stillrunning"""

        def cb(ret):
            cb.called = True
            cb.ret = ret

        cb.called = False
        tables = get_tables(self.test_dir)

        # processing
        cb.called = False
        tables['task'][0]['status'] = 'processing'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        tables['task'][0]['status'] = 'queued'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        tables['task'][0]['status'] = 'reset'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        tables['task'][0]['status'] = 'resume'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        tables['task'][0]['status'] = 'suspended'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        tables['task'][0]['status'] = 'failed'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        tables['task'][0]['status'] = 'waiting'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        tables['task'][0]['status'] = 'complete'
        self.mock.setup(tables)
        self._db.rpc_stillrunning(task_id,callback=cb)

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
        self.mock.setup()
        self.mock.failures = 1
        self._db.rpc_stillrunning(task_id,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql error: callback ret != Exception')


    @unittest_reporter
    def test_100_rpc_submit_dataset(self):
        """Test rpc_submit_dataset"""

        def cb(ret):
            cb.called = True
            cb.ret = ret

        # try giving dict
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
        ret = self._db.rpc_submit_dataset(config_dict, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is Exception')
        if cb.ret is not True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret != True')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if (not end_tables['config'] or
            end_tables['config'][0]['config_data'] != config_json):
            logger.info('%r', end_tables)
            raise Exception('bad config table')
        if len(end_tables['task_rel']) != 1:
            logger.info('%r', end_tables)
            raise Exception('bad task_rel table')

        # try giving job
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
        ret = self._db.rpc_submit_dataset(config_job, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is Exception')
        if cb.ret is not True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret != True')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if (not end_tables['config'] or
            end_tables['config'][0]['config_data'] != config_json):
            logger.info('%r', end_tables)
            raise Exception('bad config table')
        if len(end_tables['task_rel']) != 1:
            logger.info('%r', end_tables)
            raise Exception('bad task_rel table')

        # try giving json
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
        ret = self._db.rpc_submit_dataset(config_json, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is Exception')
        if cb.ret is not True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret != True')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if (not end_tables['config'] or
            end_tables['config'][0]['config_data'] != config_json):
            logger.info('%r', end_tables)
            raise Exception('bad config table')
        if len(end_tables['task_rel']) != 1:
            logger.info('%r', end_tables)
            raise Exception('bad task_rel table')

        # try giving bad json
        cb.called = False
        self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
        ret = self._db.rpc_submit_dataset("<asf>", callback=cb)
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback is not Exception')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if any(end_tables.values()):
            logger.info('%r',end_tables)
            raise Exception('tables have changed')

        # number dependency
        config_dict = {'tasks':[
            {'trays':[{'modules':[]}]},
            {'depends':['0'],'trays':[{'modules':[]}]}
        ],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
        ret = self._db.rpc_submit_dataset(config_dict, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is Exception')
        if cb.ret is not True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret != True')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if (not end_tables['config'] or
            end_tables['config'][0]['config_data'] != config_json):
            logger.info('table: %r', end_tables['config'][0]['config_data'])
            logger.info('should be: %r', config_json)
            raise Exception('bad config table')
        if (len(end_tables['task_rel']) != 2 or
            end_tables['task_rel'][1]['depends'] != end_tables['task_rel'][0]['task_rel_id']):
            logger.info('%r', end_tables['task_rel'])
            raise Exception('bad task_rel table')

        # named dependency
        config_dict = {'tasks':[
            {'name':'first','trays':[{'modules':[]}]},
            {'name':'second','depends':['first'],'trays':[{'modules':[]}]}
        ],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
        ret = self._db.rpc_submit_dataset(config_dict, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is Exception')
        if cb.ret is not True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret != True')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if (not end_tables['config'] or
            end_tables['config'][0]['config_data'] != config_json):
            logger.info('table: %r', end_tables['config'][0]['config_data'])
            logger.info('should be: %r', config_json)
            raise Exception('bad config table')
        if (len(end_tables['task_rel']) != 2 or
            end_tables['task_rel'][1]['depends'] != end_tables['task_rel'][0]['task_rel_id']):
            logger.info('%r', end_tables['task_rel'])
            raise Exception('bad task_rel table')

        # missing named dependency
        config_dict = {'tasks':[
            {'name':'first','trays':[{'modules':[]}]},
            {'name':'second','depends':['third'],'trays':[{'modules':[]}]}
        ],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
        ret = self._db.rpc_submit_dataset(config_dict, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is not Exception')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if any(end_tables.values()):
            logger.info('%r',end_tables)
            raise Exception('tables have changed')

        # dataset dependency
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_dict2 = {'tasks':[
            {'depends':['d1.0'],'trays':[{'modules':[]}]},
        ],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)
        config_job2 = serialization.dict_to_dataclasses(config_dict2)
        config_json2 = serialization.serialize_json.dumps(config_job2)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[],
            'task_rel': [
                {'task_rel_id':'tr1', 'dataset_id':'d1', 'task_index': 0,
                 'name':'0', 'depends':'', 'requirements':''},
            ],
        })
        ret = self._db.rpc_submit_dataset(config_dict2, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is Exception')
        if cb.ret is not True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret != True')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if (not end_tables['config'] or
            end_tables['config'][0]['config_data'] != config_json2):
            logger.info('table: %r', end_tables['config'][0]['config_data'])
            logger.info('should be: %r', config_json2)
            raise Exception('bad config table')
        if (len(end_tables['task_rel']) != 2 or
            end_tables['task_rel'][1]['depends'] != end_tables['task_rel'][0]['task_rel_id']):
            logger.info('%r', end_tables['task_rel'])
            raise Exception('bad task_rel table')

        # dataset dependency missing
        cb.called = False
        self.mock.setup({'dataset':[], 'config':[],'task_rel':[]})
        ret = self._db.rpc_submit_dataset(config_dict2, callback=cb)
        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is not Exception')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if any(end_tables.values()):
            logger.info('%r',end_tables)
            raise Exception('tables have changed')

        # named dataset dependency
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_dict2 = {'tasks':[
            {'depends':['d1.first'],'trays':[{'modules':[]}]},
        ],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)
        config_job2 = serialization.dict_to_dataclasses(config_dict2)
        config_json2 = serialization.serialize_json.dumps(config_job2)

        cb.called = False
        self.mock.setup({'dataset':[], 'config':[],
            'task_rel': [
                {'task_rel_id':'tr1', 'dataset_id':'d1', 'task_index': 0,
                 'name':'first', 'depends':'', 'requirements':''},
            ],
        })
        ret = self._db.rpc_submit_dataset(config_dict2, callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret is Exception')
        if cb.ret is not True:
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('callback ret != True')
        end_tables = self.mock.get(['dataset','config','task_rel'])
        if (not end_tables['config'] or
            end_tables['config'][0]['config_data'] != config_json2):
            logger.info('table: %r', end_tables['config'][0]['config_data'])
            logger.info('should be: %r', config_json2)
            raise Exception('bad config table')
        if (len(end_tables['task_rel']) != 2 or
            end_tables['task_rel'][1]['depends'] != end_tables['task_rel'][0]['task_rel_id']):
            logger.info('%r', end_tables['task_rel'])
            raise Exception('bad task_rel table')

        # test sql errors
        for i in range(1,2):
            cb.called = False
            self.mock.setup({'dataset':[], 'config':[], 'task_rel': []})
            self.mock.failures = i
            ret = self._db.rpc_submit_dataset(config_dict, callback=cb)
            if cb.called is False:
                raise Exception('callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('callback ret is not Exception')
            end_tables = self.mock.get(['dataset','config','task_rel'])
            if any(end_tables.values()):
                logger.info('%r',end_tables)
                raise Exception('tables have changed')

    @unittest_reporter
    def test_200_rpc_queue_master(self):
        """Test rpc_queue_master"""

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # single dataset
        cb.called = False
        tables = get_tables(self.test_dir)
        self.mock.setup(tables)
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
        tables = get_tables(self.test_dir)
        tables['task'] = []
        self.mock.setup(tables)
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
        self.mock.setup(tables)
        self.mock.failures = True
        self._db.rpc_queue_master('',callback=cb)

        if cb.called is False:
            raise Exception('_db_read error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('_db_read error: callback ret != Exception')

    @unittest_reporter
    def test_300_rpc_public_get_graphs(self):
        """Test rpc_public_get_graphs"""
        def cb(ret):
            cb.called = True
            cb.ret = ret

        now = datetime.utcnow()
        tables = {'graph':
            [{'graph_id':'gid0', 'name':'name1', 'value':'{"t":0}',
              'timestamp':dbmethods.datetime2str(now-timedelta(minutes=3))},
             {'graph_id':'gid1','name':'name1', 'value':'{"t":1}',
              'timestamp':dbmethods.datetime2str(now-timedelta(minutes=2))},
             {'graph_id':'gid2', 'name':'name1', 'value':'{"t":0}',
              'timestamp':dbmethods.datetime2str(now-timedelta(minutes=1))},
            ],
        }
        
        cb.called = False
        self.mock.setup(tables)
        self._db.rpc_public_get_graphs(5,callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        ret_should_be = []
        for row in tables['graph']:
            row = dict(row)
            del row['graph_id']
            row['value'] = json_decode(row['value'])
            ret_should_be.append(row)
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('callback ret incorrect')

        cb.called = False
        self._db.rpc_public_get_graphs(2,callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        ret_should_be = []
        for row in tables['graph'][-1:]:
            row = dict(row)
            del row['graph_id']
            row['value'] = json_decode(row['value'])
            ret_should_be.append(row)
        if cb.ret != ret_should_be:
            logger.error('cb.ret = %r',cb.ret)
            logger.error('ret should be = %r',ret_should_be)
            raise Exception('callback ret incorrect')
        
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 1
        self._db.rpc_public_get_graphs(5,callback=cb)

        if cb.called is False:
            raise Exception('callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('did not return exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_rpc_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_rpc_test))
    return suite
