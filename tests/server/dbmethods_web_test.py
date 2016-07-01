"""
Test script for dbmethods.web
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
import unittest

import tornado.escape
from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base,DB

def get_tables():
    tables = {
        'dataset': [
                    {'dataset_id': 'd1', 'jobs_submitted': 1, 'tasks_submitted':1, 'status': 'processing'},
                    {'dataset_id': 'd2', 'jobs_submitted': 2, 'tasks_submitted':4},
                    {'dataset_id': 'd3', 'jobs_submitted': 3, 'tasks_submitted':9},
                    {'dataset_id': 'd4', 'jobs_submitted': 1, 'tasks_submitted':1},
                    ],
            'search': [
                       {'dataset_id': 'd1', 'task_status': 'complete'},
                       ],
    }
    return tables



class dbmethods_web_test(dbmethods_base):
    @unittest_reporter
    def test_600_cron_dataset_completion(self):
        """Test cron_dataset_completion"""
        
        def cb(ret):
            cb.called = True
            cb.ret = ret
        
        
        tables = get_tables()
        self.mock.setup(tables)

        # everything working
        cb.called = False
        '''
        sql_read_task.task_ret = {'processing':datasets[0:1],
                                  datasets[0][0]:status[0:1]}
        sql_write_task.task_ret = {'complete':{}}
        '''

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')

        # no processing datasets
        cb.called = False
        tables = get_tables()
        tables['dataset'][0]['status'] = 'complete'
        self.mock.setup(tables)

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('no processing datasets: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no processing datasets: callback ret is Exception')

        # tasks not completed
        cb.called = False
        tables = get_tables()
        tables['search'][0]['task_status'] = 'processing'
        self.mock.setup(tables)

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('tasks not completed: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('tasks not completed: callback ret is Exception')


        #sql error
        for i in range(3):
            cb.called = False
            tables = get_tables()
            self.mock.setup(tables)
            self.mock.failures = i + 1
            self._db.cron_dataset_completion(callback=cb)
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')

        # multiple datasets of same status
        cb.called = False
        tables = get_tables()
        tables['dataset'][1]['status'] = 'processing'
        self.mock.setup(tables)
        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('multiple datasets of same status: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('read_sql %r %r',sql_read_task.sql,sql_read_task.bindings)
            logger.info('write_sql %r %r',sql_write_task.sql,sql_write_task.bindings)
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('multiple datasets of same status: callback ret is Exception')



def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_web_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_web_test))
    return suite
