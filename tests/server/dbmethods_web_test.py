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


class dbmethods_web_test(dbmethods_base):
    @unittest_reporter
    def test_600_cron_dataset_completion(self):
        """Test cron_dataset_completion"""
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            if bindings[0] in sql_read_task.task_ret:
                callback(sql_read_task.task_ret[bindings[0]])
            else:
                callback(Exception('sql error'))
        def sql_write_task(sql,bindings,callback):
            sql_write_task.sql = sql
            sql_write_task.bindings = bindings
            if isinstance(sql,Iterable):
                bindings = bindings[0]
            if bindings[0] in sql_write_task.task_ret:
                callback(sql_write_task.task_ret[bindings[0]])
            else:
                callback(Exception('sql error'))
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
        flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        datasets = [['d1',1,1],
                    ['d2',2,4],
                    ['d3',3,9],
                    ['d4',1,1]]
        status = [[datasets[0][0],'complete'],
                  [datasets[1][0],'complete'],
                  [datasets[1][0],'complete'],
                  [datasets[1][0],'complete'],
                  [datasets[1][0],'complete'],
                  [datasets[2][0],'complete'],
                  [datasets[2][0],'complete'],
                  [datasets[2][0],'complete'],
                  [datasets[2][0],'failed'],
                  [datasets[2][0],'failed'],
                  [datasets[2][0],'failed'],
                  [datasets[2][0],'complete'],
                  [datasets[2][0],'complete'],
                  [datasets[2][0],'complete'],
                  [datasets[3][0],'suspended']
                 ]

        # everything working
        cb.called = False
        sql_read_task.task_ret = {'processing':datasets[0:1],
                                  datasets[0][0]:status[0:1]}
        sql_write_task.task_ret = {'complete':{}}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')

        # no processing datasets
        cb.called = False
        sql_read_task.task_ret = {'processing':[]}
        sql_write_task.task_ret = {}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('no processing datasets: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no processing datasets: callback ret is Exception')

        # tasks not completed
        cb.called = False
        sql_read_task.task_ret = {'processing':datasets[0:1],
                                  datasets[0][0]:[[datasets[0][0],'processing']]}
        sql_write_task.task_ret = {}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('tasks not completed: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('tasks not completed: callback ret is Exception')

        # sql_read_task error
        cb.called = False
        sql_read_task.task_ret = {}
        sql_write_task.task_ret = {'complete':{}}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error: callback ret != Exception')

        # sql_read_task error2
        cb.called = False
        sql_read_task.task_ret = {'processing':datasets[0:1]}
        sql_write_task.task_ret = {'complete':{}}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error2: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error2: callback ret != Exception')

        # sql_write_task error
        cb.called = False
        sql_read_task.task_ret = {'processing':datasets[0:1],
                                  datasets[0][0]:status[0:1]}
        sql_write_task.task_ret = {}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('sql_write_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_write_task error: callback ret != Exception')

        # multiple datasets of same status
        cb.called = False
        sql_read_task.task_ret = {'processing':datasets[0:2],
                                  datasets[0][0]:status[0:5]}
        sql_write_task.task_ret = {'complete':{}}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('multiple datasets of same status: callback not called')
        if isinstance(cb.ret,Exception):
            logger.info('read_sql %r %r',sql_read_task.sql,sql_read_task.bindings)
            logger.info('write_sql %r %r',sql_write_task.sql,sql_write_task.bindings)
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('multiple datasets of same status: callback ret is Exception')

        # multiple datasets of different status
        cb.called = False
        sql_read_task.task_ret = {'processing':datasets,
                                  datasets[0][0]:status}
        sql_write_task.task_ret = {'complete':{},
                                   'errors':{},
                                   'suspended':{}}

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('multiple datasets of different status: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('multiple datasets of different status: callback ret is Exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_web_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_web_test))
    return suite
