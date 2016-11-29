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

from .dbmethods_test import dbmethods_base

class dbmethods_web_test(dbmethods_base):
    @unittest_reporter
    def test_000_web_get_tasks_by_status(self):
        tables = {
            'search':[
                {'task_id':'t1', 'job_id':'j1', 'dataset_id':'d1',
                 'gridspec':'g1', 'name':'0', 'task_status':'waiting'},
                {'task_id':'t2', 'job_id':'j1', 'dataset_id':'d1',
                 'gridspec':'g1', 'name':'0', 'task_status':'waiting'},
                {'task_id':'t3', 'job_id':'j1', 'dataset_id':'d2',
                 'gridspec':'g1', 'name':'0', 'task_status':'queued'},
                {'task_id':'t4', 'job_id':'j1', 'dataset_id':'d2',
                 'gridspec':'g1', 'name':'0', 'task_status':'processing'},
                {'task_id':'t5', 'job_id':'j1', 'dataset_id':'d2',
                 'gridspec':'g2', 'name':'0', 'task_status':'processing'},
            ],
        }        
        yield self.set_tables(tables)
        
        ret = yield self.db['web_get_tasks_by_status']()
        ret_should_be = {'waiting':2, 'queued':1, 'processing':2}
        self.assertEqual(ret, ret_should_be)
        
        ret = yield self.db['web_get_tasks_by_status'](dataset_id='d1')
        ret_should_be = {'waiting':2}
        self.assertEqual(ret, ret_should_be)
        
        ret = yield self.db['web_get_tasks_by_status'](gridspec='g2')
        ret_should_be = {'processing':1}
        self.assertEqual(ret, ret_should_be)
        
        ret = yield self.db['web_get_tasks_by_status'](dataset_id='d2', gridspec='g1')
        ret_should_be = {'queued':1, 'processing':1}
        self.assertEqual(ret, ret_should_be)

        # queue error
        self.set_failures([True])
        try:
            yield self.db['web_get_tasks_by_status']()
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_010_web_get_datasets(self):
        tables = {
            'dataset':[
                {'dataset_id':'d1','status':'processing','gridspec':'g1'},
                {'dataset_id':'d2','status':'errors','gridspec':'g1'},
                {'dataset_id':'d3','status':'processing','gridspec':'g1'},
                {'dataset_id':'d4','status':'processing','gridspec':'g2'},
            ],
        }
        yield self.set_tables(tables)
        starttables = yield self.get_tables(tables)
        
        ret = yield self.db['web_get_datasets']()
        self.assertEqual(ret, starttables['dataset'])

        # gridspec
        ret = yield self.db['web_get_datasets'](gridspec='2')
        self.assertEqual(ret, [starttables['dataset'][-1]])

        # group by status
        ret = yield self.db['web_get_datasets'](groups=['status'])
        self.assertEqual(ret, {'processing':3, 'errors':1})

        # group by status,gridspec
        ret = yield self.db['web_get_datasets'](groups=['status','gridspec'])
        self.assertEqual(ret, {'processing':{'g1':2,'g2':1}, 'errors':{'g1':1}})

        # filter by status
        ret = yield self.db['web_get_datasets'](status=['errors'])
        self.assertEqual(ret, [starttables['dataset'][1]])

        # filter by dataset_id
        ret = yield self.db['web_get_datasets'](dataset_id=['d1'])
        self.assertEqual(ret, [starttables['dataset'][0]])

        # non-selected filter
        ret = yield self.db['web_get_datasets'](dataset_id=None)
        self.assertEqual(ret, starttables['dataset'])

        # queue error
        self.set_failures([True])
        try:
            yield self.db['web_get_datasets']()
        except:
            pass
        else:
            raise Exception('did not raise Exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_web_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_web_test))
    return suite
