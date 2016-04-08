"""
Test script for dbmethods.cron
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

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base,DB


class dbmethods_cron_test(dbmethods_base):
    @unittest_reporter
    def test_001_cron_dataset_completion(self):
        """Test cron_dataset_completion"""
        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # everything working
        tables = {
            'dataset':[
                {'dataset_id':'d1','jobs_submitted':1,'tasks_submitted':1,'status':'processing'},
                {'dataset_id':'d2','jobs_submitted':2,'tasks_submitted':4,'status':'processing'},
            ],
            'search':[
                {'task_id':'t1','dataset_id':'d1','task_status':'complete'},
                {'task_id':'t2','dataset_id':'d2','task_status':'complete'},
                {'task_id':'t3','dataset_id':'d2','task_status':'complete'},
                {'task_id':'t4','dataset_id':'d2','task_status':'complete'},
                {'task_id':'t5','dataset_id':'d2','task_status':'complete'},
            ],
        }

        cb.called = False
        self.mock.setup(tables)
        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('everything working: callback ret is Exception')
        end_tables = self.mock.get(tables.keys())
        if 'dataset' not in end_tables:
            logger.info('tables: %r',end_tables)
            raise Exception('bad end tables')
        if any(row['status'] != 'complete' for row in end_tables['dataset']):
            logger.info('tables: %r',end_tables)
            raise Exception('datasets not marked complete')

        # no processing datasets
        cb.called = False
        # use previous output's mock
        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('no processing datasets: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('no processing datasets: callback ret is Exception')
        if end_tables != self.mock.get(tables.keys()):
            logger.info('%r\n%r',end_tables,self.mock.get(tables.keys()))
            raise Exception('no processing datasets: tables changed')

        # tasks not completed
        tables2 = {
            'dataset':[
                {'dataset_id':'d1','jobs_submitted':1,'tasks_submitted':1,'status':'processing'},
                {'dataset_id':'d2','jobs_submitted':2,'tasks_submitted':4,'status':'complete'},
            ],
            'search':[
                {'task_id':'t1','dataset_id':'d1','task_status':'processing'},
                {'task_id':'t2','dataset_id':'d2','task_status':'complete'},
                {'task_id':'t3','dataset_id':'d2','task_status':'complete'},
                {'task_id':'t4','dataset_id':'d2','task_status':'complete'},
                {'task_id':'t5','dataset_id':'d2','task_status':'complete'},
            ],
        }
        cb.called = False
        self.mock.setup(tables2)
        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('tasks not completed: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('tasks not completed: callback ret is Exception')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables2,end_tables):
            logger.info('%r\n%r',tables2,end_tables)
            raise Exception('tasks not completed: bad end_tables')

        # sql_read_task error
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 1

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error: callback ret != Exception')

        # sql_read_task error2
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 2

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error2: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error2: callback ret != Exception')

        # sql_write_task error
        cb.called = False
        self.mock.setup(tables)
        self.mock.failures = 3

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('sql_write_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('sql_write_task error: callback ret != Exception')

        # multiple datasets of different status
        tables3 = {
            'dataset':[
                {'dataset_id':'d1','jobs_submitted':1,'tasks_submitted':1,'status':'processing'},
                {'dataset_id':'d2','jobs_submitted':2,'tasks_submitted':4,'status':'processing'},
            ],
            'search':[
                {'task_id':'t1','dataset_id':'d1','task_status':'errors'},
                {'task_id':'t2','dataset_id':'d2','task_status':'complete'},
                {'task_id':'t3','dataset_id':'d2','task_status':'errors'},
                {'task_id':'t4','dataset_id':'d2','task_status':'suspended'},
                {'task_id':'t5','dataset_id':'d2','task_status':'suspended'},
            ],
        }
        cb.called = False
        self.mock.setup(tables3)

        self._db.cron_dataset_completion(callback=cb)

        if cb.called is False:
            raise Exception('multiple datasets of different status: callback not called')
        if isinstance(cb.ret,Exception):
            logger.error('cb.ret = %r',cb.ret)
            raise Exception('multiple datasets of different status: callback ret is Exception')
        end_tables = self.mock.get(tables.keys())
        if not cmp_dict(tables3,end_tables):
            logger.info('%r\n%r',tables2,end_tables)
            raise Exception('multiple datasets of different status: bad end_tables')

    @unittest_reporter
    def test_002_cron_remove_old_passkeys(self):
        now = datetime.utcnow()
        def cb(ret):
            cb.called = True
            cb.ret = ret

        tables = {
            'passkey':[
                {'passkey_id': 'p0', 'auth_key': 'k0', 'expire': (now + timedelta(1)).isoformat()},
                {'passkey_id': 'p1', 'auth_key': 'k1', 'expire': (now + timedelta(2)).isoformat()},
                {'passkey_id': 'p2', 'auth_key': 'k2', 'expire': (now + timedelta(-2)).isoformat()},
                {'passkey_id': 'p3', 'auth_key': 'k3', 'expire': (now + timedelta(-1)).isoformat()},
            ]
        }
        self.mock.setup(tables)
        cb.called = False
        self._db.cron_remove_old_passkeys(callback = cb)
        if not cb.called: raise Exception('Callback not called')
        if isinstance(cb.ret, Exception): raise Exception('Callback ret is Exception: "%r"' % cb.ret)

        passkeys = self.mock.get(['passkey'])['passkey']
        keys = [k['auth_key'] for k in passkeys]
        correct = ('k0' in keys) and ('k1' in keys) and ('k2' not in keys) and ('k3' not in keys)
        if not correct:
            raise Exception('Function result not correct')


        self.mock.setup()
        self.mock.failures = 1
        cb.called = False
        self._db.cron_remove_old_passkeys(callback = cb)
        if not cb.called:
            raise Exception('Callback not called')
        if not isinstance(cb.ret, Exception):
            raise Exception('Callback ret is not Exception: "%r"' % cb.ret)

    @unittest_reporter
    def test_003_cron_generate_web_graphs(self):
        now = datetime.utcnow()
        def cb(ret):
            cb.called = True
            cb.ret = ret

        tables = {
            'search':[
                {'task_id': 't0', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'idle'},
                {'task_id': 't1', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'waiting'},
                {'task_id': 't2', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'queued'},
                {'task_id': 't3', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'processing'},
                {'task_id': 't4', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'processing'},
                {'task_id': 't5', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'processing'},
                {'task_id': 't6', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'complete'},
                {'task_id': 't7', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'suspended'},
                {'task_id': 't8', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'failed'},
                {'task_id': 't9', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'resume'},
                {'task_id': 't10', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'reset'},
                {'task_id': 't11', 'job_id': 'j0', 'dataset_id': 'd0',
                 'gridspec': 'g', 'name': '0', 'task_status': 'reset'},
            ],
            'task':[
                {'task_id': 't0', 'status': 'idle', 'prev_status': 'idle',
                 'error_message': '', 'status_changed': dbmethods.datetime2str(now-timedelta(seconds=5)),
                 'submit_dir': '', 'grid_queue_id': '', 'failures': 0,
                 'evictions': 0, 'depends': '', 'requirements': '',
                 'task_rel_id': 'tr0'},
                {'task_id': 't1', 'status': 'complete', 'prev_status': 'processing',
                 'error_message': '', 'status_changed': dbmethods.datetime2str(now-timedelta(seconds=5)),
                 'submit_dir': '', 'grid_queue_id': '', 'failures': 0,
                 'evictions': 0, 'depends': '', 'requirements': '',
                 'task_rel_id': 'tr0'},
                {'task_id': 't2', 'status': 'complete', 'prev_status': 'processing',
                 'error_message': '', 'status_changed': dbmethods.datetime2str(now-timedelta(seconds=5)),
                 'submit_dir': '', 'grid_queue_id': '', 'failures': 0,
                 'evictions': 0, 'depends': '', 'requirements': '',
                 'task_rel_id': 'tr0'},
                {'task_id': 't3', 'status': 'complete', 'prev_status': 'processing',
                 'error_message': '', 'status_changed': dbmethods.datetime2str(now-timedelta(seconds=65)),
                 'submit_dir': '', 'grid_queue_id': '', 'failures': 0,
                 'evictions': 0, 'depends': '', 'requirements': '',
                 'task_rel_id': 'tr0'},
            ],
        }
        self.mock.setup(tables)
        cb.called = False
        self._db.cron_generate_web_graphs(callback = cb)
        if not cb.called:
            raise Exception('Callback not called')
        if isinstance(cb.ret, Exception):
            raise Exception('Callback ret is Exception: "%r"' % cb.ret)

        graphs = self.mock.get(['graph'])['graph']
        answer = {"queued":1, "processing":3, "suspended":1, "failed":1,
                  "resume":1, "reset":2}
        if (not graphs or graphs[0]['name'] != 'active_tasks' or
            json_decode(graphs[0]['value']) != answer):
            logger.info('bad result: %s', graphs[0]['value'])
            raise Exception('Bad active tasks')
        if (graphs[1]['name'] != 'completed_tasks' or
            json_decode(graphs[1]['value']) != {'completions':2}):
            logger.info('bad result: %s', graphs[1]['value'])
            raise Exception('Bad completed tasks')

        for i in range(1,5):
            self.mock.setup(tables)
            self.mock.failures = i
            cb.called = False
            self._db.cron_generate_web_graphs(callback = cb)
            if not cb.called:
                raise Exception('Callback not called')
            if not isinstance(cb.ret, Exception):
                raise Exception('Callback ret is not Exception: "%r"' % cb.ret)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_cron_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_cron_test))
    return suite
