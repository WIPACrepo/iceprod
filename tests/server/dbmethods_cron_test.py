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
import unittest

import tornado.escape

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base


class dbmethods_cron_test(dbmethods_base):
    @unittest_reporter
    def test_001_cron_dataset_completion(self):
        """Test cron_dataset_completion"""
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

        self.services.ret['master_updater']['add'] = None

        # everything working
        yield self.set_tables(tables)
        yield self.db['cron_dataset_completion']()

        end_tables = yield self.get_tables(tables.keys())
        if 'dataset' not in end_tables:
            logger.info('tables: %r',end_tables)
            raise Exception('bad end tables')
        if any(row['status'] != 'complete' for row in end_tables['dataset']):
            logger.info('tables: %r',end_tables)
            raise Exception('datasets not marked complete')

        # no processing datasets
        yield self.db['cron_dataset_completion']()

        end_tables2 = yield self.get_tables(tables.keys())
        if end_tables != end_tables2:
            logger.info('%r\n%r',end_tables,end_tables2)
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
        yield self.set_tables(tables2)
        yield self.db['cron_dataset_completion']()

        end_tables = yield self.get_tables(tables2.keys())
        if not cmp_dict(tables2,end_tables):
            logger.info('%r\n%r',tables2,end_tables)
            raise Exception('tasks not completed: bad end_tables')

        # tasks remaining
        tables2 = {
            'dataset':[
                {'dataset_id':'d1','jobs_submitted':1,'tasks_submitted':4,'status':'processing'},
            ],
            'search':[
                {'task_id':'t1','dataset_id':'d1','task_status':'complete'},
            ],
        }
        yield self.set_tables(tables2)
        yield self.db['cron_dataset_completion']()

        end_tables = yield self.get_tables(tables2.keys())
        if not cmp_dict(tables2,end_tables):
            logger.info('%r\n%r',tables2,end_tables)
            raise Exception('tasks not completed: bad end_tables')

        # errors
        tables2 = {
            'dataset':[
                {'dataset_id':'d1','jobs_submitted':1,'tasks_submitted':1,'status':'processing'},
            ],
            'search':[
                {'task_id':'t1','dataset_id':'d1','task_status':'failed'},
            ],
        }
        yield self.set_tables(tables2)
        yield self.db['cron_dataset_completion']()

        end_tables = yield self.get_tables(tables2.keys())
        if any(row['status'] != 'errors' for row in end_tables['dataset']):
            logger.info('tables: %r',end_tables)
            raise Exception('datasets not marked errors')

        # suspended
        tables2 = {
            'dataset':[
                {'dataset_id':'d1','jobs_submitted':1,'tasks_submitted':2,'status':'processing'},
            ],
            'search':[
                {'task_id':'t1','dataset_id':'d1','task_status':'suspended'},
                {'task_id':'t2','dataset_id':'d1','task_status':'failed'},
            ],
        }
        yield self.set_tables(tables2)
        yield self.db['cron_dataset_completion']()

        end_tables = yield self.get_tables(tables2.keys())
        if any(row['status'] != 'suspended' for row in end_tables['dataset']):
            logger.info('tables: %r',end_tables)
            raise Exception('datasets not marked suspended')

        # sql error
        for i in range(3):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])

            try:
                yield self.db['cron_dataset_completion']()
            except:
                pass
            else:
                raise Exception('did not raise Exception')

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
        yield self.set_tables(tables3)
        yield self.db['cron_dataset_completion']()

        end_tables = yield self.get_tables(tables3.keys())
        if not cmp_dict(tables3,end_tables):
            logger.info('%r\n%r',tables2,end_tables)
            raise Exception('multiple datasets of different status: bad end_tables')

    @unittest_reporter
    def test_002_cron_remove_old_passkeys(self):
        now = datetime.utcnow()
        tables = {
            'passkey':[
                {'passkey_id': 'p0', 'auth_key': 'k0', 'expire': (now + timedelta(1)).isoformat()},
                {'passkey_id': 'p1', 'auth_key': 'k1', 'expire': (now + timedelta(2)).isoformat()},
                {'passkey_id': 'p2', 'auth_key': 'k2', 'expire': (now + timedelta(-2)).isoformat()},
                {'passkey_id': 'p3', 'auth_key': 'k3', 'expire': (now + timedelta(-1)).isoformat()},
            ]
        }
        yield self.set_tables(tables)
        yield self.db['cron_remove_old_passkeys']()

        passkeys = (yield self.get_tables(['passkey']))['passkey']
        keys = [k['auth_key'] for k in passkeys]
        correct = ('k0' in keys) and ('k1' in keys) and ('k2' not in keys) and ('k3' not in keys)
        if not correct:
            raise Exception('Function result not correct')


        yield self.set_tables(tables)
        self.set_failures(True)
        try:
            yield self.db['cron_remove_old_passkeys']()
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_003_cron_generate_web_graphs(self):
        now = datetime.utcnow()
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
        yield self.set_tables(tables)
        
        yield self.db['cron_generate_web_graphs']()

        graphs = (yield self.get_tables(['graph']))['graph']
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

        # sql error
        for i in range(4):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])

            try:
                yield self.db['cron_generate_web_graphs']()
            except:
                pass
            else:
                raise Exception('did not raise Exception')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_cron_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_cron_test))
    return suite
