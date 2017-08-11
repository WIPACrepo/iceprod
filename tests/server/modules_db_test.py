"""
Test script for database server module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

import logging
logger = logging.getLogger('modules_db_test')

import os
import sys
import time
import shutil
import tempfile
import signal
import unittest
from functools import partial

import tornado.gen

import iceprod.core.logger
from iceprod.core import functions
from iceprod.server.modules import db

from .module_test import module_test

class modules_db_test(module_test):
    def setUp(self):
        super(modules_db_test,self).setUp()

        try:
            self.cfg = {'db':{'type':'sqlite',
                              'name':'test',
                              'name_setting':'test_setting',
                              'nthreads':3},
                        'site_id':'abcd',
                       }
            self.executor = {}
            self.modules = services_mock()
            self.modules.ret['daemon']['stop'] = True
            
            self.db = db.db(self.cfg, self.io_loop, self.executor, self.modules)
        except:
            logger.warn('error setting up modules_db', exc_info=True)
            raise

    @unittest_reporter
    def test_10_start_stop(self):
        self.db.start()
        self.assertIsInstance(self.db.db, db.SQLite)

        self.db.stop()
        self.assertIsNone(self.db.db)

    @unittest_reporter
    def test_11_start_kill(self):
        self.db.start()
        self.assertIsInstance(self.db.db, db.SQLite)

        self.db.kill()
        self.assertIsNone(self.db.db)

    @unittest_reporter
    def test_12_bad_db(self):
        self.cfg['db']['type'] = 'blah'
        self.db.start()
        self.assertEqual(self.modules.called[-1][:2], ('daemon','stop'))

    @unittest_reporter
    def test_20_read_db_conf(self):
        d = db.read_db_conf()
        self.assertTrue(d)
        self.assertIn('tables', d)

        t = db.read_db_conf('tables')
        self.assertTrue(t)
        self.assertEqual(d['tables'], t)
        self.assertIn('site', t)
        self.assertIn('queues', t['site'])

        try:
            db.read_db_conf('blah')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_30_acquire_lock(self):
        self.db.start()
        values = []
        ids = []

        @tornado.gen.coroutine
        def test_lock(iterable):
            with (yield self.db.db.acquire_lock('blah')):
                for i in iterable:
                    values.append(i)
                    ret = yield self.db.db.increment_id('task_log')
                    ids.append(ret)

        self.io_loop.add_callback(partial(test_lock,range(0,7)))
        self.io_loop.add_callback(partial(test_lock,range(7,14)))
        self.io_loop.add_callback(partial(test_lock,range(14,20)))
        self.io_loop.add_callback(partial(test_lock,range(20,26)))
        self.io_loop.add_callback(partial(test_lock,range(26,31)))
        try:
            self.wait(timeout=0.1)
        except:
            pass

        logger.info('values: %r',values)
        logger.info('ids: %r',ids)
        self.assertEqual(values,list(range(0,31)))
        self.assertEqual(len(set(ids)),len(ids))

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_db_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_db_test))
    return suite
