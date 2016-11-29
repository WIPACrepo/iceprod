"""
Test script for the schedule module.
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

import logging
logger = logging.getLogger('modules_schedule_test')

import os
import sys
from datetime import datetime,timedelta
import unittest

try:
    from unittest.mock import patch, MagicMock
except ImportError:
    from mock import patch, MagicMock

import tornado.gen

import iceprod.server
from iceprod.server.modules.schedule import schedule

from .module_test import module_test

class modules_schedule_test(module_test):
    def setUp(self):
        super(modules_schedule_test,self).setUp()
        try:
            self.cfg = {'queue':{
                            'init_queue_interval':0.1,
                            'submit_dir':self.test_dir,
                            '*':{'type':'Test1','description':'d'},
                        },
                        'master':{
                            'url':False,
                        },
                        'site_id':'abcd',
                       }
            self.executor = {}
            self.modules = services_mock()
            
            self.sched = schedule(self.cfg, self.io_loop, self.executor, self.modules)
        except:
            logger.warn('error setting up modules_schedule', exc_info=True)
            raise

    @unittest_reporter
    def test_10_start(self):
        self.sched.start()
        self.sched._make_schedule = MagicMock()
        yield tornado.gen.moment
        self.sched._make_schedule.assert_called_once_with()
        self.sched.stop()

        self.sched.start()
        self.sched._make_schedule = MagicMock(side_effect=Exception())
        yield tornado.gen.moment
        self.sched._make_schedule.assert_called_once_with()

    @patch('iceprod.server.modules.schedule.Scheduler')
    @unittest_reporter(name='start/stop/kill')
    def test_11_start_stop_kill(self, s):
        self.sched.start()
        self.assertIsNotNone(self.sched.scheduler)
        s.assert_called_once_with(self.io_loop)
        self.sched.stop()
        self.assertIsNone(self.sched.scheduler)
        self.sched.stop()

        s.reset_mock()
        self.sched.start()
        self.assertIsNotNone(self.sched.scheduler)
        self.sched.start()
        self.assertIsNotNone(self.sched.scheduler)
        s.assert_called_once_with(self.io_loop)
        self.sched.kill()
        self.assertIsNone(self.sched.scheduler)
        self.sched.kill()

    @unittest_reporter
    def test_20_make_schedule(self):
        self.sched.start()

        self.sched._master_schedule = MagicMock()
        self.sched._make_schedule()
        self.sched._master_schedule.assert_not_called()

        self.cfg['master']['status'] = True
        self.sched._make_schedule()
        self.sched._master_schedule.assert_called_once_with()

    @unittest_reporter
    def test_30_master_schedule(self):
        self.sched.start()
        self.cfg['master']['status'] = True
        self.sched._master_schedule()

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_schedule_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_schedule_test))
    return suite
