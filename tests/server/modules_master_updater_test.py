"""
Test script for config module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

import logging
logger = logging.getLogger('modules_master_updater_test')

import os
import sys
import time
import random
import signal
from datetime import datetime,timedelta
import shutil
import tempfile
import json
import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from tornado.concurrent import Future

import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server.modules.master_updater import master_updater

from .module_test import module_test
from .dbmethods_test import TestExecutor

class modules_master_updater_test(module_test):
    def setUp(self):
        super(modules_master_updater_test,self).setUp()

        try:
            self.cfg = {'master_updater':{'filename':'queue'} }
            self.executor = TestExecutor()
            self.modules = services_mock()
            
            self.up = master_updater(self.cfg, self.io_loop, self.executor, self.modules)
        except:
            logger.warn('error setting up modules_db', exc_info=True)
            raise

    @unittest_reporter
    def test_09_services(self):
        self.assertIn('add', self.up.service)

    @unittest_reporter
    def test_10_start_stop(self):
        self.up.start()
        self.assertEqual(self.up.filename, self.cfg['master_updater']['filename'])
        self.up.stop()

    @unittest_reporter
    def test_11_start_kill(self):
        self.up.start()
        self.up.kill()

    @unittest_reporter
    def test_20_add(self):
        self.up._send = lambda: None
        yield self.up.add('foobar')
        self.assertEquals(self.up.buffer[0], 'foobar')

    @unittest_reporter(name='add() start/stop')
    def test_21_add_stop_start(self):
        self.up._send = lambda: None
        self.up.start()
        yield self.up.add('foobar')
        self.up._save_to_file()
        self.up.stop()
        self.assertEquals(self.up.buffer[0], 'foobar')

        self.up = master_updater(self.cfg, self.io_loop, self.executor, self.modules)
        self.up._send = lambda: None
        self.up.start()
        self.assertEquals(self.up.buffer[0], 'foobar')
    
    @patch('iceprod.server.modules.master_updater.master_updater._save')
    @unittest_reporter(name='add() error')
    def test_23_add_error(self, save):
        self.up._send = lambda: None
        save.side_effect = IOError('error')
        try:
            yield self.up.add('foobar')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @patch('iceprod.server.modules.master_updater.send_master')
    @unittest_reporter
    def test_30_send(self, send_master):
        f = Future()
        f.set_result(None)
        send_master.return_value = f

        yield self.up.add('foobar')
        yield self.up._send()
        self.assertEquals(send_master.call_count, 1)
        self.assertEquals(send_master.call_args[1]['updates'][0], 'foobar')

        send_master.reset_mock()
        yield self.up._send()
        send_master.assert_not_called()

        # bad send
        send_master.side_effect = Exception('error')
        yield self.up.add('foobar2')
        yield self.up._send()
        self.assertEquals(self.up.buffer[0], 'foobar2')

    @patch('iceprod.server.modules.master_updater.send_master')
    @unittest_reporter(name='send() in progress')
    def test_30_send_in_progress(self, send_master):
        f = Future()
        f.set_result(None)
        send_master.return_value = f

        yield self.up.add('foobar')
        yield self.up._send()
        yield self.up.add('foobar2')
        yield self.up._send()

        self.assertEquals(send_master.call_count, 2)
        self.assertEquals(send_master.call_args[1]['updates'][0], 'foobar2')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_master_updater_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_master_updater_test))
    return suite
