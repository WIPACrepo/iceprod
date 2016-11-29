"""
Test script for proxy module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('modules_proxy_test')

import os
import sys
import time
import random
import signal
from datetime import datetime,timedelta
import shutil
import tempfile
import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server.modules.proxy import proxy

from .module_test import module_test

class modules_proxy_test(module_test):
    def setUp(self):
        super(modules_proxy_test,self).setUp()
        try:
            patcher = patch('iceprod.server.modules.proxy.Squid', autospec=True)
            self.mock_squid = patcher.start()
            self.addCleanup(patcher.stop)
        except:
            logger.error('error patching squid', exc_info=True)
            raise

    @unittest_reporter
    def test_20_getargs(self):
        cfg = {}
        executor = {}
        modules = {}

        p = proxy(cfg, self.io_loop, executor, modules)
        ret = p._getargs()
        self.assertEqual(ret, {})

        cfg = {'proxy': {'test':1,'t2':[1,2,3]} }
        p = proxy(cfg, self.io_loop, executor, modules)
        ret = p._getargs()
        self.assertEqual(ret, cfg['proxy'])

        cfg['http_username'] = 'user'
        cfg['http_password'] = 'pass'
        ret = p._getargs()
        self.assertIn('username', ret)
        self.assertEqual(ret['username'], 'user')
        self.assertIn('password', ret)
        self.assertEqual(ret['password'], 'pass')

    @unittest_reporter
    def test_30_start_stop(self):
        cfg = {}
        executor = {}
        modules = {}

        p = proxy(cfg, self.io_loop, executor, modules)
        self.assertIsNone(p.squid)
        p.start()
        self.assertIsNotNone(p.squid)
        self.mock_squid.assert_called_once_with()
        self.mock_squid.return_value.start.assert_called_once_with()

        p.stop()
        self.assertIsNone(p.squid)

    @unittest_reporter
    def test_31_multi_start_stop(self):
        cfg = {}
        executor = {}
        modules = {}

        p = proxy(cfg, self.io_loop, executor, modules)
        self.assertIsNone(p.squid)
        p.start()
        self.assertIsNotNone(p.squid)
        self.mock_squid.assert_called_once_with()

        p.start()
        self.assertIsNotNone(p.squid)
        self.mock_squid.assert_called_once_with()

        p.stop()
        self.assertIsNone(p.squid)

        p.stop()
        self.assertIsNone(p.squid)

        p.start()
        p.kill()
        self.assertIsNone(p.squid)

        p.kill()
        self.assertIsNone(p.squid)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_proxy_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_proxy_test))
    return suite
