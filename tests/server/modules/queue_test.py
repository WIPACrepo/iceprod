"""
Test script for queue module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

import logging
logger = logging.getLogger('modules_queue_test')

import os
import sys
import time
import random
from datetime import datetime,timedelta
import unittest

try:
    from unittest.mock import patch, MagicMock
except ImportError:
    from mock import patch, MagicMock

from rest_tools.client import RestClient

from tornado.concurrent import Future
import tornado.gen

import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server.modules.queue import queue

from ..module_test import module_test, TestExecutor

class queue_test(module_test):
    def setUp(self):
        super(queue_test,self).setUp()
        try:
            patcher = patch('iceprod.server.modules.queue.SiteGlobusProxy', autospec=True)
            self.mock_proxy = patcher.start()
            self.addCleanup(patcher.stop)

            patcher = patch('iceprod.server.listmodules', autospec=True)
            self.mock_listmodules = patcher.start()
            self.addCleanup(patcher.stop)

            patcher = patch('iceprod.server.run_module', autospec=True)
            self.mock_run_module = patcher.start()
            self.addCleanup(patcher.stop)

            self.cfg = {'queue':{
                            'init_queue_interval':0.1,
                            'plugin1':{'type':'Test1','description':'d',
                                        'tasks_on_queue': [10,10],},
                        },
                        'master':{
                            'url':False,
                        },
                        'site_id':'abcd',
                       }
            self.executor = TestExecutor()
            self.modules = services_mock()
            
            self.queue = queue(self.cfg, self.io_loop, self.executor, self.modules)
        except:
            logger.warn('error setting up modules_queue', exc_info=True)
            raise

    @unittest_reporter
    def test_10_start(self):
        self.mock_listmodules.return_value = ['iceprod.server.plugins.Test1']
        self.queue.rest_client = MagicMock(spec=RestClient)
        self.queue.rest_client.request_seq.return_value = {'result':'foo'}

        self.queue.start()

        self.mock_listmodules.assert_called_once_with('iceprod.server.plugins')
        self.assertTrue(self.mock_listmodules.called)
        self.assertEqual(self.queue.plugins, [self.mock_run_module.return_value])
        self.assertTrue(self.queue.rest_client.request_seq.call_count, 2)
        self.assertTrue(self.queue.rest_client.request_seq.call_args[0][1], '/grids/foo')

    @patch('tornado.ioloop.IOLoop.call_later')
    @unittest_reporter
    async def test_20_queue(self, call_later):
        plugin = MagicMock()
        async def run(*args, **kwargs):
            return None
        plugin.check_and_clean = MagicMock(side_effect=run)
        plugin.queue = MagicMock(side_effect=run)
        plugin.queue_cfg = {
            'tasks_on_queue': [10,10],
        }
        plugin.tasks_queued = 0
        plugin.tasks_processing = 0
        self.queue.plugins = [plugin]
        self.queue.check_proxy = MagicMock()
        
        await self.queue.queue_loop()
        call_later.assert_called_once()
        
        self.cfg['queue']['queue_interval'] = 123
        call_later.reset_mock()
        await self.queue.queue_loop()
        call_later.assert_called_once_with(123, self.queue.queue_loop)
        
        self.cfg['queue']['queue_interval'] = 0
        call_later.reset_mock()
        await self.queue.queue_loop()
        call_later.assert_called_once()
        self.assertNotEqual(call_later.call_args[0][0], 0)
        
        self.assertTrue(plugin.check_and_clean.called)
        self.assertTrue(plugin.queue.called)
        self.assertTrue(self.queue.check_proxy.called)

        # check that raising an exception doesn't propagate
        async def run(*args, **kwargs):
            raise Exception()
        plugin.check_and_clean = MagicMock(side_effect=run)
        plugin.queue = MagicMock(side_effect=run)
        self.queue.check_proxy = MagicMock(side_effect=Exception)
        await self.queue.queue_loop()

    @unittest_reporter
    def test_30_check_proxy(self):
        """Test check_proxy"""
        self.queue.proxy = MagicMock()
        self.queue.check_proxy()
        self.queue.proxy.update_proxy.assert_called_once_with()
        self.assertEqual(self.cfg['queue']['x509proxy'],
                         self.queue.proxy.get_proxy.return_value)

        # try duration as well
        self.queue.check_proxy(2*3600)
        self.queue.proxy.set_duration.assert_called_once_with(2)

        # try error
        self.queue.proxy.update_proxy.side_effect = Exception()
        self.queue.proxy.get_proxy.reset_mock()
        self.queue.check_proxy()
        self.queue.proxy.get_proxy.assert_not_called()


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(queue_test))
    suite.addTests(loader.loadTestsFromNames(alltests,queue_test))
    return suite
