"""
Test script for queue module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

import logging
logger = logging.getLogger('queue_test')

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

from tornado.concurrent import Future

import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server.modules.queue import queue

from .module_test import module_test
from .dbmethods_test import TestExecutor

class modules_queue_test(module_test):
    def setUp(self):
        super(modules_queue_test,self).setUp()
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
                            'plugin1':{'type':'Test1','description':'d'},
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

    @patch('iceprod.server.listmodules')
    @patch('iceprod.server.run_module')
    @unittest_reporter
    def test_10_start(self, run_module, listmodules):
        listmodules.return_value = ['iceprod.server.plugins.Test1']
        run_module.return_value = MagicMock()

        self.queue.start()

        listmodules.assert_called_once_with('iceprod.server.plugins')
        self.assertTrue(run_module.called)
        self.assertEqual(self.queue.plugins, [run_module.return_value])

    @patch('tornado.ioloop.IOLoop.call_later')
    @unittest_reporter
    def test_20_queue(self, call_later):
        plugin = MagicMock()
        f = Future()
        f.set_result(None)
        plugin.check_and_clean.return_value = f
        plugin.queue.return_value = f
        plugin.cfg = {
            'tasks_on_queue': [10,10],
        }
        plugin.tasks_queued = 0
        plugin.tasks_processing = 0
        self.queue.plugins = [plugin]
        self.queue.check_proxy = MagicMock(return_value=f)
        self.queue.buffer_jobs_tasks = MagicMock(return_value=f)
        self.queue.global_queueing = MagicMock(return_value=f)
        
        yield self.queue.queue_loop()
        call_later.assert_called_once()
        
        self.cfg['queue']['queue_interval'] = 123
        call_later.reset_mock()
        yield self.queue.queue_loop()
        call_later.assert_called_once_with(123, self.queue.queue_loop)
        
        self.cfg['queue']['queue_interval'] = 0
        call_later.reset_mock()
        yield self.queue.queue_loop()
        call_later.assert_called_once()
        self.assertNotEqual(call_later.call_args[0][0], 0)
        
        self.assertTrue(plugin.check_and_clean.called)
        self.assertTrue(plugin.queue.called)
        self.assertTrue(self.queue.check_proxy.called)
        self.assertTrue(self.queue.buffer_jobs_tasks.called)
        self.assertTrue(self.queue.global_queueing.called)

        # check connected to master
        self.cfg['master']['url'] = 'testing'
        yield self.queue.queue_loop()
        self.queue.buffer_jobs_tasks.assert_called_with([plugin.gridspec])

        # check queueing factors
        plugin.cfg.update({
            'queueing_factor_priority':1.0,
            'queueing_factor_dataset':1.0,
            'queueing_factor_tasks':1.0,
        })
        yield self.queue.queue_loop()
        self.cfg['queue'].update({
            'queueing_factor_priority':1.0,
            'queueing_factor_dataset':1.0,
            'queueing_factor_tasks':1.0,
        })
        yield self.queue.queue_loop()

        # check that we don't do global queueing
        plugin.tasks_queued = 10
        self.queue.global_queueing.reset_mock()
        yield self.queue.queue_loop()
        self.assertFalse(self.queue.global_queueing.called)

        # check that raising an exception doesn't propagate
        f = Future()
        f.set_exception(Exception('error'))
        plugin.check_and_clean.return_value = f
        plugin.queue.return_value = f
        self.queue.check_proxy = MagicMock(return_value=f)
        self.queue.buffer_jobs_tasks = MagicMock(return_value=f)
        self.queue.global_queueing = MagicMock(return_value=f)
        self.queue.queue_loop()

        # stop queue loop
        self.queue.plugins = [{}]
        call_later.reset_mock()
        yield self.queue.queue_loop()
        call_later.assert_not_called()

    @unittest_reporter
    def test_30_check_proxy(self):
        """Test check_proxy"""
        self.queue.proxy = MagicMock()
        yield self.queue.check_proxy()
        self.queue.proxy.update_proxy.assert_called_once_with()
        self.assertEqual(self.cfg['queue']['x509proxy'],
                         self.queue.proxy.get_proxy.return_value)

        # try duration as well
        yield self.queue.check_proxy(1400)
        self.queue.proxy.set_duration.assert_called_once_with(1400)

        # try error
        self.queue.proxy.update_proxy.side_effect = Exception()
        self.queue.proxy.get_proxy.reset_mock()
        yield self.queue.check_proxy()
        self.queue.proxy.get_proxy.assert_not_called()

    @patch('iceprod.server.modules.queue.send_master')
    @unittest_reporter
    def test_40_global_queueing(self, send_master):
        tables = MagicMock()
        f = Future()
        f.set_result(tables)
        send_master.return_value = f

        self.modules.ret['db']['node_get_site_resources'] = {'cpus':12,'gpus':3}
        self.modules.ret['db']['misc_update_tables'] = None
        
        yield self.queue.global_queueing()
        self.assertEqual(self.modules.called, [])

        self.cfg['master'] = {'url':'a://url','passkey':'pass'}
        yield self.queue.global_queueing()
        self.assertEqual(self.modules.called[0][:2], ('db','node_get_site_resources'))

    @unittest_reporter
    def test_50_buffer_jobs_tasks(self):
        self.cfg['queue']['task_buffer'] = 50
        gridspecs = ['abc']
        self.modules.ret['db']['queue_buffer_jobs_tasks'] = None
        yield self.queue.buffer_jobs_tasks(gridspecs)
        self.assertEqual(self.modules.called[0][:2], ('db','queue_buffer_jobs_tasks'))
        self.assertEqual(self.modules.called[0][-1]['gridspec'], gridspecs)
        self.assertEqual(self.modules.called[0][-1]['num_tasks'], 50)
        
        self.cfg['queue']['task_buffer'] = 0
        yield self.queue.buffer_jobs_tasks(gridspecs)
        self.assertNotEqual(self.modules.called[-1][-1]['num_tasks'], 0)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_queue_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_queue_test))
    return suite
