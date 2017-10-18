"""
Test script for core pilot
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, return_once

import logging
logger = logging.getLogger('pilot')

import os
import sys
import time
import random
import shutil
import tempfile
import glob
import subprocess
import unittest
import multiprocessing
from copy import deepcopy
from functools import partial
from collections import namedtuple

try:
    from unittest import mock
except ImportError:
    import mock

from iceprod.core import to_log,constants
from iceprod.core import pilot
from iceprod.core import resources

try:
    import psutil as normal_psutil
except ImportError:
    normal_psutil = None

class TestBase(unittest.TestCase):
    def setUp(self):
        super(TestBase,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        maindir = os.getcwd()
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(maindir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        # mock iceprod.core.logger.new_file
        patcher = mock.patch('iceprod.core.logger.new_file')
        patcher.start()
        self.addCleanup(patcher.stop)

        # clean up environment
        base_env = dict(os.environ)
        def reset_env():
            for k in set(os.environ).difference(base_env):
                del os.environ[k]
            for k in base_env:
                os.environ[k] = base_env[k]
        self.addCleanup(reset_env)

        # mock hostname
        pilot.gethostname = lambda: 'foo.bar'

class pilot_test(TestBase):
    def setUp(self):
        super(pilot_test,self).setUp()

        # convert multiprocessing to direct calls
        patcher = mock.patch('iceprod.core.pilot.Process')
        self.process = patcher.start()
        def run(target):
            logger.warn('Process()')
            target()
            ret = mock.MagicMock()
            ret.pid = os.getpid()
            ret.is_alive.return_value = False
            return ret
        self.process.side_effect = run
        self.addCleanup(patcher.stop)

        # disable psutil
        pilot.psutil = None
        def c():
            pilot.psutil = normal_psutil
        self.addCleanup(c)

    @mock.patch('iceprod.core.pilot.Pilot.run')
    @unittest_reporter(name='Pilot.__init__()')
    def test_001_pilot_init(self, run):
        run.return_value = None
        cfg = {'options':{'gridspec':'a'}}
        runner = None
        p = pilot.Pilot(cfg, runner, pilot_id='a')
        run.assert_called_once_with()

    @unittest_reporter(name='Pilot.run()')
    def test_012_pilot_run(self):
        task_cfg = {'options':{'task_id':'a'}}
        cfg = {'options':{'gridspec':'a'}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([task_cfg], end_value=None)
        runner = mock.MagicMock()
        runner.return_value = 0
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.001)
        runner.assert_called_once_with(task_cfg)
        update_args = rpc.update_pilot.call_args_list
        self.assertEqual(update_args[0][0], ('a',))
        self.assertEqual(update_args[0][1]['tasks'], 'a')
        self.assertEqual(update_args[1][0], ('a',))
        self.assertEqual(update_args[1][1]['tasks'], '')

    @unittest_reporter(name='Pilot.run() split resources')
    def test_013_pilot_resources(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':2,'memory':2.2,'disk':2.2}}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([task_cfg], [task_cfg2], end_value=None)
        runner = mock.MagicMock()
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.001)
        runner.assert_has_calls([mock.call(task_cfg), mock.call(task_cfg2)])
        self.assertGreaterEqual(rpc.update_pilot.call_count, 3)
        update_args = rpc.update_pilot.call_args_list
        for call in update_args:
            if call[1]['tasks'] == 'a,b':
                break
        else:
            raise Exception('did not update_pilot with both tasks running')

    @unittest_reporter(name='Pilot.run() error downloading')
    def test_014_pilot_resources(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([task_cfg], [task_cfg2])
        runner = mock.MagicMock()
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.001)
        runner.assert_has_calls([mock.call(task_cfg), mock.call(task_cfg2)])
        self.assertGreaterEqual(rpc.update_pilot.call_count, 3)
        update_args = rpc.update_pilot.call_args_list
        for call in update_args:
            if call[1]['tasks'] == 'a,b':
                break
        else:
            raise Exception('did not update_pilot with both tasks running')

    @unittest_reporter(name='Pilot.create() error')
    def test_020_pilot_create_error(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':3,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':1,'memory':1.1,'disk':1.1}}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([task_cfg], end_value=None)
        runner = mock.MagicMock()
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.001)
        runner.assert_not_called()
        rpc.task_kill.assert_called()


class pilot_multi_test(TestBase):
    @unittest_reporter(name='Pilot.monitor()')
    def test_100_pilot_monitoring(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([task_cfg], end_value=None)
        runner = lambda x:time.sleep(0.2)
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=1.1, debug=True)
        update_args = rpc.update_pilot.call_args_list
        self.assertEqual(update_args[0][0], ('a',))
        self.assertEqual(update_args[0][1]['tasks'], 'a')
        self.assertEqual(update_args[1][0], ('a',))
        self.assertEqual(update_args[1][1]['tasks'], '')

    @unittest_reporter(name='Pilot.monitor() over limit')
    def test_102_pilot_monitor_over_limit(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':0.00001,'disk':1,'time':0.001}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3,'time':0.102}}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([task_cfg], end_value=None)
        def runner(cfg):
            time.sleep(1)
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.1, debug=True)
        rpc.task_kill.assert_called()

    @unittest_reporter(name='Pilot.run() multiprocess error downloading')
    def test_104_pilot_multiprocess_error_downloading(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([task_cfg])
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        runner = lambda x:time.sleep(random.random())
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.01, debug=True)
        update_args = rpc.update_pilot.call_args_list
        self.assertEqual(update_args[0][0], ('a',))
        self.assertEqual(update_args[0][1]['tasks'], 'a')
        self.assertEqual(update_args[1][0], ('a',))
        self.assertEqual(update_args[1][1]['tasks'], '')

    @unittest_reporter(name='Pilot.run() no resource specified, sequential')
    def test_110_pilot_sequential(self):
        task_cfg = {'options':{'task_id':'a'}}
        task_cfg2 = {'options':{'task_id':'b'}}
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = return_once([deepcopy(task_cfg)], end_value=None)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        runner = lambda x:time.sleep(0.1)
        start_time = time.time()
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.01, debug=True)
        duration_single = time.time() - start_time
        
        rpc.download_task.side_effect = return_once([task_cfg], [task_cfg2], end_value=None)
        start_time = time.time()
        p = pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=0.01, debug=True)
        duration_double = time.time() - start_time
        logger.info('single: %f - double: %f', duration_single, duration_double)
        self.assertGreater(duration_double, duration_single+0.05)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(pilot_test))
    suite.addTests(loader.loadTestsFromNames(alltests,pilot_test))
    alltests = glob_tests(loader.getTestCaseNames(pilot_multi_test))
    suite.addTests(loader.loadTestsFromNames(alltests,pilot_multi_test))
    return suite
