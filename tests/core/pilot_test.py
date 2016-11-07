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

try:
    from unittest import mock
except ImportError:
    import mock

from iceprod.core import to_log,constants
from iceprod.core import pilot
import iceprod.core.exe_json

try:
    import psutil as normal_psutil
except ImportError:
    normal_psutil = None

class pilot_test(unittest.TestCase):
    def setUp(self):
        super(pilot_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        maindir = os.getcwd()
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(maindir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        # disable psutil
        pilot.psutil = None
        def c():
            pilot.psutil = normal_psutil
        self.addCleanup(c)

        # mock iceprod.core.logger.new_file
        patcher = mock.patch('iceprod.core.logger.new_file')
        patcher.start()
        self.addCleanup(patcher.stop)

        # convert multiprocessing to direct calls
        patcher = mock.patch('iceprod.core.pilot.Process')
        self.process = patcher.start()
        def run(target):
            target()
            ret = mock.MagicMock()
            ret.is_alive.return_value = False
            return ret
        self.process.side_effect = run
        self.addCleanup(patcher.stop)

    @mock.patch('iceprod.core.pilot.Pilot.run')
    @unittest_reporter(name='Pilot.__init__()')
    def test_01_pilot_init(self, run):
        run.return_value = None
        cfg = {'options':{'gridspec':'a'}}
        runner = None
        p = pilot.Pilot(cfg, runner, pilot_id='a')
        run.assert_called_once_with()

    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.run()')
    def test_02_pilot_run(self, download, update):
        task_cfg = {'options':{'task_id':'a'}}
        download.side_effect = return_once(task_cfg, end_value=None)
        cfg = {'options':{'gridspec':'a'}}
        runner = mock.MagicMock()
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.001)
        runner.assert_called_once_with(task_cfg)
        update.assert_has_calls([mock.call('a',tasks='a'), mock.call('a',tasks='')])

    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.run() split resources')
    def test_03_pilot_resources(self, download, update):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        download.side_effect = return_once(task_cfg, task_cfg2, end_value=None)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':2,'memory':2,'disk':2}}}
        runner = mock.MagicMock()
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.001)
        runner.assert_has_calls([mock.call(task_cfg), mock.call(task_cfg2)])
        self.assertGreaterEqual(update.call_count, 3)
        update.assert_any_call('a',tasks='a')
        for call in update.call_args_list:
            if call[1]['tasks'] == 'a,b':
                break
        else:
            raise Exception('did not update_pilot with both tasks running')
        update.assert_called_with('a',tasks='')

    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.run() error downloading')
    def test_04_pilot_resources(self, download, update):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        download.side_effect = return_once(task_cfg, task_cfg2)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        runner = mock.MagicMock()
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.001)
        runner.assert_has_calls([mock.call(task_cfg), mock.call(task_cfg2)])
        self.assertGreaterEqual(update.call_count, 3)
        update.assert_any_call('a',tasks='a')
        for call in update.call_args_list:
            if call[1]['tasks'] == 'a,b':
                break
        else:
            raise Exception('did not update_pilot with both tasks running')
        update.assert_called_with('a',tasks='')

    @mock.patch('iceprod.core.exe_json.task_kill')
    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.create() error')
    def test_10_pilot_create_error(self, download, update, kill):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':3,'memory':1,'disk':1}}}
        download.side_effect = return_once(task_cfg, end_value=None)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        runner = mock.MagicMock()
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.001)
        runner.assert_not_called()
        update.assert_not_called()
        kill.assert_called()

    @unittest_reporter
    def test_30_du(self):
        du_dir = os.path.join(self.test_dir,'test')
        os.mkdir(du_dir)
        for f in ('a','b','c'):
            path = os.path.join(du_dir,f)
            open(path,'w').write('a'*100)
        self.assertEqual(pilot.du(du_dir), 300)

    @unittest_reporter(name='du() symlink')
    def test_31_du_symlink(self):
        du_dir = os.path.join(self.test_dir,'test')
        os.mkdir(du_dir)
        for f in ('a','b','c'):
            path = os.path.join(du_dir,f)
            open(path,'w').write('a'*100)
        os.symlink(os.path.join(du_dir,'a'), os.path.join(du_dir,'l'))
        self.assertEqual(pilot.du(du_dir), 300)

    @unittest_reporter(name='du() dir + symlink')
    def test_32_du_dir_symlink(self):
        du_dir = os.path.join(self.test_dir,'test')
        os.mkdir(du_dir)
        for f in ('a','b','c'):
            path = os.path.join(du_dir,f)
            open(path,'w').write('a'*100)
        os.symlink(os.path.join(du_dir,'a'), os.path.join(du_dir,'l'))
        os.mkdir(os.path.join(du_dir,'subdir'))
        for f in ('a','b','c'):
            path = os.path.join(du_dir,'subdir',f)
            open(path,'w').write('a'*100)
        os.symlink(os.path.join(du_dir,'subdir'), os.path.join(du_dir,'s2'))
        os.symlink(os.path.join(du_dir,'subdir','a'), os.path.join(du_dir,'subdir','s3'))
        self.assertEqual(pilot.du(du_dir), 600)


class pilot_multi_test(unittest.TestCase):
    def setUp(self):
        super(pilot_multi_test,self).setUp()

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

    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.monitor()')
    def test_01_pilot_monitoring(self, download, update):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        download.side_effect = return_once(task_cfg, end_value=None)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        runner = lambda x:time.sleep(0.2)
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=10.01)
        update.assert_has_calls([mock.call('a',tasks='a'), mock.call('a',tasks='')])

    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.monitor() over limit')
    def test_02_pilot_monitor_over_limit(self, download, update):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':0.00001,'disk':1}}}
        download.side_effect = return_once(task_cfg, end_value=None)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        def runner(cfg):
            x = [random.random() for _ in range(1000)]
            time.sleep(random.random()+0.1)
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.1)
        update.assert_any_call('a',tasks='a')
        update.assert_called_with('a',tasks='')

    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.run() multiprocess error downloading')
    def test_04_pilot_multiprocess_error_downloading(self, download, update):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        download.side_effect = return_once(task_cfg, task_cfg2)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        runner = lambda x:time.sleep(random.random())
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.01)
        self.assertGreaterEqual(update.call_count, 3)
        update.assert_any_call('a',tasks='a')
        for call in update.call_args_list:
            if call[1]['tasks'] == 'a,b':
                break
        else:
            raise Exception('did not update_pilot with both tasks running')
        update.assert_called_with('a',tasks='')

    @mock.patch('iceprod.core.exe_json.update_pilot')
    @mock.patch('iceprod.core.exe_json.downloadtask')
    @unittest_reporter(name='Pilot.run() no resource specified, sequential')
    def test_10_pilot_sequential(self, download, update):
        task_cfg = {'options':{'task_id':'a'}}
        task_cfg2 = {'options':{'task_id':'b'}}
        download.side_effect = return_once(dict(task_cfg), end_value=None)
        cfg = {'options':{'gridspec':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        runner = lambda x:time.sleep(0.1)
        start_time = time.time()
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.01)
        duration_single = time.time() - start_time
        
        download.side_effect = return_once(task_cfg, None, task_cfg2, end_value=None)
        start_time = time.time()
        p = pilot.Pilot(cfg, runner, pilot_id='a', run_timeout=0.01)
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
