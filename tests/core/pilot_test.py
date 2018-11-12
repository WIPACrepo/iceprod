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
from unittest import mock
import asyncio

from tornado.testing import AsyncTestCase

from iceprod.core import to_log,constants
from iceprod.core import pilot
from iceprod.core import resources

try:
    import psutil as normal_psutil
except ImportError:
    normal_psutil = None

class TestBase(AsyncTestCase):
    def setUp(self):
        super(TestBase,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        maindir = os.getcwd()
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(maindir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

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

        self.pilot_args = {
            'run_timeout': 0.001,
            'backoff_delay': 0.000001,
        }

class pilot_test(TestBase):
    def setUp(self):
        super(pilot_test,self).setUp()

        # disable psutil
        pilot.psutil = None
        def c():
            pilot.psutil = normal_psutil
        self.addCleanup(c)

    @unittest_reporter(name='Pilot.__init__()')
    def test_001_pilot_init(self):
        cfg = {'options':{'gridspec':'a'}}
        runner = None
        p = pilot.Pilot(cfg, runner, pilot_id='a')

    @unittest_reporter(name='Pilot.run()')
    async def test_012_pilot_run(self):
        task_cfg = {'options':{'task_id':'a'}}
        cfg = {'options':{'gridspec':'a'}}
        async def download_task(*args, **kwargs):
            logging.debug('download_task')
            if download_task.once:
                download_task.once = False
                return [task_cfg]
            return None
        download_task.once = True
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = download_task
        update_args = []
        async def update_pilot(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            update_args.append((args,kwargs))
        rpc.update_pilot.side_effect = update_pilot
        async def runner(*args, **kwargs):
            logging.debug('runner - before')
            yield await asyncio.create_subprocess_exec('sleep','0.1')
            logging.debug('runner - after')
            runner.called = True
        runner.called = False
        async with pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, **self.pilot_args) as p:
            await p.run()
        self.assertTrue(runner.called)
        self.assertEqual(update_args[0][0], ('a',))
        self.assertEqual(update_args[0][1]['tasks'], ['a'])
        self.assertEqual(update_args[1][0], ('a',))
        self.assertEqual(update_args[1][1]['tasks'], [])

    @unittest_reporter(name='Pilot.run() split resources')
    async def test_013_pilot_resources(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':2,'memory':2.2,'disk':2.2}}}
        async def download_task(*args, **kwargs):
            logging.debug('download_task')
            if download_task.items:
                return download_task.items.pop(0)
            return None
        download_task.items = [[task_cfg], [task_cfg2]]
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = download_task
        update_args = []
        async def update_pilot(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            update_args.append((args,kwargs))
        rpc.update_pilot.side_effect = update_pilot
        async def still_running(*args, **kwargs):
            return True
        rpc.still_running.side_effect = still_running
        async def runner(*args, **kwargs):
            logging.debug('runner - before')
            yield await asyncio.create_subprocess_exec('sleep','0.1')
            logging.debug('runner - after')
            runner.called = True
        runner.called = False
        async with pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, **self.pilot_args) as p:
            await p.run()
        self.assertTrue(runner.called)
        for call in update_args:
            if call[1]['tasks'] == ['a','b']:
                break
        else:
            raise Exception('did not update_pilot with both tasks running')

    @unittest_reporter(name='Pilot.run() error downloading')
    async def test_014_pilot(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        task_cfg2 = {'options':{'task_id':'b','resources':{'cpu':1,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        async def download_task(*args, **kwargs):
            logging.debug('download_task')
            if download_task.items:
                return download_task.items.pop(0)
            raise Exception('failed download')
        download_task.items = [[task_cfg], [task_cfg2]]
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = download_task
        update_args = []
        async def update_pilot(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            update_args.append((args,kwargs))
        rpc.update_pilot.side_effect = update_pilot
        async def still_running(*args, **kwargs):
            return True
        rpc.still_running.side_effect = still_running
        async def runner(*args, **kwargs):
            logging.debug('runner - before')
            yield await asyncio.create_subprocess_exec('sleep','0.1')
            logging.debug('runner - after')
            runner.called = True
        runner.called = False
        async with pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, **self.pilot_args) as p:
            with self.assertRaises(Exception):
                await p.run()
        self.assertTrue(runner.called)
        self.assertGreaterEqual(len(update_args), 3)
        for call in update_args:
            if call[1]['tasks'] == ['a','b']:
                break
        else:
            raise Exception('did not update_pilot with both tasks running')

    @unittest_reporter(name='Pilot.create() error')
    async def test_020_pilot_create_error(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':3,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':1,'memory':1.1,'disk':1.1}}}
        async def download_task(*args, **kwargs):
            logging.debug('download_task')
            if download_task.items:
                return download_task.items.pop(0)
            return None
        download_task.items = [[task_cfg]]
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = download_task
        async def task_kill(*args, **kwargs):
            return True
        rpc.task_kill.side_effect = task_kill
        update_args = []
        async def update_pilot(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            update_args.append((args,kwargs))
        rpc.update_pilot.side_effect = update_pilot
        runner = None
        async with pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, **self.pilot_args) as p:
            await p.run()

class pilot_multi_test(TestBase):
    @unittest_reporter(name='Pilot.monitor()')
    async def test_100_pilot_monitoring(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3}}}
        async def download_task(*args, **kwargs):
            logging.debug('download_task')
            if download_task.once:
                download_task.once = False
                return [task_cfg]
            return None
        download_task.once = True
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = download_task
        update_args = []
        async def update_pilot(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            update_args.append((args,kwargs))
        rpc.update_pilot.side_effect = update_pilot
        async def still_running(*args, **kwargs):
            return True
        rpc.still_running.side_effect = still_running
        async def runner(*args, **kwargs):
            logging.debug('runner - before')
            yield await asyncio.create_subprocess_exec('sleep','2.0')
            logging.debug('runner - after')
            runner.called = True
        runner.called = False
        async with pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=2.1, debug=True) as p:
            await p.run()
        self.assertTrue(runner.called)
        self.assertEqual(update_args[0][0], ('a',))
        self.assertEqual(update_args[0][1]['tasks'], ['a'])
        self.assertEqual(update_args[1][0], ('a',))
        self.assertEqual(update_args[1][1]['tasks'], [])

    @unittest_reporter(name='Pilot.monitor() over limit')
    async def test_102_pilot_monitor_over_limit(self):
        task_cfg = {'options':{'task_id':'a','resources':{'cpu':1,'memory':0.00001,'disk':1,'time':0.001}}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':3,'memory':3,'disk':3,'time':0.102}}}
        async def download_task(*args, **kwargs):
            logging.debug('download_task')
            if download_task.once:
                download_task.once = False
                return [task_cfg]
            return None
        download_task.once = True
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = download_task
        update_args = []
        async def update_pilot(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            update_args.append((args,kwargs))
        rpc.update_pilot.side_effect = update_pilot
        async def still_running(*args, **kwargs):
            return True
        rpc.still_running.side_effect = still_running
        async def task_kill(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            task_kill.called = True
        task_kill.called = True
        rpc.task_kill.side_effect = task_kill
        async def runner(*args, **kwargs):
            logging.debug('runner - before')
            yield await asyncio.create_subprocess_exec('sleep','1.1')
            logging.debug('runner - after')
            runner.called = True
        runner.called = False
        async with pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, run_timeout=1.2) as p:
            await p.run()
        self.assertTrue(task_kill.called)

    @unittest_reporter(name='Pilot.run() no resource specified, sequential')
    async def test_110_pilot_sequential(self):
        task_cfg = {'options':{'task_id':'a'}}
        task_cfg2 = {'options':{'task_id':'b'}}
        cfg = {'options':{'gridspec':'a','resources':{'cpu':1,'memory':1,'disk':1}}}
        async def download_task(*args, **kwargs):
            logging.debug('download_task')
            if download_task.items:
                return download_task.items.pop(0)
            return None
        download_task.items = [[task_cfg], [task_cfg2]]
        rpc = mock.MagicMock()
        rpc.download_task.side_effect = download_task
        async def still_running(*args, **kwargs):
            return True
        rpc.still_running.side_effect = still_running
        update_args = []
        async def update_pilot(*args, **kwargs):
            logging.debug('update_pilot %r %r', args, kwargs)
            update_args.append((args,kwargs))
        rpc.update_pilot.side_effect = update_pilot
        async def runner(*args, **kwargs):
            logging.debug('runner - before')
            yield await asyncio.create_subprocess_exec('sleep','0.5')
            logging.debug('runner - after')
            runner.called = True
        runner.called = False
        start_time = time.time()
        async with pilot.Pilot(cfg, runner, pilot_id='a', rpc=rpc, **self.pilot_args) as p:
            await p.run()
        self.assertTrue(runner.called)
        self.assertGreater(time.time()-start_time, 1.0)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(pilot_test))
    suite.addTests(loader.loadTestsFromNames(alltests,pilot_test))
    alltests = glob_tests(loader.getTestCaseNames(pilot_multi_test))
    suite.addTests(loader.loadTestsFromNames(alltests,pilot_multi_test))
    return suite
