"""
Test script for i3exec
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('i3exec_test')

import os, sys, time
import shutil
import tempfile
import random
import string
import subprocess
import threading
from functools import partial
import asyncio
import unittest
from unittest.mock import patch
from contextlib import contextmanager

from iceprod.core import to_log
import iceprod.core.dataclasses
import iceprod.core.functions
import iceprod.core.serialization
import iceprod.core.logger
from iceprod.core import jsonUtil
from iceprod.core import i3exec


from .exe_test import DownloadTestCase

class i3exec_test(DownloadTestCase):
    def setUp(self):
        super(i3exec_test,self).setUp()

    def make_config(self):
        # create basic config file
        config = iceprod.core.dataclasses.Job()
        config['options']['job_temp'] = os.path.join(self.test_dir,'job_temp')
        config['options']['local_temp'] = os.path.join(self.test_dir,'local_temp')
        config['options']['data_directory'] = os.path.join(self.test_dir,'data')
        config['options']['loglevel'] = 'info'
        config['options']['task_id'] = 'a'
        config['options']['dataset_id'] = 'a'
        config['options']['job'] = 0
        config['options']['gridspec'] = 'foo.bar'
        config['steering'] = iceprod.core.dataclasses.Steering()
        return config

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='runner() basic')
    async def test_01(self, download):
        # create basic config
        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        async def create(*args, **kwargs):
            path = os.path.join(config['options']['local_temp'], mod['src'])
            self.mk_files(path, """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 0
""", ext=True)
            return path
        download.side_effect = create

        # try to run the config
        url = 'http://foo/'
        run = partial(i3exec.runner, config, url, debug=True,
                      offline=True)
        async for proc in run():
            await proc.wait()

    @unittest_reporter(name='runner() bad config')
    async def test_02(self):
        """Test not providing a steering file"""
        url = 'http://foo/'
        run = partial(i3exec.runner, None, url, debug=True,
                      offline=True)
        with self.assertRaises(Exception):
            async for proc in run():
                await proc.wait()

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='runner() specific task')
    async def test_10(self, download):
        # create basic config file
        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        async def create(*args, **kwargs):
            path = os.path.join(config['options']['local_temp'], mod['src'])
            self.mk_files(path, """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 0
""", ext=True)
            return path
        download.side_effect = create

        # try to run the config
        url = 'http://foo/'
        run = partial(i3exec.runner, config, url, debug=True,
                      offline=True)
        async for proc in run():
            await proc.wait()

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='runner() .so lib')
    async def test_11(self, download):
        """Test multiple tasks"""
        # create basic config file
        config = self.make_config()

        # create the task object
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'

        config['tasks'].append(task)

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'
        module['env_clear'] = False

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'mytest.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray2'
        tray['iterations'] = 3

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'
        module['env_clear'] = False

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'mytest.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

        # make .so file
        so = self.make_shared_lib()

        async def create(url, *args, **kwargs):
            if url.endswith(c['src']):
                path = os.path.join(config['options']['local_temp'], c['src'])
                self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            else:
                path = os.path.join(config['options']['local_temp'], module['src'])
                self.mk_files(path, """
def Test():
    return 'Tester2'
""", ext=True)
            return path
        download.side_effect = create

        # try to run the config
        url = 'http://foo/'
        run = partial(i3exec.runner, config, url, debug=True,
                      offline=True)
        async for proc in run():
            await proc.wait()

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='runner() failing task')
    async def test_20(self, download):
        # create basic config file
        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        async def create(*args, **kwargs):
            path = os.path.join(config['options']['local_temp'], mod['src'])
            self.mk_files(path, """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        raise Exception()
""", ext=True)
            return path
        download.side_effect = create

        # try to run the config
        url = 'http://foo/'
        run = partial(i3exec.runner, config, url, debug=True,
                      offline=True)

        # try to run the config
        with self.assertRaises(Exception):
            async for proc in run():
                await proc.wait()


    @patch('iceprod.core.i3exec.runner')
    @patch('iceprod.core.logger.set_logger')
    @unittest_reporter(name='main() offline')
    def test_90(self, logger, runner):
        async def run(*args, **kwargs):
            run.called = True
            yield await asyncio.create_subprocess_exec('sleep','0.1')
        run.called = False
        runner.side_effect = run

        config = self.make_config()
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        i3exec.main(cfgfile, offline=True, debug=False)

        self.assertTrue(run.called)

        with self.assertRaises(Exception):
            i3exec.main(None, offline=True, debug=True)

    @patch('iceprod.core.i3exec.runner')
    @patch('iceprod.core.logger.set_logger')
    @unittest_reporter(name='main() error')
    def test_91(self, logger, runner):
        async def run(*args, **kwargs):
            run.called = True
            yield await asyncio.create_subprocess_exec('sleep','0.1')
            raise Exception()
        run.called = False
        runner.side_effect = run

        config = self.make_config()
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        with self.assertRaises(Exception):
            i3exec.main(cfgfile, offline=True, logfile='foo', debug=True)

        self.assertTrue(run.called)
        self.assertEqual(logger.call_args[1]['logfile'], os.path.join(self.test_dir,'foo'))

    @patch('iceprod.core.i3exec.runner')
    @patch('iceprod.core.i3exec.ServerComms')
    @patch('iceprod.core.logger.set_logger')
    @patch('iceprod.core.i3exec.to_file')
    @unittest_reporter(name='main() online')
    def test_92(self, to_file, logger, comms, runner):
        async def run(*args, **kwargs):
            run.called = True
            yield await asyncio.create_subprocess_exec('sleep','0.1')
        run.called = False
        runner.side_effect = run
        async def processing(*args, **kwargs):
            pass
        comms.return_value.processing.side_effect = processing
        @contextmanager
        def to_file2(*args,**kwargs):
            yield
        to_file.side_effect = to_file2

        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        i3exec.main(config, url='http://foo')

        self.assertTrue(run.called)

        # test Comms
        run.called = False
        config['options']['username'] = 'u'
        config['options']['password'] = 'p'
        config['options']['ssl'] = {'key': 'k'}
        comms.return_value.processing.side_effect = Exception()

        i3exec.main(config, url='http://foo', passkey='pk')
        self.assertTrue(run.called)
        comms.assert_any_call('http://foo/jsonrpc', 'pk', None,
                username='u', password='p', key='k')

        # test errors
        del config['options']['task_id']

        with self.assertRaises(Exception):
            i3exec.main(config, url='http://foo')
            
        with self.assertRaises(Exception):
            i3exec.main(config)

    @patch('iceprod.core.pilot.Pilot')
    @patch('iceprod.core.i3exec.ServerComms')
    @patch('iceprod.core.logger.set_logger')
    @unittest_reporter(name='main() pilot')
    def test_93(self, logger, comms, pilot):
        class Run:
            def __init__(self,*args, **kwargs):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc, tb):
                pass
            async def run(self, *args, **kwargs):
                Run.called = True
        Run.called = False
        pilot.side_effect = Run

        config = self.make_config()
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        i3exec.main(cfgfile, url='http://foo', pilot_id='p')

        self.assertTrue(Run.called)
        
        # test run timeout
        config = self.make_config()
        config['options']['run_timeout'] = 20
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        Run.called = False
        i3exec.main(cfgfile, url='http://foo', pilot_id='p')
        self.assertTrue(Run.called)

        # now test errors
        with self.assertRaises(Exception):
            i3exec.main(cfgfile, url='http://foo')
            
        config = self.make_config()
        del config['options']['gridspec']
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)
        
        with self.assertRaises(Exception):
            i3exec.main(cfgfile, url='http://foo', pilot_id='p')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(i3exec_test))
    suite.addTests(loader.loadTestsFromNames(alltests,i3exec_test))
    return suite