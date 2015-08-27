"""
  Test script for main iceprod server

  copyright (c) 2013 the icecube collaboration
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('iceprod_server_test')

import os
import sys
import time
import random
from datetime import datetime,timedelta
from contextlib import contextmanager
from functools import partial
import shutil
import subprocess
import signal
import threading
import multiprocessing
import multiprocessing.queues
import _multiprocessing

try:
    import cPickle as pickle
except:
    import pickle

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

sys.path.append('bin')
import iceprod_server
import iceprod.server
import iceprod.server.basic_config
try:
    import iceprod.procname
except ImportError:
    pass

class iceprod_server_test(unittest.TestCase):
    def setUp(self):
        super(iceprod_server_test,self).setUp()

        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)

        # override listmodules, run_module
        self.listmodules_called = False
        self.listmodules_ret = {}
        self.run_module_called = False
        self.run_module_exception = False
        self.run_module_name = None
        self.run_module_args = None
        self.run_module_ret = {}
        flexmock(iceprod.server).should_receive('listmodules').replace_with(self._listmodules)
        flexmock(iceprod.server).should_receive('run_module').replace_with(self._run_module)
        def setprocname(*args,**kwargs):
            pass
        try:
            flexmock(iceprod.procname).should_receive('setprocname').replace_with(setprocname)
        except:
            pass

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(iceprod_server_test,self).tearDown()


    def _listmodules(self,package_name=''):
        self.listmodules_called = True
        if package_name and package_name in self.listmodules_ret:
            return self.listmodules_ret[package_name]
        else:
            return []

    def _run_module(self,name,args):
        self.run_module_called = True
        self.run_module_name = name
        self.run_module_args = args
        if name and args and name in self.run_module_ret:
            self.run_module_ret[name](args)
        else:
            self.run_module_exception = True
            raise Exception('could not run module')

    @unittest_reporter
    def test_001_load_config(self):
        """Test load_config"""
        cfgfile = os.path.join(self.test_dir,'cfg')
        with open(cfgfile,'w') as f:
            p = partial(print,sep='',file=f)
            p('[modules]')
            p('queue = True')
            p('db = True')
            p('website = True')

        cfg = iceprod_server.load_config(cfgfile)

        if cfg is None:
            raise Exception('did not set cfg')
        if cfg.queue is not True:
            raise Exception('did not set cfg.queue')
        if cfg.queue is not True:
            raise Exception('did not set cfg.db')
        if cfg.queue is not True:
            raise Exception('did not set cfg.website')

    @unittest_reporter
    def test_002_server_module(self):
        """Test server_module"""
        iceprod_server.server_module.process_class = threading.Thread
        def test_mod(args):
            test_mod.called = True
            test_mod.args = args
            cfg = args
        self.run_module_ret = {'test':test_mod}

        cfg = iceprod.server.basic_config.BasicConfig()

        # test successful module
        s = iceprod_server.server_module('test',cfg)

        # test starting
        test_mod.called = False
        s.process.daemon = True
        s.start()
        time.sleep(1)
        if not test_mod.called:
            raise Exception('failed to call test_mod')
        logger.info('mod args: %r',test_mod.args)
        if test_mod.args != cfg:
            raise Exception('cfg not passed as args')

    @unittest_reporter
    def test_010_main(self):
        """Test main"""
        iceprod_server.server_module.process_class = threading.Thread
        cfg = iceprod.server.basic_config.BasicConfig()
        cfg.test = True
        cfg.start_order = ['test']

        def test_mod(args):
            test_mod.called = True
            test_mod.args = args
            cfg = args
        self.listmodules_ret = {'iceprod.server.modules':['iceprod.server.modules.test']}
        self.run_module_ret = {'iceprod.server.modules.test':test_mod}

        def sig(*args):
            sig.called = True
        def log(*args):
            log.called = True
        flexmock(iceprod_server).should_receive('set_signals').replace_with(sig)
        flexmock(iceprod_server).should_receive('set_logger').replace_with(log)

        class rpc:
            def __init__(self,**kwargs):
                pass
            def setup(*args,**kwargs):
                pass
            def start(*args,**kwargs):
                pass
            def stop(*args,**kwargs):
                pass
        flexmock(iceprod.server.RPCinternal).should_receive('RPCService').replace_with(rpc)

        # test successful module
        test_mod.called = False
        def run():
            s = iceprod_server.main(cfg)
        t = threading.Thread(target=run)
        t.daemon = True
        t.start()
        time.sleep(0.1)

        if not sig.called:
            raise Exception('failed to call sig')
        if not log.called:
            raise Exception('failed to call log')
        if not test_mod.called:
            raise Exception('failed to call test_mod')
        logger.info('mod args: %r',test_mod.args)
        if test_mod.args != cfg:
            raise Exception('args != cfg')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(iceprod_server_test))
    suite.addTests(loader.loadTestsFromNames(alltests,iceprod_server_test))
    return suite
