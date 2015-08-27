"""
Test script for module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, _messaging

import logging
logger = logging.getLogger('module_test')

import os
import sys
import time
import random
from datetime import datetime,timedelta
from contextlib import contextmanager
from functools import partial
import shutil
import tempfile
import subprocess
import signal
import threading
import multiprocessing

try:
    import cPickle as pickle
except:
    import pickle

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

from iceprod.server import module
from iceprod.server import basic_config
import iceprod.core.logger

class module_test(unittest.TestCase):
    def setUp(self):
        super(module_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

        def sig(*args):
            sig.args = args
        flexmock(signal).should_receive('signal').replace_with(sig)
        def basicConfig(*args,**kwargs):
            pass
        flexmock(logging).should_receive('basicConfig').replace_with(basicConfig)
        def setLogger(*args,**kwargs):
            pass
        flexmock(iceprod.core.logger).should_receive('setlogger').replace_with(setLogger)
        def removestdout(*args,**kwargs):
            pass
        flexmock(iceprod.core.logger).should_receive('removestdout').replace_with(removestdout)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(module_test,self).tearDown()


    @unittest_reporter
    def test_01_init(self):
        """Test init"""
        # mock some functions so we don't go too far
        def start():
            start.called = True
        flexmock(module.module).should_receive('start').replace_with(start)
        start.called = False

        cfg = basic_config.BasicConfig()
        cfg.messaging_url = 'localhost'
        q = module.module(cfg)
        if not q:
            raise Exception('did not return module object')

        q.start()
        if start.called is not True:
            raise Exception('init did not call start')

        q.messaging = _messaging()
        new_cfg = {'new':1}
        q.messaging.BROADCAST.reload(cfg=new_cfg)
        if not q.messaging.called:
            raise Exception('init did not call messaging')
        if q.messaging.called != [['BROADCAST','reload',(),{'cfg':new_cfg}]]:
            raise Exception('init did not call correct message')

    @unittest_reporter
    def test_02_stop(self):
        """Test stop"""
        # mock some functions so we don't go too far
        def start():
            start.called = True
        flexmock(module.module).should_receive('start').replace_with(start)

        cfg = basic_config.BasicConfig()
        cfg.messaging_url = 'localhost'
        q = module.module(cfg)
        q.messaging = _messaging()
        if not q:
            raise Exception('did not return module object')

        q.start()

        try:
            q.stop()
        except Exception:
            logger.info('exception raised',exc_info=True)
            raise Exception('module stop and exception raised')
        if q.messaging._local_called != ['stop']:
            raise Exception('module did not call stop')

    @unittest_reporter
    def test_03_kill(self):
        """Test kill"""
        # mock some functions so we don't go too far
        def start():
            start.called = True
        flexmock(module.module).should_receive('start').replace_with(start)

        cfg = basic_config.BasicConfig()
        cfg.messaging_url = 'localhost'
        q = module.module(cfg)
        q.messaging = _messaging()
        if not q:
            raise Exception('did not return module object')

        q.start()

        try:
            q.kill()
        except Exception:
            logger.info('exception raised',exc_info=True)
            raise Exception('module kill and exception raised')
        if q.messaging._local_called != ['stop']:
            raise Exception('module did not call kill')

    @unittest_reporter
    def test_10_service(self):
        """Test service"""
        # mock some functions so we don't go too far
        def start():
            start.called = True
        flexmock(module.module).should_receive('start').replace_with(start)
        start.called = False

        def stop():
            stop.called = True
        flexmock(module.module).should_receive('stop').replace_with(stop)
        stop.called = False

        def kill():
            kill.called = True
        flexmock(module.module).should_receive('kill').replace_with(kill)
        kill.called = False

        cfg = basic_config.BasicConfig()
        cfg.messaging_url = 'localhost'
        q = module.module(cfg)
        q.messaging = _messaging()
        if not q:
            raise Exception('did not return module object')

        if not isinstance(q.service_class,module.Service):
            raise Exception('Service class not in use')

        if q.service_class.mod != q:
            raise Exception('service_class.mod != module')

        q.service_class.start()
        if not start.called:
            raise Exception('did not start module')

        q.service_class.stop()
        if not stop.called:
            raise Exception('did not stop module')

        q.service_class.kill()
        if not kill.called:
            raise Exception('did not kill module')

        start.called = False
        stop.called = False
        q.service_class.restart()
        if not stop.called:
            raise Exception('did not stop module on restart')
        if not start.called:
            raise Exception('did not start module on restart')

        start.called = False
        stop.called = False
        cfg = {'test':1}
        q.service_class.reload(cfg)
        if not stop.called:
            raise Exception('did not stop module on reload')
        if not start.called:
            raise Exception('did not start module on reload')
        if q.cfg != cfg:
            raise Exception('did not set cfg on reload')

        # and now with callbacks
        def cb():
            cb.called = True
        cb.called = False
        start.called = False
        q.service_class.start(callback=cb)
        if not start.called:
            raise Exception('did not start module')
        if not cb.called:
            raise Exception('start did not call callback')

        cb.called = False
        stop.called = False
        q.service_class.stop(callback=cb)
        if not stop.called:
            raise Exception('did not stop module')
        if not cb.called:
            raise Exception('stop did not call callback')

        cb.called = False
        kill.called = False
        q.service_class.kill(callback=cb)
        if not kill.called:
            raise Exception('did not kill module')
        if not cb.called:
            raise Exception('kill did not call callback')

        cb.called = False
        start.called = False
        stop.called = False
        q.service_class.restart(callback=cb)
        if not stop.called:
            raise Exception('did not stop module on restart')
        if not start.called:
            raise Exception('did not start module on restart')
        if not cb.called:
            raise Exception('restart did not call callback')

        cb.called = False
        start.called = False
        stop.called = False
        cfg = {'test':1}
        q.service_class.reload(cfg,callback=cb)
        if not stop.called:
            raise Exception('did not stop module on reload')
        if not start.called:
            raise Exception('did not start module on reload')
        if not cb.called:
            raise Exception('reload did not call callback')
        if q.cfg != cfg:
            raise Exception('did not set cfg on reload')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(module_test))
    suite.addTests(loader.loadTestsFromNames(alltests,module_test))
    return suite
