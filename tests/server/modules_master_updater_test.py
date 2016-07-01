"""
Test script for config module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, messaging_mock

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

from flexmock import flexmock


import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server import basic_config
import iceprod.server.modules.master_updater as master_updater_module
from iceprod.server.modules.master_updater import master_updater

class modules_master_updater_test(unittest.TestCase):
    def setUp(self):
        super(modules_master_updater_test,self).setUp()
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
        super(modules_master_updater_test,self).tearDown()

    @unittest_reporter
    def test_01_init(self):
        """Test init"""
        # mock some functions so we don't go too far
        def start():
            start.called = True
        flexmock(master_updater).should_receive('start').replace_with(start)
        start.called = False

        cfg = basic_config.BasicConfig()
        cfg.messaging_url = 'localhost'
        q = master_updater(cfg)
        if start.called != True:
            raise Exception('init did not call start')

        new_cfg = {'a':'test'}
        q.messaging = messaging_mock()

        q.service_class.reload(cfg=new_cfg)
        if q.cfg != new_cfg:
            raise Exception('q.cfg != new_cfg')

    @unittest_reporter
    def test_02_add(self):
        """Test add"""
        # mock some functions so we don't go too far
        def start():
            start.called = True
        flexmock(master_updater).should_receive('start').replace_with(start)
        start.called = False
        def send(*args,**kwargs):
            send.args = args
            send.kwargs = kwargs
            send.called = True
        flexmock(master_updater).should_receive('_send').replace_with(send)
        send.called = False


        cfg = basic_config.BasicConfig()
        cfg.messaging_url = 'localhost'
        q = master_updater(cfg)
        q.messaging = messaging_mock()

        def cb(ret=None):
            cb.ret = ret

        q.send_in_progress = True
        cb.ret = None
        message = 'a message'
        q.service_class.add(message,callback=cb)
        if cb.ret is not True:
            raise Exception('add() did not return True')
        if not q.buffer or q.buffer[0] != message:
            raise Exception('message not in buffer')
        if send.called:
            raise Exception('send called when send_in_progress')

        q.buffer.clear()
        q.send_in_progress = False
        cb.ret = None
        q.service_class.add(message,callback=cb)
        if cb.ret is not True:
            raise Exception('add() did not return True')
        if not q.buffer or q.buffer[0] != message:
            raise Exception('message not in buffer')
        if not send.called:
            raise Exception('send not called')

    @unittest_reporter
    def test_03_send(self):
        """Test _send"""
        # mock some functions so we don't go too far
        def start():
            start.called = True
        flexmock(master_updater).should_receive('start').replace_with(start)
        start.called = False
        def send(*args,**kwargs):
            send.args = args
            send.kwargs = kwargs
            send.called = True
        flexmock(master_updater_module).should_receive('send_master').replace_with(send)
        send.called = False


        cfg = basic_config.BasicConfig()
        cfg.messaging_url = 'localhost'
        q = master_updater(cfg)

        q._send()
        if send.called:
            raise Exception('send called for no msg')
        if q.send_in_progress is True:
            raise Exception('send_in_progress is True')

        msg = 'test message'
        q.buffer.append(msg)
        q._send()
        if not send.called:
            raise Exception('send not called')
        if q.send_in_progress is not True:
            raise Exception('send_in_progress not True')
        send.kwargs['callback'](Exception())
        if not q.buffer or q.buffer[0] != msg:
            raise Exception('message not in buffer')

        send.kwargs['callback']()
        if q.buffer:
            raise Exception('message still in buffer')
        if q.send_in_progress is True:
            raise Exception('did not disable send_in_progress')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_master_updater_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_master_updater_test))
    return suite
