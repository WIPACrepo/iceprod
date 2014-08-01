"""
Test script for the schedule module.
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

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

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock


import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server import basic_config
from iceprod.server.modules.schedule import schedule


class _messaging(object):
    def __init__(self):
        self.called = False
        self.args = []
        self.ret = None
    def start(self):
        pass
    def stop(self):
        pass
    def kill(self):
        pass
    def __request(self, service, method, kwargs):
        self.called = [service,method,kwargs]
        if 'callback' in kwargs:
            if ret:
                kwargs['callback'](ret)
            else:
                kwargs['callback']()
        elif 'async' in kwargs and kwargs['async'] is False:
            return ret
    def __getattr__(self,name):
        class _Method:
            def __init__(self,send,service,name):
                self.__send = send
                self.__service = service
                self.__name = name
            def __getattr__(self,name):
                return _Method(self.__send,self.__service,
                               "%s.%s"%(self.__name,name))
            def __call__(self,**kwargs):
                return self.__send(self.__service,self.__name,kwargs)
        class _Service:
            def __init__(self,send,service):
                self.__send = send
                self.__service = service
            def __getattr__(self,name):
                return _Method(self.__send,self.__service,name)
            def __call__(self,**kwargs):
                raise Exception('Service %s, method name not specified'%(
                                self.__service))
        return _Service(self.__request,name)

class _Schedule(object):
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
        self.started = False
        self.finished = False
        self.scheduled = False
        self.restarted = False
    def start(self):
        self.started = True
    def finish(self):
        self.finished = True
    def join(self,*args,**kwargs):
        pass
    def is_alive(self):
        return False
    def schedule(self,*args,**kwargs):
        self.scheduled = True
        self.args = args
        self.kwargs = kwargs

class modules_schedule_test(unittest.TestCase):
    def setUp(self):
        super(modules_schedule_test,self).setUp()
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
        super(modules_schedule_test,self).tearDown()
    
    def test_01_init(self):
        """Test init"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(schedule).should_receive('start').replace_with(start)
            start.called = False
            
            bcfg = basic_config.BasicConfig()
            bcfg.messaging_url = 'localhost'
            q = schedule(bcfg)
            if not q:
                raise Exception('did not return schedule object')
            if start.called != True:
                raise Exception('init did not call start')
            
            q.messaging = _messaging()
            
            new_cfg = {'new':1}
            q.messaging.BROADCAST.reload(cfg=new_cfg)
            if not q.messaging.called:
                raise Exception('init did not call messaging')
            if q.messaging.called != ['BROADCAST','reload',{'cfg':new_cfg}]:
                raise Exception('init did not call correct message')
            
        except Exception as e:
            logger.error('Error running modules.schedule init test - %s',str(e))
            printer('Test modules.schedule init',False)
            raise
        else:
            printer('Test modules.schedule init')
    
    def test_02_make_schedule(self):
        """Test make_schedule"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(schedule).should_receive('start').replace_with(start)
            start.called = False
            
            bcfg = basic_config.BasicConfig()
            bcfg.messaging_url = 'localhost'
            q = schedule(bcfg)
            q.messaging = _messaging()
            
            q.scheduler = _Schedule()
            q._make_schedule()
            if q.scheduler.scheduled is not True:
                raise Exception('nothing scheduled')
            
        except Exception as e:
            logger.error('Error running modules.schedule make_schedule test - %s',str(e))
            printer('Test modules.schedule make_schedule',False)
            raise
        else:
            printer('Test modules.schedule make_schedule')
    
    def test_03_start_stop(self):
        """Test start_stop"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(schedule).should_receive('start').replace_with(start)
            start.called = False
            
            bcfg = basic_config.BasicConfig()
            bcfg.messaging_url = 'localhost'
            q = schedule(bcfg)
            q.messaging = _messaging()
            sch = _Schedule()
            q.scheduler = sch
            
            q.start()
            if start.called is not True:
                raise Exception('did not start')
            
            q.scheduler.start()
            if sch.started is not True:
                raise Exeption('did not start scheduler')
            
            q.stop()
            if sch.finished is not True:
                raise Exception('did not finish scheduler')
            
            sch.finished = False
            q.scheduler = sch
            q.kill()
            if sch.finished is not True:
                raise Exception('did not finish scheduler on kill')
            
            q.scheduler = None
            try:
                q.stop()
                q.kill()
            except Exception:
                logger.info('exception raised',exc_info=True)
                raise Exception('scheduler = None and exception raised')
            
        except Exception as e:
            logger.error('Error running modules.schedule start_stop test - %s',str(e))
            printer('Test modules.schedule start_stop',False)
            raise
        else:
            printer('Test modules.schedule start_stop')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_schedule_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_schedule_test))
    return suite
