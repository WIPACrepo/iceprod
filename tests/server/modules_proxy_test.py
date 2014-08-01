"""
Test script for proxy module
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
from iceprod.server.modules.proxy import proxy


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

class _Squid(object):
    def __init__(self,**kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.killed = False
        self.updated = False
        self.restarted = False
    def start(self):
        self.started = True
    def stop(self):
        self.stopped = True
    def kill(self):
        self.killed = True
    def update(self,**kwargs):
        self.updated = True
        self.kwargs = kwargs
    def restart(self):
        self.restarted = True

class modules_proxy_test(unittest.TestCase):
    def setUp(self):
        super(modules_proxy_test,self).setUp()
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
        super(modules_proxy_test,self).tearDown()
    
    def test_01_init(self):
        """Test init"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(proxy).should_receive('start').replace_with(start)
            start.called = False
            
            cfg = basic_config.BasicConfig()
            cfg.messaging_url = 'localhost'
            q = proxy(cfg)
            if not q:
                raise Exception('did not return proxy object')
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
            logger.error('Error running modules.proxy init test - %s',str(e))
            printer('Test modules.proxy init',False)
            raise
        else:
            printer('Test modules.proxy init')
    
    def test_02_getargs(self):
        """Test getargs"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(proxy).should_receive('start').replace_with(start)
            start.called = False
            
            cfg = basic_config.BasicConfig()
            cfg.messaging_url = 'localhost'
            q = proxy(cfg)
            q.messaging = _messaging()
            q.cfg = {}
            ret = q._getargs()
            if ret != {}:
                raise Exception('getargs did not return empty')
            
            proxy_cfg = {'test':1,'t2':[1,2,3]}
            q.cfg['proxy'] = proxy_cfg
            ret = q._getargs()
            if ret != proxy_cfg:
                raise Exception('getargs did not return proxy config')
            
            q.cfg['http_username'] = 'user'
            q.cfg['http_password'] = 'pass'
            ret = q._getargs()
            if 'username' not in ret or ret['username'] != 'user':
                raise Exception('getargs did not have username')
            if 'password' not in ret or ret['password'] != 'pass':
                raise Exception('getargs did not have password')
            
        except Exception as e:
            logger.error('Error running modules.proxy getargs test - %s',str(e))
            printer('Test modules.proxy getargs',False)
            raise
        else:
            printer('Test modules.proxy getargs')
    
    def test_03_start_stop(self):
        """Test start_stop"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(proxy).should_receive('start').replace_with(start)
            start.called = False
            
            cfg = basic_config.BasicConfig()
            cfg.messaging_url = 'localhost'
            q = proxy(cfg)
            q.messaging = _messaging()
            q.squid = _Squid()
            
            q.start()
            if start.called is not True:
                raise Exception('did not start')
            
            q.squid.start()
            if q.squid.started is not True:
                raise Exeption('did not start squid')
            
            q.stop()
            if q.squid.stopped is not True:
                raise Exception('did not stop squid')
            
            q.kill()
            if q.squid.killed is not True:
                raise Exception('did not kill squid')
            
            new_cfg = {'test':1,'proxy':{'test2':2}}
            q.update_cfg(new_cfg)
            if q.squid.updated is not True:
                raise Exception('did not update squid')
            if q.squid.kwargs != {'test2':2}:
                raise Exception('squid update did not have correct kwargs')
            if q.squid.restarted is not True:
                raise Exception('did not restart squid')
            
            q.squid = None
            try:
                q.stop()
                q.kill()
                q.update_cfg({})
            except Exception:
                logger.info('exception raised',exc_info=True)
                raise Exception('squid = None and exception raised')
            
        except Exception as e:
            logger.error('Error running modules.proxy start_stop test - %s',str(e))
            printer('Test modules.proxy start_stop',False)
            raise
        else:
            printer('Test modules.proxy start_stop')
    
    def test_04_proxyservice(self):
        """Test ProxyService"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(proxy).should_receive('start').replace_with(start)
            start.called = False
            
            cfg = basic_config.BasicConfig()
            cfg.messaging_url = 'localhost'
            q = proxy(cfg)
            q.messaging = _messaging()
            q.cfg = {}
            
            new_cfg = {'test':1}
            q.service_class.reload(cfg=new_cfg)
            if q.cfg != new_cfg:
                raise Exception('reload() did not update cfg')
            
            def cb():
                cb.called = True
            cb.called = False
            q.service_class.reload(cfg=new_cfg,callback=cb)
            if cb.called is not True:
                raise Exception('reload() did not call callback')
            
        except Exception as e:
            logger.error('Error running modules.proxy ProxyService test - %s',str(e))
            printer('Test modules.proxy ProxyService',False)
            raise
        else:
            printer('Test modules.proxy ProxyService')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_proxy_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_proxy_test))
    return suite
