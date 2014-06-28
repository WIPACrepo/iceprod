"""
Test script for queue module
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests, _messaging

import logging
logger = logging.getLogger('queue_test')

import os
import sys
import time
import random
import signal
from datetime import datetime,timedelta
import shutil
import tempfile
from multiprocessing import Queue,Pipe

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock


import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server.modules.queue import queue

class queue_test(unittest.TestCase):
    def setUp(self):
        super(queue_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        
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
        super(queue_test,self).tearDown()
    
    
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
            return self.run_module_ret[name]
        else:
            self.run_module_exception = True
            raise Exception('could not run module')
    
    class _db(object):
        def __init__(self):
            self.called = False
            self.func_name = []
            self.args = []
            self.ret = {}
        def run(self):
            pass
        def stop(self):
            pass
        def __getattr__(self,name):
            def fun(*args,**kwargs):
                self.args.append((args,kwargs))
                if name in self.ret:
                    return self.ret[name]
                else:
                    raise Exception('db error')
            self.called = True
            self.func_name.append(name)
            return fun
    
    def test_01_init(self):
        """Test init"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            start.called = False
            
            url = 'localhost'
            q = queue(url)
            if not q:
                raise Exception('did not return queue object')
            if start.called is not True:
                raise Exception('init did not call start')
            
            q.messaging = _messaging()
            new_cfg = {'new':1}
            q.messaging.BROADCAST.reload(cfg=new_cfg)
            if not q.messaging.called:
                raise Exception('init did not call messaging')
            if q.messaging.called != [['BROADCAST','reload',(),{'cfg':new_cfg}]]:
                raise Exception('init did not call correct message')
            
        except Exception as e:
            logger.error('Error running queue init test - %s',str(e))
            printer('Test queue init',False)
            raise
        else:
            printer('Test queue init')
    
    def test_02_start_stop(self):
        """Test start_stop"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            class Test1(object):
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        time.sleep(1)
                    return func
            
            self.listmodules_called = False
            self.listmodules_ret = {'iceprod.server.plugins': [
                                            'iceprod.server.plugins.Test1']
                                       }
            self.run_module_called = False
            self.run_module_exception = False
            self.run_module_name = None
            self.run_module_args = None
            self.run_module_ret = {'iceprod.server.plugins.Test1':Test1()}
            
            # make cfg
            cfg = {'site_id':'thesite',
                   'queue':{'init_queue_interval':0.1,
                            'queue_interval':1,
                            'plugin1':{'type':'Test1','description':'d'},
                           }
                  }
            url = 'localhost'
            q = queue(url)
            q.messaging = _messaging()
            q.cfg = cfg
            if not q:
                raise Exception('did not return queue object')
            
            q._start()
            time.sleep(1)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            try:
                if not self.listmodules_called:
                    raise Exception('listmodules not called')
                if not self.run_module_called:
                    raise Exception('run_module not called')
                if self.run_module_exception:
                    logger.info('run_module name: %s',self.run_module_name)
                    logger.info('run_module args: %s',self.run_module_args)
                    raise Exception('run_module raised an Exception')
            finally:
                try:
                    q.stop()
                except Exception:
                    logger.info('exception raised',exc_info=True)
                    raise Exception('queue stop and exception raised')
                
                time.sleep(0.5)
                if q.queue_thread.is_alive():
                    raise Exception('queue thread still running')
            
        except Exception as e:
            logger.error('Error running queue start_stop test - %s',str(e))
            printer('Test queue start_stop',False)
            raise
        else:
            printer('Test queue start_stop')
    
    def test_03_start_kill(self):
        """Test start_kill"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            class Test1(object):
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        time.sleep(1)
                    return func
            
            self.listmodules_called = False
            self.listmodules_ret = {'iceprod.server.plugins': [
                                            'iceprod.server.plugins.Test1']
                                       }
            self.run_module_called = False
            self.run_module_exception = False
            self.run_module_name = None
            self.run_module_args = None
            self.run_module_ret = {'iceprod.server.plugins.Test1':Test1()}
            
            # make cfg
            cfg = {'site_id':'thesite',
                   'queue':{'init_queue_interval':0.1,
                            'queue_interval':1,
                            'plugin1':{'type':'Test1','description':'d'},
                           }
                  }
            url = 'localhost'
            q = queue(url)
            q.messaging = _messaging()
            q.cfg = cfg
            if not q:
                raise Exception('did not return queue object')
            
            q._start()
            time.sleep(1)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            try:
                if not self.listmodules_called:
                    raise Exception('listmodules not called')
                if not self.run_module_called:
                    raise Exception('run_module not called')
                if self.run_module_exception:
                    logger.info('run_module name: %s',self.run_module_name)
                    logger.info('run_module args: %s',self.run_module_args)
                    raise Exception('run_module raised an Exception')
            finally:
                try:
                    q.kill()
                except Exception:
                    logger.info('exception raised',exc_info=True)
                    raise Exception('queue kill and exception raised')
                
                time.sleep(2)
                if q.queue_thread.is_alive():
                    raise Exception('queue thread still running')
            
        except Exception as e:
            logger.error('Error running queue start_kill test - %s',str(e))
            printer('Test queue start_kill',False)
            raise
        else:
            printer('Test queue start_kill')
    
    def test_04_queue_timeout(self):
        """Test queue_timeout"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            class Test1(object):
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        time.sleep(1)
                    return func
            
            self.listmodules_called = False
            self.listmodules_ret = {'iceprod.server.plugins': [
                                            'iceprod.server.plugins.Test1']
                                       }
            self.run_module_called = False
            self.run_module_exception = False
            self.run_module_name = None
            self.run_module_args = None
            self.run_module_ret = {'iceprod.server.plugins.Test1':Test1()}
            
            # make cfg
            cfg = {'site_id':'thesite',
                   'queue':{'init_queue_interval':0.1,
                            'queue_interval':1,
                            'plugin1':{'type':'Test1','description':'d'},
                           }
                  }
            url = 'localhost'
            q = queue(url)
            q.messaging = _messaging()
            q.cfg = cfg
            if not q:
                raise Exception('did not return queue object')
            
            q._start()
            time.sleep(2)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            try:
                q.stop()
            except Exception:
                logger.info('exception raised',exc_info=True)
                raise Exception('queue stop and exception raised')
            
            time.sleep(0.5)
            if q.queue_thread.is_alive():
                raise Exception('queue thread still running')
            
            
        except Exception as e:
            logger.error('Error running queue queue_timeout test - %s',str(e))
            printer('Test queue queue_timeout',False)
            raise
        else:
            printer('Test queue queue_timeout')
    
    def test_10_grid_calls(self):
        """Test grid_calls"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            class Test1(object):
                name = []
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        pass
                    logging.info('called %s',name)
                    Test1.name.append(name)
                    return func
            
            self.listmodules_called = False
            self.listmodules_ret = {'iceprod.server.plugins': [
                                            'iceprod.server.plugins.Test1']
                                       }
            self.run_module_called = False
            self.run_module_exception = False
            self.run_module_name = None
            self.run_module_args = None
            self.run_module_ret = {'iceprod.server.plugins.Test1':Test1()}
            
            # make cfg
            cfg = {'site_id':'thesite',
                   'queue':{'init_queue_interval':0.1,
                            'queue_interval':1,
                            'task_buffer':10,
                            'plugin1':{'type':'Test1','description':'d'},
                           }
                  }
            url = 'localhost'
            q = queue(url)
            q.messaging = _messaging()
            q.messaging.ret = {'db':{'buffer_jobs_tasks':None}}
            q.cfg = cfg
            if not q:
                raise Exception('did not return queue object')
            
            q._start()
            time.sleep(1)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            try:
                if 'check_and_clean' not in Test1.name:
                    raise Exception('grid.check_and_clean() not called')
                if not any('buffer_jobs_tasks' == x[1] for x in q.messaging.called):
                    logger.info('messages: %r',q.messaging.called)
                    raise Exception('db.buffer_jobs_tasks() not called')
                if 'queue' not in Test1.name:
                    raise Exception('grid.queue() not called')
            finally:
                try:
                    q.stop()
                except Exception:
                    logger.info('exception raised',exc_info=True)
                    raise Exception('queue stop and exception raised')
                
                time.sleep(0.5)
                if q.queue_thread.is_alive():
                    raise Exception('queue thread still running')
            
        except Exception as e:
            logger.error('Error running queue grid_calls test - %s',str(e))
            printer('Test queue grid_calls',False)
            raise
        else:
            printer('Test queue grid_calls')
    
    def test_11_partial_match(self):
        """Test partial_match"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            class Test1(object):
                name = []
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        pass
                    logging.info('called %s',name)
                    Test1.name.append(name)
                    return func
            
            self.listmodules_called = False
            self.listmodules_ret = {'iceprod.server.plugins': [
                                            'iceprod.server.plugins.Test1']
                                       }
            self.run_module_called = False
            self.run_module_exception = False
            self.run_module_name = None
            self.run_module_args = None
            self.run_module_ret = {'iceprod.server.plugins.Test1':Test1()}
            
            # make cfg
            cfg = {'site_id':'thesite',
                   'queue':{'init_queue_interval':0.1,
                            'queue_interval':1,
                            'task_buffer':10,
                            'plugin1':{'type':'Test1.dag','description':'d'},
                           }
                  }
            url = 'localhost'
            q = queue(url)
            q.messaging = _messaging()
            q.messaging.ret = {'db':{'buffer_jobs_tasks':None}}
            q.cfg = cfg
            if not q:
                raise Exception('did not return queue object')
            
            q._start()
            time.sleep(1)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            try:
                if 'check_and_clean' not in Test1.name:
                    raise Exception('grid.check_and_clean() not called')
                if not any('buffer_jobs_tasks' == x[1] for x in q.messaging.called):
                    logger.info('messages: %r',q.messaging.called)
                    raise Exception('db.buffer_jobs_tasks() not called')
                if 'queue' not in Test1.name:
                    raise Exception('grid.queue() not called')
            finally:
                try:
                    q.stop()
                except Exception:
                    logger.info('exception raised',exc_info=True)
                    raise Exception('queue stop and exception raised')
                
                time.sleep(0.5)
                if q.queue_thread.is_alive():
                    raise Exception('queue thread still running')
            
        except Exception as e:
            logger.error('Error running queue partial_match test - %s',str(e))
            printer('Test queue partial_match',False)
            raise
        else:
            printer('Test queue partial_match')
    
    def test_12_multi_partial_match(self):
        """Test multi_partial_match"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            class Test1(object):
                name = []
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        pass
                    logging.info('called %s',name)
                    Test1.name.append(name)
                    return func
            class Test1d(object):
                name = []
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        pass
                    logging.info('called %s',name)
                    Test1d.name.append(name)
                    return func
            
            self.listmodules_called = False
            self.listmodules_ret = {'iceprod.server.plugins': [
                                            'iceprod.server.plugins.Test1',
                                            'iceprod.server.plugins.Test1d']
                                       }
            self.run_module_called = False
            self.run_module_exception = False
            self.run_module_name = None
            self.run_module_args = None
            self.run_module_ret = {'iceprod.server.plugins.Test1':Test1(),
                                   'iceprod.server.plugins.Test1d':Test1d()
                                  }
            
            # make cfg
            cfg = {'site_id':'thesite',
                   'queue':{'init_queue_interval':0.1,
                            'queue_interval':1,
                            'task_buffer':10,
                            'plugin1':{'type':'Test1dag','description':'d'},
                           }
                  }
            url = 'localhost'
            q = queue(url)
            q.messaging = _messaging()
            q.messaging.ret = {'db':{'buffer_jobs_tasks':None}}
            q.cfg = cfg
            if not q:
                raise Exception('did not return queue object')
            
            q._start()
            time.sleep(1)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            try:
                if 'check_and_clean' not in Test1d.name:
                    raise Exception('grid.check_and_clean() not called')
                if not any('buffer_jobs_tasks' == x[1] for x in q.messaging.called):
                    logger.info('messages: %r',q.messaging.called)
                    raise Exception('db.buffer_jobs_tasks() not called')
                if 'queue' not in Test1d.name:
                    raise Exception('grid.queue() not called')
            finally:
                try:
                    q.stop()
                except Exception:
                    logger.info('exception raised',exc_info=True)
                    raise Exception('queue stop and exception raised')
                
                time.sleep(0.5)
                if q.queue_thread.is_alive():
                    raise Exception('queue thread still running')
            
        except Exception as e:
            logger.error('Error running queue multi_partial_match test - %s',str(e))
            printer('Test queue multi_partial_match',False)
            raise
        else:
            printer('Test queue multi_partial_match')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(queue_test))
    suite.addTests(loader.loadTestsFromNames(alltests,queue_test))
    return suite
