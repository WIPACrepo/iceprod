"""
  Test script for queue module

  copyright (c) 2013 the icecube collaboration  
"""

from __future__ import print_function
import logging
try:
    from server_tester import printer, glob_tests
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    logging.basicConfig()
logger = logging.getLogger('queue_test')

import os
import sys
import time
import random
import signal
from datetime import datetime,timedelta
import shutil
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
try:
    import iceprod.procname
except ImportError:
    pass

#module.module.__init__ = oldinit

class queue_test(unittest.TestCase):
    def setUp(self):
        super(queue_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
        # override listmodules, run_module, and get_db_handle
        self.listmodules_called = False
        self.listmodules_ret = {}
        self.run_module_called = False
        self.run_module_exception = False
        self.run_module_name = None
        self.run_module_args = None
        self.run_module_ret = {}
        self.db = self._db()
        flexmock(iceprod.server).should_receive('listmodules').replace_with(self._listmodules)
        flexmock(iceprod.server).should_receive('run_module').replace_with(self._run_module)
        flexmock(module).should_receive('get_db_handle').and_return(self.db)
        
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
        def setprocname(*args,**kwargs):
            pass
        try:
            flexmock(iceprod.procname).should_receive('setprocname').replace_with(setprocname)
        except:
            pass
        
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
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
            def start():
                start.called = True
            flexmock(queue).should_receive('start').replace_with(start)
            
            message_handling_loop.called = False
            start.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
            
            cfg = {'test':1,
                   'db':{'address':None,'ssl':False}}
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            if message_handling_loop.called != True:
                raise Exception('init did not call message_handling_loop')
            if start.called != True:
                raise Exception('init did not call start')
            if not self.db.called or 'get_site_id' not in self.db.func_name:
                raise Exception('init did not call DB.get_site_id')
            if not q.cfg or 'test' not in q.cfg or q.cfg['test'] != 1:
                raise Exception('init did not copy cfg properly')
            
            self.db.ret = {}
            args = [cfg,Queue(),Pipe()[1],Queue()]
            try:
                q = queue(args)
            except:
                pass
            else:
                raise Exception('did not raise error on bad site id')
            
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
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
            class Test1(object):
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        time.sleep(5)
                    return func
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
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
            cfg = {'queue':{'queue_interval':1,
                            'plugin1':{'type':'Test1','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            q.start()
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
                q.stop()
                time.sleep(0.5)
            
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
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
            class Test1(object):
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        pass
                    return func
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
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
            cfg = {'queue':{'queue_interval':1,
                            'plugin1':{'type':'Test1','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            q.start()
            time.sleep(1)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            q.kill()
            time.sleep(2)
            
            if q.queue_thread.is_alive():
                raise Exception('queue thread still running')
            
        except Exception as e:
            logger.error('Error running queue start_kill test - %s',str(e))
            printer('Test queue start_kill',False)
            raise
        else:
            printer('Test queue start_kill')
    
    def test_04_update_cfg(self):
        """Test update_cfg"""
        try:
            # mock some functions so we don't go too far
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
            class Test1(object):
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        pass
                    return func
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
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
            cfg = {'queue':{'queue_interval':1,
                            'plugin1':{'type':'Test1','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            q.start()
            time.sleep(1)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            
            cfg2 = {'queue':{'queue_interval':1,
                             'plugin2':{'type':'Test1','description':'d'},
                            },
                   'db':{'address':None,'ssl':False}
                   }
            q.update_cfg(cfg2)
            
            try:
                if (not q.cfg or 'queue' not in q.cfg or 
                    'plugin2' not in q.cfg['queue']):
                    raise Exception('did not update cfg properly')
            finally:
                q.stop()
                time.sleep(0.5)
            
        except Exception as e:
            logger.error('Error running queue update_cfg test - %s',str(e))
            printer('Test queue update_cfg',False)
            raise
        else:
            printer('Test queue update_cfg')
    
    def test_05_handle_message(self):
        """Test handle_message"""
        try:
            # mock some functions so we don't go too far
            def message_handling_loop():
                message_handling_loop.called = True
            def start():
                start.called = True
            def stop():
                stop.called = True
            def kill():
                kill.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
            flexmock(queue).should_receive('start').replace_with(start)
            flexmock(queue).should_receive('stop').replace_with(stop)
            flexmock(queue).should_receive('kill').replace_with(kill)
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
            
            # make cfg
            cfg = {'queue':{'queue_interval':60,
                            'plugin1':{'type':'Test1','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            start.called = False
            q.handle_message('start')
            if not start.called:
                raise Exception('start not called')
            
            start.called = False
            q.handle_message('queue')
            if not start.called:
                raise Exception('start not called')
            
            stop.called = False
            q.handle_message('stop')
            if not stop.called:
                raise Exception('stop not called')
            
            kill.called = False
            q.handle_message('kill')
            if not kill.called:
                raise Exception('kill not called')
            
        except Exception as e:
            logger.error('Error running queue handle_message test - %s',str(e))
            printer('Test queue handle_message',False)
            raise
        else:
            printer('Test queue handle_message')
    
    def test_06_queue_timeout(self):
        """Test queue_timeout"""
        try:
            # mock some functions so we don't go too far
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
            class Test1(object):
                def __init__(self,*args,**kwargs):
                    pass
                def __getattr__(self,name):
                    def func(*args,**kwargs):
                        pass
                    return func
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
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
            cfg = {'queue':{'queue_interval':-1,
                            'plugin1':{'type':'Test1','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            q.start()
            time.sleep(14)
            
            if not q.queue_thread.is_alive():
                raise Exception('queue thread died immediately')
            
            q.stop()
            time.sleep(0.5)
            
            
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
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
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
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
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
            cfg = {'queue':{'queue_interval':60,
                            'plugin1':{'type':'Test1','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            q.start()
            time.sleep(11)
            
            try:
                if 'check_and_clean' not in Test1.name:
                    raise Exception('grid.check_and_clean() not called')
                if 'queue' not in Test1.name:
                    raise Exception('grid.queue() not called')
            finally:
                q.stop()
                time.sleep(0.5)
            
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
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
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
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
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
            cfg = {'queue':{'queue_interval':60,
                            'plugin1':{'type':'Test1.dag','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            q.start()
            time.sleep(11)
            
            try:
                if 'check_and_clean' not in Test1.name:
                    raise Exception('grid.check_and_clean() not called')
                if 'queue' not in Test1.name:
                    raise Exception('grid.queue() not called')
            finally:
                q.stop()
                time.sleep(0.5)
            
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
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(queue).should_receive('message_handling_loop').replace_with(message_handling_loop)
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
            
            message_handling_loop.called = False
            self.db.ret = {'get_site_id':'thesite'}
            self.db.called = False
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
            cfg = {'queue':{'queue_interval':60,
                            'plugin1':{'type':'Test1dag','description':'d'},
                           },
                   'db':{'address':None,'ssl':False}
                  }
            args = [cfg,Queue(),Pipe()[1],Queue()]
            q = queue(args)
            if not q:
                raise Exception('did not return queue object')
            
            q.start()
            time.sleep(11)
            
            try:
                if 'check_and_clean' not in Test1d.name:
                    raise Exception('grid.check_and_clean() not called')
                if 'queue' not in Test1d.name:
                    raise Exception('grid.queue() not called')
            finally:
                q.stop()
                time.sleep(0.5)
            
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
