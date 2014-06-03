"""
  Test script for main iceprod server

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

import iceprod_server
import iceprod.server
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
    
    def test_001_load_config(self):
        """Test load_config"""
        try:
            cfgfile = os.path.join(self.test_dir,'cfg')
            with open(cfgfile,'w') as f:
                p = partial(print,sep='',file=f)
                p('[server_modules]')
                p('  queue = True')
                p('  db = True')
                p('  website = True')
                p('[queue]')
                p('  queue_interval = 180')
                p('[db]')
                p('  name = iceprod_db')
                p('[webserver]')
                p('  port = 9080')
                p('[download]')
                p('  http_username = icecube')
                p('[logging]')
                p('  level = DEBUG')
                p('  format = %(asctime)s %(levelname)s : %(message)s')
                p('  size = 100000')
                p('  num = 2')
                p('  logfile = test.log')
                p('[system]')
                p('  ssl = False')
                p('  test = True')
            
            iceprod_server.load_config(cfgfile)
            
            if iceprod_server.cfg is None:
                raise Exception('did not set cfg')
            if 'server_modules' not in iceprod_server.cfg:
                raise Exception('did not set server_modules')
            if 'queue' not in iceprod_server.cfg['server_modules']:
                raise Exception('did not set server_modules[queue]')
            if 'system' not in iceprod_server.cfg:
                raise Exception('did not set system')
            if 'test' not in iceprod_server.cfg['system']:
                raise Exception('did not set system[test]')
            if iceprod_server.cfg['system']['test'] != 'True':
                raise Exception('did not set system[test]=True')
            
            iceprod_server.cfg = None
            iceprod_server.load_config(iceprod.server.getconfig(cfgfile))
            
            if iceprod_server.cfg is None:
                raise Exception('did not set cfg')
            if 'server_modules' not in iceprod_server.cfg:
                raise Exception('did not set server_modules')
            if 'queue' not in iceprod_server.cfg['server_modules']:
                raise Exception('did not set server_modules[queue]')
            if 'system' not in iceprod_server.cfg:
                raise Exception('did not set system')
            if 'test' not in iceprod_server.cfg['system']:
                raise Exception('did not set system[test]')
            if iceprod_server.cfg['system']['test'] != 'True':
                raise Exception('did not set system[test]=True')
            
        except Exception as e:
            logger.error('Error running iceprod_server load_config test - %s',str(e))
            printer('Test iceprod_server load_config',False)
            raise
        else:
            printer('Test iceprod_server load_config')
    
    def test_002_server_module(self):
        """Test server_module"""
        try:
            iceprod_server.server_module.process_class = threading.Thread
            def test_mod(args):
                test_mod.called = True
                test_mod.args = args
                cfg,queue,pipe,pqueue = args
                while True:
                    ret = queue.get()
                    if ret == 'pipe':
                        test_mod.obj = pipe.recv()
                        continue
                    elif ret == 'stop':
                        break
                    test_mod.ret = ret
                    pqueue.put(ret)
            self.run_module_ret = {'test':test_mod}
            
            # test successful module
            s = iceprod_server.server_module('test')
            
            # test starting
            test_mod.called = False
            s.process.daemon = True
            s.start()
            time.sleep(1)
            if not test_mod.called:
                raise Exception('failed to call test_mod')
            logger.info('mod args: %r',test_mod.args)
            if len(test_mod.args) != 4:
                raise Exception('wrong number of mod args')
            if not isinstance(test_mod.args[0],dict):
                raise Exception('mod arg 1 was not cfg')
            if not isinstance(test_mod.args[1],multiprocessing.queues.Queue):
                raise Exception('mod arg 2 was not Queue')
            if not isinstance(test_mod.args[2],_multiprocessing.Connection):
                raise Exception('mod arg 3 was not Pipe')
            if not isinstance(test_mod.args[3],multiprocessing.queues.Queue):
                raise Exception('mod arg 4 was not Queue')
            
            # test put_message
            s.put_message('test')
            time.sleep(0.1)
            if test_mod.ret != 'test':
                raise Exception('put_message failed')
            
            # test put_object
            obj = {'t':'test'}
            s.put_object(obj)
            s.put_message('pipe')
            time.sleep(0.1)
            if test_mod.obj != obj:
                raise Exception('put_object failed')
            
            # test stop
            s.stop()
            time.sleep(0.1)
            if s.process.is_alive():
                raise Exception('stop failed')
            
        except Exception as e:
            logger.error('Error running iceprod_server server_module test - %s',str(e))
            printer('Test iceprod_server server_module',False)
            raise
        else:
            printer('Test iceprod_server server_module')
    
    def test_010_main(self):
        """Test main"""
        try:
            iceprod_server.server_module.process_class = threading.Thread
            cfgfile = os.path.join(self.test_dir,'cfg')
            with open(cfgfile,'w') as f:
                p = partial(print,sep='',file=f)
                p('[server_modules]')
                p('  test = True')
                p('  db = True')
                p('  website = True')
                p('[queue]')
                p('  queue_interval = 180')
                p('[db]')
                p('  name = iceprod_db')
                p('[webserver]')
                p('  port = 9080')
                p('[download]')
                p('  http_username = icecube')
                p('[logging]')
                p('  level = DEBUG')
                p('  format = %(asctime)s %(levelname)s : %(message)s')
                p('  size = 100000')
                p('  num = 2')
                p('  logfile = test.log')
                p('[system]')
                p('  ssl = False')
                p('  test = True')
            iceprod_server.load_config(cfgfile)
            iceprod_server.cfg['server_modules']['test'] = True
            
            def test_mod(args):
                test_mod.called = True
                test_mod.args = args
                cfg,queue,pipe,pqueue = args
                while True:
                    ret = queue.get()
                    if ret == 'newcfg':
                        test_mod.obj = pipe.recv()
                        continue
                    elif ret in ('stop','kill'):
                        break
                    test_mod.ret = ret
            self.listmodules_ret = {'iceprod.server.modules':['iceprod.server.modules.test']}
            self.run_module_ret = {'iceprod.server.modules.test':test_mod}
            
            def sig():
                sig.called = True
            def log():
                log.called = True
            flexmock(iceprod_server).should_receive('set_signals').replace_with(sig)
            flexmock(iceprod_server).should_receive('set_logger').replace_with(log)
            
            # test successful module
            test_mod.called = False
            def run():
                s = iceprod_server.main(cfgfile,iceprod_server.cfg)
            t = threading.Thread(target=run)
            t.daemon = True
            t.start()
            time.sleep(5)
            
            if not sig.called:
                raise Exception('failed to call sig')
            if not log.called:
                raise Exception('failed to call log')
            if not test_mod.called:
                raise Exception('failed to call test_mod')
            logger.info('mod args: %r',test_mod.args)
            if len(test_mod.args) != 4:
                raise Exception('wrong number of mod args')
            if not isinstance(test_mod.args[0],dict):
                raise Exception('mod arg 1 was not cfg')
            if not isinstance(test_mod.args[1],multiprocessing.queues.Queue):
                raise Exception('mod arg 2 was not Queue')
            if not isinstance(test_mod.args[2],_multiprocessing.Connection):
                raise Exception('mod arg 3 was not Pipe')
            if not isinstance(test_mod.args[3],multiprocessing.queues.Queue):
                raise Exception('mod arg 4 was not Queue')
            
            # test new cfg
            with open(cfgfile,'w') as f:
                p = partial(print,sep='',file=f)
                p('[server_modules]')
                p('  queue = True')
                p('  db = True')
                p('  website = True')
                p('[queue]')
                p('  queue_interval = 180')
                p('[db]')
                p('  name = iceprod_db')
                p('[webserver]')
                p('  port = 9080')
                p('[download]')
                p('  http_username = icecube')
                p('[logging]')
                p('  level = DEBUG')
                p('  format = %(asctime)s %(levelname)s : %(message)s')
                p('  size = 100000')
                p('  num = 2')
                p('  logfile = test.log')
                p('[system]')
                p('  ssl = False')
                p('  test = False')
            iceprod_server.server_module.message_queue.put('reload')
            time.sleep(1)
            obj = test_mod.obj
            if not (isinstance(obj,dict) and
                    'system' in obj and
                    'test' in obj['system'] and 
                    obj['system']['test'] == 'False'):
                raise Exception('cfg not updated')
            
            # try stopping
            iceprod_server.server_module.message_queue.put('stop')
            time.sleep(1)
            if t.is_alive():
                raise Exception('stop failed')
            
            # try killing
            t = threading.Thread(target=run)
            t.daemon = True
            t.start()
            time.sleep(1)
            iceprod_server.server_module.message_queue.put('kill')
            time.sleep(1)
            if t.is_alive():
                raise Exception('kill failed')
            
        except Exception as e:
            logger.error('Error running iceprod_server main test - %s',str(e))
            printer('Test iceprod_server main',False)
            raise
        else:
            printer('Test iceprod_server main')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(iceprod_server_test))
    suite.addTests(loader.loadTestsFromNames(alltests,iceprod_server_test))
    return suite
