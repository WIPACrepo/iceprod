"""
  Test script for module

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
logger = logging.getLogger('module_test')

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
import iceprod.core.logger
try:
    import iceprod.procname
except ImportError:
    pass

class module_test(unittest.TestCase):
    def setUp(self):
        super(module_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(module_test,self).tearDown()
    
    
    def test_01_module(self):
        """Test module"""
        try:
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
            
            cfg = {'logging':{'iceprod_server.module':'m'}}
            queue = multiprocessing.Queue()
            mypipe,pipe = multiprocessing.Pipe()
            pqueue = multiprocessing.Queue()
            args = [cfg,queue,pipe,pqueue]
            m = module.module(args)
            
            if (m.cfg != cfg or m.queue != queue or m.pipe != pipe or
                m.pqueue != pqueue):
                raise Exception('args is wrong')
            
            t = threading.Thread(target=test_mod,args=(args,))
            t.daemon = True
            t.start()
            
            queue.put('test')
            time.sleep(0.1)
            tmp = pqueue.get(False)
            if tmp != 'test':
                raise Exception('put_message fails')
            
            m.put_message('module',module='mod')
            time.sleep(0.1)
            tmp = pqueue.get(False)
            if tmp != 'mod|module':
                raise Exception('put_message module fails')
            
            queue.put('stop')
            time.sleep(0.1)
            
            try:
                m.handle_message('test')
            except Exception:
                pass
            else:
                raise Exception('handle_message did not raise Exception')
            
            t = threading.Thread(target=m.message_handling_loop)
            t.daemon = True
            t.start()
            
            newcfg = {'test2':2}
            mypipe.send(newcfg)
            queue.put('newcfg')
            time.sleep(0.1)
            if m.cfg != newcfg:
                raise Exception('message_handling newcfg fails')
            
            queue.put('stop')
            time.sleep(0.1)
            if t.is_alive():
                raise Exception('message_handling stop fails')
            
        except Exception as e:
            logger.error('Error running module test - %s',str(e))
            printer('Test module',False)
            raise
        else:
            printer('Test module')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(module_test))
    suite.addTests(loader.loadTestsFromNames(alltests,module_test))
    return suite
