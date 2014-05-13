"""
  Test script for daemon

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
logger = logging.getLogger('daemon_test')

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
import stat
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

from iceprod.server import daemon

class daemon_test(unittest.TestCase):
    def setUp(self):
        super(daemon_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(daemon_test,self).tearDown()
    
    
    def test_01(self):
        """Test daemon"""
        try:
            print('%r'%signal.getsignal(signal.SIGINT))
            print('%r'%signal.getsignal(signal.SIGQUIT))
            print('%r'%signal.getsignal(signal.SIGTERM))
        
            def main(cfgfile,cfgdata):
                message_queue = multiprocessing.Queue()
                def handler1(signum, frame):
                   logging.info('Signal handler called with signal %s' % signum)
                   logging.info('Reloading...')
                   message_queue.put('reload')
                def handler2(signum, frame):
                   logging.info('Signal handler2 called with signal %s' % signum)
                   logging.info('Stopping...')
                   message_queue.put('stop')
                def handler3(signum, frame):
                   logging.info('Signal handler3 called with signal %s' % signum)
                   logging.info('Killing...')
                   message_queue.put('kill')
                   time.sleep(2)
                   sys.exit(1)
                signal.signal(signal.SIGINT, handler1)
                signal.signal(signal.SIGQUIT, handler2)
                signal.signal(signal.SIGTERM, handler3)
                with open('test','w') as f:
                    f.write('test')
                while True:
                    try:
                        m = message_queue.get(True,10)
                    except:
                        pass
                    if m == 'reload':
                        pass
                    elif m == 'stop':
                        break
                    elif m == 'kill':
                        break
            
            pidfile = os.path.expanduser(os.path.expandvars(
                            os.path.join(self.test_dir,'pidfile')))
            chdir = os.path.expanduser(os.path.expandvars(self.test_dir))
            umask = 077
            stdout = os.path.join(self.test_dir,'stdout')
            stderr = os.path.join(self.test_dir,'stderr')
            d = daemon.Daemon(pidfile,partial(main,'cfgfile','cfgdata'),
                       chdir=chdir,
                       umask=umask,
                       stdout=stdout,
                       stderr=stderr)
            multiprocessing.Process(target=d.start).start()
            time.sleep(1)
            if not os.path.exists(pidfile):
                raise Exception('pidfile creation failed')
            if not os.path.exists(os.path.join(chdir,'test')):
                raise Exception('chdir failed')
            st = os.stat(os.path.join(chdir,'test'))
            if oct(stat.S_IMODE(st[stat.ST_MODE])) != '0600':
                logger.info('mode: %r',oct(stat.S_IMODE(st[stat.ST_MODE])))
                raise Exception('umask failed')
            
            d.reload()
            time.sleep(1)
            if not os.path.exists(pidfile):
                raise Exception('pidfile does not exist after reload')
            
            d.stop()
            time.sleep(1)
            if os.path.exists(pidfile):
                raise Exception('pidfile still exists - stop failed')
            
            multiprocessing.Process(target=d.start).start()
            time.sleep(1)
            if not os.path.exists(pidfile):
                raise Exception('pidfile creation failed for start2')
            
            multiprocessing.Process(target=d.restart).start()
            time.sleep(2)
            if not os.path.exists(pidfile):
                raise Exception('pidfile creation failed for restart')
            
            d.kill()
            time.sleep(3)
            if os.path.exists(pidfile):
                raise Exception('pidfile still exists - kill failed')
            
        except Exception as e:
            logger.error('Error running daemon test - %s',str(e))
            printer('Test daemon',False)
            raise
        else:
            printer('Test daemon')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(daemon_test))
    suite.addTests(loader.loadTestsFromNames(alltests,daemon_test))
    return suite
