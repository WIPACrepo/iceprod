"""
Test script for daemon
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('daemon_test')

import os
import sys
import time
from functools import partial
import shutil
import tempfile
import signal
import stat
import multiprocessing

try:
    pass
except:
    pass

import unittest

from iceprod.server import daemon


def main(cfgfile,cfgdata):
    message_queue = multiprocessing.Queue()
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
    signal.signal(signal.SIGINT, handler2)
    signal.signal(signal.SIGQUIT, handler3)
    with open('test','w') as f:
        f.write('test')
    while True:
        try:
            m = message_queue.get(True,10)
        except:
            raise
        else:
            if m == 'stop':
                break
            elif m == 'kill':
                break


class daemon_test(unittest.TestCase):
    def setUp(self):
        super(daemon_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(daemon_test,self).tearDown()

    @unittest_reporter
    def test_01_Daemon(self):
        """Test daemon"""

        pidfile = os.path.expanduser(os.path.expandvars(
                        os.path.join(self.test_dir,'pidfile')))
        chdir = os.path.expanduser(os.path.expandvars(self.test_dir))
        umask = 0o077
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
        if oct(stat.S_IMODE(st[stat.ST_MODE])) != '0o600':
            logger.info('mode: %r',oct(stat.S_IMODE(st[stat.ST_MODE])))
            raise Exception('umask failed')

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


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(daemon_test))
    suite.addTests(loader.loadTestsFromNames(alltests,daemon_test))
    return suite
