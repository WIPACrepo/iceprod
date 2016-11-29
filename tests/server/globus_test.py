"""
Test script for globus utilities
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('globus_test')

import os, sys, time
import shutil
import random
import string
import subprocess
import tempfile

import unittest

from iceprod.core import to_log
from iceprod.server.globus import SiteGlobusProxy

skip_tests = False
if subprocess.call(['which','grid-proxy-init']):
    skip_tests = True

class siteglobusproxy_test(unittest.TestCase):
    def setUp(self):
        self._timeout = 1
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        # clear any proxies
        FNULL = open(os.devnull, 'w')
        subprocess.call(['grid-proxy-destroy'],stdout=FNULL,stderr=FNULL)
        super(siteglobusproxy_test,self).setUp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(siteglobusproxy_test,self).tearDown()

    @unittest_reporter(skip=skip_tests, module='globus.SiteGlobusProxy')
    def test_01_init(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile)
        if not os.path.exists(cfgfile):
            raise Exception('cfgfile does not exist')

    @unittest_reporter(skip=skip_tests, module='globus.SiteGlobusProxy')
    def test_02_init_duration(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile, duration=10)
        if not os.path.exists(cfgfile):
            raise Exception('cfgfile does not exist')

    @unittest_reporter(skip=skip_tests, module='globus.SiteGlobusProxy')
    def test_10_update_proxy(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile)
        try:
            p.update_proxy()
        except Exception as e:
            pass
        else:
            raise Exception('did not raise Exception')

        p.set_passphrase('gibberish')
        try:
            with to_log(sys.stderr), to_log(sys.stdout):
                p.update_proxy()
        except Exception as e2:
            if str(e) == str(e2):
                logger.info('%r\n%r',e,e2)
                raise Exception('Exception is the same')
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter(skip=skip_tests, module='globus.SiteGlobusProxy')
    def test_11_get_proxy(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile)
        try:
            p.get_proxy()
        except Exception as e:
            pass
        else:
            raise Exception('did not raise Exception')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(siteglobusproxy_test))
    suite.addTests(loader.loadTestsFromNames(alltests,siteglobusproxy_test))
    return suite
