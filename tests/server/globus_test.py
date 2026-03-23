"""
Test script for globus utilities
"""

from __future__ import absolute_import, division, print_function

import pytest

from tests.util import glob_tests

import logging
logger = logging.getLogger('globus_test')

import os
import sys
import shutil
import subprocess
import tempfile

import unittest

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
        if not skip_tests:
            # clear any proxies
            subprocess.call(['grid-proxy-destroy'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        super(siteglobusproxy_test,self).setUp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(siteglobusproxy_test,self).tearDown()

    @pytest.mark.skipif(skip_tests)
    def test_01_init(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile)
        if not os.path.exists(cfgfile):
            raise Exception('cfgfile does not exist')

    @pytest.mark.skipif(skip_tests)
    def test_02_init_duration(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile, duration=10)
        if not os.path.exists(cfgfile):
            raise Exception('cfgfile does not exist')

    @pytest.mark.skipif(skip_tests)
    def test_10_update_proxy(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile)
        with self.assertRaises(Exception):
            p.update_proxy()

        p.set_passphrase('gibberish')
        with self.assertRaises(Exception):
            p.update_proxy()

    @pytest.mark.skipif(skip_tests)
    def test_10_5_update_proxy(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile)
        p.set_voms_vo('test_vo')
        p.set_voms_role('test_role')
        with self.assertRaises(Exception):
            p.update_proxy()
        p.set_passphrase('gibberish')
        with self.assertRaises(Exception):
            p.update_proxy()

    @pytest.mark.skipif(skip_tests)
    def test_11_get_proxy(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = SiteGlobusProxy(cfgfile=cfgfile)
        with self.assertRaises(Exception):
            p.get_proxy()

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(siteglobusproxy_test))
    suite.addTests(loader.loadTestsFromNames(alltests,siteglobusproxy_test))
    return suite
