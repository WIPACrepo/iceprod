"""
Test script for ssl_cert
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('ssl_cert_test')

import os, sys, time
import shutil
import tempfile
import random
import unittest

import iceprod.server.ssl_cert

class ssl_cert_test(unittest.TestCase):
    def setUp(self):
        super(ssl_cert_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(ssl_cert_test,self).tearDown()

    @unittest_reporter
    def test_01_create_ca(self):
        """Test create_ca"""
        ssl_cert = os.path.join(self.test_dir,'ca.crt')
        ssl_key = os.path.join(self.test_dir,'ca.key')

        # make cert
        iceprod.server.ssl_cert.create_ca(ssl_cert,ssl_key)

        # verify cert
        if iceprod.server.ssl_cert.verify_cert(ssl_cert,ssl_key) is False:
            raise Exception('verify failed')

    @unittest_reporter(name='create_cert() self-signed')
    def test_02_create_cert(self):
        """Test create_cert for self-signed"""
        ssl_cert = os.path.join(self.test_dir,'self.crt')
        ssl_key = os.path.join(self.test_dir,'self.key')

        # make cert
        iceprod.server.ssl_cert.create_cert(ssl_cert,ssl_key)

        # verify cert
        if iceprod.server.ssl_cert.verify_cert(ssl_cert,ssl_key) is False:
            raise Exception('verify failed')

    @unittest_reporter(name='create_cert() ca-signed')
    def test_03_create_cert(self):
        """Test create_cert for ca-signed"""
        ssl_cert = os.path.join(self.test_dir,'cert.crt')
        ssl_key = os.path.join(self.test_dir,'cert.key')
        ca_cert = os.path.join(self.test_dir,'ca.crt')
        ca_key = os.path.join(self.test_dir,'ca.key')

        # make ca
        iceprod.server.ssl_cert.create_ca(ca_cert,ca_key)

        # make cert
        iceprod.server.ssl_cert.create_cert(ssl_cert,ssl_key,
                                            cacert=ca_cert,cakey=ca_key)

        # verify cert
        if iceprod.server.ssl_cert.verify_cert(ssl_cert,ssl_key) is False:
            raise Exception('verify failed')

    @unittest_reporter(name='verify_cert expired')
    def test_10_expired_cert(self):
        """Test expired cert"""
        ssl_cert = os.path.join(self.test_dir,'self.crt')
        ssl_key = os.path.join(self.test_dir,'self.key')

        # make cert
        iceprod.server.ssl_cert.create_cert(ssl_cert,ssl_key,days=-1)

        # verify cert
        if iceprod.server.ssl_cert.verify_cert(ssl_cert,ssl_key) is True:
            raise Exception('verify did not flag expired cert')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(ssl_cert_test))
    suite.addTests(loader.loadTestsFromNames(alltests,ssl_cert_test))
    return suite







