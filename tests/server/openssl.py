#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
  Test script for openssl

  copyright (c) 2012 the icecube collaboration  
"""

from __future__ import print_function
try:
    from server_tester import printer, glob_tests, logger
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
    logger = logging.getLogger('server_tester')

import os, sys, time
import shutil
import random
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from iceprod.server import openssl


class openssl_test(unittest.TestCase):
    def setUp(self):
        super(openssl_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(openssl_test,self).tearDown()
    
    def test_01_create_ca(self):
        """Test create_ca"""
        try:
            ssl_cert = os.path.join(self.test_dir,'ca.crt')
            ssl_key = os.path.join(self.test_dir,'ca.key')

            # make cert
            openssl.create_ca(ssl_cert,ssl_key)
            
            # verify cert
            if openssl.verify_cert(ssl_cert,ssl_key) is False:
                raise Exception('verify failed')

        except Exception, e:
            logger.error('Error running openssl.create_ca test - %s',str(e))
            printer('Test openssl.create_ca',False)
            raise
        else:
            printer('Test openssl.create_ca')

    def test_02_create_cert(self):
        """Test create_cert for self-signed"""
        try:
            ssl_cert = os.path.join(self.test_dir,'self.crt')
            ssl_key = os.path.join(self.test_dir,'self.key')

            # make cert
            openssl.create_cert(ssl_cert,ssl_key)
            
            # verify cert
            if openssl.verify_cert(ssl_cert,ssl_key) is False:
                raise Exception('verify failed')

        except Exception, e:
            logger.error('Error running openssl.create_cert self-signed test - %s',str(e))
            printer('Test openssl.create_cert self-signed',False)
            raise
        else:
            printer('Test openssl.create_cert self-signed')

    def test_03_create_cert(self):
        """Test create_cert for ca-signed"""
        try:
            ssl_cert = os.path.join(self.test_dir,'cert.crt')
            ssl_key = os.path.join(self.test_dir,'cert.key')
            ca_cert = os.path.join(self.test_dir,'ca.crt')
            ca_key = os.path.join(self.test_dir,'ca.key')
            
            # make ca
            openssl.create_ca(ca_cert,ca_key)

            # make cert
            openssl.create_cert(ssl_cert,ssl_key,cacert=ca_cert,cakey=ca_key)
            
            # verify cert
            if openssl.verify_cert(ssl_cert,ssl_key) is False:
                raise Exception('verify failed')

        except Exception, e:
            logger.error('Error running openssl.create_cert ca-signed test - %s',str(e))
            printer('Test openssl.create_cert ca-signed',False)
            raise
        else:
            printer('Test openssl.create_cert ca-signed')



def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(openssl_test))
    suite.addTests(loader.loadTestsFromNames(alltests,openssl_test))
    return suite







