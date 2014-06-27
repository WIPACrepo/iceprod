"""
Test script for server init scripts
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('server_init_test')

import os, sys, time
import shutil
import tempfile
import random
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import iceprod.server


class init_test(unittest.TestCase):
    def setUp(self):
        super(init_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(init_test,self).tearDown()
    
    def test_10_GlobalID_int2char(self):
        """Test GlobalID_int2char"""
        try:
            inputs = [0,26,61,62,100,124,126,59428801]
            outputs = ['a','A','9','aa','aM','ba','bc','david']
            for i,input in enumerate(inputs):
                output = iceprod.server.GlobalID.int2char(input)
                if outputs[i] != output:
                    raise Exception('input=%d, given output=%s, correct output=%s'%(input,str(output),outputs[i]))
            
        except Exception as e:
            logger.error('Error running GlobalID.int2char test - %s',str(e))
            printer('Test GlobalID.int2char',False)
            raise
        else:
            printer('Test GlobalID.int2char')

    def test_11_GlobalID_char2int(self):
        """Test GlobalID_char2int"""
        try:
            inputs = ['a','A','9','aa','aM','ba','bc','david']
            outputs = [0,26,61,62,100,124,126,59428801]
            for i,input in enumerate(inputs):
                output = iceprod.server.GlobalID.char2int(input)
                if outputs[i] != output:
                    raise Exception('input=%s, given output=%s, correct output=%d'%(input,str(output),outputs[i]))
            
        except Exception as e:
            logger.error('Error running GlobalID.char2int test - %s',str(e))
            printer('Test GlobalID.char2int',False)
            raise
        else:
            printer('Test GlobalID.char2int')

    def test_12_GlobalID_siteID_gen(self):
        """Test GlobalID_siteID_gen"""
        try:
            for i in xrange(1000):
                ret = iceprod.server.GlobalID.siteID_gen()
                ret2 = iceprod.server.GlobalID.char2int(ret)
                if ret2 < 0 or ret2 >= iceprod.server.GlobalID.MAXSITEID:
                    raise Exception('outsite permissible range: %s : %d'%(ret,ret2))
            
        except Exception as e:
            logger.error('Error running GlobalID.siteID_gen test - %s',str(e))
            printer('Test GlobalID.siteID_gen',False)
            raise
        else:
            printer('Test GlobalID.siteID_gen')

    def test_13_GlobalID_globalID_gen(self):
        """Test GlobalID_globalID_gen"""
        try:
            for i in xrange(1000):
                ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
                ret = iceprod.server.GlobalID.globalID_gen(ran,iceprod.server.GlobalID.siteID_gen())
                ret2 = iceprod.server.GlobalID.char2int(ret)
                if ret2 < ran or ret2 >= (iceprod.server.GlobalID.MAXSITEID*iceprod.server.GlobalID.MAXLOCALID):
                    raise Exception('outsite permissible range: (0<x<%d)  %d = %s'%((iceprod.server.GlobalID.MAXSITEID*iceprod.server.GlobalID.MAXLOCALID),ret2,ret))
            
        except Exception as e:
            logger.error('Error running GlobalID.globalID_gen test - %s',str(e))
            printer('Test GlobalID.globalID_gen',False)
            raise
        else:
            printer('Test GlobalID.globalID_gen')

    def test_14_GlobalID_localID_ret(self):
        """Test GlobalID_localID_ret"""
        try:
            for i in xrange(1000):
                ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
                ret = iceprod.server.GlobalID.globalID_gen(ran,iceprod.server.GlobalID.siteID_gen())
                ret2 = iceprod.server.GlobalID.localID_ret(ret,type='int')
                if ret2 != ran:
                    raise Exception('returned local id does not match initial id: %d != %d'%(ran,ret2))
            
        except Exception as e:
            logger.error('Error running GlobalID.localID_ret test - %s',str(e))
            printer('Test GlobalID.localID_ret',False)
            raise
        else:
            printer('Test GlobalID.localID_ret')

    def test_15_GlobalID_siteID_ret(self):
        """Test GlobalID_siteID_ret"""
        try:
            for i in xrange(1000):
                ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
                ran2 = iceprod.server.GlobalID.siteID_gen()
                ret = iceprod.server.GlobalID.globalID_gen(ran,ran2)
                ret2 = iceprod.server.GlobalID.siteID_ret(ret)
                if ret2 != ran2:
                    raise Exception('returned site id does not match initial id: %d != %d'%(ran,ret2))
            
        except Exception as e:
            logger.error('Error running GlobalID.siteID_ret test - %s',str(e))
            printer('Test GlobalID.siteID_ret',False)
            raise
        else:
            printer('Test GlobalID.siteID_ret')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(init_test))
    suite.addTests(loader.loadTestsFromNames(alltests,init_test))
    return suite
