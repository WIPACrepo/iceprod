"""
Test script for jsonUtil
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('jsonUtil')

import os, sys, time
import shutil
import random
import string
import subprocess
from threading import Event

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from datetime import date,datetime,time
import base64
import iceprod.core.jsonUtil


class jsonUtil_test(unittest.TestCase):
    def setUp(self):
        super(jsonUtil_test,self).setUp()
        self.test_dir = os.path.join(os.getcwd(),'test')
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(jsonUtil_test,self).tearDown()
    
    def test_01_recursive_unicode(self):
        """Test recursive_unicode"""
        try:
            # test byte string
            input = 'a test'
            expected = u'a test'
            output = iceprod.core.jsonUtil.recursive_unicode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test tuple of strings
            input = ('a test','another test')
            expected = (u'a test',u'another test')
            output = iceprod.core.jsonUtil.recursive_unicode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test list of strings
            input = ['a test','another test']
            expected = [u'a test',u'another test']
            output = iceprod.core.jsonUtil.recursive_unicode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test dict of strings
            input = {'a':'a test','b':'another test'}
            expected = {u'a':u'a test',u'b':u'another test'}
            output = iceprod.core.jsonUtil.recursive_unicode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test something that is not a string
            input = 1
            expected = 1
            output = iceprod.core.jsonUtil.recursive_unicode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
        except Exception, e:
            logger.error('Error running recursive_unicode test: %s',str(e))
            printer('Test jsonUtil.recursive_unicode()',False)
            raise
        else:
            printer('Test jsonUtil.recursive_unicode()')
    
    def test_02_objToJSON(self):
        """Test objToJSON"""
        try:
            # test byte string
            input = 'a test'
            expected = 'a test'
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test int
            input = 1
            expected = 1
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test dict
            input = {'a':1}
            expected = {'a':1}
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = datetime(2012,3,6,12,34,29)
            expected = {'__jsonclass__': ['datetime', '2012-03-06T12:34:29']}
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = datetime(2012,3,6,12,34,29,120)
            expected = {'__jsonclass__': ['datetime', '2012-03-06T12:34:29.000120']}
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test date
            input = date(2012,3,6)
            expected = {'__jsonclass__': ['date', '2012-03-06']}
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = time(12,34,29)
            expected = {'__jsonclass__': ['time', '12:34:29']}
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = time(12,34,29,120)
            expected = {'__jsonclass__': ['time', '12:34:29.000120']}
            output = iceprod.core.jsonUtil.objToJSON(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
        except Exception, e:
            logger.error('Error running objToJSON test: %s',str(e))
            printer('Test jsonUtil.objToJSON()',False)
            raise
        else:
            printer('Test jsonUtil.objToJSON()')
    
    def test_03_JSONToObj(self):
        """Test JSONToObj"""
        try:
            # test byte string
            input = 'a test'
            expected = 'a test'
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test int
            input = 1
            expected = 1
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test dict
            input = {'a':1}
            expected = {'a':1}
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = {'__jsonclass__': ['datetime', '2012-03-06T12:34:29']}
            expected = datetime(2012,3,6,12,34,29)
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = {'__jsonclass__': ['datetime', '2012-03-06T12:34:29.000120']}
            expected = datetime(2012,3,6,12,34,29,120)
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = {'__jsonclass__': ['datetime', '2012-03-06 12:34:29']}
            expected = datetime(2012,3,6,12,34,29)
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test date
            input = {'__jsonclass__': ['date', '2012-03-06']}
            expected = date(2012,3,6)
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = {'__jsonclass__': ['time', '12:34:29']}
            expected = time(12,34,29)
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = {'__jsonclass__': ['time', '12:34:29.000120']}
            expected = time(12,34,29,120)
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test binary
            input = {"__jsonclass__":["binary","dGVzdGluZw=="]}
            expected = 'testing'
            output = iceprod.core.jsonUtil.JSONToObj(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
        except Exception, e:
            logger.error('Error running JSONToObj test: %s',str(e))
            printer('Test jsonUtil.JSONToObj()',False)
            raise
        else:
            printer('Test jsonUtil.JSONToObj()')
    
    def test_05_json_encode(self):
        """Test json_encode"""
        try:
            # test byte string
            input = 'a test'
            expected = '"a test"'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test tuple of strings
            input = ('a test','another test')
            expected = '["a test","another test"]'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test list of strings
            input = ['a test','another test']
            expected = '["a test","another test"]'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test dict of strings
            input = {'a':'a test','b':'another test'}
            expected = '{"a":"a test","b":"another test"}'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test int
            input = 1
            expected = '1'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test float
            input = 1.05
            expected = '1.05'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = datetime(2012,3,6,12,34,29)
            expected = '{"__jsonclass__":["datetime","2012-03-06T12:34:29"]}'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = datetime(2012,3,6,12,34,29,120)
            expected = '{"__jsonclass__":["datetime","2012-03-06T12:34:29.000120"]}'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test date
            input = date(2012,3,6)
            expected = '{"__jsonclass__":["date","2012-03-06"]}'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = time(12,34,29)
            expected = '{"__jsonclass__":["time","12:34:29"]}'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = time(12,34,29,120)
            expected = '{"__jsonclass__":["time","12:34:29.000120"]}'
            output = iceprod.core.jsonUtil.json_encode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
        except Exception, e:
            logger.error('Error running json_encode test: %s',str(e))
            printer('Test jsonUtil.json_encode()',False)
            raise
        else:
            printer('Test jsonUtil.json_encode()')
    
    def test_06_json_decode(self):
        """Test json_decode"""
        try:
            # test byte string
            input = '"a test"'
            expected = 'a test'
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test list of strings
            input = '["a test","another test"]'
            expected = ['a test','another test']
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test dict of strings
            input = '{"a":"a test","b":"another test"}'
            expected = {'a':'a test','b':'another test'}
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test int
            input = '1'
            expected = 1
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test float
            input = '1.05'
            expected = 1.05
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = '{"__jsonclass__":["datetime","2012-03-06T12:34:29"]}'
            expected = datetime(2012,3,6,12,34,29)
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test datetime
            input = '{"__jsonclass__":["datetime","2012-03-06T12:34:29.000120"]}'
            expected = datetime(2012,3,6,12,34,29,120)
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test date
            input = '{"__jsonclass__":["date","2012-03-06"]}'
            expected = date(2012,3,6)
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = '{"__jsonclass__":["time","12:34:29"]}'
            expected = time(12,34,29)
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test time
            input = '{"__jsonclass__":["time","12:34:29.000120"]}'
            expected = time(12,34,29,120)
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
            # test binary
            input = '{"__jsonclass__":["binary","dGVzdGluZw=="]}'
            expected = 'testing'
            output = iceprod.core.jsonUtil.json_decode(input)
            if expected != output:
                raise Exception('expected != output:  %r != %r'%(expected,output))
            
        except Exception, e:
            logger.error('Error running json_decode test: %s',str(e))
            printer('Test jsonUtil.json_decode()',False)
            raise
        else:
            printer('Test jsonUtil.json_decode()')
    
    
    
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(jsonUtil_test))
    suite.addTests(loader.loadTestsFromNames(alltests,jsonUtil_test))
    return suite