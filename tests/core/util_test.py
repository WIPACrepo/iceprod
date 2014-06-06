"""
Test script for tuil
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('util')

import os
import sys
import json

try:
    import cPickle as pickle
except:
    import pickle

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from iceprod.core import to_log
import iceprod.core.util


class util_test(unittest.TestCase):
    def setUp(self):
        super(util_test,self).setUp()
    
    def tearDown(self):
        super(util_test,self).tearDown()

    def test_01_noncritical_error(self):
        """Test the NoncriticalError class"""
        try:
            e = iceprod.core.util.NoncriticalError()
            
            if not isinstance(e,Exception):
                raise Exception('NoncriticalError is not an Exception')
            if e.value != '':
                raise Exception('Empty NoncriticalError has non-empty value')
            if str(e) != "NoncriticalError()":
                logger.info('repr: %s',str(e))
                raise Exception('bad string representation of NoncriticalError')
            
            e = iceprod.core.util.NoncriticalError('some text')
            
            if not isinstance(e,Exception):
                raise Exception('NoncriticalError is not an Exception')
            if e.value != 'some text':
                raise Exception('NoncriticalError has different value')
            if str(e) != "NoncriticalError('some text')":
                logger.info('repr: %s',str(e))
                raise Exception('bad string representation of NoncriticalError')
            
        except Exception as e:
            logger.error('Error running NoncriticalError class test: %s',str(e))
            printer('Test util.NoncriticalError',False)
            raise
        else:
            printer('Test util.NoncriticalError')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(util_test))
    suite.addTests(loader.loadTestsFromNames(alltests,util_test))
    return suite
