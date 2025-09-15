"""
Test script for util
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('util')


try:
    pass
except:
    pass

import unittest
import iceprod.core.util


class util_test(unittest.TestCase):
    def setUp(self):
        super(util_test,self).setUp()

    def tearDown(self):
        super(util_test,self).tearDown()

    @unittest_reporter
    def test_01_NoncriticalError(self):
        """Test the NoncriticalError class"""
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

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(util_test))
    suite.addTests(loader.loadTestsFromNames(alltests,util_test))
    return suite
