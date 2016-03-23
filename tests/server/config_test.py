"""
Test script for config
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('config_test')

import os, sys, time
import shutil
import tempfile
import random

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import iceprod.server.config


class config_test(unittest.TestCase):
    def setUp(self):
        super(config_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    @unittest_reporter
    def test_01_IceProdConfig(self):
        """Test config.IceProdConfig()"""
        os.chdir(self.test_dir)
        try:
            cfg = iceprod.server.config.IceProdConfig()
            cfg.save()
            if not os.path.exists(cfg.filename):
                raise Exception('did not save configfile')

            cfg['testing'] = [1,2,3]
            if cfg['testing'] != [1,2,3]:
                raise Exception('did not set param')
            expected = '{"testing":[1,2,3]}'
            actual = open(cfg.filename).read()
            if actual != expected:
                logger.info('expected: %s',expected)
                logger.info('actual: %s',actual)
                raise Exception('did not save addition to file')

            cfg.load()
            if cfg['testing'] != [1,2,3]:
                raise Exception('param does not exist after load')

            del cfg['testing']
            if 'testing' in cfg:
                raise Exception('did not delete param')
            expected = '{}'
            actual = open(cfg.filename).read()
            if actual != expected:
                logger.info('expected: %s',expected)
                logger.info('actual: %s',actual)
                raise Exception('did not save delete to file')

        finally:
            os.chdir('..')

    @unittest_reporter(name='IceProdConfig(filename)')
    def test_02_IceProdConfig(self):
        """Test config.IceProdConfig()"""
        os.chdir(self.test_dir)
        try:
            cfg = iceprod.server.config.IceProdConfig(filename='test.json')
            if cfg.filename != 'test.json':
                raise Exception('did not use given filename')
        finally:
            os.chdir('..')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(config_test))
    suite.addTests(loader.loadTestsFromNames(alltests,config_test))
    return suite
