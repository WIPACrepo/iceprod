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
import unittest
import json

import iceprod.server.config


class config_test(unittest.TestCase):
    def setUp(self):
        super(config_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)
        os.mkdir(os.path.join(self.test_dir, 'etc'))

        original_dir = os.getcwd()
        os.chdir(self.test_dir)
        def reset_dir():
            os.chdir(original_dir)
        self.addCleanup(reset_dir)

    @unittest_reporter
    def test_01_IceProdConfig(self):
        """Test config.IceProdConfig()"""
        cfg = iceprod.server.config.IceProdConfig()
        cfg.save()
        if not os.path.exists(cfg.filename):
            raise Exception('did not save configfile')

        cfg['testing'] = [1,2,3]
        if cfg['testing'] != [1,2,3]:
            raise Exception('did not set param')
        expected = dict(cfg)
        with open(cfg.filename) as f:
            actual = json.load(f)
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
        expected = dict(cfg)
        with open(cfg.filename) as f:
            actual = json.load(f)
        if actual != expected:
            logger.info('expected: %s',expected)
            logger.info('actual: %s',actual)
            raise Exception('did not save delete to file')

    @unittest_reporter(name='IceProdConfig(filename)')
    def test_02_IceProdConfig(self):
        """Test config.IceProdConfig()"""
        cfg = iceprod.server.config.IceProdConfig(filename='test.json')
        if cfg.filename != 'test.json':
            raise Exception('did not use given filename')

    @unittest_reporter(name='IceProdConfig.apply_overrides')
    def test_10_config_override(self):
        vals = ['test=foo']
        cfg = iceprod.server.config.IceProdConfig(override=vals)
        assert cfg['test'] == 'foo'
        del cfg['test']

        vals = ['test.test2.test3=123', 'test.test4=456.5', 'test2={"foo":123}','test3=true']
        cfg = iceprod.server.config.IceProdConfig(override=vals)
        assert cfg['test']['test2']['test3'] == 123
        assert cfg['test']['test4'] == 456.5
        assert cfg['test2'] == {'foo': 123}
        assert cfg['test3'] is True

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(config_test))
    suite.addTests(loader.loadTestsFromNames(alltests,config_test))
    return suite
