"""
Test script for basic_config
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('basic_config_test')

import os, sys, time
import shutil
import tempfile
import random
import socket

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import flexmock

import iceprod.server.basic_config


class basic_config_test(unittest.TestCase):
    def setUp(self):
        super(basic_config_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(basic_config_test,self).tearDown()

    @unittest_reporter
    def test_01_locateconfig(self):
        """Test basic_config.locateconfig()"""
        os.chdir(self.test_dir)
        try:
            filename = os.path.join(os.getcwd(),'iceprod.cfg')
            with open(filename,'w') as f:
                f.write('test')
            try:
                cfg = iceprod.server.basic_config.locateconfig()
            except Exception as e:
                logger.error('locateconfig returned exception: %r',e,exc_info=True)
                raise Exception('locateconfig returned exception')
            if cfg != filename:
                raise Exception('locatecfg found wrong config file')

        finally:
            os.chdir('..')

    @unittest_reporter
    def test_02_BasicConfig(self):
        """Test basic_config.BasicConfig()"""
        try:
            cfg = iceprod.server.basic_config.BasicConfig()
        except Exception as e:
            logger.error('init returned exception: %r',e,exc_info=True)
            raise Exception('init returned exception')
        logger.info(cfg.messaging_url)

        for mod in cfg.start_order:
            if not isinstance(getattr(cfg,mod),bool):
                raise Exception('%s is not a bool'%mod)
        if '://' not in cfg.messaging_url:
            logger.info(cfg.messaging_url)
            raise Exception('invalid messaging_url')
        if 'logfile' not in cfg.logging:
            raise Exception('invalid logging default')

        if hasattr(socket, 'AF_UNIX'):
            # forceful test of tcp-based address
            try:
                cfg = iceprod.server.basic_config.BasicConfig(force_tcp=True)
            except Exception as e:
                logger.error('init returned exception: %r',e,exc_info=True)
                raise Exception('init returned exception')
            logger.info(cfg.messaging_url)

            if '://' not in cfg.messaging_url:
                logger.info(cfg.messaging_url)
                raise Exception('invalid messaging_url')

    @unittest_reporter
    def test_03_read_file(self):
        """Test basic_config.BasicConfig.read_file()"""
        # create empty file
        filename = os.path.join(self.test_dir,'test.cfg')
        with open(filename,'w') as f:
            f.write('')

        try:
            cfg = iceprod.server.basic_config.BasicConfig()
            cfg.read_file(filename)
        except Exception as e:
            logger.error('read_file returned exception: %r',e,exc_info=True)
            raise Exception('empty cfgfile returned exception')

        # create valid file
        filename = os.path.join(self.test_dir,'test.cfg')
        with open(filename,'w') as f:
            f.write('[modules]\nqueue=True\ndb=False\n[messaging]\nmessaging_url=localhost:12384')

        try:
            cfg = iceprod.server.basic_config.BasicConfig()
            cfg.read_file(filename)
        except Exception as e:
            logger.error('read_file returned exception: %r',e,exc_info=True)
            raise Exception('valid cfgfile returned exception')
        if (cfg.queue is not True or cfg.db is not False or
            cfg.messaging_url != 'localhost:12384'):
            logger.info('cfg: %r',cfg.__dict__)
            raise Exception('valid cfgfile mistake')

        # create incorrect file
        with open(filename,'w') as f:
            f.write('[modules]\nqueue=testing\n')

        try:
            cfg = iceprod.server.basic_config.BasicConfig()
            cfg.read_file(filename)
        except Exception as e:
            logger.info('read_file returned exception: %r',e,exc_info=True)
        else:
            raise Exception('incorrect cfgfile did not return exception')

        # create missing file
        filename = os.path.join(self.test_dir,'test_missing.cfg')

        try:
            cfg = iceprod.server.basic_config.BasicConfig()
            cfg.read_file(filename)
        except Exception as e:
            logger.info('read_file returned exception: %r',e,exc_info=True)
        else:
            raise Exception('missing cfgfile did not return exception')

    @unittest_reporter
    def test_04_logging(self):
        """Test basic_config.BasicConfig logging options"""
        # create valid file
        filename = os.path.join(self.test_dir,'test.cfg')
        with open(filename,'w') as f:
            f.write('[logging]\nlevel=info\nsize=1000000\nlogfile=test.log')

        try:
            cfg = iceprod.server.basic_config.BasicConfig()
            cfg.read_file(filename)
        except Exception as e:
            logger.error('read_file returned exception: %r',e,exc_info=True)
            raise Exception('valid cfgfile returned exception')
        if ('level' not in cfg.logging or cfg.logging['level'] != 'info' or
            'size' not in cfg.logging or cfg.logging['size'] != 1000000 or
            'logfile' not in cfg.logging or cfg.logging['logfile'] != 'test.log'):
            raise Exception('valid cfgfile mistake')

        # create incorrect but ignored param
        with open(filename,'w') as f:
            f.write('[logging]\nother=testing\n')

        try:
            cfg = iceprod.server.basic_config.BasicConfig()
            cfg.read_file(filename)
        except Exception:
            logger.info('incorrect but ignored param',exc_info=True)
            raise Exception('error parsing cfgfile')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(basic_config_test))
    suite.addTests(loader.loadTestsFromNames(alltests,basic_config_test))
    return suite
