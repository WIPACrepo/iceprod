"""
Test script for basic_config
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('basic_config_test')

import os, sys, time
import shutil
import tempfile
import random

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import iceprod.server.basic_config


class basic_config_test(unittest.TestCase):
    def setUp(self):
        super(basic_config_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(basic_config_test,self).tearDown()
    
    def test_01_locateconfig(self):
        """Test basic_config.locateconfig()"""
        try:
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
            
        except Exception as e:
            logger.error('Error running basic_config.locateconfig test - %s',str(e))
            printer('Test basic_config.locateconfig',False)
            raise
        else:
            printer('Test basic_config.locateconfig')
    
    def test_02_configdefault(self):
        """Test basic_config.BasicConfig()"""
        try:
            try:
                cfg = iceprod.server.basic_config.BasicConfig()
            except Exception as e:
                logger.error('init returned exception: %r',e,exc_info=True)
                raise Exception('init returned exception')
            
            if (cfg.db is not True or
                cfg.proxy is not False or
                cfg.queue is not True or
                cfg.schedule is not True or
                cfg.website is not True or
                cfg.config is not True or
                cfg.messaging_url != os.path.join('ipc://',os.getcwd(),'unix_socket.sock')):
                raise Exception('failed default check')
            
        except Exception as e:
            logger.error('Error running basic_config.BasicConfig test - %s',str(e))
            printer('Test basic_config.BasicConfig',False)
            raise
        else:
            printer('Test basic_config.BasicConfig')
    
    def test_03_getconfig(self):
        """Test basic_config.BasicConfig.read_file()"""
        try:
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
                f.write('[modules]\nqueue=True\ndb=False\n[messaging]\nurl=localhost:12384')
            
            try:
                cfg = iceprod.server.basic_config.BasicConfig()
                cfg.read_file(filename)
            except Exception as e:
                logger.error('read_file returned exception: %r',e,exc_info=True)
                raise Exception('valid cfgfile returned exception')
            if (cfg.queue is not True or cfg.db is not False or
                cfg.messaging_url != 'localhost:12384'):
                raise Exception('valid cfgfile mistake')
            
            # create incorrect file
            with open(filename,'w') as f:
                f.write('[modules]\n  queue=testing\n')
            
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
            
        except Exception as e:
            logger.error('Error running basic_config.read_file test - %s',str(e))
            printer('Test basic_config.read_file',False)
            raise
        else:
            printer('Test basic_config.read_file')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(basic_config_test))
    suite.addTests(loader.loadTestsFromNames(alltests,basic_config_test))
    return suite
