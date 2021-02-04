"""
Test Server
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('iceprod_server_test')

import os
import sys
import time
from functools import partial
import tempfile
import shutil
import subprocess
import importlib
from datetime import datetime
import glob

import unittest

try:
    from unittest.mock import patch, MagicMock
except ImportError:
    from mock import patch, MagicMock

from iceprod.server.server import Server, roll_files
from iceprod.server.module import module

class server_test(unittest.TestCase):
    def setUp(self):
        super(server_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    @patch('importlib.import_module')
    @patch('iceprod.server.server.IceProdConfig')
    @unittest_reporter
    def test_01_init(self, config, import_module):
        config.return_value = {'modules':{'test':True},'logging':{'level':'debug'}}
        class obj:
            def test(self, *args, **kwargs):
                return MagicMock(wraps=module(*args, **kwargs))
        import_module.return_value = obj()
        s = Server()
        self.assertTrue(import_module.called)

    @patch('importlib.import_module')
    @unittest_reporter
    def test_10_stop(self, import_module):
        import_module.return_value = MagicMock()
        s = Server()

        m = MagicMock(spec=module)
        s.modules = {'m':m}
        s.stop()
        m.stop.assert_called_once_with()

    @patch('importlib.import_module')
    @unittest_reporter
    def test_11_kill(self, import_module):
        import_module.return_value = MagicMock()
        s = Server()

        m = MagicMock(spec=module)
        s.modules = {'m':m}
        s.kill()
        m.kill.assert_called_once_with()

    @patch('importlib.import_module')
    @unittest_reporter
    def test_12_reload(self, import_module):
        import_module.return_value = MagicMock()
        s = Server()

        m = MagicMock(spec=module)
        s.modules = {'m':m}
        s.reload()
        m.stop.assert_called_once_with()
        m.start.assert_called_once_with()

    @unittest_reporter
    def test_90_roll_files(self):
        filename = os.path.join(self.test_dir, 'file')
        fd = open(filename, 'ba+')
        fd.write(b'foo')
        fd = roll_files(fd, filename)
        self.assertTrue(os.path.exists(filename))
        ext = datetime.utcnow().strftime('%Y-%m')
        files = glob.glob(f'{filename}.{ext}*')
        self.assertTrue(len(files) == 1)

        fd.write(b'bar')
        fd.close()

        with open(files[0], 'br') as f:
            self.assertEqual(f.read(), b'foo')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(server_test))
    suite.addTests(loader.loadTestsFromNames(alltests,server_test))
    return suite
