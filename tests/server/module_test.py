"""
Test script for module
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('module_test')

import os
import sys
import shutil
import tempfile

from asyncio import Future

try:
    import cPickle as pickle
except:
    import pickle

import unittest

from tornado.testing import AsyncTestCase

from iceprod.server import module


class TestExecutor(object):
    def __init__(self, *args, **kwargs):
        pass
    def submit(self, fn, *args, **kwargs):
        f = Future()
        f.set_result(fn(*args, **kwargs))
        return f
    def map(self, fn, *iterables, **kwargs):
        for i in iterables:
            yield self.submit(fn, i)
    def shutdown(self, wait=True):
        pass
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        pass


class module_test(AsyncTestCase):
    def setUp(self):
        super(module_test,self).setUp()
        orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp(dir=orig_dir)
        os.chdir(self.test_dir)
        def clean_dir():
            os.chdir(orig_dir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(clean_dir)

    @unittest_reporter
    def test_00_init(self):
        """Test init"""
        cfg = {}
        executor = {}
        modules = {}

        m = module.module(cfg, executor, modules)
        self.assertIs(m.cfg, cfg)
        self.assertIs(m.executor, executor)
        self.assertIs(m.modules, modules)
        for method in ('start','stop','kill'):
            self.assertIn(method, m.service)
            # try calling it
            m.service[method]()


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(module_test))
    suite.addTests(loader.loadTestsFromNames(alltests,module_test))
    return suite
