"""
Test script for tornado
"""

import logging
logger = logging.getLogger('tornado_test')

import os
import sys
import time
import random
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from threading import Thread

import requests

from tests.util import unittest_reporter, glob_tests

import tornado.web
import tornado.ioloop

import iceprod.server.tornado

class tornado_test(unittest.TestCase):
    def setUp(self):
        super(tornado_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        # setup fake REST class
        foo = MagicMock()
        def setup(c):
            class FooHandler(tornado.web.RequestHandler):
                def get(self):
                    self.write('foo')
            return [tornado.web.URLSpec(r'/foo', FooHandler,name='foo')]
        foo.setup = setup
        patcher = patch.dict('sys.modules', **{
            'iceprod.server.rest.foo': foo
        })
        patcher.start()
        self.addCleanup(patcher.stop)

    @unittest_reporter
    def test_01_setup_rest(self):
        config = {'rest': {'foo': {}}}
        app = iceprod.server.tornado.setup_rest(config)
        app.reverse_url('foo')

    @unittest_reporter
    def test_10_startup(self):
        config = {'rest': {'foo': True}}
        app = iceprod.server.tornado.setup_rest(config)
        port = random.randint(32000,38000)
        iceprod.server.tornado.startup(app, port=port)

        io_loop = tornado.ioloop.IOLoop.current()
        t = Thread(target=io_loop.start)
        t.start()
        try:
            r = requests.get('http://localhost:%d/foo'%port)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.text, 'foo')
        finally:
            io_loop.stop()
            t.join()


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(tornado_test))
    suite.addTests(loader.loadTestsFromNames(alltests,tornado_test))
    return suite
