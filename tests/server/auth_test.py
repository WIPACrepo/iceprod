"""
Test script for auth
"""

import logging
logger = logging.getLogger('auth_test')

import os
import sys
import time
import random
import shutil
import tempfile
import unittest

import jwt

from tests.util import unittest_reporter, glob_tests

from iceprod.server import auth

class auth_test(unittest.TestCase):
    def setUp(self):
        super(auth_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    @unittest_reporter
    def test_01_create_token(self):
        a = auth.Auth('secret')
        now = time.time()
        tok = a.create_token('subj', expiration=20, type='foo')

        data = jwt.decode(tok, 'secret')
        self.assertEqual(data['sub'], 'subj')
        self.assertEqual(data['type'], 'foo')
        self.assertLess(data['exp'], now+21)
        self.assertGreater(data['nbf'], now-1)

    @unittest_reporter
    def test_10_validate(self):
        a = auth.Auth('secret')
        now = time.time()
        tok = a.create_token('subj', expiration=20, type='foo')
        data = a.validate(tok)
        self.assertEqual(data['sub'], 'subj')
        self.assertEqual(data['type'], 'foo')

        tok = jwt.encode({'sub':'subj','exp':now-1}, 'secret', algorithm='HS512')
        with self.assertRaises(Exception):
            a.validate(tok)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(auth_test))
    suite.addTests(loader.loadTestsFromNames(alltests,auth_test))
    return suite
