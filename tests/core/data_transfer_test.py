"""
Test script for core data_transfer
"""

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('data_transfer_test')


import os
import sys
import time
import shutil
import tempfile
import random
import string
import subprocess
from functools import partial, reduce

try:
    import cPickle as pickle
except:
    import pickle
import unittest
from unittest.mock import patch, MagicMock

from tornado.testing import AsyncTestCase

from iceprod.core import to_log,constants
import iceprod.core.serialization
import iceprod.core.functions
import iceprod.core.exe
import iceprod.core.data_transfer
from iceprod.core.jsonUtil import json_encode,json_decode


class data_transfer_test(AsyncTestCase):
    def setUp(self):
        super(data_transfer_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.symlink(os.path.join(curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        # clean up environment
        base_env = dict(os.environ)
        def reset_env():
            for k in set(os.environ).difference(base_env):
                del os.environ[k]
            for k in base_env:
                os.environ[k] = base_env[k]
        self.addCleanup(reset_env)

    @unittest_reporter
    def test_001_get_current_task(self):
        config = {
            'options': {},
            'tasks': [
                {'name': 'foo'},
            ],
        }

        with self.assertRaises(Exception):
            iceprod.core.data_transfer.get_current_task(config)

        config['options']['task'] = 'foo'
        ret = iceprod.core.data_transfer.get_current_task(config)
        self.assertEqual(ret, config['tasks'][0])

    @patch('iceprod.core.exe.functions.download')
    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='process - download')
    async def test_100_process(self, upload, download):
        config = iceprod.core.serialization.dict_to_dataclasses({
            'options': {
                'task': 'foo',
                'data_url': 'http://localhost/',
            },
            'steering': {
                'value': 'blah',
            },
            'tasks': [
                {
                    'name': 'foo',
                    'iterations': 1,
                    'trays': [
                        {
                            'modules': [
                                {
                                    'data': [
                                        {
                                            'movement':'input',
                                            'remote': 'foo',
                                            'local': 'bar',
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        })

        async def d(url, local, **kwargs):
            with open(local, 'w') as f:
                f.write('test')
        download.side_effect = d

        await iceprod.core.data_transfer.process(config)
        download.assert_called()

    @patch('iceprod.core.exe.functions.download')
    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='process - upload')
    async def test_110_process(self, upload, download):
        config = iceprod.core.serialization.dict_to_dataclasses({
            'options': {
                'task': 'foo',
                'data_url': 'http://localhost/',
            },
            'steering': {
                'value': 'blah',
            },
            'tasks': [
                {
                    'name': 'foo',
                    'iterations': 1,
                    'trays': [
                        {
                            'modules': [
                                {
                                    'data': [
                                        {
                                            'movement':'output',
                                            'remote': 'foo',
                                            'local': 'bar',
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        })

        with open('bar', 'w') as f:
            f.write('test')

        async def passthrough(*args, **kwargs):
            pass
        upload.side_effect = passthrough

        await iceprod.core.data_transfer.process(config)
        download.assert_not_called()
        upload.assert_called()

    @patch('iceprod.core.exe.functions.download')
    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='process - both')
    async def test_120_process(self, upload, download):
        config = iceprod.core.serialization.dict_to_dataclasses({
            'options': {
                'task': 'foo',
                'data_url': 'http://localhost/',
            },
            'steering': {
                'value': 'blah',
            },
            'tasks': [
                {
                    'name': 'foo',
                    'iterations': 1,
                    'trays': [
                        {
                            'modules': [
                                {
                                    'data': [
                                        {
                                            'movement':'both',
                                            'remote': 'foo',
                                            'local': 'bar',
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        })

        async def d(url, local, **kwargs):
            with open(local, 'w') as f:
                f.write('test')
        download.side_effect = d

        async def passthrough(*args, **kwargs):
            pass
        upload.side_effect = passthrough

        await iceprod.core.data_transfer.process(config)
        download.assert_called()
        upload.assert_called()

    @patch('iceprod.core.exe.functions.download')
    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='process - iterations')
    async def test_200_process(self, upload, download):
        config = iceprod.core.serialization.dict_to_dataclasses({
            'options': {
                'task': 'foo',
                'data_url': 'http://localhost/',
            },
            'steering': {
                'value': 'blah',
            },
            'tasks': [
                {
                    'name': 'foo',
                    'trays': [
                        {
                            'iterations': 3,
                            'modules': [
                                {
                                    'data': [
                                        {
                                            'movement':'output',
                                            'remote': 'foo',
                                            'local': 'bar.$(iter)',
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        })

        for i in range(0,3):
            with open(f'bar.{i}', 'w') as f:
                f.write('test')

        async def passthrough(*args, **kwargs):
            pass
        upload.side_effect = passthrough

        await iceprod.core.data_transfer.process(config)
        self.assertEqual(upload.call_count, 3)
        download.assert_not_called()


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(data_transfer_test))
    suite.addTests(loader.loadTestsFromNames(alltests,data_transfer_test))
    return suite
