"""
Test script for scheduled_tasks/queue_tasks
"""

import logging
logger = logging.getLogger('scheduled_tasks_queue_tasks_test')

import os
import sys
import shutil
import tempfile
import unittest
from functools import partial
from unittest.mock import patch, MagicMock

from tornado.testing import AsyncTestCase
from rest_tools.client import RestClient

from tests.util import unittest_reporter, glob_tests

from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import queue_tasks

class queue_tasks_test(AsyncTestCase):
    def setUp(self):
        super(queue_tasks_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        self.cfg = {
            'queue':{
                'init_queue_interval':0.1,
                'submit_dir':self.test_dir,
                '*':{'type':'Test1','description':'d'},
            },
            'master':{
                'url':False,
            },
            'site_id':'abcd',
        }

    @unittest_reporter
    def test_100_queue_tasks(self):
        s = schedule(self.cfg,None,None,None)
        queue_tasks.queue_tasks(s)

    @unittest_reporter
    async def test_200_run(self):
        rc = MagicMock(spec=RestClient)
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                return {'priority':2}
            elif url.startswith('/datasets/foo/task_counts/status'):
                return {'waiting':100,'queued':2}
            elif url == '/task_actions/queue' and method == 'POST':
                client.called = True
                return {}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await queue_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {}
            elif url == '/task_actions/queue' and method == 'POST':
                client.called = True
                return {}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await queue_tasks.run(rc, debug=True)
        self.assertFalse(client.called)
        
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                return {'priority':2}
            elif url.startswith('/datasets/foo/task_counts/status'):
                return {}
            elif url == '/task_actions/queue' and method == 'POST':
                client.called = True
                return {}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await queue_tasks.run(rc, debug=True)
        self.assertFalse(client.called)

        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                return {'priority':2}
            elif url.startswith('/datasets/foo/task_counts/status'):
                return {'waiting':100,'queued':100000}
            elif url == '/task_actions/queue' and method == 'POST':
                client.called = True
                return {}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await queue_tasks.run(rc, debug=True)
        self.assertFalse(client.called)

    @unittest_reporter(name='run() - error')
    async def test_300_run(self):
        rc = MagicMock(spec=RestClient)
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                client.called = True
                return {'processing':['foo']}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        with self.assertRaises(Exception):
            await queue_tasks.run(rc, debug=True)
        self.assertTrue(client.called)

        # internally catch the error
        await queue_tasks.run(rc)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(queue_tasks_test))
    suite.addTests(loader.loadTestsFromNames(alltests,queue_tasks_test))
    return suite
