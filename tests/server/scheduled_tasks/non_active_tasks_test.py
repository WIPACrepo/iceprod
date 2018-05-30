"""
Test script for scheduled_tasks/non_active_tasks
"""

import logging
logger = logging.getLogger('scheduled_tasks_non_active_tasks_test')

import os
import sys
import shutil
import tempfile
import unittest
from functools import partial
from unittest.mock import patch, MagicMock

from tornado.testing import AsyncTestCase

from tests.util import unittest_reporter, glob_tests

from iceprod.core import rest_client
from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import non_active_tasks

class non_active_tasks_test(AsyncTestCase):
    def setUp(self):
        super(non_active_tasks_test,self).setUp()
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
    def test_100_non_active_tasks(self):
        s = schedule(self.cfg,None,None,None)
        non_active_tasks.non_active_tasks(s)

    @unittest_reporter
    async def test_200_run(self):
        rc = MagicMock(spec=rest_client.Client)
        pilots = {}
        dataset_summaries = {'processing':['foo']}
        tasks = {}
        pilots = {}
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/dataset_summaries'):
                return dataset_summaries
            elif url.startswith('/pilots'):
                return pilots
            elif url.startswith('/datasets/foo/task_summaries'):
                return tasks
            elif url == '/datasets/foo/tasks/bar/status' and method == 'PUT':
                client.called = True
                return {}
            else:
                raise Exception()
        client.called = False
        rc.request = client

        await non_active_tasks.run(rc, debug=True)
        self.assertFalse(client.called)

        tasks['processing'] = ['bar']
        await non_active_tasks.run(rc, debug=True)
        self.assertTrue(client.called)

        client.called = False
        del dataset_summaries['processing']
        dataset_summaries['truncated'] = ['foo']
        pilots['a'] = {'tasks':['bar']}
        await non_active_tasks.run(rc, debug=True)
        self.assertFalse(client.called)


    @unittest_reporter(name='run() - error')
    async def test_201_run(self):
        rc = MagicMock(spec=rest_client.Client)
        pilots = {'a':{}}
        # try tasks error
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url.startswith('/pilots'):
                return pilots
            else:
                raise Exception()
        rc.request = client
        with self.assertRaises(Exception):
            await non_active_tasks.run(rc, debug=True)

        # check it normally hides the error
        await non_active_tasks.run(rc, debug=False)

        # try dataset level error
        async def client(method, url, args=None):
            raise Exception()
        rc.request = client
        with self.assertRaises(Exception):
            await non_active_tasks.run(rc, debug=True)

        # check it normally hides the error
        await non_active_tasks.run(rc, debug=False)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(non_active_tasks_test))
    suite.addTests(loader.loadTestsFromNames(alltests,non_active_tasks_test))
    return suite
