"""
Test script for scheduled_tasks/update_task_priority
"""

import logging
logger = logging.getLogger('scheduled_tasks_update_task_priority_test')

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
from iceprod.server.scheduled_tasks import update_task_priority

class update_task_priority_test(AsyncTestCase):
    def setUp(self):
        super(update_task_priority_test,self).setUp()
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
    async def test_200_run(self):
        rc = MagicMock(spec=RestClient)
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url == '/tasks/bar':
                client.called = True
                return {}
            elif url == '/tasks':
                return {'tasks':[{'task_id':'bar','dataset_id':'foo'}]}
            elif url == '/datasets':
                return {'foo':{'dataset_id':'foo','username':'a','group':'g','tasks_submitted':200,'jobs_submitted':100,'priority':1.}}
            elif url == '/datasets/foo/tasks':
                return {'bar':{'task_id':'bar','dataset_id':'foo','task_index':0,'job_index':12}}
            elif url == '/datasets/foo/tasks/bar':
                return {'task_id':'bar','dataset_id':'foo','task_index':0,'job_index':12}
            elif url == '/users':
                return {'results':[{'username':'a','priority':0.5}]}
            elif url == '/groups':
                return {'results':[{'name':'g','priority':0.5}]}
            else:
                raise Exception()
        client.called = False
        rc.request = client

        await update_task_priority.run(rc, debug=True)
        self.assertTrue(client.called)


    @unittest_reporter(name='run() - error')
    async def test_201_run(self):
        rc = MagicMock(spec=RestClient)
        pilots = {'a':{}}
        # try tasks error
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            raise Exception()
        rc.request = client

        # check it normally hides the error
        await update_task_priority.run(rc, debug=False)

        with self.assertRaises(Exception):
            await reset_tasks.run(rc, debug=True)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(update_task_priority_test))
    suite.addTests(loader.loadTestsFromNames(alltests,update_task_priority_test))
    return suite
