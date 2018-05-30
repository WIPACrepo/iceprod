"""
Test script for scheduled_tasks/job_completion
"""

import logging
logger = logging.getLogger('scheduled_tasks_job_completion_test')

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
from iceprod.server.scheduled_tasks import job_completion

class job_completion_test(AsyncTestCase):
    def setUp(self):
        super(job_completion_test,self).setUp()
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
    def test_100_job_completion(self):
        s = schedule(self.cfg,None,None,None)
        job_completion.job_completion(s)

    @unittest_reporter
    async def test_200_run(self):
        rc = MagicMock(spec=rest_client.Client)
        dataset_summaries = {}
        job_summaries = {}
        tasks = {
            't1':{'task_id':'t1','status':'processing'},
            't2':{'task_id':'t2','status':'waiting'},
        }
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/dataset_summaries'):
                return dataset_summaries
            if url.startswith('/datasets/foo/job_summaries'):
                return job_summaries
            elif url.startswith('/datasets/foo/tasks'):
                return tasks
            elif url == '/datasets/foo/jobs/1/status' and method == 'PUT':
                client.called = True
                client.status = args['status']
                return {}
            else:
                raise Exception()
        client.called = False
        client.status = None
        rc.request = client

        await job_completion.run(rc, debug=True)
        self.assertFalse(client.called)
        
        dataset_summaries['processing'] = ['foo']
        await job_completion.run(rc, debug=True)
        self.assertFalse(client.called)
        
        job_summaries['processing'] = ['1']
        await job_completion.run(rc, debug=True)
        self.assertFalse(client.called)

        logger.info('test processing')
        await job_completion.run(rc, debug=True)
        self.assertFalse(client.called)

        logger.info('test errors')
        tasks['t1']['status'] = 'failed'
        tasks['t2']['status'] = 'failed'
        await job_completion.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertEqual(client.status, 'errors')

        logger.info('test processing and suspended')
        tasks['t1']['status'] = 'processing'
        tasks['t2']['status'] = 'suspended'
        client.called = False
        client.status = None
        await job_completion.run(rc, debug=True)
        self.assertFalse(client.called)

        logger.info('test suspended')
        tasks['t1']['status'] = 'complete'
        tasks['t2']['status'] = 'suspended'
        await job_completion.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertEqual(client.status, 'suspended')

        logger.info('test complete')
        tasks['t1']['status'] = 'complete'
        tasks['t2']['status'] = 'complete'
        client.called = False
        client.status = None
        await job_completion.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertEqual(client.status, 'complete')

    @unittest_reporter(name='run() - error')
    async def test_201_run(self):
        rc = MagicMock(spec=rest_client.Client)
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            else:
                raise Exception()
        rc.request = client
        with self.assertRaises(Exception):
            await job_completion.run(rc, debug=True)

        # check it normally hides the error
        await job_completion.run(rc, debug=False)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(job_completion_test))
    suite.addTests(loader.loadTestsFromNames(alltests,job_completion_test))
    return suite
