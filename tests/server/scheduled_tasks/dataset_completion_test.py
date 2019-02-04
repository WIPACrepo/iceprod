"""
Test script for scheduled_tasks/dataset_completion
"""

import logging
logger = logging.getLogger('scheduled_tasks_dataset_completion_test')

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
from iceprod.server.scheduled_tasks import dataset_completion

class dataset_completion_test(AsyncTestCase):
    def setUp(self):
        super(dataset_completion_test,self).setUp()
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
    def test_100_dataset_completion(self):
        s = schedule(self.cfg,None,None,None)
        dataset_completion.dataset_completion(s)

    @unittest_reporter
    async def test_200_run(self):
        rc = MagicMock(spec=RestClient)
        job_counts = {}
        dataset_summaries = {'processing':['foo']}
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/dataset_summaries'):
                return dataset_summaries
            elif url == '/datasets/foo':
                return {'jobs_submitted':2, 'tasks_submitted':2}
            elif url.startswith('/datasets/foo/job_counts'):
                return job_counts
            elif url == '/datasets/foo/status' and method == 'PUT':
                client.called = True
                client.status = args['status']
                return {}
            else:
                raise Exception()
        client.called = False
        client.status = None
        rc.request = client

        logger.info('test non-buffered')
        await dataset_completion.run(rc, debug=True)
        self.assertFalse(client.called)

        logger.info('test processing')
        job_counts['processing'] = 2
        await dataset_completion.run(rc, debug=True)
        self.assertFalse(client.called)

        logger.info('test errors')
        job_counts['errors'] = 2
        del job_counts['processing']
        await dataset_completion.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertEqual(client.status, 'errors')

        logger.info('test processing and suspended')
        del job_counts['errors']
        job_counts['processing'] = 1
        job_counts['suspended'] = 1
        client.called = False
        client.status = None
        await dataset_completion.run(rc, debug=True)
        self.assertFalse(client.called)

        logger.info('test suspended')
        del job_counts['processing']
        job_counts['suspended'] = 1
        job_counts['complete'] = 1
        await dataset_completion.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertEqual(client.status, 'suspended')

        logger.info('test complete')
        del job_counts['suspended']
        job_counts['complete'] = 2
        client.called = False
        client.status = None
        await dataset_completion.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertEqual(client.status, 'complete')

        logger.info('test truncated dataset')
        del dataset_summaries['processing']
        dataset_summaries['truncated'] = ['foo']
        client.called = False
        client.status = None
        await dataset_completion.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertEqual(client.status, 'complete')

    @unittest_reporter(name='run() - error')
    async def test_201_run(self):
        rc = MagicMock(spec=RestClient)
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            else:
                raise Exception()
        rc.request = client
        with self.assertRaises(Exception):
            await dataset_completion.run(rc, debug=True)

        # check it normally hides the error
        await dataset_completion.run(rc, debug=False)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dataset_completion_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dataset_completion_test))
    return suite
