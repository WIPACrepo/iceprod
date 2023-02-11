"""
Test script for scheduled_tasks/job_temp_cleaning
"""

import logging
logger = logging.getLogger('scheduled_tasks_job_temp_cleaning_test')

import os
import sys
import shutil
import tempfile
import unittest
from functools import partial
from datetime import datetime,timedelta
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor

from tornado.testing import AsyncTestCase
from rest_tools.client import RestClient

from tests.util import unittest_reporter, glob_tests

from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import job_temp_cleaning

class FakeFile:
    def __init__(self, d):
        self.directory = True
        self.name = d

class job_temp_cleaning_test(AsyncTestCase):
    def setUp(self):
        super(job_temp_cleaning_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        self.cfg = {
            'queue':{
                'init_queue_interval':0.1,
                'submit_dir':self.test_dir,
                'site_temp':'foo',
                '*':{'type':'Test1','description':'d'},
            },
            'master':{
                'url':False,
            },
            'site_id':'abcd',
        }
        self.executor = ThreadPoolExecutor(max_workers=2)

    @patch('iceprod.server.scheduled_tasks.job_temp_cleaning.GridFTP')
    @unittest_reporter
    async def test_200_run(self, gridftp):
        rc = MagicMock(spec=RestClient)
        jobs = {}
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/datasets?'):
                return {'0':{'dataset_id':'0', 'dataset':0}}
            elif url.startswith('/datasets/0/jobs'):
                client.called = True
                return jobs
            else:
                raise Exception()
        client.called = False
        rc.request = client
        gridftp.list.return_value = []
        
        await job_temp_cleaning.run(rc, {}, self.executor, debug=True)
        self.assertFalse(client.called)

        await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)
        self.assertFalse(client.called)

        gridftp.list.side_effect = [[FakeFile('0')], [FakeFile('1')]]
        await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)
        self.assertTrue(client.called)
        self.assertFalse(gridftp.rmtree.called)

        logger.info('try deleting completed job')
        client.called = False
        jobs['bar'] = {'job_index':1,'status':'complete'}
        gridftp.list.side_effect = [[FakeFile('0')], [FakeFile('1')]]
        await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(gridftp.rmtree.called)
        self.assertEqual(gridftp.rmtree.call_args[0][0], 'foo/0/1')

        logger.info('skip suspended job')
        client.called = False
        jobs['bar'] = {'job_index':1,'status':'suspended','status_changed':datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')}
        gridftp.list.side_effect = [[FakeFile('0')], [FakeFile('1')]]
        gridftp.rmtree.reset_mock()
        await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)
        self.assertTrue(client.called)
        self.assertFalse(gridftp.rmtree.called)

        logger.info('try deleting old suspended job')
        client.called = False
        jobs['bar'] = {'job_index':1,'status':'suspended','status_changed':
                       (datetime.utcnow()-timedelta(days=100)).strftime('%Y-%m-%dT%H:%M:%S')}
        gridftp.list.side_effect = [[FakeFile('0')], [FakeFile('1')]]
        gridftp.rmtree.reset_mock()
        await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(gridftp.rmtree.called)
        self.assertEqual(gridftp.rmtree.call_args[0][0], 'foo/0/1')

    @patch('iceprod.server.scheduled_tasks.job_temp_cleaning.GridFTP')
    @unittest_reporter(name='run() - error')
    async def test_201_run(self, gridftp):
        rc = MagicMock(spec=RestClient)
        jobs = {}
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/datasets?'):
                return {'0':{'dataset_id':'0', 'dataset':0}}
            elif url.startswith('/datasets/0/jobs'):
                client.called = True
                return jobs
            else:
                raise Exception()
        rc.request = client

        gridftp.list.side_effect = [[FakeFile('0')], [FakeFile('foobar')]]
        jobs['bar'] = {'job_index':1,'status':'suspended','status_changed':
                       (datetime.utcnow()-timedelta(days=100)).isoformat()}
        with self.assertRaises(Exception):
            await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)

        gridftp.list.side_effect = [[FakeFile('0')], [FakeFile('1')]]
        jobs['bar'] = {'job_index':1,'status':'suspended','status_changed':
                       (datetime.utcnow()-timedelta(days=100)).isoformat()}
        gridftp.rmtree.side_effect = Exception()
        with self.assertRaises(Exception):
            await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(gridftp.rmtree.called)
        self.assertEqual(gridftp.rmtree.call_args[0][0], 'foo/0/1')

        gridftp.list.side_effect = Exception()
        jobs['bar'] = {'job_index':1,'status':'suspended','status_changed':
                       (datetime.utcnow()-timedelta(days=100)).isoformat()}
        with self.assertRaises(Exception):
            await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=True)

        # check it normally hides the error
        await job_temp_cleaning.run(rc, self.cfg, self.executor, debug=False)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(job_temp_cleaning_test))
    suite.addTests(loader.loadTestsFromNames(alltests,job_temp_cleaning_test))
    return suite
