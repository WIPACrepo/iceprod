"""
Test script for scheduled_tasks/dataset_monitor
"""

import logging
logger = logging.getLogger('scheduled_tasks_dataset_monitor_test')

import os
import sys
import shutil
import tempfile
import unittest
from functools import partial
from unittest.mock import patch, MagicMock
from collections import defaultdict

from tornado.testing import AsyncTestCase

from statsd import TCPStatsClient as StatsClient
from rest_tools.client import RestClient

from tests.util import unittest_reporter, glob_tests

from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import dataset_monitor

class dataset_monitor_test(AsyncTestCase):
    def setUp(self):
        super(dataset_monitor_test,self).setUp()
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
        pilots = {}
        jobs = {}
        tasks = defaultdict(dict)
        stats = {}
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if url.startswith('/dataset_summaries/status'):
                return {'processing':['foo']}
            elif url.startswith('/datasets/foo/job_counts/status'):
                return jobs
            elif url.startswith('/datasets/foo/task_counts/name_status'):
                client.called = True
                return tasks
            elif url.startswith('/datasets/foo/task_stats'):
                return stats
            if url.startswith('/datasets/foo'):
                return {'dataset':123,'status':'processing','jobs_submitted':1,'tasks_submitted':1}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        mon = MagicMock(spec=StatsClient)

        await dataset_monitor.run(rc, mon, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(mon.gauge.called)

        jobs['processing'] = 1
        mon.reset_mock()
        await dataset_monitor.run(rc, mon, debug=True)
        self.assertTrue(mon.gauge.called)

        tasks['generate']['queued'] = 1
        mon.reset_mock()
        await dataset_monitor.run(rc, mon, debug=True)
        self.assertTrue(mon.gauge.called)

    @unittest_reporter(name='run() - error')
    async def test_201_run(self):
        rc = MagicMock(spec=RestClient)
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            raise Exception()
        rc.request = client
        mon = MagicMock(spec=StatsClient)
        with self.assertRaises(Exception):
            await dataset_monitor.run(rc, mon, debug=True)

        # check it normally hides the error
        await dataset_monitor.run(rc, mon, debug=False)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dataset_monitor_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dataset_monitor_test))
    return suite
