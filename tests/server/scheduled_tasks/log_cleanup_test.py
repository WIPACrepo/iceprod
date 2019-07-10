"""
Test script for scheduled_tasks/log_cleanup
"""

import logging
logger = logging.getLogger('scheduled_tasks_log_cleanup_test')

import os
import sys
import shutil
import tempfile
import unittest
from functools import partial
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from tornado.testing import AsyncTestCase
from statsd import TCPStatsClient as StatsClient
from rest_tools.client import RestClient

from tests.util import unittest_reporter, glob_tests

from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import log_cleanup
from iceprod.server.util import datetime2str, nowstr

class log_cleanup_test(AsyncTestCase):
    def setUp(self):
        super(log_cleanup_test,self).setUp()
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
    def test_100_log_cleanup(self):
        s = schedule(self.cfg,None,None,None)
        log_cleanup.log_cleanup(s)

    @unittest_reporter
    async def test_200_run(self):
        rc = MagicMock(spec=RestClient)
        logs = {}
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if method == 'GET' and url.startswith('/logs'):
                return logs
            elif method == 'DELETE' and url.startswith('/logs'):
                client.called = True
            else:
                raise Exception()
        client.called = False
        rc.request = client

        await log_cleanup.run(rc, debug=True)
        self.assertFalse(client.called)

        client.called = False
        logs['a'] = {'log_id':'a'}
        await log_cleanup.run(rc, debug=True)
        self.assertTrue(client.called)

    @unittest_reporter(name='run() - error')
    async def test_201_run(self):
        rc = MagicMock(spec=RestClient)
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            raise Exception()
        rc.request = client
        with self.assertRaises(Exception):
            await log_cleanup.run(rc, debug=True)

        # check it normally hides the error
        await log_cleanup.run(rc, debug=False)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(log_cleanup_test))
    suite.addTests(loader.loadTestsFromNames(alltests,log_cleanup_test))
    return suite
