"""
Test script for scheduled_tasks/pilot_cleanup
"""

import logging
logger = logging.getLogger('scheduled_tasks_pilot_cleanup_test')

import os
import sys
import shutil
import tempfile
import unittest
from functools import partial
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from tornado.testing import AsyncTestCase
from statsd import StatsClient
from rest_tools.client import RestClient

from tests.util import unittest_reporter, glob_tests

from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import pilot_cleanup
from iceprod.server.util import datetime2str, nowstr

class pilot_cleanup_test(AsyncTestCase):
    def setUp(self):
        super(pilot_cleanup_test,self).setUp()
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
    def test_100_pilot_cleanup(self):
        s = schedule(self.cfg,None,None,None)
        pilot_cleanup.pilot_cleanup(s)

    @unittest_reporter
    async def test_200_run(self):
        rc = MagicMock(spec=RestClient)
        pilots = {}
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            if method == 'GET' and url.startswith('/pilots'):
                return pilots
            elif method == 'DELETE' and url.startswith('/pilots'):
                client.called = True
            else:
                raise Exception()
        client.called = False
        rc.request = client

        await pilot_cleanup.run(rc, debug=True)
        self.assertFalse(client.called)

        pilots['a'] = {'pilot_id':'a', 'grid_queue_id':'', 'last_update':nowstr()}
        await pilot_cleanup.run(rc, debug=True)
        self.assertTrue(client.called)

        client.called = False
        pilots['a'] = {'pilot_id':'a', 'grid_queue_id':'blah', 'last_update':datetime2str(datetime.utcnow()-timedelta(days=30))}
        await pilot_cleanup.run(rc, debug=True)
        self.assertTrue(client.called)

        client.called = False
        pilots['a'] = {'pilot_id':'a', 'grid_queue_id':'blah', 'last_update':nowstr()}
        await pilot_cleanup.run(rc, debug=True)
        self.assertFalse(client.called)

    @unittest_reporter(name='run() - error')
    async def test_201_run(self):
        rc = MagicMock(spec=RestClient)
        async def client(method, url, args=None):
            logger.info('REST: %s, %s', method, url)
            raise Exception()
        rc.request = client
        with self.assertRaises(Exception):
            await pilot_cleanup.run(rc, debug=True)

        # check it normally hides the error
        await pilot_cleanup.run(rc, debug=False)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(pilot_cleanup_test))
    suite.addTests(loader.loadTestsFromNames(alltests,pilot_cleanup_test))
    return suite
