"""
Test script for the schedule module.
"""

import logging
logger = logging.getLogger('modules_schedule_test')

import os
import sys
from datetime import datetime,timedelta
import unittest
from unittest.mock import patch, MagicMock

import iceprod.server
from iceprod.server.modules.schedule import schedule
from iceprod.core import rest_client

from tests.util import unittest_reporter, glob_tests, services_mock
from ..module_test import module_test

class schedule_test(module_test):
    def setUp(self):
        super(schedule_test,self).setUp()
        try:
            self.cfg = {'queue':{
                            'init_queue_interval':0.1,
                            'submit_dir':self.test_dir,
                            '*':{'type':'Test1','description':'d'},
                        },
                        'master':{
                            'url':False,
                        },
                        'site_id':'abcd',
                       }
            self.executor = {}
            self.modules = services_mock()
            
            self.sched = schedule(self.cfg, self.io_loop, self.executor, self.modules)
            self.sched.rest_client = MagicMock(spec=rest_client.Client)
        except:
            logger.warn('error setting up modules_schedule', exc_info=True)
            raise

    @unittest_reporter(name='start/stop')
    def test_10_start(self):
        async def req(method, url, args=None):
            return {}
        self.sched.rest_client.request = req
        self.sched.start()
        self.sched.stop()

    @unittest_reporter(name='start/kill')
    def test_11_start_kill(self):
        async def req(method, url, args=None):
            return {}
        self.sched.rest_client.request = req
        self.sched.start()
        self.sched.kill()

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(schedule_test))
    suite.addTests(loader.loadTestsFromNames(alltests,schedule_test))
    return suite
