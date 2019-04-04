"""
Test script for the schedule module.
"""

import logging
logger = logging.getLogger('modules_rest_api_test')

import os
import sys
from datetime import datetime,timedelta
import unittest
import random
from unittest.mock import patch, MagicMock

from rest_tools.client import RestClient

import iceprod.server
from iceprod.server.modules.rest_api import rest_api

from tests.util import unittest_reporter, glob_tests, services_mock
from ..module_test import module_test

class rest_api_test(module_test):
    def setUp(self):
        super(rest_api_test,self).setUp()
        try:
            self.cfg = {'queue':{
                            'init_queue_interval':0.1,
                            'submit_dir':self.test_dir,
                            '*':{'type':'Test1','description':'d'},
                        },
                        'rest_api':{
                            'address': 'localhost',
                            'port': random.randint(10000,50000),
                        },
                        'site_id':'abcd',
                       }
            self.executor = {}
            self.modules = services_mock()
            
            self.mod = rest_api(self.cfg, self.io_loop, self.executor, self.modules)
            self.mod.rest_client = MagicMock(spec=RestClient)
        except:
            logger.warning('error setting up modules_schedule', exc_info=True)
            raise

    @unittest_reporter(name='start/stop')
    def test_10_start(self):
        async def req(method, url, args=None):
            return {}
        self.mod.rest_client.request = req
        self.mod.start()
        self.mod.stop()

    @unittest_reporter(name='start/kill')
    def test_11_start_kill(self):
        async def req(method, url, args=None):
            return {}
        self.mod.rest_client.request = req
        self.mod.start()
        self.mod.kill()

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_api_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_api_test))
    return suite
