"""
Test script for REST/auth
"""

import logging
logger = logging.getLogger('rest_auth_test')

import os
import sys
import time
import random
import shutil
import tempfile
import unittest
import subprocess
import json
from functools import partial
from unittest.mock import patch, MagicMock

from tests.util import unittest_reporter, glob_tests

import ldap3
import tornado.web
import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.testing import AsyncTestCase

from rest_tools.utils import Auth
from rest_tools.server import RestServer

from iceprod.server.modules.rest_api import setup_rest
import iceprod.server.rest.config

from . import RestTestCase

class rest_config_test(RestTestCase):
    def setUp(self):
        config = {'rest':{'config':{}}}
        super(rest_config_test,self).setUp(config=config)

    @unittest_reporter(name='REST GET    /config/<dataset_id>')
    def test_100_config(self):
        client = AsyncHTTPClient()
        with self.assertRaises(HTTPError) as e:
            r = yield client.fetch('http://localhost:%d/config/bar'%self.port,
                    headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(e.exception.code, 404)

    @unittest_reporter(name='REST PUT    /config/<dataset_id>')
    def test_110_config(self):
        client = AsyncHTTPClient()
        data = {
            'name': 'foo'
        }
        r = yield client.fetch('http://localhost:%d/config/bar'%self.port,
                method='PUT', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/config/bar'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data, ret)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_config_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_config_test))
    return suite
