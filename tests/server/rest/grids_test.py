"""
Test script for REST/grids
"""

import logging
logger = logging.getLogger('rest_grids_test')

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

from rest_tools.server import Auth, RestServer

from iceprod.server.modules.rest_api import setup_rest

from . import RestTestCase

class rest_grids_test(RestTestCase):
    def setUp(self):
        config = {'rest':{'grids':{}}}
        super(rest_grids_test,self).setUp(config=config)

    @unittest_reporter(name='REST GET    /grids')
    def test_100_grids(self):
        client = AsyncHTTPClient()
        r = yield client.fetch('http://localhost:%d/grids'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})

    @unittest_reporter(name='REST POST   /grids')
    def test_105_grids(self):
        client = AsyncHTTPClient()
        data = {
            'host': 'foo.bar.baz',
            'queues': {'foo': 'HTCondor'},
            'version': '1.2.3',
        }
        r = yield client.fetch('http://localhost:%d/grids'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        grid_id = ret['result']

        r = yield client.fetch('http://localhost:%d/grids'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn(grid_id, ret)
        for k in data:
            self.assertIn(k, ret[grid_id])
            self.assertEqual(data[k], ret[grid_id][k])

    @unittest_reporter(name='REST GET    /grids/<grid_id>')
    def test_110_grids(self):
        client = AsyncHTTPClient()
        data = {
            'host': 'foo.bar.baz',
            'queues': {'foo': 'HTCondor'},
            'version': '1.2.3',
        }
        r = yield client.fetch('http://localhost:%d/grids'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        grid_id = ret['result']

        r = yield client.fetch('http://localhost:%d/grids/%s'%(self.port,grid_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])

    @unittest_reporter(name='REST PATCH  /grids/<grid_id>')
    def test_120_grids(self):
        client = AsyncHTTPClient()
        data = {
            'host': 'foo.bar.baz',
            'queues': {'foo': 'HTCondor'},
            'version': '1.2.3',
        }
        r = yield client.fetch('http://localhost:%d/grids'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        grid_id = ret['result']

        new_data = {
            'queues': {'foo': 'HTCondor', 'bar': 'HTCondor'},
            'version': '1.2.8',
        }
        r = yield client.fetch('http://localhost:%d/grids/%s'%(self.port,grid_id),
                method='PATCH', body=json.dumps(new_data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in new_data:
            self.assertIn(k, ret)
            self.assertEqual(new_data[k], ret[k])

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_grids_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_grids_test))
    return suite
