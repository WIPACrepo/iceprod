"""
Test script for REST/pilots
"""

import logging
logger = logging.getLogger('rest_pilots_test')

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

from . import RestTestCase

class rest_pilots_test(RestTestCase):
    def setUp(self):
        config = {'rest':{'pilots':{}}}
        super(rest_pilots_test,self).setUp(config=config)

    @unittest_reporter(name='REST GET    /pilots')
    def test_100_pilots(self):
        client = AsyncHTTPClient()
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})

    @unittest_reporter(name='REST POST   /pilots')
    def test_105_pilots(self):
        client = AsyncHTTPClient()
        data = {
            'queue_host': 'foo.bar.baz',
            'queue_version': '1.2.3',
            'resources': {'foo':1}
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        pilot_id = ret['result']

        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn(pilot_id, ret)
        for k in data:
            self.assertIn(k, ret[pilot_id])
            self.assertEqual(data[k], ret[pilot_id][k])

    @unittest_reporter(name='REST GET    /pilots/<pilot_id>')
    def test_110_pilots(self):
        client = AsyncHTTPClient()
        data = {
            'queue_host': 'foo.bar.baz',
            'queue_version': '1.2.3',
            'resources': {'foo':1}
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        pilot_id = ret['result']

        r = yield client.fetch('http://localhost:%d/pilots/%s'%(self.port,pilot_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])
        self.assertIn('tasks', ret)
        self.assertEqual(ret['tasks'], [])

    @unittest_reporter(name='REST PATCH  /pilots/<pilot_id>')
    def test_120_pilots(self):
        client = AsyncHTTPClient()
        data = {
            'queue_host': 'foo.bar.baz',
            'queue_version': '1.2.3',
            'resources': {'foo':1}
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        pilot_id = ret['result']

        new_data = {
            'queues': {'foo': 'HTCondor', 'bar': 'HTCondor'},
            'version': '1.2.8',
            'tasks': ['baz'],
        }
        r = yield client.fetch('http://localhost:%d/pilots/%s'%(self.port,pilot_id),
                method='PATCH', body=json.dumps(new_data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in new_data:
            self.assertIn(k, ret)
            self.assertEqual(new_data[k], ret[k])

    @unittest_reporter(name='REST DELETE /pilots/<pilot_id>')
    def test_130_pilots(self):
        client = AsyncHTTPClient()
        data = {
            'queue_host': 'foo.bar.baz',
            'queue_version': '1.2.3',
            'resources': {'foo':1}
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        pilot_id = ret['result']

        r = yield client.fetch('http://localhost:%d/pilots/%s'%(self.port,pilot_id),
                method='DELETE',
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/pilots/%s'%(self.port,pilot_id),
                    headers={'Authorization': 'bearer '+self.token})

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_pilots_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_pilots_test))
    return suite
