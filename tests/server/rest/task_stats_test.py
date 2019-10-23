"""
Test script for REST/task_stats
"""

import logging
logger = logging.getLogger('rest_task_stats_test')

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

class rest_task_stats_test(RestTestCase):
    def setUp(self):
        config = {'rest':{'task_stats':{}}}
        super(rest_task_stats_test,self).setUp(config=config)


    @unittest_reporter(name='REST POST   /tasks/<task_id>/task_stats')
    def test_100_task_stats(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'bar': 1.23456,
            'baz': [1,2,3,4],
        }
        r = yield client.fetch('http://localhost:%d/tasks/%s/task_stats'%(self.port,'bar'),
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_stat_id = ret['result']

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>/task_stats')
    def test_200_task_stats(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'bar': 1.23456,
            'baz': [1,2,3,4],
        }
        task_id = 'bar'
        r = yield client.fetch('http://localhost:%d/tasks/%s/task_stats'%(self.port,task_id),
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_stat_id = ret['result']

        time.sleep(0.1)
        r = yield client.fetch('http://localhost:%d/tasks/%s/task_stats'%(self.port,task_id),
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_stat_id2 = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/task_stats'%(self.port,'foo',task_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(len(ret), 2)
        self.assertIn(task_stat_id, ret)
        self.assertIn(task_stat_id2, ret)
        self.assertIn('task_id', ret[task_stat_id])
        self.assertEqual(task_id, ret[task_stat_id]['task_id'])
        self.assertEqual(data, ret[task_stat_id]['stats'])

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/task_stats?last=true'%(self.port,'foo',task_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(len(ret), 1)
        self.assertIn(task_stat_id2, ret)
        self.assertIn('task_id', ret[task_stat_id2])
        self.assertEqual(task_id, ret[task_stat_id2]['task_id'])
        self.assertEqual(data, ret[task_stat_id2]['stats'])

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/task_stats?last=true&keys=task_id'%(self.port,'foo',task_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(len(ret), 1)
        self.assertIn(task_stat_id2, ret)
        self.assertIn('task_id', ret[task_stat_id2])
        self.assertEqual(task_id, ret[task_stat_id2]['task_id'])
        self.assertNotIn('stats', ret[task_stat_id2])

    # note: the name is so long it needs a break to wrap correctly
    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>/task_stats/<task_stat_id>')
    def test_210_task_stats(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'bar': 1.23456,
            'baz': [1,2,3,4],
        }
        task_id = 'bar'
        r = yield client.fetch('http://localhost:%d/tasks/%s/task_stats'%(self.port,task_id),
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_stat_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/task_stats/%s'%(self.port,'foo',task_id,task_stat_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(task_stat_id, ret['task_stat_id'])
        self.assertEqual(task_id, ret['task_id'])
        self.assertEqual(data, ret['stats'])

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_task_stats_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_task_stats_test))
    return suite
