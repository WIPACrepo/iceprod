"""
Test script for REST/datasets
"""

import logging
logger = logging.getLogger('rest_datasets_test')

import os
import sys
import time
import random
import re
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
import tornado.gen
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest, HTTPResponse
from tornado.testing import AsyncTestCase

from rest_tools.utils import Auth
from rest_tools.server import RestServer

from iceprod.server.modules.rest_api import setup_rest

from . import RestTestCase

orig_fetch = tornado.httpclient.AsyncHTTPClient.fetch

class rest_datasets_test(RestTestCase):
    def setUp(self):
        self.module_auth_token = Auth('secret').create_token('foo', type='system', payload={'role':'system','username':'admin'})
        config = {
            'rest':{
                'datasets':{},
            },
            'rest_api': {
                'auth_key': self.module_auth_token,
            },
        }
        super(rest_datasets_test,self).setUp(config=config)

    @unittest_reporter(name='REST GET    /datasets')
    def test_100_datasets(self):
        client = AsyncHTTPClient()
        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})

    @unittest_reporter(name='REST POST   /datasets')
    def test_110_datasets(self):
        client = AsyncHTTPClient()
        data = {
            'description': 'blah',
            'tasks_per_job': 4,
            'jobs_submitted': 1,
            'tasks_submitted': 4,
            'group': 'foo',
        }
        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        uri = ret['result']
        dataset_id = uri.split('/')[-1]

        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn(dataset_id, ret)
        for k in data:
            self.assertIn(k, ret[dataset_id])
            self.assertEqual(data[k], ret[dataset_id][k])

        r = yield client.fetch('http://localhost:%d/datasets?status=suspended'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertNotIn(dataset_id, ret)

        r = yield client.fetch('http://localhost:%d/datasets?keys=dataset_id|dataset'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn(dataset_id, ret)
        self.assertCountEqual(['dataset_id','dataset'], ret[dataset_id])

        r = yield client.fetch('http://localhost:%d/datasets/%s'%(self.port,dataset_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>')
    def test_120_datasets(self):
        client = AsyncHTTPClient()
        with self.assertRaises(HTTPError) as e:
            r = yield client.fetch('http://localhost:%d/datasets/bar'%self.port,
                    headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(e.exception.code, 404)

    @unittest_reporter(name='REST PUT    /datasets/<dataset_id>/description')
    def test_200_datasets(self):
        client = AsyncHTTPClient()
        data = {
            'description': 'blah',
            'tasks_per_job': 4,
            'jobs_submitted': 1,
            'tasks_submitted': 4,
            'group': 'foo',
        }
        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        uri = ret['result']
        dataset_id = uri.split('/')[-1]

        data = {'description': 'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/description'%(self.port,dataset_id),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})
        
        r = yield client.fetch('http://localhost:%d/datasets/%s'%(self.port,dataset_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['description'], data['description'])

    @unittest_reporter(name='REST PUT    /datasets/<dataset_id>/status')
    def test_210_datasets(self):
        client = AsyncHTTPClient()
        data = {
            'description': 'blah',
            'tasks_per_job': 4,
            'jobs_submitted': 1,
            'tasks_submitted': 4,
            'group': 'foo',
        }
        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        uri = ret['result']
        dataset_id = uri.split('/')[-1]

        data = {'status': 'suspended'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/status'%(self.port,dataset_id),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})
        
        r = yield client.fetch('http://localhost:%d/datasets/%s'%(self.port,dataset_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], data['status'])

    @unittest_reporter(name='REST PUT    /datasets/<dataset_id>/status')
    def test_220_datasets(self):
        client = AsyncHTTPClient()
        data = {
            'description': 'blah',
            'tasks_per_job': 4,
            'jobs_submitted': 1,
            'tasks_submitted': 4,
            'group': 'foo',
        }
        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        uri = ret['result']
        dataset_id = uri.split('/')[-1]

        data = {'jobs_submitted': 2}
        r = yield client.fetch('http://localhost:%d/datasets/%s/jobs_submitted'%(self.port,dataset_id),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})
        
        r = yield client.fetch('http://localhost:%d/datasets/%s'%(self.port,dataset_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['jobs_submitted'], data['jobs_submitted'])

        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/datasets/%s/jobs_submitted'%(self.port,'blah'),
                    method='PUT', body=json.dumps(data),
                    headers={'Authorization': 'bearer '+self.token})

        data = {}
        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/datasets/%s/jobs_submitted'%(self.port,dataset_id),
                    method='PUT', body=json.dumps(data),
                    headers={'Authorization': 'bearer '+self.token})
        
        data = {'jobs_submitted': 'foo'}
        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/datasets/%s/jobs_submitted'%(self.port,dataset_id),
                    method='PUT', body=json.dumps(data),
                    headers={'Authorization': 'bearer '+self.token})

        data = {'jobs_submitted': 0}
        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/datasets/%s/jobs_submitted'%(self.port,dataset_id),
                    method='PUT', body=json.dumps(data),
                    headers={'Authorization': 'bearer '+self.token})

        data = {
            'description': 'blah',
            'tasks_per_job': 4,
            'jobs_submitted': 1,
            'tasks_submitted': 4,
            'group': 'foo',
            'jobs_immutable': True,
        }
        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        uri = ret['result']
        dataset_id = uri.split('/')[-1]

        data = {'jobs_submitted': 4}
        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/datasets/%s/jobs_submitted'%(self.port,dataset_id),
                    method='PUT', body=json.dumps(data),
                    headers={'Authorization': 'bearer '+self.token})

    @unittest_reporter(name='REST GET    /dataset_summaries/status')
    def test_300_datasets(self):
        client = AsyncHTTPClient()
        data = {
            'description': 'blah',
            'tasks_per_job': 4,
            'jobs_submitted': 1,
            'tasks_submitted': 4,
            'group': 'foo',
        }
        r = yield client.fetch('http://localhost:%d/datasets'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        uri = ret['result']
        dataset_id = uri.split('/')[-1]
        
        r = yield client.fetch('http://localhost:%d/dataset_summaries/status'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {'processing': [dataset_id]})

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_datasets_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_datasets_test))
    return suite
