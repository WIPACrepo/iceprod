"""
Test script for REST/logs
"""

import logging
logger = logging.getLogger('rest_logs_test')

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
import string

from tests.util import unittest_reporter, glob_tests

import tornado.web
import tornado.ioloop
from tornado.httputil import url_concat
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.testing import AsyncTestCase
import boto3
from moto import mock_s3

from rest_tools.server import Auth, RestServer

from iceprod.server.modules.rest_api import setup_rest
import iceprod.server.rest.logs

from . import RestTestCase


def fake_data(N):
    return ''.join(random.choices(string.printable, k=N))

class rest_logs_test(RestTestCase):
    def setUp(self):
        config = {'rest':{'logs':{}}}
        super(rest_logs_test,self).setUp(config=config)

    @unittest_reporter(name='REST POST   /logs')
    def test_100_logs(self):
        client = AsyncHTTPClient()
        data = {'data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

    @unittest_reporter(name='REST GET    /logs')
    def test_105_logs(self):
        client = AsyncHTTPClient()
        data = {'name': 'stdlog', 'data': 'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='GET',
                headers={'Authorization': 'bearer '+self.token})
        ret = json.loads(r.body)
        self.assertIn(log_id, ret)
        self.assertEqual(len(ret), 1)
        for k in data:
            self.assertIn(k, ret[log_id])
            self.assertEqual(data[k], ret[log_id][k])

        args = {'name': 'stdlog', 'keys': 'log_id|name|data'}
        r = yield client.fetch(url_concat('http://localhost:%d/logs'%self.port, args),
                method='GET',
                headers={'Authorization': 'bearer '+self.token})
        ret = json.loads(r.body)
        self.assertIn(log_id, ret)

    @unittest_reporter(name='REST GET    /logs/<log_id>')
    def test_110_logs(self):
        client = AsyncHTTPClient()
        data = {'data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        r = yield client.fetch('http://localhost:%d/logs/%s'%(self.port,log_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @unittest_reporter(name='REST POST   /datasets/<dataset_id>/logs')
    def test_120_logs(self):
        client = AsyncHTTPClient()
        data = {'data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/datasets/12345/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/logs/<log_id>')
    def test_130_logs(self):
        client = AsyncHTTPClient()
        data = {'dataset_id':'12345','data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/12345/logs/%s'%(self.port,log_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>/logs')
    def test_140_logs(self):
        client = AsyncHTTPClient()
        data = {'data':'foo', 'dataset_id': 'foo', 'task_id': 'bar', 'name': 'stdout'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs'%(self.port,),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        self.assertEqual(len(ret['logs']), 1)
        self.assertEqual(ret['logs'][0]['log_id'], log_id)
        self.assertEqual(data['data'], ret['logs'][0]['data'])

        # now try for groupings
        data = {'data':'bar', 'dataset_id': 'foo', 'task_id': 'bar', 'name': 'stderr'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        
        data = {'data':'baz', 'dataset_id': 'foo', 'task_id': 'bar', 'name': 'stdout'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        
        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs?group=true'%(self.port,),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        logging.debug('logs: %r', ret['logs'])
        self.assertEqual(len(ret['logs']), 2)
        self.assertEqual('baz', ret['logs'][0]['data'])
        self.assertEqual('bar', ret['logs'][1]['data'])

        # now check order, num, and keys
        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs?order=asc&num=1&keys=log_id|data'%(self.port,),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        logging.debug('logs: %r', ret['logs'])
        self.assertEqual(len(ret['logs']), 1)
        self.assertEqual(ret['logs'][0]['log_id'], log_id)
        self.assertEqual('foo', ret['logs'][0]['data'])
        self.assertCountEqual(['log_id','data'], list(ret['logs'][0].keys()))

class rest_logs_test2(RestTestCase):
    def setUp(self):
        config = {
            'rest':{
                'logs':{},
            },
            's3': {
                'access_key': 'XXX',
                'secret_key': 'XXX',
            },
        }
        super(rest_logs_test2,self).setUp(config=config)

    @mock_s3
    @unittest_reporter(name='REST POST   /logs - S3')
    def test_200_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])
        
        data = {'data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

    @mock_s3
    @unittest_reporter(name='REST GET    /logs/<log_id> - S3')
    def test_210_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        r = yield client.fetch('http://localhost:%d/logs/%s'%(self.port,log_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])
        
        data = {'data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

        r = yield client.fetch('http://localhost:%d/logs/%s'%(self.port,log_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @mock_s3
    @unittest_reporter(name='REST POST   /datasets/<dataset_id>/logs - S3')
    def test_220_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/datasets/12345/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        data = {'data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/datasets/12345/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

    @mock_s3
    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/logs/<log_id> - S3')
    def test_230_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        client = AsyncHTTPClient()
        data = {'dataset_id':'12345','data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        r = yield client.fetch('http://localhost:%d/datasets/12345/logs/%s'%(self.port,log_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])
        
        data = {'dataset_id':'12345','data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

        r = yield client.fetch('http://localhost:%d/datasets/12345/logs/%s'%(self.port,log_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @mock_s3
    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>/logs - S3')
    def test_240_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000), 'dataset_id': 'foo', 'task_id': 'bar'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs'%(self.port,),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        self.assertEqual(len(ret['logs']), 1)
        self.assertEqual(ret['logs'][0]['log_id'], log_id)
        self.assertEqual(data['data'], ret['logs'][0]['data'])
        
        data = {'data':fake_data(200000), 'dataset_id': 'foo', 'task_id': 'bar'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs?order=asc'%(self.port,),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        self.assertEqual(len(ret['logs']), 2)
        self.assertEqual(ret['logs'][1]['log_id'], log_id)
        self.assertEqual(data['data'], ret['logs'][1]['data'])


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_logs_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_logs_test))
    alltests = glob_tests(loader.getTestCaseNames(rest_logs_test2))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_logs_test2))
    return suite
