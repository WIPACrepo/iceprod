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
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.testing import AsyncTestCase
import boto3
from moto import mock_s3

import iceprod.server.tornado
import iceprod.server.rest.config
import iceprod.server.rest.logs
from iceprod.server.auth import Auth


def fake_data(N):
    return ''.join(random.choices(string.printable, k=N))

class rest_logs_test(AsyncTestCase):
    def setUp(self):
        super(rest_logs_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        try:
            self.port = random.randint(10000,50000)
            self.mongo_port = random.randint(10000,50000)
            dbpath = os.path.join(self.test_dir,'db')
            os.mkdir(dbpath)
            dblog = os.path.join(dbpath,'logfile')

            m = subprocess.Popen(['mongod', '--port', str(self.mongo_port),
                                  '--dbpath', dbpath, '--smallfiles',
                                  '--quiet', '--nounixsocket',
                                  '--logpath', dblog])
            self.addCleanup(partial(time.sleep, 0.05))
            self.addCleanup(m.terminate)

            config = {
                'auth': {
                    'secret': 'secret'
                },
                'rest': {
                    'logs': {
                        'database': {'port':self.mongo_port},
                    }
                },
            }
            self.app = iceprod.server.tornado.setup_rest(config)
            self.token = Auth('secret').create_token('foo', type='user', payload={'role':'admin'})
        except Exception:
            logger.info('failed setup', exc_info=True)

    @unittest_reporter(name='REST POST   /logs')
    def test_100_logs(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

    @unittest_reporter(name='REST GET    /logs/<log_id>')
    def test_110_logs(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        r = yield client.fetch('http://localhost:%d/logs/%s'%(self.port,log_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @unittest_reporter(name='REST POST   /datasets/<dataset_id>/logs')
    def test_120_logs(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/datasets/12345/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/logs/<log_id>')
    def test_130_logs(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'dataset_id':'12345','data':'foo bar baz'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/12345/logs/%s'%(self.port,log_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>/logs')
    def test_140_logs(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':'foo', 'dataset_id': 'foo', 'task_id': 'bar', 'name': 'stdout'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs'%(self.port,),
                headers={'Authorization': b'bearer '+self.token})
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
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        
        data = {'data':'baz', 'dataset_id': 'foo', 'task_id': 'bar', 'name': 'stdout'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        
        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs?group=true'%(self.port,),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        logging.debug('logs: %r', ret['logs'])
        self.assertEqual(len(ret['logs']), 2)
        self.assertEqual('baz', ret['logs'][0]['data'])
        self.assertEqual('bar', ret['logs'][1]['data'])

        # now check order, num, and keys
        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs?order=asc&num=1&keys=log_id|data'%(self.port,),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        logging.debug('logs: %r', ret['logs'])
        self.assertEqual(len(ret['logs']), 1)
        self.assertEqual(ret['logs'][0]['log_id'], log_id)
        self.assertEqual('foo', ret['logs'][0]['data'])
        self.assertCountEqual(['log_id','data'], list(ret['logs'][0].keys()))

class rest_logs_test2(AsyncTestCase):
    def setUp(self):
        super(rest_logs_test2,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        try:
            self.port = random.randint(10000,50000)
            self.mongo_port = random.randint(10000,50000)
            dbpath = os.path.join(self.test_dir,'db')
            os.mkdir(dbpath)
            dblog = os.path.join(dbpath,'logfile')

            m = subprocess.Popen(['mongod', '--port', str(self.mongo_port),
                                  '--dbpath', dbpath, '--smallfiles',
                                  '--quiet', '--nounixsocket',
                                  '--logpath', dblog])
            self.addCleanup(partial(time.sleep, 0.05))
            self.addCleanup(m.terminate)

            config = {
                'auth': {
                    'secret': 'secret'
                },
                'rest': {
                    'logs': {
                        'database': {'port':self.mongo_port},
                    }
                },
                's3': {
                    'access_key': 'XXX',
                    'secret_key': 'XXX',
                },
            }
            self.app = iceprod.server.tornado.setup_rest(config)
            self.token = Auth('secret').create_token('foo', type='user', payload={'role':'admin'})
        except Exception:
            logger.info('failed setup', exc_info=True)

    @mock_s3
    @unittest_reporter(name='REST POST   /logs - S3')
    def test_200_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])
        
        data = {'data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
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

        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        r = yield client.fetch('http://localhost:%d/logs/%s'%(self.port,log_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])
        
        data = {'data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

        r = yield client.fetch('http://localhost:%d/logs/%s'%(self.port,log_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @mock_s3
    @unittest_reporter(name='REST POST   /datasets/<dataset_id>/logs - S3')
    def test_220_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/datasets/12345/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        data = {'data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/datasets/12345/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
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

        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'dataset_id':'12345','data':fake_data(2000000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        r = yield client.fetch('http://localhost:%d/datasets/12345/logs/%s'%(self.port,log_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])
        
        data = {'dataset_id':'12345','data':fake_data(200000)}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

        r = yield client.fetch('http://localhost:%d/datasets/12345/logs/%s'%(self.port,log_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(data['data'], ret['data'])

    @mock_s3
    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>/logs - S3')
    def test_240_logs(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')

        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {'data':fake_data(2000000), 'dataset_id': 'foo', 'task_id': 'bar'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        body = conn.Object('iceprod2-logs', log_id).get()['Body'].read().decode('utf-8')
        self.assertEqual(body, data['data'])

        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs'%(self.port,),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('logs', ret)
        self.assertEqual(len(ret['logs']), 1)
        self.assertEqual(ret['logs'][0]['log_id'], log_id)
        self.assertEqual(data['data'], ret['logs'][0]['data'])
        
        data = {'data':fake_data(200000), 'dataset_id': 'foo', 'task_id': 'bar'}
        r = yield client.fetch('http://localhost:%d/logs'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        log_id = ret['result']

        with self.assertRaises(Exception):
            conn.Object('iceprod2-logs', log_id).get()

        r = yield client.fetch('http://localhost:%d/datasets/foo/tasks/bar/logs'%(self.port,),
                headers={'Authorization': b'bearer '+self.token})
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
