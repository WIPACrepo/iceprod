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

from tests.util import unittest_reporter, glob_tests

import ldap3
import tornado.web
import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.testing import AsyncTestCase

import iceprod.server.tornado
import iceprod.server.rest.config
from iceprod.server.auth import Auth

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
                        'database': 'mongodb://localhost:'+str(self.mongo_port),
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
        data = {'data':'foo bar baz'}
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
        data = {'data':'foo bar baz', 'dataset_id': 'foo', 'task_id': 'bar'}
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

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_logs_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_logs_test))
    return suite
