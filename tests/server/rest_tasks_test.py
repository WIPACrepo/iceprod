"""
Test script for REST/tasks
"""

import logging
logger = logging.getLogger('rest_tasks_test')

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

class rest_tasks_test(AsyncTestCase):
    def setUp(self):
        super(rest_tasks_test,self).setUp()
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
                    'tasks': {
                        'database': 'mongodb://localhost:'+str(self.mongo_port),
                    }
                },
            }
            self.app = iceprod.server.tornado.setup_rest(config)
            self.token = Auth('secret').create_token('foo', type='user', payload={'role':'admin','username':'admin'})
        except Exception:
            logger.info('failed setup', exc_info=True)

    @unittest_reporter(name='REST POST   /tasks')
    def test_105_tasks(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'dataset_id': 'foo',
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        self.assertIn('result', ret)

    @unittest_reporter(name='REST GET    /tasks/<task_id>')
    def test_110_tasks(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'dataset_id': 'foo',
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/tasks/%s'%(self.port,task_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])
        for k in ('status','status_changed','failures','evictions','walltime',
                  'walltime_err','walltime_err_n'):
            self.assertIn(k, ret)
        self.assertEqual(ret['status'], 'idle')

    @unittest_reporter(name='REST PATCH  /tasks/<task_id>')
    def test_120_tasks(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'dataset_id': 'foo',
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        new_data = {
            'status': 'processing',
            'failures': 1,
        }
        r = yield client.fetch('http://localhost:%d/tasks/%s'%(self.port,task_id),
                method='PATCH', body=json.dumps(new_data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in new_data:
            self.assertIn(k, ret)
            self.assertEqual(new_data[k], ret[k])

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks')
    def test_200_tasks(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'dataset_id': 'foo',
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks'%(self.port,data['dataset_id']),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn(task_id, ret)
        for k in data:
            self.assertIn(k, ret[task_id])
            self.assertEqual(data[k], ret[task_id][k])

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>')
    def test_210_tasks(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'dataset_id': 'foo',
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])
        for k in ('status','status_changed','failures','evictions','walltime',
                  'walltime_err','walltime_err_n'):
            self.assertIn(k, ret)
        self.assertEqual(ret['status'], 'idle')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_tasks_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_tasks_test))
    return suite
