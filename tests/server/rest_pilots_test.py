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

import iceprod.server.tornado
import iceprod.server.rest.config
from iceprod.server.auth import Auth

class rest_pilots_test(AsyncTestCase):
    def setUp(self):
        super(rest_pilots_test,self).setUp()
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
                    'pilots': {
                        'database': 'mongodb://localhost:'+str(self.mongo_port),
                    }
                },
            }
            self.app = iceprod.server.tornado.setup_rest(config)
            self.token = Auth('secret').create_token('foo', type='user', payload={'role':'admin','username':'admin'})
        except Exception:
            logger.info('failed setup', exc_info=True)

    @unittest_reporter(name='REST GET    /pilots')
    def test_100_pilots(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})

    @unittest_reporter(name='REST POST   /pilots')
    def test_105_pilots(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {
            'host': 'foo.bar.baz',
            'version': '1.2.3',
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        pilot_id = ret['result']

        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn(pilot_id, ret)
        for k in data:
            self.assertIn(k, ret[pilot_id])
            self.assertEqual(data[k], ret[pilot_id][k])

    @unittest_reporter(name='REST GET    /pilots/<pilot_id>')
    def test_110_pilots(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {
            'host': 'foo.bar.baz',
            'version': '1.2.3',
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        pilot_id = ret['result']

        r = yield client.fetch('http://localhost:%d/pilots/%s'%(self.port,pilot_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])
        self.assertIn('tasks', ret)
        self.assertEqual(ret['tasks'], [])

    @unittest_reporter(name='REST PATCH  /pilots/<pilot_id>')
    def test_120_pilots(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {
            'host': 'foo.bar.baz',
            'version': '1.2.3',
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
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
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in new_data:
            self.assertIn(k, ret)
            self.assertEqual(new_data[k], ret[k])

    @unittest_reporter(name='REST DELETE /pilots/<pilot_id>')
    def test_130_pilots(self):
        iceprod.server.tornado.startup(self.app, port=self.port)

        client = AsyncHTTPClient()
        data = {
            'host': 'foo.bar.baz',
            'version': '1.2.3',
        }
        r = yield client.fetch('http://localhost:%d/pilots'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        pilot_id = ret['result']

        r = yield client.fetch('http://localhost:%d/pilots/%s'%(self.port,pilot_id),
                method='DELETE',
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/pilots/%s'%(self.port,pilot_id),
                    headers={'Authorization': b'bearer '+self.token})

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_pilots_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_pilots_test))
    return suite
