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

import tornado.web
import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient
from tornado.testing import AsyncTestCase

import iceprod.server.tornado
from iceprod.server.auth import Auth

class rest_auth_test(AsyncTestCase):
    def setUp(self):
        super(rest_auth_test,self).setUp()
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
                    'auth': {
                        'database': 'mongodb://localhost:'+str(self.mongo_port),
                    }
                },
            }
            self.app = iceprod.server.tornado.setup_rest(config)
            self.token = Auth('secret').create_token('foo', type='user', payload={'role':'admin'})
        except Exception:
            logger.info('failed setup', exc_info=True)



    @unittest_reporter(name='REST GET    /roles')
    def test_100_role(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        r = yield client.fetch('http://localhost:%d/roles'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [])

    @unittest_reporter(name='REST PUT    /roles/<role_name>')
    def test_110_role(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': 'foo'
        }
        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/roles'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [{'name': 'foo'}])

    @unittest_reporter(name='REST GET    /roles/<role_name>')
    def test_120_role(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': 'foo'
        }
        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'name': 'foo'})

    @unittest_reporter(name='REST DELETE /roles/<role_name>')
    def test_130_role(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': 'foo'
        }
        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'name': 'foo'})

        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                method='DELETE',
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                    headers={'Authorization': b'bearer '+self.token})

        r = yield client.fetch('http://localhost:%d/roles'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [])

    @unittest_reporter(name='REST bad access to PUT /roles/<role_name>')
    def test_140_role(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': 'foo'
        }
        user_token = Auth('secret').create_token('foo', type='user', payload={'role':'user'})
        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port,data['name']),
                    method='PUT', body=json.dumps(data),
                    headers={'Authorization': b'bearer '+user_token})

    @unittest_reporter(name='REST GET    /groups')
    def test_200_group(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [])

    @unittest_reporter(name='REST POST   /groups')
    def test_210_group(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': '/foo/bar'
        }
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        group_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [{'group_id':group_id, 'name': '/foo/bar'}])

    @unittest_reporter(name='REST GET    /groups/<group_id>')
    def test_220_group(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': '/foo/bar'
        }
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        group_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/groups/%s'%(self.port, group_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'group_id':group_id, 'name': '/foo/bar'})

    @unittest_reporter(name='REST DELETE /groups/<group_id>')
    def test_230_group(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': '/foo/bar'
        }
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        group_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/groups/%s'%(self.port, group_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'group_id':group_id, 'name': '/foo/bar'})

        r = yield client.fetch('http://localhost:%d/groups/%s'%(self.port, group_id),
                method='DELETE',
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/groups/%s'%(self.port, group_id),
                    headers={'Authorization': b'bearer '+self.token})
        
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [])

    @unittest_reporter(name='REST bad access to POST /groups')
    def test_240_group(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'name': '/foo/bar'
        }
        user_token = Auth('secret').create_token('foo', type='user', payload={'role':'user'})
        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/groups'%self.port,
                    method='POST', body=json.dumps(data),
                    headers={'Authorization': b'bearer '+user_token})

    @unittest_reporter(name='REST GET    /users')
    def test_300_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [])

    @unittest_reporter(name='REST POST   /users')
    def test_310_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'username': 'foo'
        }
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        user_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/users'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [{'user_id':user_id, 'username':'foo'}])

    @unittest_reporter(name='REST GET    /users/<user_id>')
    def test_320_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'username': 'foo'
        }
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        user_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/users/%s'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'user_id':user_id, 'username':'foo'})

    @unittest_reporter(name='REST DELETE /users/<user_id>')
    def test_330_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'username': 'foo'
        }
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        user_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/users/%s'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'user_id':user_id, 'username':'foo'})

        r = yield client.fetch('http://localhost:%d/users/%s'%(self.port, user_id),
                method='DELETE',
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/users/%s'%(self.port, user_id),
                    headers={'Authorization': b'bearer '+self.token})
        
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertIn('results', data)
        self.assertEqual(data['results'], [])

    @unittest_reporter(name='REST bad access to POST /users')
    def test_340_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'username': 'foo'
        }
        user_token = Auth('secret').create_token('foo', type='user', payload={'role':'user'})
        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/users'%self.port,
                    method='POST', body=json.dumps(data),
                    headers={'Authorization': b'bearer '+user_token})

    @unittest_reporter(name='REST POST   /users/<user_id>/groups')
    def test_410_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'username': 'foo',
            'groups': ['bar']
        }
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        user_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/users/%s/groups'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'results': ['bar']})

        # now add the new group
        data = {
            'name': 'baz'
        }
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        group_id = data['result'].rsplit('/')[-1]

        data = {'group': group_id}
        r = yield client.fetch('http://localhost:%d/users/%s/groups'%(self.port, user_id),
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/users/%s/groups'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'results': ['bar',group_id]})

    @unittest_reporter(name='REST PUT    /users/<user_id>/groups')
    def test_420_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'username': 'foo',
            'groups': ['bar']
        }
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        user_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/users/%s/groups'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'results': ['bar']})

        # now add the new group
        data = {
            'name': 'baz'
        }
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        group_id = data['result'].rsplit('/')[-1]

        data = {
            'name': 'blah'
        }
        r = yield client.fetch('http://localhost:%d/groups'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        group_id2 = data['result'].rsplit('/')[-1]

        data = {'groups': [group_id, group_id2]}
        r = yield client.fetch('http://localhost:%d/users/%s/groups'%(self.port, user_id),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/users/%s/groups'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data, {'results': [group_id, group_id2]})

    @unittest_reporter(name='REST PUT    /users/<user_id>/roles')
    def test_500_user(self):
        iceprod.server.tornado.startup(self.app, port=self.port, io_loop=self.io_loop)

        client = AsyncHTTPClient(self.io_loop)
        data = {
            'username': 'foo',
            'roles': ['bar']
        }
        r = yield client.fetch('http://localhost:%d/users'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 201)
        data = json.loads(r.body)
        self.assertIn('result', data)
        self.assertEqual(data['result'], r.headers['Location'])
        user_id = data['result'].rsplit('/')[-1]

        r = yield client.fetch('http://localhost:%d/users/%s'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data['roles'], ['bar'])

        # now add the new role
        data = {
            'name': 'baz'
        }
        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port, 'baz'),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        data = {
            'name': 'blah'
        }
        r = yield client.fetch('http://localhost:%d/roles/%s'%(self.port, 'blah'),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        data = {'roles': ['baz','blah']}
        r = yield client.fetch('http://localhost:%d/users/%s/roles'%(self.port, user_id),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/users/%s'%(self.port, user_id),
                headers={'Authorization': b'bearer '+self.token})
        self.assertEqual(r.code, 200)
        data = json.loads(r.body)
        self.assertEqual(data['roles'], ['baz','blah'])


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_auth_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_auth_test))
    return suite
