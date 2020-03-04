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

from rest_tools.server import Auth, RestServer

from iceprod.server.modules.rest_api import setup_rest

from . import RestTestCase

class rest_tasks_test(RestTestCase):
    def setUp(self):
        config = {'rest':{'tasks':{}}}
        super(rest_tasks_test,self).setUp(config=config)

    @unittest_reporter(name='REST POST   /tasks')
    def test_105_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        self.assertIn('result', ret)

    @unittest_reporter(name='REST GET    /tasks')
    def test_106_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('tasks', ret)
        self.assertEqual(len(ret['tasks']), 1)
        self.assertIn('task_id', ret['tasks'][0])
        self.assertEqual(ret['tasks'][0]['task_id'], task_id)

        r = yield client.fetch('http://localhost:%d/tasks?keys=dataset_id|name'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('tasks', ret)
        self.assertEqual(len(ret['tasks']), 1)
        self.assertNotIn('task_id', ret['tasks'][0])
        self.assertIn('dataset_id', ret['tasks'][0])
        self.assertIn('name', ret['tasks'][0])

        data = {'status':'queued'}
        r = yield client.fetch('http://localhost:%d/tasks/%s/status'%(self.port,task_id),
                method='PUT', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})

        r = yield client.fetch('http://localhost:%d/tasks?status=waiting'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('tasks', ret)
        self.assertEqual(len(ret['tasks']), 0)

        r = yield client.fetch('http://localhost:%d/tasks?status=queued'%self.port,
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('tasks', ret)
        self.assertEqual(len(ret['tasks']), 1)
        self.assertIn('task_id', ret['tasks'][0])
        self.assertEqual(ret['tasks'][0]['task_id'], task_id)

    @unittest_reporter(name='REST GET    /tasks/<task_id>')
    def test_110_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/tasks/%s'%(self.port,task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])
        for k in ('status','status_changed','failures','evictions','walltime',
                  'walltime_err','walltime_err_n'):
            self.assertIn(k, ret)
        self.assertEqual(ret['status'], 'waiting')

    @unittest_reporter(name='REST PATCH  /tasks/<task_id>')
    def test_120_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        new_data = {
            'status': 'processing',
            'failures': 1,
        }
        r = yield client.fetch('http://localhost:%d/tasks/%s'%(self.port,task_id),
                method='PATCH', body=json.dumps(new_data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in new_data:
            self.assertIn(k, ret)
            self.assertEqual(new_data[k], ret[k])

    @unittest_reporter(name='REST PUT    /tasks/<task_id>/status')
    def test_130_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data2 = {'status':'failed'}
        r = yield client.fetch('http://localhost:%d/tasks/%s/status'%(self.port,task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/tasks/%s'%(self.port,task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('status', ret)
        self.assertEqual(ret['status'], 'failed')

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks')
    def test_200_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks'%(self.port,data['dataset_id']),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn(task_id, ret)
        for k in data:
            self.assertIn(k, ret[task_id])
            self.assertEqual(data[k], ret[task_id][k])

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/tasks/<task_id>')
    def test_210_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        for k in data:
            self.assertIn(k, ret)
            self.assertEqual(data[k], ret[k])
        for k in ('status','status_changed','failures','evictions','walltime',
                  'walltime_err','walltime_err_n'):
            self.assertIn(k, ret)
        self.assertEqual(ret['status'], 'waiting')

    @unittest_reporter(name='REST PUT    /datasets/<dataset_id>/tasks/<task_id>/status')
    def test_220_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data2 = {'status':'failed'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('status', ret)
        self.assertEqual(ret['status'], 'failed')

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/task_summaries/status')
    def test_300_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/task_summaries/status'%(self.port,data['dataset_id']),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {'waiting': [task_id]})

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/task_counts/status')
    def test_400_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/task_counts/status'%(self.port,data['dataset_id']),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {'waiting': 1})

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/task_counts/name_status')
    def test_410_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/task_counts/name_status'%(self.port,data['dataset_id']),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {'bar':{'waiting': 1}})

    @unittest_reporter(name='REST GET    /datasets/<dataset_id>/task_stats')
    def test_450_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/datasets/%s/task_stats'%(self.port,data['dataset_id']),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {})

        # now mark complete
        data2 = {'status':'complete'}
        r = yield client.fetch('http://localhost:%d/tasks/%s/status'%(self.port,task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/task_stats'%(self.port,data['dataset_id']),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        logger.info('ret: %r', ret)
        self.assertIn('bar', ret)
        for s in ('count','total_hrs','total_err_hrs','avg_hrs','stddev_hrs','min_hrs','max_hrs','efficiency'):
            self.assertIn(s, ret['bar'])

    @unittest_reporter(name='REST POST   /task_actions/queue')
    def test_500_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'status': 'waiting',
            'priority': .5,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        r = yield client.fetch('http://localhost:%d/task_actions/queue'%(self.port,),
                method='POST', body=json.dumps({}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('queued', ret)
        self.assertEqual(ret['queued'], 1)
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'queued')

    @unittest_reporter(name='REST POST   /task_actions/queue - with priorities')
    def test_501_tasks(self):
        # now try with dataset priorities
        client = AsyncHTTPClient()

        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'status': 'waiting',
            'priority': 1,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data = {
            'dataset_id': 'bar',
            'job_id': 'bar1',
            'task_index': 0,
            'job_index': 0,
            'status': 'waiting',
            'priority': 10,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id2 = ret['result']

        r = yield client.fetch('http://localhost:%d/task_actions/queue'%(self.port,),
                method='POST', body=json.dumps({'num_tasks':1}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('queued', ret)
        self.assertEqual(ret['queued'], 1)
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,'foo',task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'waiting')
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,'bar',task_id2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'queued')

    @unittest_reporter(name='REST POST   /task_actions/process')
    def test_600_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data2 = {'status':'queued'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/task_actions/process'%(self.port,),
                method='POST', body=json.dumps({}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(task_id, ret['task_id'])
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'processing')

        # now try with requirements
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {'memory':4.5, 'disk':100},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data2 = {'status':'queued'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/task_actions/process'%(self.port,),
                    method='POST', body=json.dumps({'requirements':{'memory':2.0,'disk':120}}),
                    headers={'Authorization': 'bearer '+self.token})

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'queued')

        r = yield client.fetch('http://localhost:%d/task_actions/process'%(self.port,),
                method='POST', body=json.dumps({'requirements':{'memory':6.0,'disk':120}}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(task_id, ret['task_id'])

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'processing')

    @unittest_reporter(name='REST POST   /tasks/<task_id>/task_actions/reset')
    def test_700_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {'memory':5.6, 'gpu':1},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data2 = {'status':'queued'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/tasks/%s/task_actions/reset'%(self.port,task_id),
                method='POST', body=json.dumps({}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'reset')

        # now try with time_used
        data2 = {'status':'queued'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/tasks/%s/task_actions/reset'%(self.port,task_id),
                method='POST', body=json.dumps({'time_used':7200}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'reset')
        self.assertEqual(ret['walltime_err_n'], 1)
        self.assertEqual(ret['walltime_err'], 2.0)

        # now try with resources
        data2 = {'status':'queued'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/tasks/%s/task_actions/reset'%(self.port,task_id),
                method='POST', body=json.dumps({'resources':{'time':2.5, 'memory':3.5, 'disk': 20.3, 'gpu': 23}}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'reset')
        self.assertEqual(ret['walltime_err_n'], 2)
        self.assertEqual(ret['walltime_err'], 4.5)
        self.assertEqual(ret['requirements']['memory'], data['requirements']['memory'])
        self.assertEqual(ret['requirements']['time'], 2.5)
        self.assertEqual(ret['requirements']['disk'], 20.3)
        self.assertNotEqual(ret['requirements']['gpu'], 23)

        # now try with a bad status
        data2 = {'status':'complete'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/tasks/%s/task_actions/reset'%(self.port,task_id),
                    method='POST', body=json.dumps({}),
                    headers={'Authorization': 'bearer '+self.token})

    @unittest_reporter(name='REST POST   /tasks/<task_id>/task_actions/complete')
    def test_710_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data2 = {'status':'processing'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/tasks/%s/task_actions/complete'%(self.port,task_id),
                method='POST', body=json.dumps({}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'complete')

        # now try with time_used
        data2 = {'status':'processing'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/tasks/%s/task_actions/complete'%(self.port,task_id),
                method='POST', body=json.dumps({'time_used':7200}),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret['status'], 'complete')
        self.assertEqual(ret['walltime'], 2.0)

        # now try with a bad status
        data2 = {'status':'idle'}
        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s/status'%(self.port,data['dataset_id'],task_id),
                method='PUT', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/tasks/%s/task_actions/complete'%(self.port,task_id),
                    method='POST', body=json.dumps({}),
                    headers={'Authorization': 'bearer '+self.token})



    @unittest_reporter(name='REST POST   /datasets/<dataset_id>/task_actions/bulk_status/<status>')
    def test_800_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        data2 = {'tasks':[task_id]}
        r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_status/%s'%(self.port,data['dataset_id'],'failed'),
                method='POST', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('status', ret)
        self.assertEqual(ret['status'], 'failed')

        data = {
            'dataset_id': 'foo',
            'job_id': 'foo2',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id2 = ret['result']

        data2 = {'tasks':[task_id, task_id2]}
        r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_status/%s'%(self.port,data['dataset_id'],'reset'),
                method='POST', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('status', ret)
        self.assertEqual(ret['status'], 'reset')

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('status', ret)
        self.assertEqual(ret['status'], 'reset')

        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_status/%s'%(self.port,data['dataset_id'],'blah'),
                    method='POST', body=json.dumps(data2),
                    headers={'Authorization': 'bearer '+self.token})

        with self.assertRaises(Exception):
            yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_status/%s'%(self.port,data['dataset_id'],'failed'),
                    method='POST', body=json.dumps({}),
                    headers={'Authorization': 'bearer '+self.token})
                    

    @unittest_reporter(name='REST PATCH   /datasets/<dataset_id>/task_actions/bulk_requirements/<name>')
    def test_810_tasks(self):
        client = AsyncHTTPClient()
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']
        
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'task_index': 0,
            'job_index': 0,
            'name': 'baz',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id2 = ret['result']

        data2 = {'cpu':2}
        r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_requirements/%s'%(self.port,data['dataset_id'],'bar'),
                method='PATCH', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('requirements', ret)
        self.assertIn('cpu', ret['requirements'])
        self.assertEqual(ret['requirements']['cpu'], 2)

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('requirements', ret)
        self.assertNotIn('cpu', ret['requirements'])

        data2 = {'gpu':4}
        r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_requirements/%s'%(self.port,data['dataset_id'],'baz'),
                method='PATCH', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('requirements', ret)
        self.assertIn('gpu', ret['requirements'])
        self.assertEqual(ret['requirements']['gpu'], 4)

        data2 = {'os':['foo','bar']}
        r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_requirements/%s'%(self.port,data['dataset_id'],'baz'),
                method='PATCH', body=json.dumps(data2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/tasks/%s'%(self.port,data['dataset_id'],task_id2),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('requirements', ret)
        self.assertIn('os', ret['requirements'])
        self.assertEqual(ret['requirements']['os'], ['foo','bar'])

        data2 = {'blah':4}
        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_requirements/%s'%(self.port,data['dataset_id'],'bar'),
                    method='PATCH', body=json.dumps(data2),
                    headers={'Authorization': 'bearer '+self.token})

        data2 = {'memory':'ten'}
        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_requirements/%s'%(self.port,data['dataset_id'],'bar'),
                    method='PATCH', body=json.dumps(data2),
                    headers={'Authorization': 'bearer '+self.token})

        data2 = {'gpu':3.5}
        with self.assertRaises(Exception):
            r = yield client.fetch('http://localhost:%d/datasets/%s/task_actions/bulk_requirements/%s'%(self.port,data['dataset_id'],'bar'),
                    method='PATCH', body=json.dumps(data2),
                    headers={'Authorization': 'bearer '+self.token})

    @unittest_reporter(name='REST GET     /datasets/<dataset_id>/files')
    def test_900_tasks(self):
        client = AsyncHTTPClient()
        r = yield client.fetch('http://localhost:%d/datasets/%s/files'%(self.port, 'foo'),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertEqual(ret, {'files':[]})

    @unittest_reporter(name='REST POST    /datasets/<dataset_id>/files')
    def test_910_tasks(self):
        client = AsyncHTTPClient()

        # create a task
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'job_index': 0,
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)

        file_data = {
            'job_index': 0,
            'task_name': 'bar',
            'filename': 'blah',
            'movement': 'input',
        }
        r = yield client.fetch('http://localhost:%d/datasets/%s/files'%(self.port, 'foo'),
                method='POST', body=json.dumps(file_data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)

        r = yield client.fetch('http://localhost:%d/datasets/%s/files'%(self.port, 'foo'),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('files', ret)
        self.assertEqual(len(ret['files']), 1)
        self.assertEqual(ret['files'][0]['remote'], file_data['filename'])

    @unittest_reporter(name='REST GET     /datasets/<dataset_id>/files/<task_id>')
    def test_920_tasks(self):
        client = AsyncHTTPClient()

        # create a task
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'job_index': 0,
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        file_data = {
            'job_index': 0,
            'task_name': 'bar',
            'filename': 'blah',
            'movement': 'input',
        }
        r = yield client.fetch('http://localhost:%d/datasets/%s/files'%(self.port, 'foo'),
                method='POST', body=json.dumps(file_data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)

        r = yield client.fetch('http://localhost:%d/datasets/%s/files/%s'%(self.port, 'foo', task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('files', ret)
        self.assertEqual(len(ret['files']), 1)
        self.assertEqual(ret['files'][0]['remote'], file_data['filename'])

    @unittest_reporter(name='REST POST    /datasets/<dataset_id>/files/<task_id>')
    def test_930_tasks(self):
        client = AsyncHTTPClient()

        # create a task
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'job_index': 0,
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        file_data = {
            'filename': 'blah',
            'movement': 'input',
        }
        r = yield client.fetch('http://localhost:%d/datasets/%s/files/%s'%(self.port, 'foo', task_id),
                method='POST', body=json.dumps(file_data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)

        r = yield client.fetch('http://localhost:%d/datasets/%s/files/%s'%(self.port, 'foo', task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('files', ret)
        self.assertEqual(len(ret['files']), 1)
        self.assertEqual(ret['files'][0]['remote'], file_data['filename'])

    @unittest_reporter(name='REST DELETE  /datasets/<dataset_id>/files/<task_id>')
    def test_940_tasks(self):
        client = AsyncHTTPClient()

        # create a task
        data = {
            'dataset_id': 'foo',
            'job_id': 'foo1',
            'job_index': 0,
            'task_index': 0,
            'name': 'bar',
            'depends': [],
            'requirements': {},
        }
        r = yield client.fetch('http://localhost:%d/tasks'%self.port,
                method='POST', body=json.dumps(data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)
        ret = json.loads(r.body)
        task_id = ret['result']

        file_data = {
            'filename': 'blah',
            'movement': 'input',
        }
        r = yield client.fetch('http://localhost:%d/datasets/%s/files/%s'%(self.port, 'foo', task_id),
                method='POST', body=json.dumps(file_data),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 201)

        r = yield client.fetch('http://localhost:%d/datasets/%s/files/%s'%(self.port, 'foo', task_id),
                method='DELETE',
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)

        r = yield client.fetch('http://localhost:%d/datasets/%s/files/%s'%(self.port, 'foo', task_id),
                headers={'Authorization': 'bearer '+self.token})
        self.assertEqual(r.code, 200)
        ret = json.loads(r.body)
        self.assertIn('files', ret)
        self.assertEqual(ret['files'], [])



def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(rest_tasks_test))
    suite.addTests(loader.loadTestsFromNames(alltests,rest_tasks_test))
    return suite
