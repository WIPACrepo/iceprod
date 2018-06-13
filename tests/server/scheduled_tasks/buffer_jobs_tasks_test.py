"""
Test script for scheduled_tasks/buffer_jobs_tasks_test
"""

import logging
logger = logging.getLogger('scheduled_tasks_buffer_jobs_tasks_test')

import os
import sys
import shutil
import tempfile
import unittest
from functools import partial
from unittest.mock import patch, MagicMock

from tornado.testing import AsyncTestCase

from tests.util import unittest_reporter, glob_tests

from iceprod.core import rest_client
from iceprod.server.modules.schedule import schedule
from iceprod.server.scheduled_tasks import buffer_jobs_tasks

class buffer_jobs_tasks_test(AsyncTestCase):
    def setUp(self):
        super(buffer_jobs_tasks_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        self.cfg = {
            'queue':{
                'init_queue_interval':0.1,
                'submit_dir':self.test_dir,
                '*':{'type':'Test1','description':'d'},
            },
            'master':{
                'url':False,
            },
            'site_id':'abcd',
        }

    @unittest_reporter
    def test_100_buffer_jobs_tasks(self):
        s = schedule(self.cfg,None,None,None)
        buffer_jobs_tasks.buffer_jobs_tasks(s)

    @unittest_reporter
    async def test_200_run(self):
        rc = MagicMock(spec=rest_client.Client)
        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':[]},
                    {'name':'b','requirements':{'memory':4.5},'depends':['a']},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(len(jobs) == 2)
        self.assertTrue(len(tasks) == 4)
        self.assertTrue([0,0,1,1], [t['job_id'] for t in tasks])
        self.assertTrue([[],[0],[],[2]], [t['depends'] for t in tasks])
        
        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':[]},
                    {'name':'b','requirements':{},'depends':[0]},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(len(jobs) == 2)
        self.assertTrue(len(tasks) == 4)
        self.assertTrue([0,0,1,1], [t['job_id'] for t in tasks])
        self.assertTrue([[],[0],[],[2]], [t['depends'] for t in tasks])
        
        job_ids = list(range(1,2))
        task_ids = list(range(2,4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {0:{'job_index':0}}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':[]},
                    {'name':'b','requirements':{},'depends':[0]},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(len(jobs) == 1)
        self.assertTrue(len(tasks) == 2)
        self.assertTrue([1,1], [t['job_id'] for t in tasks])
        self.assertTrue([[],[2]], [t['depends'] for t in tasks])

    @unittest_reporter(name='run() - ext dep')
    async def test_201_run(self):
        rc = MagicMock(spec=rest_client.Client)
        job_ids = list(range(2,4))
        task_ids = list(range(4,8))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/datasets/bar/tasks'):
                return {
                    0:{'task_id':0,'name':'generate','task_index':0},
                    1:{'task_id':1,'name':'filter','task_index':1},
                    2:{'task_id':2,'name':'generate','task_index':2},
                    3:{'task_id':3,'name':'filter','task_index':3},
                }
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':['bar:1']},
                    {'name':'b','requirements':{},'depends':['a']},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(len(jobs) == 2)
        self.assertTrue(len(tasks) == 4)
        self.assertTrue([2,2,3,3], [t['job_id'] for t in tasks])
        self.assertTrue([[1],[4],[3],[6]], [t['depends'] for t in tasks])

        job_ids = list(range(2,4))
        task_ids = list(range(4,8))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url == '/tasks/3':
                return {'task_id':3,'name':'filter','task_index':3}
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':[3]},
                    {'name':'b','requirements':{},'depends':['a']},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertTrue(len(jobs) == 2)
        self.assertTrue(len(tasks) == 4)
        self.assertTrue([2,2,3,3], [t['job_id'] for t in tasks])
        self.assertTrue([[1],[4],[3],[6]], [t['depends'] for t in tasks])

    @unittest_reporter(name='run() - dep err')
    async def test_202_run(self):
        rc = MagicMock(spec=rest_client.Client)
        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            logger.info('RPC: %s %s', method, url)
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':['b']},
                    {'name':'b','requirements':{},'depends':['a']},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        with self.assertRaises(Exception):
            await buffer_jobs_tasks.run(rc, debug=True)

        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            logger.info('RPC: %s %s', method, url)
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':[0]},
                    {'name':'b','requirements':{},'depends':['a']},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        with self.assertRaises(Exception):
            await buffer_jobs_tasks.run(rc, debug=True)
        
        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            logger.info('RPC: %s %s', method, url)
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':['lalala']},
                    {'name':'b','requirements':{},'depends':['a']},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        with self.assertRaises(Exception):
            await buffer_jobs_tasks.run(rc, debug=True)

        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            logger.info('RPC: %s %s', method, url)
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url == '/datasets/foo/jobs':
                return {}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url.startswith('/datasets/bar/tasks'):
                return {
                    0:{'task_id':0,'name':'generate','task_index':0},
                    1:{'task_id':1,'name':'filter','task_index':1},
                    2:{'task_id':2,'name':'generate','task_index':2},
                    3:{'task_id':3,'name':'filter','task_index':3},
                }
            elif url.startswith('/config'):
                return {'tasks': [
                    {'name':'a','requirements':{},'depends':['bar:lalala']},
                    {'name':'b','requirements':{},'depends':['a']},
                ]}
            elif url == '/jobs' and method == 'POST':
                jobs.append(args)
                return {'result': job_ids.pop()}
            elif url == '/tasks' and method == 'POST':
                tasks.append(args)
                return {'result': task_ids.pop()}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        with self.assertRaises(Exception):
            await buffer_jobs_tasks.run(rc, debug=True)

    @unittest_reporter(name='run() - no buffer')
    async def test_203_run(self):
        rc = MagicMock(spec=rest_client.Client)
        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url.startswith('/datasets/foo/task_counts'):
                return {'waiting':3000}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertFalse(jobs)
        self.assertFalse(tasks)

        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                return {'processing':['foo']}
            elif url == '/datasets/foo':
                client.called = True
                return {'jobs_to_queue':2, 'tasks_to_queue':2}
            elif url.startswith('/datasets/foo/task_counts'):
                return {}
            elif url == '/datasets/foo/jobs':
                return {0:{},1:{}}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertFalse(jobs)
        self.assertFalse(tasks)

        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                client.called = True
                return {}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertFalse(jobs)
        self.assertFalse(tasks)

    @unittest_reporter(name='run() - error')
    async def test_300_run(self):
        rc = MagicMock(spec=rest_client.Client)
        job_ids = list(range(2))
        task_ids = list(range(4))
        jobs = []
        tasks = []
        async def client(method, url, args=None):
            if url.startswith('/dataset_summaries'):
                client.called = True
                return {'processing':['foo']}
            else:
                raise Exception()
        client.called = False
        rc.request = client
        with self.assertRaises(Exception):
            await buffer_jobs_tasks.run(rc, debug=True)
        self.assertTrue(client.called)
        self.assertFalse(jobs)
        self.assertFalse(tasks)

        # internally catch the error
        await buffer_jobs_tasks.run(rc)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(buffer_jobs_tasks_test))
    suite.addTests(loader.loadTestsFromNames(alltests,buffer_jobs_tasks_test))
    return suite
