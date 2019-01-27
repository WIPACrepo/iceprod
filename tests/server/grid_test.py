"""
Test script for grid
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

import logging
logger = logging.getLogger('grid_test')

import os
import sys
import time
import random
from datetime import datetime,timedelta
from contextlib import contextmanager
import shutil
import socket
import tempfile
from multiprocessing import Queue,Pipe

try:
    import cPickle as pickle
except:
    import pickle

import unittest
from unittest.mock import patch, MagicMock

import tornado.gen
from tornado.concurrent import Future
from tornado.testing import AsyncTestCase

import iceprod.server
from iceprod.server import module
from iceprod.server.grid import BaseGrid
from iceprod.core.resources import Resources
from rest_tools.client import RestClient

from .module_test import module_test, TestExecutor

class grid_test(AsyncTestCase):
    def setUp(self):
        super(grid_test,self).setUp()
        orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp(dir=orig_dir)
        os.chdir(self.test_dir)
        def clean_dir():
            os.chdir(orig_dir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(clean_dir)

        self.executor = TestExecutor()

        # override self.db_handle
        self.services = services_mock()

    @unittest_reporter
    def test_001_init(self):
        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'test':1}},
               'db':{'address':None,'ssl':False}}

        # call normal init
        g = BaseGrid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor, module.FakeStatsClient(),
                 None)

        self.assertTrue(g)
        self.assertEqual(g.gridspec, gridspec)
        self.assertEqual(g.queue_cfg, cfg['queue'][name])
        self.assertEqual(g.cfg, cfg)


        # call init with too few args
        try:
            g = BaseGrid(gridspec, cfg['queue'][name], cfg)
        except:
            pass
        else:
            raise Exception('too few args did not raise exception')

    @patch('iceprod.server.grid.BaseGrid._delete_dirs')
    @patch('iceprod.server.grid.BaseGrid.remove')
    @patch('iceprod.server.grid.BaseGrid.get_grid_status')
    @unittest_reporter
    async def test_010_check_and_clean(self, get_grid_status, remove, delete_dirs):
        """Test check_and_clean"""
        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'test':1,'monitor_address':'localhost'}},
               'db':{'address':None,'ssl':False}}

        # init
        client = MagicMock(spec=RestClient)
        g = BaseGrid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor, module.FakeStatsClient(),
                 client)
        if not g:
            raise Exception('init did not return grid object')

        # call with empty queue
        f = Future()
        f.set_result({})
        client.request.return_value = f

        f = Future()
        f.set_result({})
        get_grid_status.return_value = f

        await g.check_and_clean()

        self.assertEqual(client.request.call_args_list[0][0][1], '/pilots')
        get_grid_status.assert_called()
        remove.assert_not_called()
        delete_dirs.assert_not_called()

        # call with one pilot in iceprod, nothing on queue
        client.request.reset_mock()
        host = socket.getfqdn()
        f = Future()
        f.set_result({'123':{'pilot_id':'123','grid_queue_id':'foo','submit_dir':'bar','queue_host':host}})
        client.request.return_value = f

        f = Future()
        f.set_result({})
        get_grid_status.return_value = f

        f = Future()
        f.set_result(MagicMock())
        remove.return_value = f

        f = Future()
        f.set_result(MagicMock())
        delete_dirs.return_value = f

        await g.check_and_clean()

        self.assertEqual(client.request.call_args_list[0][0][1], '/pilots')
        get_grid_status.assert_called()
        remove.assert_not_called()
        delete_dirs.assert_not_called()
        self.assertEqual(client.request.call_args_list[1][0][1], '/pilots/123')

    @patch('iceprod.server.grid.BaseGrid.setup_pilots')
    @unittest_reporter
    async def test_011_queue(self, setup_pilots):
        f = Future()
        f.set_result(None)
        setup_pilots.return_value = f

        site = 's1'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'tasks_on_queue':[1,5,2],
                              'monitor_address':'localhost'}},
               'db':{'address':None,'ssl':False}}

        # init
        client = MagicMock(spec=RestClient)
        g = BaseGrid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor, module.FakeStatsClient(),
                 client)
        if not g:
            raise Exception('init did not return grid object')

        tasks = [{'task_id':'1', 'dataset_id':'bar', 'requirements':{}},
                 {'task_id':'2', 'dataset_id':'bar', 'requirements':{}},
                 {'task_id':'3', 'dataset_id':'baz', 'requirements':{}},]
        dataset = {'dataset_id':'bar', 'priority':1}
        dataset2 = {'dataset_id':'baz', 'priority':2}
        async def req(method, path, args=None):
            logger.info('req path=%r, args=%r', path, args)
            if 'task' in path:
                return {'tasks':tasks}
            if 'bar' in path:
                return dataset
            else:
                return dataset2
        client.request.side_effect = req

        # call normally
        g.tasks_queued = 0
        await g.queue()
        self.assertTrue(setup_pilots.called)
        expected = [{'task_id':'3', 'dataset_id':'baz', 'requirements':{}},
                    {'task_id':'1', 'dataset_id':'bar', 'requirements':{}},
                    {'task_id':'2', 'dataset_id':'bar', 'requirements':{}},]
        self.assertEqual(setup_pilots.call_args[0][0], expected)

    @patch('iceprod.server.grid.BaseGrid.setup_submit_directory')
    @patch('iceprod.server.grid.BaseGrid.submit')
    @unittest_reporter
    async def test_020_setup_pilots(self, submit, setup_submit_directory):
        async def submit_func(task):
            task['grid_queue_id'] = ','.join('123' for _ in range(task['num']))
        submit.side_effect = submit_func
        f = Future()
        f.set_result(None)
        setup_submit_directory.return_value = f

        site = 'thesite'
        self.check_run_stop = False
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'queueing_factor_priority':1,
                              'queueing_factor_dataset':1,
                              'queueing_factor_tasks':1,
                              'max_task_queued_time':1000,
                              'max_task_processing_time':1000,
                              'max_task_reset_time':300,
                              'pilots_on_queue': [5,10],
                              'ping_interval':60,
                              'monitor_address':'localhost'
                              }},
               'db':{'address':None,'ssl':False}}

        # init
        client = MagicMock(spec=RestClient)
        g = BaseGrid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor, module.FakeStatsClient(),
                 client)
        if not g:
            raise Exception('init did not return grid object')

        pilot_ids = list(range(100))
        async def req(method, path, args=None):
            logger.info('req path=%r, args=%r', path, args)
            if method == 'GET':
                return {'foo':{'pilot_id':'foo','host':None,'resources':{'cpu':1,'gpu':0,'disk':10,'memory':3,'time':1}},
                        'bar':{'pilot_id':'bar','host':'baz','resources':{}},
                       }
            elif method == 'POST':
                return {'result':str(pilot_ids.pop(0))}
            else: # PATCH
                req.num_queued += 1
                return None
        req.num_queued = 0
        client.request.side_effect = req

        # call normally
        tasks = [{'task_id':'3', 'dataset_id':'baz', 'requirements':{'cpu':1,'memory':4}},
                 {'task_id':'1', 'dataset_id':'bar', 'requirements':{'cpu':1,'memory':2}},
                 {'task_id':'2', 'dataset_id':'bar', 'requirements':{'cpu':1,'memory':2}},]
        await g.setup_pilots(tasks)
        self.assertTrue(submit.called)
        self.assertTrue(setup_submit_directory.called)
        self.assertEqual(req.num_queued, 3)
        self.assertEqual(req.num_queued, 3)

        # test error
        setup_submit_directory.side_effect = Exception()
        await g.setup_pilots(tasks)

        f = Future()
        f.set_result(None)
        setup_submit_directory.return_value = f
        submit.side_effect = Exception()
        await g.setup_pilots(tasks)

    @patch('iceprod.server.grid.BaseGrid.generate_submit_file')
    @patch('iceprod.server.grid.BaseGrid.write_cfg')
    @unittest_reporter
    async def test_023_setup_submit_directory(self, write_cfg, generate_submit_file):
        site = 'thesite'
        self.check_run_stop = False
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'tasks_on_queue':[1,5,2],
                              'max_task_queued_time':1000,
                              'max_task_processing_time':1000,
                              'max_task_reset_time':300,
                              'ping_interval':60,
                              'monitor_address':'localhost'
                             }
                       },
              }

        # init
        client = MagicMock(spec=RestClient)
        g = BaseGrid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor, module.FakeStatsClient(),
                 client)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        tokens = list(range(100,200))
        async def req(method, path, args=None):
            logger.info('req path=%r, args=%r', path, args)
            return {'result':str(tokens.pop(0))}
        req.num_queued = 0
        client.request.side_effect = req

        f = Future()
        f.set_result(None)
        generate_submit_file.return_value = f
        write_cfg.return_value = (None, None)

        task = {'task_id':'1','name':'0','debug':0,'dataset_id':'d1',
                'job':0,'jobs_submitted':1}
        await g.setup_submit_directory(task)

        self.assertTrue(generate_submit_file.called)
        self.assertTrue(write_cfg.called)

    @unittest_reporter
    def test_026_write_cfg(self):
        site = 'thesite'
        self.check_run_stop = False
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'tasks_on_queue':[1,5,2],
                              'max_task_queued_time':1000,
                              'max_task_processing_time':1000,
                              'max_task_reset_time':300,
                              'ping_interval':60,
                              'monitor_address':'localhost'
                             }
                       },
              }

        # init
        client = MagicMock(spec=RestClient)
        g = BaseGrid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor, module.FakeStatsClient(),
                 client)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        task = {'task_id':'1','name':'0','debug':0,'dataset_id':'d1',
                'job':0,'jobs_submitted':1,'submit_dir':submit_dir}
        config, filelist = g.write_cfg(task)
        self.assertEqual(filelist[0], os.path.join(submit_dir,'task.cfg'))
        self.assertTrue(os.path.exists(filelist[0]))

        # call with extra opts
        task = {'task_id':'1','name':'0','debug':0,'submit_dir':submit_dir,
                'reqs':{'OS':'RHEL6'}}
        cfg['queue']['site_temp'] = 'tmp'
        cfg['download'] = {'http_username':'foo','http_password':'bar'}
        cfg['system'] = {'remote_cacert': 'baz'}
        with open('baz', 'w') as f:
            f.write('bazbaz')
        cfg['queue']['x509proxy'] = 'x509'
        with open('x509', 'w') as f:
            f.write('x509x509')
        config, filelist = g.write_cfg(task)
        self.assertEqual(filelist[0], os.path.join(submit_dir,'task.cfg'))
        self.assertTrue(os.path.exists(filelist[0]))
        self.assertEqual(len(filelist),3)
        self.assertIn('baz', filelist[1])
        self.assertIn('x509', filelist[2])

    @unittest_reporter
    def test_100_get_resources(self):
        tasks = [
            {'reqs':{'cpu':1,'memory':4.6}},
        ]
        reqs = list(BaseGrid._get_resources(tasks))
        self.assertIn('cpu', reqs[0])
        self.assertEqual(reqs[0]['cpu'], tasks[0]['reqs']['cpu'])
        self.assertIn('memory', reqs[0])
        self.assertEqual(reqs[0]['memory'], tasks[0]['reqs']['memory'])

        tasks = [
            {'reqs':{'os':'RHEL_7_x86_64'}},
        ]
        reqs = list(BaseGrid._get_resources(tasks))
        self.assertIn('os', reqs[0])
        self.assertEqual(reqs[0]['os'], tasks[0]['reqs']['os'])

        tasks = [
            {'reqs':{'cpu':1,'memory':4.6,'foo':'bar'}},
        ]
        reqs = list(BaseGrid._get_resources(tasks))
        self.assertIn('cpu', reqs[0])
        self.assertEqual(reqs[0]['cpu'], tasks[0]['reqs']['cpu'])
        self.assertIn('memory', reqs[0])
        self.assertEqual(reqs[0]['memory'], tasks[0]['reqs']['memory'])


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(grid_test))
    suite.addTests(loader.loadTestsFromNames(alltests,grid_test))
    return suite
