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
import tempfile
from multiprocessing import Queue,Pipe

try:
    import cPickle as pickle
except:
    import pickle

import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import tornado.gen
from tornado.concurrent import Future
from tornado.testing import AsyncTestCase

import iceprod.server
from iceprod.server import module
from iceprod.server.grid import grid

from .module_test import module_test
from .dbmethods_test import TestExecutor

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
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)

        self.assertTrue(g)
        self.assertEqual(g.gridspec, gridspec)
        self.assertEqual(g.queue_cfg, cfg['queue'][name])
        self.assertEqual(g.cfg, cfg)


        # call init with too few args
        try:
            g = grid(gridspec, cfg['queue'][name], cfg)
        except:
            pass
        else:
            raise Exception('too few args did not raise exception')

    @patch('iceprod.server.grid.grid.check_iceprod')
    @patch('iceprod.server.grid.grid.check_grid')
    @unittest_reporter
    def test_010_check_and_clean(self, check_grid, check_iceprod):
        """Test check_and_clean"""
        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'test':1,'monitor_address':'localhost'}},
               'db':{'address':None,'ssl':False}}

        f = Future()
        f.set_result(None)
        check_grid.return_value = f
        check_iceprod.return_value = f

        # init
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        yield g.check_and_clean()

        check_iceprod.assert_called_once_with()
        check_grid.assert_called_once_with()

    @patch('iceprod.server.dataset_prio.calc_dataset_prio')
    @patch('iceprod.server.grid.grid.setup_submit_directory')
    @patch('iceprod.server.grid.grid.submit')
    @unittest_reporter
    def test_011_queue(self, submit, setup_submit_directory, calc_dataset_prio):
        def c(d,*args,**kwargs):
            calc_dataset_prio.called = True
            if 'dataset_id' in d and d['dataset_id'] in calc_dataset_prio.ret:
                return calc_dataset_prio.ret[d['dataset_id']]
            else:
                raise Exception('bad dataset prio')
        calc_dataset_prio.side_effect = c

        f = Future()
        f.set_result(None)
        setup_submit_directory.return_value = f
        submit.return_value = f

        site = 'thesite'
        datasets = {1:{'dataset_id':1},
                   2:{'dataset_id':2},
                   3:{'dataset_id':3},
                  }
        tasks = {1:{'task_id':1},
                 2:{'task_id':2},
                }
        self.services.ret['db']['queue_get_queueing_datasets'] = datasets
        self.services.ret['db']['queue_get_queueing_tasks'] = tasks
        self.services.ret['db']['queue_set_task_status'] = True
        self.services.ret['db']['rpc_get_groups'] = None

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
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)
        self.assertTrue(g)

        # call normally
        calc_dataset_prio.ret = {1:1.0, 2:3.0, 3:2.0}
        g.tasks_queued = 0

        yield g.queue()
        self.assertTrue(calc_dataset_prio.called)
        self.assertTrue(setup_submit_directory.called)
        self.assertTrue(submit.called)

        # submit multi
        submit.reset_mock()
        setup_submit_directory.reset_mock()
        calc_dataset_prio.reset_mock()
        g.tasks_queued = 0
        g.submit_multi = True

        yield g.queue()
        self.assertTrue(calc_dataset_prio.called)
        self.assertTrue(setup_submit_directory.called)
        self.assertTrue(submit.called)

        # get_queueing_datasets error
        self.services.ret['db']['queue_get_queueing_datasets'] = Exception()
        self.services.ret['db']['queue_get_queueing_tasks'] = tasks
        self.services.ret['db']['queue_set_task_status'] = True
        try:
            yield g.queue()
        except:
            pass
        else:
            raise Exception('get_queueing_datasets did not throw exception')

        # calc_dataset_prio error
        self.services.ret['db']['queue_get_queueing_datasets'] = datasets
        self.services.ret['db']['queue_get_queueing_tasks'] = tasks
        self.services.ret['db']['queue_set_task_status'] = True
        calc_dataset_prio.ret = {1:1.0, 3:2.0}
        g.tasks_queued = 0
        try:
            yield g.queue()
        except:
            pass
        else:
            raise Exception('calc_dataset_prio did not throw exception')

        # get_queueing_tasks error
        self.services.ret['db']['queue_get_queueing_datasets'] = datasets
        self.services.ret['db']['queue_get_queueing_tasks'] = Exception()
        self.services.ret['db']['queue_set_task_status'] = True
        calc_dataset_prio.ret = {1:1.0, 2:3.0, 3:2.0}
        try:
            yield g.queue()
        except:
            pass
        else:
            raise Exception('get_queueing_tasks did not throw exception')

        # setup_submit_directory error
        self.services.ret['db']['queue_get_queueing_datasets'] = datasets
        self.services.ret['db']['queue_get_queueing_tasks'] = tasks
        self.services.ret['db']['queue_set_task_status'] = True
        setup_submit_directory.side_effect = Exception()
        try:
            yield g.queue()
        except:
            pass
        else:
            raise Exception('setup_submit_directory did not throw exception')

        # submit error
        self.services.ret['db']['queue_get_queueing_datasets'] = datasets
        self.services.ret['db']['queue_get_queueing_tasks'] = tasks
        self.services.ret['db']['queue_set_task_status'] = True
        setup_submit_directory.return_value = f
        submit.side_effect = Exception()
        try:
            yield g.queue()
        except:
            pass
        else:
            raise Exception('submit did not throw exception')


        # set_task_status error
        self.services.ret['db']['queue_get_queueing_datasets'] = datasets
        self.services.ret['db']['queue_get_queueing_tasks'] = tasks
        self.services.ret['db']['queue_set_task_status'] = Exception()
        try:
            yield g.queue()
        except:
            pass
        else:
            raise Exception('set_task_status did not throw exception')

    @unittest_reporter
    def test_020_check_iceprod(self):
        site = 'thesite'
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
                              'monitor_address':'localhost'
                              }},
               'db':{'address':None,'ssl':False}}

        # init
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)
        self.assertTrue(g)

        now = datetime.utcnow()

        # call normally
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.services.ret['db']['queue_get_active_tasks'] = active_tasks
        self.services.ret['db']['queue_reset_tasks'] = True
        self.services.ret['db']['queue_set_task_status'] = True
        g.tasks_queued = 0

        yield g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.services.called):
            raise Exception('normal: did not call get_active_tasks')
        if any('queue_reset_tasks' == x[1] for x in self.services.called):
            raise Exception('normal: called reset_tasks when nothing to reset')

        # queued task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.services.ret['db']['queue_get_active_tasks'] = active_tasks
        self.services.called = []
        g.tasks_queued = 0

        yield g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.services.called):
            raise Exception('queued task reset: did not call get_active_tasks')
        if not any('queue_reset_tasks' == x[1] for x in self.services.called):
            raise Exception('queued task reset: did not call reset')

        # processing task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.services.ret['db']['queue_get_active_tasks'] = active_tasks
        self.services.called = []
        g.tasks_queued = 0

        yield g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.services.called):
            raise Exception('processing task reset: did not call get_active_tasks')
        if not any('queue_reset_tasks' == x[1] for x in self.services.called):
            raise Exception('processing task reset: did not call reset')

        # reset task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=550)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.services.ret['db']['queue_get_active_tasks'] = active_tasks
        self.services.called = []
        g.tasks_queued = 0

        yield g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.services.called):
            raise Exception('reset task reset: did not call get_active_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('reset task reset: did not call set_task_status')

        # resume task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=550)}},
                       }
        self.services.ret['db']['queue_get_active_tasks'] = active_tasks
        self.services.called = []
        g.tasks_queued = 0

        yield g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.services.called):
            raise Exception('resume task reset: did not call get_active_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('resume task reset: did not call set_task_status')

    @patch('iceprod.server.grid.grid.remove')
    @patch('iceprod.server.grid.grid.get_grid_status')
    @unittest_reporter(name='check_grid() - tasks')
    def test_021_check_grid(self, get_grid_status, remove):
        site = 'thesite'
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
                              'monitor_address':'localhost'
                              }},
               'db':{'address':None,'ssl':False}}

        # init
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)
        self.assertTrue(g)

        now = datetime.utcnow()

        # call normally
        active_tasks = [{'task_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'task_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':''},
                      1:{'status':'processing','submit_dir':''}}
        self.services.ret['db']['queue_get_grid_tasks'] = active_tasks
        self.services.ret['db']['queue_reset_tasks'] = True
        self.services.ret['db']['queue_set_task_status'] = True
        f = Future()
        f.set_result(grid_tasks)
        get_grid_status.return_value = f
        f = Future()
        f.set_result(None)
        remove.return_value = f

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        self.assertFalse(remove.called)
        if not any('queue_get_grid_tasks' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_grid_tasks')
        if any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('called queue_set_task_status when nothing to reset')

        # queued error
        active_tasks = [{'task_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                        {'task_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        self.services.ret['db']['queue_get_grid_tasks'] = active_tasks
        self.services.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        self.assertFalse(remove.called)
        if not any('queue_get_grid_tasks' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_grid_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('did not call queue_set_task_status')

        # processing error
        active_tasks = [{'task_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'task_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                       ]
        self.services.ret['db']['queue_get_grid_tasks'] = active_tasks
        self.services.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        self.assertFalse(remove.called)
        if not any('queue_get_grid_tasks' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_grid_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('did not call queue_set_task_status')

        # grid error
        active_tasks2 = [{'task_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'task_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks2 = {0:{'status':'queued','submit_dir':''},
                      1:{'status':'processing','submit_dir':''},
                      2:{'status':'processing','submit_dir':''}}
        f = Future()
        f.set_result(grid_tasks2)
        get_grid_status.return_value = f
        self.services.ret['db']['queue_get_grid_tasks'] = active_tasks2
        self.services.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        self.assertTrue(remove.called)
        if not any('queue_get_grid_tasks' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_grid_tasks')
        if any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('called queue_set_task_status')

        # error getting tasks
        self.services.ret['db']['queue_get_grid_tasks'] = Exception()
        self.services.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            yield g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        self.assertFalse(get_grid_status.called)
        self.assertFalse(remove.called)
        if not any('queue_get_grid_tasks' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_grid_tasks')
        if any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('called queue_set_task_status')

        # error resetting
        self.services.ret['db']['queue_get_grid_tasks'] = active_tasks
        self.services.ret['db']['queue_set_task_status'] = Exception()
        self.services.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            yield g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        self.assertTrue(get_grid_status.called)
        self.assertFalse(remove.called)
        if not any('queue_get_grid_tasks' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_grid_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.services.called):
            raise Exception('did not call queue_set_task_status')

    @patch('iceprod.server.grid.grid.remove')
    @patch('iceprod.server.grid.grid.get_grid_status')
    @unittest_reporter(name='check_grid() - pilots')
    def test_022_check_grid_pilots(self, get_grid_status, remove):
        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        'submit_pilots':True,
                        name:{'tasks_on_queue':[1,5,2],
                              'max_task_queued_time':1000,
                              'max_task_processing_time':1000,
                              'max_task_reset_time':300,
                              'suspend_submit_dir_time':1000,
                              'monitor_address':'localhost'
                              }},
               'db':{'address':None,'ssl':False}}

        # init
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)
        self.assertTrue(g)

        now = datetime.utcnow()

        # call normally
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':''},
                      1:{'status':'processing','submit_dir':''}}
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.ret['db']['queue_del_pilots'] = True
        f = Future()
        f.set_result(grid_tasks)
        get_grid_status.return_value = f
        f = Future()
        f.set_result(None)
        remove.return_value = f

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('called queue_del_pilots when nothing to reset')

        # queued error
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        yield g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_del_pilots')

        # processing error
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                       ]
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_del_pilots')

        # error getting tasks
        self.services.ret['db']['queue_get_pilots'] = Exception()
        self.services.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            yield g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        get_grid_status.assert_not_called()
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('called queue_del_pilots')

        # error resetting
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.ret['db']['queue_del_pilots'] = Exception()
        self.services.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            yield g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if remove.called:
            raise Exception('called remove')
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_del_pilots')

        # mixup between grid and iceprod
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'foo',
                         'submit_time':now-timedelta(seconds=150)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':'bar'},
                      1:{'status':'processing','submit_dir':''}}
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.ret['db']['queue_del_pilots'] = True
        self.services.called = []
        get_grid_status.reset_mock()
        f = Future()
        f.set_result(grid_tasks)
        get_grid_status.return_value = f
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        remove.assert_called_once_with(set([0]))
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_del_pilots')

        # old pilots on queue
        active_tasks = [{'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':''},
                      1:{'status':'processing','submit_dir':''}}
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.called = []
        get_grid_status.reset_mock()
        f = Future()
        f.set_result(grid_tasks)
        get_grid_status.return_value = f
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        remove.assert_called_once_with(set([0]))
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('called queue_del_pilots when nothing to reset')

        # ok submit dirs
        if not os.path.exists(submit_dir):
            os.makedirs(submit_dir)
        os.makedirs(os.path.join(submit_dir,'s_1'))
        os.makedirs(os.path.join(submit_dir,'s_2'))
        active_tasks = [{'pilot_id':2,'grid_queue_id':1,'submit_dir':os.path.join(submit_dir,'s_2'),
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':os.path.join(submit_dir,'s_1')},
                      1:{'status':'processing','submit_dir':os.path.join(submit_dir,'s_2')}}
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.called = []
        get_grid_status.reset_mock()
        f = Future()
        f.set_result(grid_tasks)
        get_grid_status.return_value = f
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        remove.assert_called_once_with(set([0]))
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('called queue_del_pilots when nothing to reset')

        # bad submit dirs
        os.makedirs(os.path.join(submit_dir,'s_0'))
        os.makedirs(os.path.join(submit_dir,'secure'))
        old_time = time.time()-1500
        os.utime(os.path.join(submit_dir,'s_0'), (old_time,old_time))
        os.utime(os.path.join(submit_dir,'secure'), (old_time,old_time))
        grid_tasks = {1:{'status':'processing','submit_dir':os.path.join(submit_dir,'s_2')}}
        self.services.ret['db']['queue_get_pilots'] = active_tasks
        self.services.called = []
        get_grid_status.reset_mock()
        f = Future()
        f.set_result(grid_tasks)
        get_grid_status.return_value = f
        remove.reset_mock()

        yield g.check_grid()

        self.assertTrue(get_grid_status.called)
        remove.assert_not_called()
        if os.path.exists(os.path.join(submit_dir,'s_0')):
            raise Exception('did not clean old submit dir')
        if not os.path.exists(os.path.join(submit_dir,'s_1')):
            raise Exception('cleaned suspended submit dir')
        if not os.path.exists(os.path.join(submit_dir,'secure')):
            raise Exception('cleaned non-submit dir')
        if not any('queue_get_pilots' == x[1] for x in self.services.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.services.called):
            raise Exception('called queue_del_pilots when nothing to reset')

    @patch('iceprod.server.grid.grid.generate_submit_file')
    @unittest_reporter
    def test_023_setup_submit_directory(self, generate_submit_file):
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
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)
        self.assertTrue(g)

        # call normally
        thecfg = """
{"tasks":[
    {"name":"task1",
     "trays":[
        {"name":"Corsika",
         "iter":"1",
         "modules":[
            {"name":"generate_corsika",
             "class":"generators.CorsikaIC"
            }]
        }]
    }]
}"""
        self.services.ret['db']['queue_set_submit_dir'] = True
        self.services.ret['db']['queue_get_cfg_for_task'] = thecfg
        self.services.ret['db']['auth_new_passkey'] = 'passkey'
        g.tasks_queued = 0
        f = Future()
        f.set_result(None)
        generate_submit_file.return_value = f

        task = {'task_id':'1','name':'0','debug':0,'dataset_id':'d1',
                'job':0,'jobs_submitted':1}
        yield g.setup_submit_directory(task)

        if not any('queue_set_submit_dir' == x[1] for x in self.services.called):
            raise Exception('normal: did not call set_submit_dir')
        self.assertTrue(generate_submit_file.called)
        shutil.rmtree(submit_dir)

        # full cfg options
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
               'download':{'http_username':'user',
                           'http_password':'pass'
                          }
              }
        g.cfg = cfg
        g.x509 = 'my x509'
        self.services.called = []

        yield g.setup_submit_directory(task)

        if not any('queue_set_submit_dir' == x[1] for x in self.services.called):
            raise Exception('full cfg opts: did not call set_submit_dir')
        self.assertTrue(generate_submit_file.called)
        shutil.rmtree(submit_dir)

        # generate_submit_file error
        self.services.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()
        f = Future()
        f.set_exception(Exception())
        generate_submit_file.return_value = f

        task = {'task_id':'1','name':'0','debug':0}
        try:
            yield g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('generate_submit_file error: did not raise Exception')
        shutil.rmtree(submit_dir)

        # set_submit_dir error
        self.services.ret['db']['queue_set_submit_dir'] = Exception()
        self.services.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()
        f = Future()
        f.set_result(None)
        generate_submit_file.return_value = f

        task = {'task_id':'1','name':'0','debug':0}
        try:
            yield g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('set_submit_dir error: did not raise Exception')
        shutil.rmtree(submit_dir)

        # new_passkey error
        self.services.ret['db']['queue_set_submit_dir'] = True  
        self.services.ret['db']['auth_new_passkey'] = Exception()
        self.services.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()

        task = {'task_id':'1','name':'0','debug':0}
        try:
            yield g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('new_passkey error: did not raise Exception')
        shutil.rmtree(submit_dir)

        # get_cfg_for_task error
        self.services.ret['db']['auth_new_passkey'] = 'passkey'
        self.services.ret['db']['queue_get_cfg_for_task'] = Exception()
        self.services.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()

        task = {'task_id':'1','name':'0','debug':0}
        try:
            yield g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('get_cfg_for_task error: did not raise Exception')
        shutil.rmtree(submit_dir)

    @patch('iceprod.server.grid.grid.submit')
    @patch('iceprod.server.grid.grid.generate_submit_file')
    @unittest_reporter
    def test_024_setup_pilots(self, generate_submit_file, submit):
        def s(t):
            if submit.ret:
                t['grid_queue_id'] = submit.ret
            else:
                raise Exception('bad submit')
            f = Future()
            f.set_result(None)
            return f
        submit.side_effect = s
        f = Future()
        f.set_result(None)
        generate_submit_file.return_value = f

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
                              'tasks_on_queue': [1,10],
                              'ping_interval':60,
                              'monitor_address':'localhost'
                              }},
               'db':{'address':None,'ssl':False}}

        # init
        self.services.ret['db']['queue_add_pilot'] = True
        self.services.ret['db']['queue_new_pilot_ids'] = ['a']
        self.services.ret['db']['auth_new_passkey'] = 'blah'
        self.services.called = []
        g = grid(gridspec, cfg['queue'][name], cfg, self.services,
                 self.io_loop, self.executor)
        self.assertTrue(g)

        # call normally
        g.tasks_queued = 0
        submit.ret = '12345'

        tasks = {'thetaskid':{}}
        yield g.setup_pilots(tasks)
        if not generate_submit_file.called:
            raise Exception('did not call generate_submit_file')
        if not submit.called:
            raise Exception('did not call submit')
        if (self.services.called[0][1] != 'queue_new_pilot_ids' or
            self.services.called[1][1] != 'auth_new_passkey' or
            self.services.called[2][1] != 'queue_add_pilot'):
            raise Exception('unexpected messages')
        pilot_dict = self.services.called[2][3]['pilot']
        if (os.path.dirname(pilot_dict['submit_dir']) != submit_dir or
            'grid_queue_id' not in pilot_dict or
            pilot_dict['num'] != 1):
            logger.info('%r',self.services.called)
            raise Exception('bad pilot dict')

        # test error
        generate_submit_file.side_effect = Exception()
        try:
            yield g.setup_pilots(tasks)
        except:
            pass
        else:
            raise Exception('did not raise an Exception')

        generate_submit_file.return_value = f
        submit.ret = None
        try:
            yield g.setup_pilots(tasks)
        except:
            pass
        else:
            raise Exception('did not raise an Exception')

        submit.ret = '12345'
        self.services.ret['db']['queue_add_pilot'] = Exception()
        try:
            yield g.setup_pilots(tasks)
        except:
            pass
        else:
            raise Exception('did not raise an Exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(grid_test))
    suite.addTests(loader.loadTestsFromNames(alltests,grid_test))
    return suite
