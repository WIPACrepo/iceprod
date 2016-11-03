"""
Test script for grid
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, messaging_mock

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

import iceprod.server
from iceprod.server import module
from iceprod.server.grid import grid


class grid_test(unittest.TestCase):
    def setUp(self):
        super(grid_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.symlink(os.path.join(curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        # override self.db_handle
        self.messaging = messaging_mock()
        self.check_run_stop = False

    @contextmanager
    def _check_run(self):
        if self.check_run_stop:
            raise Exception('check_run_stop')
        yield

    @unittest_reporter
    def test_001_init(self):
        """Test init"""
        site = 'thesite'
        self.messaging.ret = {}
        self.check_run_stop = False
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'test':1}},
               'db':{'address':None,'ssl':False}}

        # call normal init
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        g = grid(args)

        if not g:
            raise Exception('init did not return grid object')
        if g.gridspec != gridspec:
            raise Exception('init did not copy gridspec properly')
        if (not g.queue_cfg or 'test' not in g.queue_cfg or
            g.queue_cfg['test'] != 1):
            raise Exception('init did not copy queue_cfg properly')
        if (not g.cfg or 'queue' not in g.cfg or
            name not in g.cfg['queue'] or
            'test' not in g.cfg['queue'][name] or
            g.cfg['queue'][name]['test'] != 1):
            raise Exception('init did not copy cfg properly')
        if g.check_run != self._check_run:
            raise Exception('init did not copy check_run properly')

        # call init with too few args
        args = (gridspec,cfg['queue'][name],cfg)
        try:
            g = grid(args)
        except:
            pass
        else:
            raise Exception('too few args did not raise exception')

        # call init with bad check_run
        args = (gridspec,cfg['queue'][name],cfg,'test',
                getattr(self.messaging,'db'))
        try:
            g = grid(args)
        except:
            pass
        else:
            raise Exception('bad check_run did not raise exception')

    @patch('iceprod.server.grid.grid.check_iceprod')
    @patch('iceprod.server.grid.grid.check_grid')
    @unittest_reporter
    def test_010_check_and_clean(self, check_grid, check_iceprod):
        """Test check_and_clean"""
        site = 'thesite'
        self.check_run_stop = False
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        cfg = {'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        name:{'test':1,'monitor_address':'localhost'}},
               'db':{'address':None,'ssl':False}}

        # init
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        g = grid(args)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        g.check_and_clean()

        if not check_iceprod.called:
            raise Exception('did not call check_iceprod()')
        if not check_grid.called:
            raise Exception('did not call check_grid()')

        # kill early
        self.check_run_stop = True
        try:
            g.check_and_clean()
        except:
            pass
        else:
            raise Exception('check_run did not throw exception')

    @patch('iceprod.server.calc_dataset_prio')
    @patch('iceprod.server.grid.grid.setup_submit_directory')
    @patch('iceprod.server.grid.grid.submit')
    @unittest_reporter
    def test_011_queue(self, submit, setup_submit_directory, calc_dataset_prio):
        """Test queue"""
        def c(d,*args,**kwargs):
            calc_dataset_prio.called = True
            if 'dataset_id' in d and d['dataset_id'] in calc_dataset_prio.ret:
                return calc_dataset_prio.ret[d['dataset_id']]
            else:
                raise Exception('bad dataset prio')
        calc_dataset_prio.side_effect = c

        site = 'thesite'
        datasets = {1:{'dataset_id':1},
                   2:{'dataset_id':2},
                   3:{'dataset_id':3},
                  }
        tasks = {1:{'task_id':1},
                 2:{'task_id':2},
                }
        self.messaging.ret = {'db':{'queue_get_queueing_datasets':datasets,
                                    'queue_get_queueing_tasks':tasks,
                                    'queue_set_task_status':True}}
        self.check_run_stop = False
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
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        g = grid(args)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        self.check_run_stop = False
        calc_dataset_prio.ret = {1:1.0, 2:3.0, 3:2.0}
        g.tasks_queued = 0

        g.queue()

        if not calc_dataset_prio.called:
            raise Exception('did not call calc_dataset_prio()')
        if not setup_submit_directory.called:
            raise Exception('did not call setup_submit_directory()')
        if not submit.called:
            raise Exception('did not call submit()')

        # submit multi
        self.check_run_stop = False
        submit.reset_mock()
        setup_submit_directory.reset_mock()
        calc_dataset_prio.reset_mock()
        g.tasks_queued = 0
        g.submit_multi = True

        g.queue()

        if not calc_dataset_prio.called:
            raise Exception('did not call calc_dataset_prio()')
        if not setup_submit_directory.called:
            raise Exception('did not call setup_submit_directory()')
        if not submit.called:
            raise Exception('did not call submit()')

        # kill early
        self.check_run_stop = True
        g.tasks_queued = 0
        try:
            g.queue()
        except:
            pass
        else:
            raise Exception('check_run did not throw exception')

        # get_queueing_datasets error
        self.check_run_stop = False
        self.messaging.ret = {'db':{'queue_get_queueing_tasks':tasks,
                                    'queue_get_queueing_datasets':Exception(),
                                    'queue_set_task_status':True}}
        try:
            g.queue()
        except:
            pass
        else:
            raise Exception('get_queueing_datasets did not throw exception')

        # calc_dataset_prio error
        self.messaging.ret = {'db':{'queue_get_queueing_datasets':datasets,
                                    'queue_get_queueing_tasks':tasks,
                                    'queue_set_task_status':True}}
        calc_dataset_prio.ret = {1:1.0, 3:2.0}
        g.tasks_queued = 0
        try:
            g.queue()
        except:
            pass
        else:
            raise Exception('calc_dataset_prio did not throw exception')

        # get_queueing_tasks error
        self.messaging.ret = {'db':{'queue_get_queueing_datasets':datasets,
                                    'queue_get_queueing_tasks':Exception(),
                                    'queue_set_task_status':True}}
        calc_dataset_prio.ret = {1:1.0, 2:3.0, 3:2.0}
        try:
            g.queue()
        except:
            pass
        else:
            raise Exception('get_queueing_tasks did not throw exception')

        # setup_submit_directory error
        self.messaging.ret = {'db':{'queue_get_queueing_datasets':datasets,
                                    'queue_get_queueing_tasks':tasks,
                                    'queue_set_task_status':True}}
        setup_submit_directory.side_effect = Exception()
        try:
            g.queue()
        except:
            pass
        else:
            raise Exception('setup_submit_directory did not throw exception')

        # submit error
        self.messaging.ret = {'db':{'queue_get_queueing_datasets':datasets,
                                    'queue_get_queueing_tasks':tasks,
                                    'queue_set_task_status':True}}
        submit.side_effect = Exception()
        try:
            g.queue()
        except:
            pass
        else:
            raise Exception('submit did not throw exception')


        # set_task_status error
        self.messaging.ret = {'db':{'queue_get_queueing_datasets':datasets,
                                    'queue_get_queueing_tasks':tasks}}
        try:
            g.queue()
        except:
            pass
        else:
            raise Exception('set_task_status did not throw exception')

    @unittest_reporter
    def test_020_check_iceprod(self):
        """Test check_iceprod"""
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
                              'monitor_address':'localhost'
                              }},
               'db':{'address':None,'ssl':False}}

        # init
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        g = grid(args)
        if not g:
            raise Exception('init did not return grid object')

        now = datetime.utcnow()

        # call normally
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.messaging.ret = {'db':{'queue_get_active_tasks':active_tasks,
                                    'queue_reset_tasks':True}}
        g.tasks_queued = 0

        g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.messaging.called):
            raise Exception('normal: did not call get_active_tasks')
        if any('queue_reset_tasks' == x[1] for x in self.messaging.called):
            raise Exception('normal: called reset_tasks when nothing to reset')

        # queued task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.messaging.ret = {'db':{'queue_get_active_tasks':active_tasks,
                                    'queue_reset_tasks':True}}
        self.messaging.called = []
        g.tasks_queued = 0

        g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.messaging.called):
            raise Exception('queued task reset: did not call get_active_tasks')
        if not any('queue_reset_tasks' == x[1] for x in self.messaging.called):
            raise Exception('queued task reset: did not call reset')

        # processing task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.messaging.ret = {'db':{'queue_get_active_tasks':active_tasks,
                                    'queue_reset_tasks':True}}
        self.messaging.called = []
        g.tasks_queued = 0

        g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.messaging.called):
            raise Exception('processing task reset: did not call get_active_tasks')
        if not any('queue_reset_tasks' == x[1] for x in self.messaging.called):
            raise Exception('processing task reset: did not call reset')

        # reset task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=550)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                       }
        self.messaging.ret = {'db':{'queue_get_active_tasks':active_tasks,
                                    'queue_reset_tasks':True}}
        self.messaging.called = []
        g.tasks_queued = 0

        g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.messaging.called):
            raise Exception('reset task reset: did not call get_active_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.messaging.called):
            raise Exception('reset task reset: did not call set_task_status')

        # resume task reset
        active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                        'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=550)}},
                       }
        self.messaging.ret = {'db':{'queue_get_active_tasks':active_tasks,
                                    'queue_reset_tasks':True}}
        self.messaging.called = []
        g.tasks_queued = 0

        g.check_iceprod()

        if not any('queue_get_active_tasks' == x[1] for x in self.messaging.called):
            raise Exception('resume task reset: did not call get_active_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.messaging.called):
            raise Exception('resume task reset: did not call set_task_status')

    @patch('iceprod.server.grid.grid.remove')
    @patch('iceprod.server.grid.grid.get_grid_status')
    @unittest_reporter
    def test_021_check_grid(self, get_grid_status, remove):
        """Test check_grid"""
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
                              'monitor_address':'localhost'
                              }},
               'db':{'address':None,'ssl':False}}

        # init
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        g = grid(args)
        if not g:
            raise Exception('init did not return grid object')

        now = datetime.utcnow()

        # call normally
        active_tasks = [{'task_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'task_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':''},
                      1:{'status':'processing','submit_dir':''}}
        self.messaging.ret = {'db':{'queue_get_grid_tasks':active_tasks,
                                    'queue_set_task_status':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        get_grid_status.return_value = grid_tasks
        remove.return_value = None

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if remove.called:
            raise Exception('called remove')
        if not any('queue_get_grid_tasks' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_grid_tasks')
        if any('queue_set_task_status' == x[1] for x in self.messaging.called):
            raise Exception('called queue_set_task_status when nothing to reset')

        # queued error
        active_tasks = [{'task_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                        {'task_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        self.messaging.ret = {'db':{'queue_get_grid_tasks':active_tasks,
                                    'queue_set_task_status':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if remove.called:
            raise Exception('called remove')
        if not any('queue_get_grid_tasks' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_grid_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_set_task_status')

        # processing error
        active_tasks = [{'task_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'task_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                       ]
        self.messaging.ret = {'db':{'queue_get_grid_tasks':active_tasks,
                                    'queue_set_task_status':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if remove.called:
            raise Exception('called remove')
        if not any('queue_get_grid_tasks' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_grid_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_set_task_status')

        # error getting tasks
        self.messaging.ret = {'db':{'queue_get_grid_tasks':Exception(),
                                    'queue_set_task_status':True}}
        self.messaging.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        if get_grid_status.called:
            raise Exception('called get_grid_status')
        if remove.called:
            raise Exception('called remove')
        if not any('queue_get_grid_tasks' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_grid_tasks')
        if any('queue_set_task_status' == x[1] for x in self.messaging.called):
            raise Exception('called queue_set_task_status')

        # error resetting
        self.messaging.ret = {'db':{'queue_get_grid_tasks':active_tasks,
                                    'queue_set_task_status':Exception()}}
        self.messaging.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if remove.called:
            raise Exception('called remove')
        if not any('queue_get_grid_tasks' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_grid_tasks')
        if not any('queue_set_task_status' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_set_task_status')

    @patch('iceprod.server.grid.grid.remove')
    @patch('iceprod.server.grid.grid.get_grid_status')
    @unittest_reporter
    def test_022_check_grid_pilots(self, get_grid_status, remove):
        """Test check_grid with pilots"""
        site = 'thesite'
        self.check_run_stop = False
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
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        g = grid(args)
        if not g:
            raise Exception('init did not return grid object')

        now = datetime.utcnow()

        # call normally
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':''},
                      1:{'status':'processing','submit_dir':''}}
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        get_grid_status.return_value = grid_tasks
        remove.return_value = None

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('called queue_del_pilots when nothing to reset')

        # queued error
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_del_pilots')

        # processing error
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=1500)},
                       ]
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        remove.reset_mock()

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_del_pilots')

        # error getting tasks
        self.messaging.ret = {'db':{'queue_get_pilots':Exception(),
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        if get_grid_status.called:
            raise Exception('called get_grid_status')
        remove.assert_not_called()
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('called queue_del_pilots')

        # error resetting
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':Exception()}}
        self.messaging.called = []
        g.tasks_queued = 0
        get_grid_status.reset_mock()
        remove.reset_mock()

        try:
            g.check_grid()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if remove.called:
            raise Exception('called remove')
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_del_pilots')

        # mixup between grid and iceprod
        active_tasks = [{'pilot_id':1,'grid_queue_id':0,'submit_dir':'foo',
                         'submit_time':now-timedelta(seconds=150)},
                        {'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':'bar'},
                      1:{'status':'processing','submit_dir':''}}
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        get_grid_status.return_value = grid_tasks
        remove.reset_mock()
        remove.return_value = True

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if not remove.called:
            raise Exception('did not call remove')
        remove.assert_called_once_with(set([0]))
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if not any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_del_pilots')

        # old pilots on queue
        active_tasks = [{'pilot_id':2,'grid_queue_id':1,'submit_dir':'',
                         'submit_time':now-timedelta(seconds=150)},
                       ]
        grid_tasks = {0:{'status':'queued','submit_dir':''},
                      1:{'status':'processing','submit_dir':''}}
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        get_grid_status.return_value = grid_tasks
        remove.reset_mock()

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if not remove.called:
            raise Exception('did not call remove')
        remove.assert_called_once_with(set([0]))
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.messaging.called):
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
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        get_grid_status.return_value = grid_tasks
        remove.reset_mock()

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        if not remove.called:
            raise Exception('did not call remove')
        remove.assert_called_once_with(set([0]))
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('called queue_del_pilots when nothing to reset')

        # bad submit dirs
        os.makedirs(os.path.join(submit_dir,'s_0'))
        os.makedirs(os.path.join(submit_dir,'secure'))
        old_time = time.time()-1500
        os.utime(os.path.join(submit_dir,'s_0'), (old_time,old_time))
        os.utime(os.path.join(submit_dir,'secure'), (old_time,old_time))
        grid_tasks = {1:{'status':'processing','submit_dir':os.path.join(submit_dir,'s_2')}}
        self.messaging.ret = {'db':{'queue_get_pilots':active_tasks,
                                    'queue_del_pilots':True}}
        self.messaging.called = []
        get_grid_status.reset_mock()
        get_grid_status.return_value = grid_tasks
        remove.reset_mock()

        g.check_grid()

        if not get_grid_status.called:
            raise Exception('did not call get_grid_status')
        remove.assert_not_called()
        if os.path.exists(os.path.join(submit_dir,'s_0')):
            raise Exception('did not clean old submit dir')
        if not os.path.exists(os.path.join(submit_dir,'s_1')):
            raise Exception('cleaned suspended submit dir')
        if not os.path.exists(os.path.join(submit_dir,'secure')):
            raise Exception('cleaned non-submit dir')
        if not any('queue_get_pilots' == x[1] for x in self.messaging.called):
            raise Exception('did not call queue_get_pilots')
        if any('queue_del_pilots' == x[1] for x in self.messaging.called):
            raise Exception('called queue_del_pilots when nothing to reset')

    @patch('iceprod.server.grid.grid.generate_submit_file')
    @unittest_reporter
    def test_023_setup_submit_directory(self, generate_submit_file):
        """Test setup_submit_directory"""
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
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        g = grid(args)
        if not g:
            raise Exception('init did not return grid object')

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
        self.messaging.ret = {'db':{'queue_set_submit_dir':True,
                                    'queue_get_cfg_for_task':thecfg,
                                    'auth_new_passkey':'passkey'}}
        self.messaging.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()
        generate_submit_file.return_value = True

        task = {'task_id':'1','name':'0','debug':0,'dataset_id':'d1',
                'job':0,'jobs_submitted':1}
        g.setup_submit_directory(task)

        if not any('queue_set_submit_dir' == x[1] for x in self.messaging.called):
            raise Exception('normal: did not call set_submit_dir')
        if not generate_submit_file.called:
            raise Exception('normal: did not call generate_submit_file')
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
        g.setup_submit_directory(task)

        if not any('queue_set_submit_dir' == x[1] for x in self.messaging.called):
            raise Exception('full cfg opts: did not call set_submit_dir')
        if not generate_submit_file.called:
            raise Exception('full cfg opts: did not call generate_submit_file')
        shutil.rmtree(submit_dir)

        # generate_submit_file error
        self.messaging.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()
        generate_submit_file.return_value = False

        task = {'task_id':'1','name':'0','debug':0}
        try:
            g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('generate_submit_file error: did not raise Exception')
        shutil.rmtree(submit_dir)

        # set_submit_dir error
        self.messaging.ret = {'db':{'queue_set_submit_dir':Exception(),
                                    'queue_get_cfg_for_task':thecfg,
                                    'auth_new_passkey':'passkey'}}
        self.messaging.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()
        generate_submit_file.return_value = True

        task = {'task_id':'1','name':'0','debug':0}
        try:
            g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('set_submit_dir error: did not raise Exception')
        shutil.rmtree(submit_dir)

        # new_passkey error
        self.messaging.ret = {'db':{'queue_set_submit_dir':True,
                                    'queue_get_cfg_for_task':thecfg,
                                    'auth_new_passkey':Exception()}}
        self.messaging.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()

        task = {'task_id':'1','name':'0','debug':0}
        try:
            g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('new_passkey error: did not raise Exception')
        shutil.rmtree(submit_dir)

        # get_cfg_for_task error
        self.messaging.ret = {'db':{'queue_set_submit_dir':True,
                                    'queue_get_cfg_for_task':Exception(),
                                    'auth_new_passkey':'passkey'}}
        self.messaging.called = []
        g.tasks_queued = 0
        generate_submit_file.reset_mock()

        task = {'task_id':'1','name':'0','debug':0}
        try:
            g.setup_submit_directory(task)
        except:
            pass
        else:
            raise Exception('get_cfg_for_task error: did not raise Exception')
        shutil.rmtree(submit_dir)

    @patch('iceprod.server.grid.grid.submit')
    @patch('iceprod.server.grid.grid.generate_submit_file')
    @unittest_reporter
    def test_024_setup_pilots(self, generate_submit_file, submit):
        """Test setup_pilots"""
        def s(t):
            if submit.ret:
                t['grid_queue_id'] = submit.ret
            else:
                raise Exception('bad submit')
        submit.side_effect = s

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
        args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                getattr(self.messaging,'db'))
        self.messaging.ret = {'db':{'queue_add_pilot':True}}
        self.messaging.called = []
        g = grid(args)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        g.tasks_queued = 0
        generate_submit_file.return_value = True
        submit.ret = '12345'

        tasks = {'thetaskid':{}}
        g.setup_pilots(tasks)
        if not generate_submit_file.called:
            raise Exception('did not call generate_submit_file')
        if not submit.called:
            raise Exception('did not call submit')
        if (self.messaging.called[0][1] != 'queue_new_pilot_ids' or
            self.messaging.called[1][1] != 'auth_new_passkey' or
            self.messaging.called[2][1] != 'queue_add_pilot'):
            raise Exception('unexpected messages')
        pilot_dict = self.messaging.called[2][3]['pilot']
        if (os.path.dirname(pilot_dict['submit_dir']) != submit_dir or
            'grid_queue_id' not in pilot_dict or
            pilot_dict['num'] != 1):
            logger.info('%r',self.messaging.called)
            raise Exception('bad pilot dict')

        # test error
        generate_submit_file.side_effect = Exception()
        try:
            g.setup_pilots(tasks)
        except:
            pass
        else:
            raise Exception('did not raise an Exception')

        generate_submit_file.side_effect = True
        submit.ret = None
        try:
            g.setup_pilots(tasks)
        except:
            pass
        else:
            raise Exception('did not raise an Exception')

        generate_submit_file.return_value = True
        submit.ret = '12345'
        self.messaging.ret = {'db':{'queue_add_pilot':Exception()}}
        try:
            g.setup_pilots(tasks)
        except:
            pass
        else:
            raise Exception('did not raise an Exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(grid_test))
    suite.addTests(loader.loadTestsFromNames(alltests,grid_test))
    return suite
