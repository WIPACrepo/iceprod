"""
Test script for grid
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests, _messaging

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
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock


import iceprod.server
from iceprod.server import module
from iceprod.server.grid import grid


class grid_test(unittest.TestCase):
    def setUp(self):
        super(grid_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        
        # override self.db_handle
        self.messaging = _messaging()
        self.check_run_stop = False
        
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(grid_test,self).tearDown()
    
    @contextmanager
    def _check_run(self):
        if self.check_run_stop:
            raise Exception('check_run_stop')
        yield
    
    def test_001_init(self):
        """Test init"""
        try:
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
            
        except Exception as e:
            logger.error('Error running grid init test - %s',str(e))
            printer('Test grid init',False)
            raise
        else:
            printer('Test grid init')
    
    
    def test_010_check_and_clean(self):
        """Test check_and_clean"""
        try:
            def check_iceprod():
                check_iceprod.called = True
            def check_grid():
                check_grid.called = True
            def clean():
                clean.called = True
            flexmock(grid).should_receive('check_iceprod').replace_with(check_iceprod)
            flexmock(grid).should_receive('check_grid').replace_with(check_grid)
            flexmock(grid).should_receive('clean').replace_with(clean)
            
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
            check_iceprod.called = False
            check_grid.called = False
            clean.called = False
            g.check_and_clean()
            
            if not check_iceprod.called:
                raise Exception('did not call check_iceprod()')
            if not check_grid.called:
                raise Exception('did not call check_grid()')
            if not clean.called:
                raise Exception('did not call clean()')
            
            # kill early
            self.check_run_stop = True
            try:
                g.check_and_clean()
            except:
                pass
            else:
                raise Exception('check_run did not throw exception')
            
        except Exception as e:
            logger.error('Error running grid check_and_clean test - %s',str(e))
            printer('Test grid check_and_clean',False)
            raise
        else:
            printer('Test grid check_and_clean')
    
    def test_011_queue(self):
        """Test queue"""
        try:
            def calc_dataset_prio(d):
                calc_dataset_prio.called = True
                if 'dataset_id' in d and d['dataset_id'] in calc_dataset_prio.ret:
                    return calc_dataset_prio.ret[d['dataset_id']]
                else:
                    raise Exception('bad dataset prio')
            def setup_submit_directory(task):
                setup_submit_directory.called = True
                if setup_submit_directory.error:
                    raise Exception('bad io')
            def submit(task):
                submit.called = True
                if submit.error:
                    raise Exception('bad submit')
            flexmock(grid).should_receive('calc_dataset_prio').replace_with(calc_dataset_prio)
            flexmock(grid).should_receive('setup_submit_directory').replace_with(setup_submit_directory)
            flexmock(grid).should_receive('submit').replace_with(submit)
            
            site = 'thesite'
            datasets = {1:{'dataset_id':1},
                       2:{'dataset_id':2},
                       3:{'dataset_id':3},
                      }
            tasks = {1:{'task_id':1},
                     2:{'task_id':2},
                    }
            self.messaging.ret = {'db':{'get_queueing_datasets':datasets,
                                        'get_queueing_tasks':tasks,
                                        'set_task_status':True}}
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
            calc_dataset_prio.called = False
            calc_dataset_prio.ret = {1:1.0, 2:3.0, 3:2.0}
            setup_submit_directory.called = False
            setup_submit_directory.error = False
            submit.called = False
            submit.error = False
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
            calc_dataset_prio.called = False
            calc_dataset_prio.ret = {1:1.0, 2:3.0, 3:2.0}
            setup_submit_directory.called = False
            setup_submit_directory.error = False
            submit.called = False
            submit.error = False
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
            self.messaging.ret = {'db':{'get_queueing_tasks':tasks,
                                        'get_queueing_datasets':Exception(),
                                        'set_task_status':True}}
            try:
                g.queue()
            except:
                pass
            else:
                raise Exception('get_queueing_datasets did not throw exception')
            
            # calc_dataset_prio error
            self.messaging.ret = {'db':{'get_queueing_datasets':datasets,
                                        'get_queueing_tasks':tasks,
                                        'set_task_status':True}}
            calc_dataset_prio.ret = {1:1.0, 3:2.0}
            g.tasks_queued = 0
            try:
                g.queue()
            except:
                pass
            else:
                raise Exception('calc_dataset_prio did not throw exception')
            
            # get_queueing_tasks error
            self.messaging.ret = {'db':{'get_queueing_datasets':datasets,
                                        'get_queueing_tasks':Exception(),
                                        'set_task_status':True}}
            calc_dataset_prio.ret = {1:1.0, 2:3.0, 3:2.0}
            try:
                g.queue()
            except:
                pass
            else:
                raise Exception('get_queueing_tasks did not throw exception')
            
            # setup_submit_directory error
            self.messaging.ret = {'db':{'get_queueing_datasets':datasets,
                                        'get_queueing_tasks':tasks,
                                        'set_task_status':True}}
            setup_submit_directory.error = True
            try:
                g.queue()
            except:
                pass
            else:
                raise Exception('setup_submit_directory did not throw exception')
            
            # submit error
            self.messaging.ret = {'db':{'get_queueing_datasets':datasets,
                                        'get_queueing_tasks':tasks,
                                        'set_task_status':True}}
            submit.error = True
            try:
                g.queue()
            except:
                pass
            else:
                raise Exception('submit did not throw exception')
            
            
            # set_task_status error
            self.messaging.ret = {'db':{'get_queueing_datasets':datasets,
                                        'get_queueing_tasks':tasks}}
            try:
                g.queue()
            except:
                pass
            else:
                raise Exception('set_task_status did not throw exception')
            
            
        except Exception as e:
            logger.error('Error running grid queue test - %s',str(e))
            printer('Test grid queue',False)
            raise
        else:
            printer('Test grid queue')
    
    def test_020_check_iceprod(self):
        """Test check_iceprod"""
        try:
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
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'reset_tasks':True}}
            g.tasks_queued = 0
            
            g.check_iceprod()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('normal: did not call get_active_tasks')
            if any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('normal: called reset_tasks when nothing to reset')
            
            # queued task reset
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                           }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                         'reset_tasks':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            
            g.check_iceprod()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('queued task reset: did not call get_active_tasks')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('queued task reset: did not call reset')
            
            # processing task reset
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                            'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                           }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                         'reset_tasks':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            
            g.check_iceprod()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('processing task reset: did not call get_active_tasks')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('processing task reset: did not call reset')
            
            # reset task reset
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=550)}},
                            'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                           }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                         'reset_tasks':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            
            g.check_iceprod()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('reset task reset: did not call get_active_tasks')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('reset task reset: did not call reset')
            
            # resume task reset
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'reset':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'resume':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=550)}},
                           }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                         'reset_tasks':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            
            g.check_iceprod()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('resume task reset: did not call get_active_tasks')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('resume task reset: did not call reset')
            
        except Exception as e:
            logger.error('Error running grid check_iceprod test - %s',str(e))
            printer('Test grid check_iceprod',False)
            raise
        else:
            printer('Test grid check_iceprod')
    
    def test_021_check_grid(self):
        """Test check_grid"""
        try:
            def get_task_status(t=None):
                get_task_status.called = True
                get_task_status.args = t
                if get_task_status.ret:
                    return get_task_status.ret
                else:
                    raise Exception('bad task status')
            flexmock(grid).should_receive('get_task_status').replace_with(get_task_status)
            
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
                           }
            self.messaging.ret = {'db':{'reset_tasks':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = active_tasks
            
            g.check_grid()
            
            if not get_task_status.called:
                raise Exception('normal: did not call get_task_status')
            if any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('normal: called reset_tasks when nothing to reset')
            
            # add error task
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'error':{3:{'task_id':3,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                           }
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = active_tasks
            
            g.check_grid()
            
            if not get_task_status.called:
                raise Exception('error task: did not call get_task_status')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('error task: did not call reset_tasks')
            
            # add unknown task
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'unknown':{4:{'task_id':4,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                           }
            g.tasks_queued = 0
            self.messaging.called = []
            get_task_status.called = False
            get_task_status.ret = active_tasks
            
            g.check_grid()
            
            if not get_task_status.called:
                raise Exception('unknown task: did not call get_task_status')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('unknown task: did not call reset_tasks')
            
            # queued error
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                           }
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = active_tasks
            
            g.check_grid()
            
            if not get_task_status.called:
                raise Exception('queued error: did not call get_task_status')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('queued error: did not call reset_tasks')
            
            # processing error
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                           }
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = active_tasks
            
            g.check_grid()
            
            if not get_task_status.called:
                raise Exception('processing error: did not call get_task_status')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('processing error: did not call reset_tasks')
            
            # processing failure
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':20,'status_changed':now-timedelta(seconds=1500)}},
                           }
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = active_tasks
            
            g.check_grid()
            
            if not get_task_status.called:
                raise Exception('processing failure: did not call get_task_status')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('processing failure: did not call reset_tasks')
            if ('fail' not in self.messaging.called[-1][3] or
                ('fail' in self.messaging.called[-1][3] and 
                 not self.messaging.called[-1][3]['fail'])):
                raise Exception('processing failure: did not set fail arg')
            
            # error resetting
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,'status_changed':now-timedelta(seconds=1500)}},
                           }
            self.messaging.ret = {'db':{'reset_tasks':Exception()}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = active_tasks
            
            try:
                g.check_grid()
            except:
                pass
            else:
                raise Exception('error resetting: did not raise Exception')
            if not get_task_status.called:
                raise Exception('error resetting: did not call get_task_status')
            if not any('reset_tasks' == x[1] for x in self.messaging.called):
                raise Exception('error resetting: did not call reset_tasks')
            
        except Exception as e:
            logger.error('Error running grid check_grid test - %s',str(e))
            printer('Test grid check_grid',False)
            raise
        else:
            printer('Test grid check_grid')
    
    def test_022_clean(self):
        """Test clean"""
        try:
            def get_task_status(t=None):
                get_task_status.called = True
                get_task_status.args = t
                if get_task_status.ret:
                    return get_task_status.ret
                else:
                    raise Exception('bad task status')
            def remove(dirs):
                remove.called = True
                remove.args = dirs
                if remove.ret:
                    return remove.ret
                else:
                    raise Exception('bad remove')
            flexmock(grid).should_receive('get_task_status').replace_with(get_task_status)
            flexmock(grid).should_receive('remove').replace_with(remove)
            
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
                                  'suspend_submit_dir_time':10000,
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
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)}},
                           }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = active_tasks
            remove.called = False
            remove.ret = True
            
            g.clean()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('normal: did not call get_active_tasks')
            if not get_task_status.called:
                raise Exception('normal: did not call get_task_status')
            if remove.called:
                raise Exception('normal: called remove when nothing to remove')
            if any('set_task_status' == x[1] for x in self.messaging.called):
                raise Exception('normal: called set_task_status when nothing to change')
            
            # reset by DB
            reset_submit_dir = os.path.join(submit_dir,'reset')
            os.mkdir(reset_submit_dir)
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)}},
                            'reset':{3:{'task_id':3,'failures':0,
                                        'status':'reset',
                                        'submit_dir':reset_submit_dir,
                                        'status_changed':now-timedelta(seconds=30)}},
                           }
            grid_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                       'status':'queued',
                                       'status_changed':now-timedelta(seconds=150)}},
                          'processing':{2:{'task_id':2,'failures':0,
                                           'status':'processing',
                                           'status_changed':now-timedelta(seconds=150)}},
                         }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            g.clean()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('reset by DB: did not call get_active_tasks')
            if not get_task_status.called:
                raise Exception('reset by DB: did not call get_task_status')
            if remove.called:
                raise Exception('reset by DB: called remove when nothing to remove')
            if not any('set_task_status' == x[1] for x in self.messaging.called):
                raise Exception('reset by DB: did not call set_task_status')
            if os.path.isdir(reset_submit_dir):
                raise Exception('reset by DB: did not delete submit dir')
            
            # resume by DB
            resume_submit_dir = os.path.join(submit_dir,'resume')
            os.mkdir(resume_submit_dir)
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)}},
                            'resume':{3:{'task_id':3,'failures':0,
                                        'status':'resume',
                                        'submit_dir':resume_submit_dir,
                                        'status_changed':now-timedelta(seconds=30)}},
                           }
            grid_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                       'status':'queued',
                                       'status_changed':now-timedelta(seconds=150)}},
                          'processing':{2:{'task_id':2,'failures':0,
                                           'status':'processing',
                                           'status_changed':now-timedelta(seconds=150)}},
                         }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            g.clean()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('resume by DB: did not call get_active_tasks')
            if not get_task_status.called:
                raise Exception('resume by DB: did not call get_task_status')
            if remove.called:
                raise Exception('resume by DB: called remove when nothing to remove')
            if not any('set_task_status' == x[1] for x in self.messaging.called):
                raise Exception('resume by DB: did not call set_task_status')
            if os.path.isdir(resume_submit_dir):
                raise Exception('resume by DB: did not delete submit dir')
            
            # old submit dir
            old_submit_dir = os.path.join(submit_dir,'12_old')
            old_submit_dir2 = os.path.join(submit_dir,'1_old')
            old_submit_dir3 = os.path.join(submit_dir,'24_old')
            os.mkdir(old_submit_dir)
            os.utime(old_submit_dir,(time.time()-20000,time.time()-20000))
            os.mkdir(old_submit_dir2)
            os.utime(old_submit_dir2,(time.time()-20000,time.time()-20000))
            os.mkdir(old_submit_dir3)
            os.utime(old_submit_dir3,(time.time()-200,time.time()-200))
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'submit_dir':'something_else',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)}},
                           }
            grid_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                       'status':'queued',
                                       'status_changed':now-timedelta(seconds=150)}},
                          'processing':{2:{'task_id':2,'failures':0,
                                           'status':'processing',
                                           'status_changed':now-timedelta(seconds=150)}},
                         }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            g.clean()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('old_submit_dir: did not call get_active_tasks')
            if not get_task_status.called:
                raise Exception('old_submit_dir: did not call get_task_status')
            if remove.called:
                raise Exception('old_submit_dir: called remove when nothing to remove')
            if any('set_task_status' == x[1] for x in self.messaging.called):
                raise Exception('old_submit_dir: called set_task_status when nothing to change')
            if os.path.isdir(old_submit_dir):
                raise Exception('old_submit_dir: did not delete submit dir')
            if os.path.isdir(old_submit_dir2):
                raise Exception('old_submit_dir: did not delete submit dir2')
            if not os.path.isdir(old_submit_dir3):
                raise Exception('old_submit_dir: deleted submit dir3 when not supposed to')
            os.rmdir(old_submit_dir3)
            
            # error by grid
            error_submit_dir = os.path.join(submit_dir,'3_error')
            os.mkdir(error_submit_dir)
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)},
                                          3:{'task_id':3,'failures':0,
                                             'status':'processing',
                                             'submit_dir':error_submit_dir,
                                             'status_changed':now-timedelta(seconds=150)}
                                         },
                           }
            grid_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                       'status':'queued',
                                       'status_changed':now-timedelta(seconds=150)}},
                          'processing':{2:{'task_id':2,'failures':0,
                                           'status':'processing',
                                           'status_changed':now-timedelta(seconds=150)}},
                          'error':{3:{'task_id':3,'failures':0,
                                      'status':'processing',
                                      'submit_dir':error_submit_dir,
                                      'status_changed':now-timedelta(seconds=430)}},
                         }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            g.clean()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('error by grid: did not call get_active_tasks')
            if not get_task_status.called:
                raise Exception('error by grid: did not call get_task_status')
            if not remove.called:
                raise Exception('error by grid: did not call remove')
            if not any('set_task_status' == x[1] for x in self.messaging.called):
                raise Exception('error by grid: did not call set_task_status')
            if not os.path.isdir(error_submit_dir):
                raise Exception('error by grid: deleted submit dir when not supposed to')
            os.rmdir(error_submit_dir)
            
            # error by grid2
            error_submit_dir = os.path.join(submit_dir,'3_error')
            os.mkdir(error_submit_dir)
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)}},
                            'reset':{3:{'task_id':3,'failures':0,
                                       'status':'reset',
                                       'submit_dir':error_submit_dir,
                                       'status_changed':now-timedelta(seconds=50)}},
                           }
            grid_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                       'status':'queued',
                                       'status_changed':now-timedelta(seconds=150)}},
                          'processing':{2:{'task_id':2,'failures':0,
                                           'status':'processing',
                                           'status_changed':now-timedelta(seconds=150)}},
                          'error':{3:{'task_id':3,'failures':0,
                                      'status':'reset',
                                      'submit_dir':error_submit_dir,
                                      'status_changed':now-timedelta(seconds=430)}},
                         }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            g.clean()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('error by grid2: did not call get_active_tasks')
            if not get_task_status.called:
                raise Exception('error by grid2: did not call get_task_status')
            if not remove.called:
                raise Exception('error by grid2: did not call remove')
            if not any('set_task_status' == x[1] for x in self.messaging.called):
                raise Exception('error by grid2: did not call set_task_status')
            if os.path.isdir(error_submit_dir):
                raise Exception('error by grid: did not delete submit dir')
            
            # error by grid3
            error_submit_dir = os.path.join(submit_dir,'3_error')
            os.mkdir(error_submit_dir)
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)}},
                           }
            grid_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                       'status':'queued',
                                       'status_changed':now-timedelta(seconds=150)}},
                          'processing':{2:{'task_id':2,'failures':0,
                                           'status':'processing',
                                           'status_changed':now-timedelta(seconds=150)}},
                          'error':{3:{'task_id':3,'failures':0,
                                      'status':'processing',
                                      'submit_dir':error_submit_dir,
                                      'status_changed':now-timedelta(seconds=430)}},
                         }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':True}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            g.clean()
            
            if not any('get_active_tasks' == x[1] for x in self.messaging.called):
                raise Exception('error by grid3: did not call get_active_tasks')
            if not get_task_status.called:
                raise Exception('error by grid3: did not call get_task_status')
            if not remove.called:
                raise Exception('error by grid3: did not call remove')
            if any('set_task_status' == x[1] for x in self.messaging.called):
                raise Exception('error by grid3: called set_task_status when not supposed to')
            if not os.path.isdir(error_submit_dir):
                raise Exception('error by grid3: deleted submit dir when not supposed to')
            os.rmdir(error_submit_dir)
            
            # get_active_tasks error
            self.messaging.ret = {'db':{'get_active_tasks':Exception()}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            try:
                g.clean()
            except:
                pass
            else:
                raise Exception('get_active_tasks error: did not raise Exception')
            
            # get_active_tasks error2
            self.messaging.ret = {'db':{'get_active_tasks':None}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            try:
                g.clean()
            except:
                pass
            else:
                raise Exception('get_active_tasks error2: did not raise Exception')
            
            # set_task_status error
            active_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                         'status':'queued',
                                         'status_changed':now-timedelta(seconds=150)}},
                            'processing':{2:{'task_id':2,'failures':0,
                                             'status':'processing',
                                             'status_changed':now-timedelta(seconds=150)}},
                            'reset':{3:{'task_id':3,'failures':0,
                                      'status':'reset',
                                      'submit_dir':error_submit_dir,
                                      'status_changed':now-timedelta(seconds=430)}},
                           }
            grid_tasks = {'queued':{1:{'task_id':1,'failures':0,
                                       'status':'queued',
                                       'status_changed':now-timedelta(seconds=150)}},
                          'processing':{2:{'task_id':2,'failures':0,
                                           'status':'processing',
                                           'status_changed':now-timedelta(seconds=150)}},
                         }
            self.messaging.ret = {'db':{'get_active_tasks':active_tasks,
                                        'set_task_status':Exception()}}
            self.messaging.called = []
            g.tasks_queued = 0
            get_task_status.called = False
            get_task_status.ret = grid_tasks
            remove.called = False
            remove.ret = True
            
            try:
                g.clean()
            except:
                pass
            else:
                raise Exception('set_task_status error: did not raise Exception')
            
        except Exception as e:
            logger.error('Error running grid clean test - %s',str(e))
            printer('Test grid clean',False)
            raise
        else:
            printer('Test grid clean')

    def test_023_setup_submit_directory(self):
        """Test setup_submit_directory"""
        try:
            def generate_submit_file(t,cfg=None,passkey=None):
                generate_submit_file.called = True
                generate_submit_file.args = (t,cfg,passkey)
                if generate_submit_file.ret:
                    return generate_submit_file.ret
                else:
                    raise Exception('bad submit file generation')
            flexmock(grid).should_receive('generate_submit_file').replace_with(generate_submit_file)
            
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
            self.messaging.ret = {'db':{'set_submit_dir':True,
                                        'get_cfg_for_task':thecfg,
                                        'new_passkey':'passkey'}}
            self.messaging.called = []
            g.tasks_queued = 0
            generate_submit_file.called = False
            generate_submit_file.ret = True
            
            task = {'task_id':'1','name':'0','debug':0}
            g.setup_submit_directory(task)
            
            if not any('set_submit_dir' == x[1] for x in self.messaging.called):
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
            
            if not any('set_submit_dir' == x[1] for x in self.messaging.called):
                raise Exception('full cfg opts: did not call set_submit_dir')
            if not generate_submit_file.called:
                raise Exception('full cfg opts: did not call generate_submit_file')
            shutil.rmtree(submit_dir)
            
            # generate_submit_file error
            self.messaging.called = []
            g.tasks_queued = 0
            generate_submit_file.called = False
            generate_submit_file.ret = False
            
            task = {'task_id':'1','name':'0','debug':0}
            try:
                g.setup_submit_directory(task)
            except:
                pass
            else:
                raise Exception('generate_submit_file error: did not raise Exception')
            shutil.rmtree(submit_dir)
            
            # set_submit_dir error
            self.messaging.ret = {'db':{'set_submit_dir':Exception(),
                                        'get_cfg_for_task':thecfg,
                                        'new_passkey':'passkey'}}
            self.messaging.called = []
            g.tasks_queued = 0
            generate_submit_file.called = False
            generate_submit_file.ret = True
            
            task = {'task_id':'1','name':'0','debug':0}
            try:
                g.setup_submit_directory(task)
            except:
                pass
            else:
                raise Exception('set_submit_dir error: did not raise Exception')
            shutil.rmtree(submit_dir)
            
            # new_passkey error
            self.messaging.ret = {'db':{'set_submit_dir':True,
                                        'get_cfg_for_task':thecfg,
                                        'new_passkey':Exception()}}
            self.messaging.called = []
            g.tasks_queued = 0
            generate_submit_file.called = False
            generate_submit_file.ret = True
            
            task = {'task_id':'1','name':'0','debug':0}
            try:
                g.setup_submit_directory(task)
            except:
                pass
            else:
                raise Exception('new_passkey error: did not raise Exception')
            shutil.rmtree(submit_dir)
            
            # get_cfg_for_task error
            self.messaging.ret = {'db':{'set_submit_dir':True,
                                        'get_cfg_for_task':Exception(),
                                        'new_passkey':'passkey'}}
            self.messaging.called = []
            g.tasks_queued = 0
            generate_submit_file.called = False
            generate_submit_file.ret = True
            
            task = {'task_id':'1','name':'0','debug':0}
            try:
                g.setup_submit_directory(task)
            except:
                pass
            else:
                raise Exception('get_cfg_for_task error: did not raise Exception')
            shutil.rmtree(submit_dir)
            
        except Exception as e:
            logger.error('Error running grid setup_submit_directory test - %s',str(e))
            printer('Test grid setup_submit_directory',False)
            raise
        else:
            printer('Test grid setup_submit_directory')

    def test_024_calc_dataset_prio(self):
        """Test calc_dataset_prio"""
        try:
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
                                  'monitor_address':'localhost'
                                  }},
                   'db':{'address':None,'ssl':False}}
            
            # init
            args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                    getattr(self.messaging,'db'))
            g = grid(args)
            if not g:
                raise Exception('init did not return grid object')
            
            # call normally
            g.tasks_queued = 0
            
            dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                       'tasks_submitted':0,
                       'priority':0,
                      }
            prio1 = g.calc_dataset_prio(dataset)
            if not isinstance(prio1,(int,float)):
                raise Exception('dataset prio is not a number')
            
            dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                       'tasks_submitted':0,
                       'priority':1,
                      }
            prio2 = g.calc_dataset_prio(dataset)
            if prio2 < prio1:
                raise Exception('priority is not winning')
            
            dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                       'tasks_submitted':100,
                       'priority':1,
                      }
            prio3 = g.calc_dataset_prio(dataset)
            if prio2 < prio3:
                raise Exception('greater # tasks submitted is not losing')
            
            dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(2,site),
                       'tasks_submitted':0,
                       'priority':1,
                      }
            prio4 = g.calc_dataset_prio(dataset)
            if prio2 < prio4:
                raise Exception('greater dataset_id is not losing')
            
        except Exception as e:
            logger.error('Error running grid calc_dataset_prio test - %s',str(e))
            printer('Test grid calc_dataset_prio',False)
            raise
        else:
            printer('Test grid calc_dataset_prio')
    
    def test_030_task_to_grid_name(self):
        """Test task_to_grid_name"""
        try:
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
                                  'monitor_address':'localhost'
                                  }},
                   'db':{'address':None,'ssl':False}}
            
            # init
            args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                    getattr(self.messaging,'db'))
            g = grid(args)
            if not g:
                raise Exception('init did not return grid object')
            
            # call normally
            g.tasks_queued = 0
            
            task = {'task_id':'thetaskid'}
            ret = g.task_to_grid_name(task)
            if ret != 'i'+task['task_id']:
                raise Exception('task_to_grid_name default is incorrect')
            
        except Exception as e:
            logger.error('Error running grid task_to_grid_name test - %s',str(e))
            printer('Test grid task_to_grid_name',False)
            raise
        else:
            printer('Test grid task_to_grid_name')
    
    def test_031_grid_name_to_task_id(self):
        """Test grid_name_to_task_id"""
        try:
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
                                  'monitor_address':'localhost'
                                  }},
                   'db':{'address':None,'ssl':False}}
            
            # init
            args = (gridspec,cfg['queue'][name],cfg,self._check_run,
                    getattr(self.messaging,'db'))
            g = grid(args)
            if not g:
                raise Exception('init did not return grid object')
            
            # call normallycfg
            g.tasks_queued = 0
            
            task = {'task_id':'thetaskid'}
            ret = g.grid_name_to_task_id('i'+task['task_id'])
            if ret != task['task_id']:
                raise Exception('grid_name_to_task_id default is incorrect')
                
            try:
                g.grid_name_to_task_id('garbage')
            except:
                pass
            else:
                raise Exception('grid_name_to_task_id did not raise Exception')
            
        except Exception as e:
            logger.error('Error running grid grid_name_to_task_id test - %s',str(e))
            printer('Test grid grid_name_to_task_id',False)
            raise
        else:
            printer('Test grid grid_name_to_task_id')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(grid_test))
    suite.addTests(loader.loadTestsFromNames(alltests,grid_test))
    return suite
