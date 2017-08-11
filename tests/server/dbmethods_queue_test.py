"""
Test script for dbmethods.queue
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, cmp_dict

import logging
logger = logging.getLogger('dbmethods_test')

import os, sys, time
import shutil
import tempfile
import random
import stat
try:
    import StringIO
except ImportError:
    from io import StringIO
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable
import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import tornado.escape

from iceprod.core import functions
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.core.resources import Resources
from iceprod.server import dbmethods
import iceprod.server.dbmethods.queue

from .dbmethods_test import dbmethods_base


class dbmethods_queue_test(dbmethods_base):
    @unittest_reporter
    def test_000_queue_set_site_queues(self):
        """Test queue_set_site_queues"""
        now = dbmethods.nowstr()
        tables = {
            'site':[
                {'site_id':'s0','name':'n','institution':'inst',
                 'queues':'{}','auth_key':None,'website_url':'',
                 'version':'2','last_update':now,'admin_name':'',
                 'admin_email':''},
            ],
        }

        yield self.set_tables(tables)
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}

        yield self.db['queue_set_site_queues']('s0', queues)
        endtables = yield self.get_tables(['site'])
        if ((not endtables['site']) or
            endtables['site'][0]['site_id'] != 's0'):
            raise Exception('did not set site')
        expected = json_encode(queues)
        self.assertEqual(endtables['site'][0]['queues'], expected)

        # update no queue
        yield self.set_tables(tables)
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}

        yield self.db['queue_set_site_queues']('s1', queues)
        endtables = yield self.get_tables(['site'])
        if not cmp_dict(tables,endtables):
            raise Exception('tables modified')

        # update existing
        tables2 = {
            'site':[
                {'site_id':'s1','name':'n','institution':'inst',
                 'queues':'{"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}','auth_key':None,'website_url':'',
                 'version':'2','last_update':now,'admin_name':'',
                 'admin_email':''},
            ],
        }
        yield self.set_tables(tables2)
        queues = {"g1":{"type":"b","description":"desc","resources":{"disk":[20,10]}}}

        yield self.db['queue_set_site_queues']('s1', queues)
        endtables = yield self.get_tables(['site'])
        if ((not endtables['site']) or
            endtables['site'][0]['site_id'] != 's1'):
            raise Exception('did not set site')
        expected = {"g1":{"type":"b","description":"desc","resources":{"mem":[20,10],"disk":[20,10]}}}
        self.assertEqual(json_decode(endtables['site'][0]['queues']), expected)

        # bad queue db info
        tables3 = {
            'site':[
                {'site_id':'s1','name':'n','institution':'inst',
                 'queues':'garbage','auth_key':None,'website_url':'',
                 'version':'2','last_update':now,'admin_name':'',
                 'admin_email':''},
            ],
        }
        yield self.set_tables(tables3)
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}

        try:
            yield self.db['queue_set_site_queues']('s1', queues)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(['site'])
        if not cmp_dict(tables3,endtables):
            raise Exception('tables modified')

        # bad queue insert info
        yield self.set_tables({'site':[]})
        queues = lambda a:a+1 # something that can't be json

        try:
            yield self.db['queue_set_site_queues']('s0', queues)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(['site'])
        if endtables['site']:
            raise Exception('tables modified')

        # sql error
        queues = {"g1":{"type":"t","description":"desc","resources":{"mem":[20,10]}}}
        for i in range(2):
            yield self.set_tables({'site':[]})
            self.set_failures([False for _ in range(i)]+[True])

            try:
                yield self.db['queue_set_site_queues']('s0', queues)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(['site'])
            if endtables['site']:
                raise Exception('tables modified')

    @unittest_reporter
    def test_010_queue_get_active_tasks(self):
        """Test queue_get_active_tasks"""
        gridspec = 'klsjdfl.grid1'
        now = dbmethods.nowstr()

        # single task
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now, 'submit_dir':self.test_dir,
                 'grid_queue_id':'lkn', 'failures':0, 'evictions':0,
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
            ],
        }

        yield self.set_tables(tables)
        ret = yield self.db['queue_get_active_tasks'](gridspec)

        ret_should_be = {'queued':{'asdf':tables['task'][0].copy()}}
        ret_should_be['queued']['asdf']['status_changed'] = dbmethods.str2datetime(now)
        self.assertEqual(ret, ret_should_be)

        # without gridspec
        yield self.set_tables(tables)
        ret = yield self.db['queue_get_active_tasks']()
        self.assertEqual(ret, ret_should_be)

        # no tasks
        yield self.set_tables({'task':[],'search':[]})
        ret = yield self.db['queue_get_active_tasks'](gridspec)
        self.assertEqual(ret, {})

        # several tasks
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'mertert', 'status':'processing', 'prev_status':'queued',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':1, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                {'task_id':'gdf', 'job_id':'gew', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                {'task_id':'mertert', 'job_id':'asd', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'processing'},
            ],
        }
        yield self.set_tables(tables2)
        ret = yield self.db['queue_get_active_tasks'](gridspec)

        ret_should_be = {'queued':{'asdf':tables2['task'][0].copy(),
                                   'gdf':tables2['task'][1].copy()},
                         'processing':{'mertert':tables2['task'][2].copy()}
                        }
        for d in ret_should_be['queued'].values():
            d['status_changed'] = dbmethods.str2datetime(now)
        for d in ret_should_be['processing'].values():
            d['status_changed'] = dbmethods.str2datetime(now)
        if not cmp_dict(ret, ret_should_be):
            logger.error('cb.ret = %r', ret)
            logger.error('ret should be = %r', ret_should_be)
            raise Exception('several tasks: callback ret != task task2 task3')

        # sql error
        for i in range(2):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['queue_get_active_tasks'](gridspec)
            except:
                pass
            else:
                raise Exception('did not raise Exception')

    @unittest_reporter
    def test_015_queue_get_grid_tasks(self):
        """Test queue_get_grid_tasks"""
        # single task
        now = dbmethods.nowstr()
        gridspec = 'skldfnk'
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
            ],
        }
        task = 'asdf'
        status = 'waiting'
        yield self.set_tables(tables)

        ret = yield self.db['queue_get_grid_tasks'](gridspec)
        expected = {'task_id':'asdf','grid_queue_id':'lkn',
                    'submit_time':dbmethods.str2datetime(now),
                    'submit_dir':self.test_dir}
        self.assertEqual(ret[0], expected)

        # query error
        for i in range(2):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['queue_get_grid_tasks'](gridspec)
            except:
                pass
            else:
                raise Exception('did not raise Exception')

    @unittest_reporter
    def test_020_queue_set_task_status(self):
        """Test queue_set_task_status"""
        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
            ],
        }
        task = 'asdf'
        status = 'waiting'
        yield self.set_tables(tables)

        ret = yield self.db['queue_set_task_status'](task, status)

        end_tables = yield self.get_tables(tables.keys())
        if (end_tables['task'][0]['status'] != status or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != status):
            logger.info('%r',end_tables)
            raise Exception('set status failed')

        # no task
        task = None
        status = 'waiting'
        yield self.set_tables({'task':[],'search':[]})

        try:
            yield self.db['queue_set_task_status'](task, status)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # multiple tasks (dict)
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'mertert', 'status':'processing', 'prev_status':'queued',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':1, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'gdf', 'job_id':'gew', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'mertert', 'job_id':'asd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'processing'},
            ],
        }
        task = {'asdf':{},'gdf':{}}
        status = 'waiting'
        yield self.set_tables(tables2)

        ret = yield self.db['queue_set_task_status'](task, status)

        end_tables = yield self.get_tables(tables.keys())
        if (end_tables['task'][0]['status'] != status or
            end_tables['task'][1]['status'] != status or
            end_tables['task'][2]['status'] != 'processing' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] != now or
            end_tables['search'][0]['task_status'] != status or
            end_tables['search'][1]['task_status'] != status or
            end_tables['search'][2]['task_status'] != 'processing'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (dict): set status failed')

        # multiple tasks (list)
        task = ['asdf','gdf']
        status = 'waiting'
        yield self.set_tables(tables2)

        ret = yield self.db['queue_set_task_status'](task, status)

        end_tables = yield self.get_tables(tables.keys())
        if (end_tables['task'][0]['status'] != status or
            end_tables['task'][1]['status'] != status or
            end_tables['task'][2]['status'] != 'processing' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] != now or
            end_tables['search'][0]['task_status'] != status or
            end_tables['search'][1]['task_status'] != status or
            end_tables['search'][2]['task_status'] != 'processing'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (list): set status failed')

        # sql error
        task = 'asdf'
        status = 'waiting'
        
        yield self.set_tables(tables)
        self.set_failures([True])
        try:
            yield self.db['queue_set_task_status'](task, status)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        
        endtables = yield self.get_tables(['task'])
        self.assertEqual(endtables['task'], tables['task'])

    @unittest_reporter
    def test_030_queue_reset_tasks(self):
        """Test queue_reset_tasks"""
        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
            ],
        }
        reset = 'asdf'
        yield self.set_tables(tables)

        yield self.db['queue_reset_tasks'](reset)

        end_tables = yield self.get_tables(tables.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'reset'):
            logger.info('%r',end_tables)
            raise Exception('reset failed')

        # single task with fail
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                  'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'mertert', 'status':'processing', 'prev_status':'queued',
                  'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':1, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
            'search':[
                {'task_id':'asdf', 'job_id':'bfsd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'gdf', 'job_id':'gew', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'queued'},
                {'task_id':'mertert', 'job_id':'asd', 'dataset_id':'d1',
                 'gridspec':'skldfnk', 'name':'0', 'task_status':'processing'},
            ],
        }
        reset = 'asdf'
        fail = 'gdf'
        yield self.set_tables(tables2)

        yield self.db['queue_reset_tasks'](reset, fail)

        end_tables = yield self.get_tables(tables2.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][1]['status'] != 'failed' or
            end_tables['task'][2]['status'] != 'processing' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] != now or
            end_tables['search'][0]['task_status'] != 'reset' or
            end_tables['search'][1]['task_status'] != 'failed' or
            end_tables['search'][2]['task_status'] != 'processing'):
            logger.info('%r',end_tables)
            raise Exception('reset with fail failed')

        # single fail task
        fail = 'asdf'
        yield self.set_tables(tables)

        yield self.db['queue_reset_tasks'](fail=fail)

        end_tables = yield self.get_tables(tables.keys())
        if (end_tables['task'][0]['status'] != 'failed' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'failed'):
            logger.info('%r',end_tables)
            raise Exception('single task w/fail')

        # multiple tasks (dict)
        reset = {'asdf':{},'gdf':{}}
        fail = {'mertert':{}}
        yield self.set_tables(tables2)

        yield self.db['queue_reset_tasks'](reset, fail)

        end_tables = yield self.get_tables(tables2.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][1]['status'] != 'reset' or
            end_tables['task'][2]['status'] != 'failed' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][2]['prev_status'] != 'processing' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'reset' or
            end_tables['search'][1]['task_status'] != 'reset' or
            end_tables['search'][2]['task_status'] != 'failed'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (dict): reset with fail failed')

        # multiple tasks (list)
        reset = ['asdf','gdf']
        fail = ['mertert']
        yield self.set_tables(tables2)

        yield self.db['queue_reset_tasks'](reset, fail)

        end_tables = yield self.get_tables(tables2.keys())
        if (end_tables['task'][0]['status'] != 'reset' or
            end_tables['task'][1]['status'] != 'reset' or
            end_tables['task'][2]['status'] != 'failed' or
            end_tables['task'][0]['prev_status'] != 'queued' or
            end_tables['task'][1]['prev_status'] != 'queued' or
            end_tables['task'][2]['prev_status'] != 'processing' or
            end_tables['task'][0]['status_changed'] <= now or
            end_tables['task'][1]['status_changed'] <= now or
            end_tables['task'][2]['status_changed'] <= now or
            end_tables['search'][0]['task_status'] != 'reset' or
            end_tables['search'][1]['task_status'] != 'reset' or
            end_tables['search'][2]['task_status'] != 'failed'):
            logger.info('%r',end_tables)
            raise Exception('multiple tasks (list): reset with fail failed')

        # sql error in reset
        reset = 'asdf'
        yield self.set_tables(tables)
        self.set_failures([True])

        try:
            yield self.db['queue_reset_tasks'](reset)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        end_tables = yield self.get_tables(tables.keys())
        self.assertEqual(end_tables['task'], tables['task'])

        # sql error in fail
        reset = None
        fail = 'asdf'
        yield self.set_tables(tables)
        self.set_failures([True])

        try:
            yield self.db['queue_reset_tasks'](reset, fail)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        end_tables = yield self.get_tables(tables.keys())
        self.assertEqual(end_tables['task'], tables['task'])

    @unittest_reporter
    def test_040_queue_get_task(self):
        """Test queue_get_task"""
        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
        }
        task_id = 'asdf'
        yield self.set_tables(tables)

        ret = yield self.db['queue_get_task'](task_id)

        ret_should_be = {t['task_id']:t.copy() for t in tables['task']}
        for d in ret_should_be.values():
            d['status_changed'] = dbmethods.str2datetime(now)
        self.assertEqual(ret, ret_should_be)
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

        # no tasks
        yield self.set_tables({'task':[]})

        ret = yield self.db['queue_get_task'](task_id)
        if ret:
            raise Exception('returned task when there are none')

        # no task_id
        task_id = None
        yield self.set_tables(tables)

        try:
            yield self.db['queue_get_task'](task_id)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # several tasks
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'mertert', 'status':'processing', 'prev_status':'queued',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':1, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
        }

        task_id = [t['task_id'] for t in tables2['task']]
        yield self.set_tables(tables2)

        ret = yield self.db['queue_get_task'](task_id)

        ret_should_be = {t['task_id']:t.copy() for t in tables2['task']}
        for d in ret_should_be.values():
            d['status_changed'] = dbmethods.str2datetime(now)
        self.assertEqual(ret, ret_should_be)
        end_tables = yield self.get_tables(tables2.keys())
        if not cmp_dict(tables2,end_tables):
            raise Exception('tables were modified')

        # sql error
        yield self.set_tables(tables)
        self.set_failures([True])

        try:
            yield self.db['queue_get_task'](task_id)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_050_queue_get_task_by_grid_queue_id(self):
        """Test queue_get_task_by_grid_queue_id"""
        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
        }
        task_id = 'lkn'
        yield self.set_tables(tables)

        ret = yield self.db['queue_get_task_by_grid_queue_id'](task_id)

        ret_should_be = {t['task_id']:t.copy() for t in tables['task']}
        for d in ret_should_be.values():
            d['status_changed'] = dbmethods.str2datetime(now)
        self.assertEqual(ret, ret_should_be)
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

        # no tasks
        yield self.set_tables({'task':[]})

        ret = yield self.db['queue_get_task_by_grid_queue_id'](task_id)
        if ret:
            raise Exception('returned task when there are none')

        # no task_id
        task_id = None
        yield self.set_tables(tables)

        try:
            yield self.db['queue_get_task_by_grid_queue_id'](task_id)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # several tasks
        tables2 = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'gdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn2',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
                {'task_id':'mertert', 'status':'processing', 'prev_status':'queued',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn3',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':1, 'depends': '', 'requirements': '',
                 'task_rel_id':None},
            ],
        }
        task_id = [t['grid_queue_id'] for t in tables2['task']]
        yield self.set_tables(tables2)

        ret = yield self.db['queue_get_task_by_grid_queue_id'](task_id)

        ret_should_be = {t['task_id']:t.copy() for t in tables2['task']}
        for d in ret_should_be.values():
            d['status_changed'] = dbmethods.str2datetime(now)
        self.assertEqual(ret, ret_should_be)
        end_tables = yield self.get_tables(tables2.keys())
        if not cmp_dict(tables2,end_tables):
            raise Exception('tables were modified')

        # sql error
        yield self.set_tables(tables)
        self.set_failures([True])

        try:
            yield self.db['queue_get_task_by_grid_queue_id'](task_id)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

    @unittest_reporter
    def test_060_queue_set_submit_dir(self):
        """Test queue_set_submit_dir"""
        # single task
        now = dbmethods.nowstr()
        tables = {
            'task':[
                {'task_id':'asdf', 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'submit_dir':self.test_dir, 'grid_queue_id':'lkn',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '',
                 'task_rel_id':None},
            ],
        }
        task = 'asdf'
        submit_dir = '/test/submit'
        yield self.set_tables(tables)

        yield self.db['queue_set_submit_dir'](task, submit_dir)

        end_tables = yield self.get_tables(tables.keys())
        if end_tables['task'][0]['submit_dir'] != submit_dir:
            raise Exception('submit_dir not set')

        # no task
        task = None
        yield self.set_tables(tables)

        try:
            yield self.db['queue_set_submit_dir'](task, submit_dir)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

        # sql error
        task = 'asdf'
        yield self.set_tables(tables)
        self.set_failures([True])

        try:
            yield self.db['queue_set_submit_dir'](task, submit_dir)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            raise Exception('tables were modified')

    @patch('iceprod.server.dbmethods.queue.random.sample')
    @unittest_reporter
    def test_100_queue_buffer_jobs_tasks(self, sample):
        """Test queue_buffer_jobs_tasks"""
        sample.side_effect = lambda a,b:a[:b]

        gridspec = 'msdfiner'
        now = dbmethods.nowstr()
        tables = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }

        # single task
        yield self.set_tables(tables)

        num = 10
        yield self.db['queue_buffer_jobs_tasks'](gridspec, num)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 task')
        if 'search' not in end_tables or len(end_tables['search']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 search')
        if 'job' not in end_tables or len(end_tables['job']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 job')

        # check that it doesn't get resubmitted
        yield self.db['queue_buffer_jobs_tasks'](gridspec, num)
        end_tables2 = yield self.get_tables(['task','job','search'])
        if not cmp_dict(end_tables,end_tables2):
            logger.info('%r',end_tables)
            logger.info('%r',end_tables2)
            raise Exception('buffer resubmit: tables modified')

        # now try for multiple jobs
        tables2 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':2,
                 'tasks_submitted':2,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables2)

        yield self.db['queue_buffer_jobs_tasks'](gridspec, num)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 job')

        # now try for multiple gridspecs and datasets
        tables3 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
                {'dataset_id':'d2', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec+'a',
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
                {'task_rel_id':'tr2','dataset_id':'d2','task_index':0,
                 'name':'task1','depends':'','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables3)

        num = 10
        yield self.db['queue_buffer_jobs_tasks'](gridspec, num)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 job')

        # now try with task names
        tables4 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':'{"task1":"'+gridspec+'"}',
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables4)

        yield self.db['queue_buffer_jobs_tasks'](gridspec, num)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 task')
        if 'search' not in end_tables or len(end_tables['search']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 search')
        if 'job' not in end_tables or len(end_tables['job']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 job')

        # now try with a different gridspec
        tables4 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':'{"task1":"othergs"}',
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables4)

        yield self.db['queue_buffer_jobs_tasks'](gridspec, num)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' in end_tables and end_tables['task']:
            logger.info('%r',end_tables)
            raise Exception('created more than 0 tasks')
        if 'search' in end_tables and end_tables['search']:
            logger.info('%r',end_tables)
            raise Exception('created more than 0 search')
        if 'job' in end_tables and end_tables['job']:
            logger.info('%r',end_tables)
            raise Exception('created more than 0 job')

        # now try emulating global queueing
        tables4 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':'{"task1":"'+gridspec+'"}',
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables4)

        yield self.db['queue_buffer_jobs_tasks'](None, num)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 task')
        if 'search' not in end_tables or len(end_tables['search']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 search')
        if 'job' not in end_tables or len(end_tables['job']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 job')
            
        tables5 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1, 'tasks_submitted':2,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
                {'task_rel_id':'tr2','dataset_id':'d1','task_index':1,
                 'name':'task2','depends':'tr1','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables5)

        yield self.db['queue_buffer_jobs_tasks'](None, num)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 1:
            logger.info('%r',end_tables)
            raise Exception('did not create 1 job')
        if end_tables['task'][0]['depends'] != '':
            logger.info('%r',end_tables['task'])
            raise Exception('task1 has a dependency')
        if end_tables['task'][1]['depends'] != end_tables['task'][0]['task_id']:
            logger.info('%r',end_tables['task'])
            raise Exception('task2 does not depend on task1')

        tables5 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'tr1','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables5)

        yield self.db['queue_buffer_jobs_tasks'](None, 10)
        end_tables = yield self.get_tables(['task','job','search'])
        if any(end_tables.values()):
            logger.info('%r',end_tables)
            raise Exception('tables have changed')

        tables5 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
                {'dataset_id':'d2', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
                {'task_rel_id':'tr2','dataset_id':'d2','task_index':0,
                 'name':'task1','depends':'tr1','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables5)

        yield self.db['queue_buffer_jobs_tasks'](None, 10)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 job')
        if end_tables['task'][0]['depends'] != '':
            logger.info('%r',end_tables['task'])
            raise Exception('task1 has a dependency')
        if end_tables['task'][1]['depends'] != end_tables['task'][0]['task_id']:
            logger.info('%r',end_tables['task'])
            raise Exception('task2 does not depend on task1')
            

        tables5 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
                {'dataset_id':'d2', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':'blah',
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','task_index':0,
                 'name':'task1','depends':'','requirements':''},
                {'task_rel_id':'tr2','dataset_id':'d2','task_index':0,
                 'name':'task1','depends':'tr1','requirements':''},
            ],
            'job':[],'task':[],'search':[],
        }
        yield self.set_tables(tables5)

        yield self.db['queue_buffer_jobs_tasks'](None, 10)
        end_tables = yield self.get_tables(['task','job','search'])
        if 'task' not in end_tables or len(end_tables['task']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 task')
        if 'search' not in end_tables or len(end_tables['search']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 search')
        if 'job' not in end_tables or len(end_tables['job']) != 2:
            logger.info('%r',end_tables)
            raise Exception('did not create 2 job')
        if end_tables['task'][0]['depends'] != '':
            logger.info('%r',end_tables['task'])
            raise Exception('task1 has a dependency')

        logger.warn('now testing SQL error handling')
        for i in range(3):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])

            try:
                yield self.db['queue_buffer_jobs_tasks'](gridspec, num)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            end_tables = yield self.get_tables(['task','job','search'])
            if any(end_tables.values()):
                logger.info('%r',end_tables)
                raise Exception('tables have changed')

    @unittest_reporter
    def test_200_queue_get_queueing_datasets(self):
        """Test queue_get_queueing_datasets"""
        gridspec = 'lksdf.grid1'
        now = dbmethods.nowstr()

        tables = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1, 'tasks_submitted':1,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'idle'},
            ],
        }

        # single dataset
        yield self.set_tables(tables)
        ret = yield self.db['queue_get_queueing_datasets'](gridspec)
        expected = {'d1':tables['dataset'][0]}
        self.assertEqual(ret, expected)
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # no dataset
        yield self.set_tables({'dataset':[],'search':[]})
        ret = yield self.db['queue_get_queueing_datasets'](gridspec)
        self.assertEqual(ret, {})
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # no gridspec
        yield self.set_tables(tables)
        ret = yield self.db['queue_get_queueing_datasets'](None)
        expected = {'d1':tables['dataset'][0]}
        self.assertEqual(ret, expected)
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # sql error
        yield self.set_tables(tables)
        self.set_failures([True])
        try:
            yield self.db['queue_get_queueing_datasets'](gridspec)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

    @unittest_reporter
    def test_210_queue_get_queueing_tasks(self):
        """Test queue_get_queueing_tasks"""
        gridspec = 'ggg.g1'
        now = dbmethods.nowstr()
        tables = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t2', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t3', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t4', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'job':[
                {'job_id':'j1', 'status':'processing', 'job_index':0,
                 'status_changed':now},
            ],
            'task':[
                {'task_id':'t1', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':'tr1'},
                {'task_id':'t2', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':'tr2'},
                {'task_id':'t3', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':'tr3'},
                {'task_id':'t4', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':'tr4'},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','dataset_id':'d1','depends':None,'requirements':None},
                {'task_rel_id':'tr2','dataset_id':'d1','depends':None,'requirements':None},
                {'task_rel_id':'tr3','dataset_id':'d1','depends':None,'requirements':None},
                {'task_rel_id':'tr4','dataset_id':'d1','depends':None,'requirements':None},
            ],
        }

        # single dataset
        dataset_prios = {'d1':1}
        yield self.set_tables(tables)

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,1)
        self.assertEqual(len(ret), 1)
        self.assertIn(list(ret)[0], [t['task_id'] for t in tables['task']])

        # no tasks
        yield self.set_tables({'search':[],'task':[]})

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,1)
        self.assertEqual(ret, {})

        # no tasks sql error
        yield self.set_tables({'search':[],'task':[]})
        self.set_failures([True])

        try:
            ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,1)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        end_tables = yield self.get_tables(tables.keys())
        if not cmp_dict(tables,end_tables):
            logger.info('%r\n%r',tables,end_tables)
            raise Exception('tables modified')

        # no dataset_prios
        yield self.set_tables(tables)
        self.set_failures(None)

        try:
            ret = yield self.db['queue_get_queueing_tasks'](None,1)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # several tasks in same dataset
        yield self.set_tables(tables)

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,1)
        self.assertEqual(len(ret), 1)
        self.assertIn(list(ret)[0], [t['task_id'] for t in tables['task']])

        # several tasks in diff dataset
        tables2 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':2,
                 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
                {'dataset_id':'d2', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':2,
                 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t2', 'job_id': 'j2', 'dataset_id': 'd2',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t3', 'job_id': 'j3', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t4', 'job_id': 'j4', 'dataset_id': 'd2',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'job':[
                {'job_id':'j1', 'status':'processing', 'job_index':0,
                 'status_changed':now},
                {'job_id':'j2', 'status':'processing', 'job_index':0,
                 'status_changed':now},
                {'job_id':'j3', 'status':'processing', 'job_index':1,
                 'status_changed':now},
                {'job_id':'j4', 'status':'processing', 'job_index':1,
                 'status_changed':now},
            ],
            'task':tables['task'],
            'task_rel':tables['task_rel'],
        }
        dataset_prios = {'d1':1.1,'d2':1}
        yield self.set_tables(tables2)

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,3)

        ret_should_be = {x['task_id']:dict(x) for x in tables2['search'] if x['task_id'] != 't4'}
        for k in ret_should_be:
            ret_should_be[k]['task_status'] = 'queued'
            ret_should_be[k]['debug'] = tables2['dataset'][0]['debug']
            ret_should_be[k]['reqs'] = None
            ret_should_be[k]['job'] = 0 if k in ('t1','t2') else 1
            ret_should_be[k]['jobs_submitted'] = tables2['dataset'][0]['jobs_submitted']
        self.assertEqual(ret.keys(), ret_should_be.keys())

        # priority weighted towards one dataset
        dataset_prios = {'d1':.2,'d2':.8}
        yield self.set_tables(tables2)

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,3)

        ret_should_be = {x['task_id']:dict(x) for x in tables2['search'] if x['task_id'] != 't3'}
        for k in ret_should_be:
            ret_should_be[k]['task_status'] = 'queued'
            ret_should_be[k]['debug'] = tables2['dataset'][0]['debug']
            ret_should_be[k]['reqs'] = ''
            ret_should_be[k]['job'] = 0 if k in ('t1','t2') else 1
            ret_should_be[k]['jobs_submitted'] = tables2['dataset'][0]['jobs_submitted']
        self.assertEqual(ret, ret_should_be)

        # testing dependencies
        tables3 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'complete'},
                {'task_id':'t2', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'processing'},
                {'task_id':'t3', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t4', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t5', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t6', 'job_id': 'j2', 'dataset_id': 'd2',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
                {'task_id':'t7', 'job_id': 'j3', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'job':[
                {'job_id':'j1', 'status':'processing', 'job_index':0,
                 'status_changed':now},
                {'job_id':'j2', 'status':'processing', 'job_index':0,
                 'status_changed':now},
                {'job_id':'j3', 'status':'processing', 'job_index':1,
                 'status_changed':now},
            ],
            'task':[
                {'task_id':'t1', 'status':'complete', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': '', 'requirements': '',
                 'task_rel_id':'tr1'},
                {'task_id':'t2', 'status':'processing', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': 't1', 'requirements': '',
                 'task_rel_id':'tr2'},
                {'task_id':'t3', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': 't1,t2', 'requirements': '',
                 'task_rel_id':'tr3'},
                {'task_id':'t4', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': 't25', 'requirements': '',
                 'task_rel_id':'tr4'},
                {'task_id':'t5', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': 't6', 'requirements': '',
                 'task_rel_id':'tr5'},
                {'task_id':'t7', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': 'unknown', 'requirements': '',
                 'task_rel_id':'tr1'},
            ],
            'task_rel':[
                {'task_rel_id':'tr1','depends':None,'requirements':None},
                {'task_rel_id':'tr2','depends':'tr1','requirements':None},
                {'task_rel_id':'tr3','depends':'tr2','requirements':None},
                {'task_rel_id':'tr4','depends':'tr25','requirements':None},
                {'task_rel_id':'tr5','depends':'tr6','requirements':None},
            ],
        }
        dataset_prios = {'d1':1}
        yield self.set_tables(tables3)

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,3)
        self.assertEqual(ret, {})

        # testing resources
        tables4 = {
            'dataset':[
                {'dataset_id':'d1', 'name':'test dataset',
                 'description':'a simple test', 'gridspec':gridspec,
                 'status':'processing', 'username':'user',
                 'institution':'inst', 'submit_host':'localhost',
                 'priority':0, 'jobs_submitted':1,
                 'tasks_submitted':4,
                 'start_date':now, 'end_date':'',
                 'temporary_storage':'', 'global_storage':'',
                 'groups_id':'', 'stat_keys':'[]',
                 'categoryvalue_ids':'', 'debug':True},
            ],
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': gridspec, 'name': '1', 'task_status': 'waiting'},
            ],
            'job':tables['job'],
            'task':[
                {'task_id':'t1', 'status':'waiting', 'prev_status':'idle',
                 'status_changed':now,
                 'submit_dir':'', 'grid_queue_id':'',
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'failures':0, 'evictions':0, 'depends': None, 'requirements': '',
                 'task_rel_id':'tr1'},
            ],
            'task_rel':[
                {'task_rel_id':'tr1', 'dataset_id':'d1', 'task_index':0,
                 'name':'nn', 'depends':None, 'requirements':'["cpu","gpu"]'},
            ],
        }
        dataset_prios = {'d1':1}
        yield self.set_tables(tables4)
        resources = {'cpu':200,'gpu':10}

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,3,
                                                        resources=resources)

        ret_should_be = {x['task_id']:dict(x) for x in tables4['search']}
        for k in ret_should_be:
            ret_should_be[k]['task_status'] = 'queued'
            ret_should_be[k]['debug'] = tables['dataset'][0]['debug']
            ret_should_be[k]['reqs'] = ["cpu","gpu"]
            ret_should_be[k]['job'] = tables['job'][0]['job_index']
            ret_should_be[k]['jobs_submitted'] = tables['dataset'][0]['jobs_submitted']
        self.assertEqual(ret, ret_should_be)

        # resources no match
        yield self.set_tables(tables4)
        resources = {'none':None}

        ret = yield self.db['queue_get_queueing_tasks'](dataset_prios,3,
                                                        resources=resources)
        self.assertEqual(ret, {})

    @unittest_reporter
    def test_300_queue_new_pilot_ids(self):
        """Test queue_new_pilot_ids"""
        ret = yield self.db['queue_new_pilot_ids'](1)
        self.assertEquals(len(ret), 1)
        
        ret2 = yield self.db['queue_new_pilot_ids'](1)
        self.assertEquals(len(ret2), 1)
        self.assertGreater(ret2, ret)

        for i in range(100,5):
            ret = yield self.db['queue_new_pilot_ids'](i)
            self.assertEquals(len(ret), i)

        try:
            yield self.db['queue_new_pilot_ids']('blah')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_310_queue_add_pilot(self):
        """Test queue_add_pilot"""
        submit_dir = os.path.join(self.test_dir,'submit')
        pilot = {'task_id':'pilot', 'name':'pilot', 'debug':False, 'reqs':{},
                 'submit_dir': submit_dir, 'grid_queue_id':'12345',
                 'pilot_ids': ['a']}

        yield self.set_tables({'pilot':[]})
        yield self.db['queue_add_pilot'](pilot)
        endtable = (yield self.get_tables(['pilot']))['pilot']
        if (len(endtable) != 1 or endtable[0]['submit_dir'] != submit_dir or
            endtable[0]['grid_queue_id'] != '12345.0'):
            logger.info('table: %r',endtable)
            raise Exception('bad table state')

        # try 3 at once
        pilot['num'] = 3
        pilot['pilot_ids'] = ['a', 'b', 'c']
        yield self.set_tables({'pilot':[]})
        yield self.db['queue_add_pilot'](pilot)
        endtable = (yield self.get_tables(['pilot']))['pilot']
        if (len(endtable) != 3 or
            endtable[0]['submit_dir'] != submit_dir or
            endtable[0]['grid_queue_id'] != '12345.0' or
            endtable[1]['submit_dir'] != submit_dir or
            endtable[1]['grid_queue_id'] != '12345.1' or
            endtable[2]['submit_dir'] != submit_dir or
            endtable[2]['grid_queue_id'] != '12345.2'):
            logger.info('table: %r',endtable)
            raise Exception('bad table state')

        yield self.set_tables({'pilot':[]})
        self.set_failures([True])
        try:
            yield self.db['queue_add_pilot'](pilot)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtable = (yield self.get_tables(['pilot']))['pilot']
        if endtable:
            raise Exception('tables modified')

    @unittest_reporter
    def test_320_queue_get_pilot(self):
        """Test queue_get_pilot"""
        submit_dir = os.path.join(self.test_dir,'submit')
        pilot = {'task_id':'pilot', 'name':'pilot', 'debug':False, 'reqs':{},
                 'submit_dir': submit_dir, 'grid_queue_id':'12345',
                 'pilot_ids': ['a']}

        yield self.set_tables({'pilot':[]})
        yield self.db['queue_add_pilot'](pilot)
        ret = yield self.db['queue_get_pilots']()
        self.assertEqual(len(ret), 1)
        self.assertEqual([x['pilot_id'] for x in ret], pilot['pilot_ids'])

        # try 3 pilots
        pilot['num'] = 3
        pilot['pilot_ids'] = ['a', 'b', 'c']
        yield self.set_tables({'pilot':[]})
        yield self.db['queue_add_pilot'](pilot)
        ret = yield self.db['queue_get_pilots']()
        self.assertEqual(len(ret), 3)
        self.assertEqual([x['pilot_id'] for x in ret], pilot['pilot_ids'])

        self.set_failures([True])
        try:
            yield self.db['queue_get_pilots'](pilot)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_330_queue_del_pilots(self):
        """Test queue_del_pilots"""
        submit_dir = os.path.join(self.test_dir,'submit')
        pilot = {'task_id':'pilot', 'name':'pilot', 'debug':False, 'reqs':{},
                 'submit_dir': submit_dir, 'grid_queue_id':'12345',
                 'pilot_ids': ['a']}

        yield self.set_tables({'pilot':[]})
        yield self.db['queue_add_pilot'](pilot)
        yield self.db['queue_del_pilots']('a')
        ret = yield self.get_tables(['pilot'])
        if ret['pilot']:
            raise Exception('did not delete pilot')

        # try deleting 2, leaving 1
        pilot['num'] = 3
        pilot['pilot_ids'] = ['a', 'b', 'c']
        yield self.set_tables({'pilot':[]})
        yield self.db['queue_add_pilot'](pilot)
        yield self.db['queue_del_pilots'](['a','b'])
        ret = yield self.get_tables(['pilot'])
        self.assertEqual([x['pilot_id'] for x in ret['pilot']], ['c'])

        # reset task
        tables = {'pilot':[
                    {'pilot_id':'a', 'grid_queue_id':'', 'submit_time':'',
                     'submit_dir':'', 'tasks': 't1'}
                  ],
                  'search':[
                    {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                     'gridspec': '', 'name': '1', 'task_status': 'processing'},
                  ],
                  'task':[
                    {'task_id':'t1', 'status':'processing', 'prev_status':'idle',
                     'status_changed':'',
                     'submit_dir':'', 'grid_queue_id':'',
                     'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                     'failures':0, 'evictions':0, 'depends': None, 'requirements': '',
                     'task_rel_id':'tr1'},
                  ],
                 }
        yield self.set_tables(tables)
        yield self.db['queue_del_pilots']('a')
        ret = yield self.get_tables(['pilot','search'])
        if ret['pilot']:
            raise Exception('did not delete pilot')
        self.assertEqual(ret['search'][0]['task_status'], 'reset')

        # query error
        for i in range(2):
            yield self.set_tables({'pilot':[]})
            yield self.db['queue_add_pilot'](pilot)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['queue_del_pilots']({'a','c'})
            except:
                pass
            else:
                raise Exception('did not raise Exception')

    @unittest_reporter
    def test_400_queue_get_cfg_for_task(self):
        """Test queue_get_cfg_for_task"""
        config_data = """
{"version":3,
 "parent_id":0,
 "tasks":[
    {"name":"task1",
     "trays":[
        {"name":"Corsika",
         "modules":[
            {"name":"generate_corsika",
             "class":"generators.CorsikaIC"
            }
        ]}
    ]}
]}
"""
        tables = {
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': 'grid', 'name': '1', 'task_status': 'waiting'},
            ],
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
            ],
        }

        yield self.set_tables(tables)
        ret = yield self.db['queue_get_cfg_for_task']('t1')
        self.assertEqual(ret, config_data)

        # sql error
        yield self.set_tables(tables)
        self.set_failures([True])

        try:
            yield self.db['queue_get_cfg_for_task']('t1')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # bad task
        self.set_failures(None)
        try:
            yield self.db['queue_get_cfg_for_task']('t32')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # bad task_id
        try:
            yield self.db['queue_get_cfg_for_task'](None)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_410_queue_get_cfg_for_dataset(self):
        """Test queue_get_cfg_for_dataset"""
        config_data = """
{"version":3,
 "parent_id":0,
 "tasks":[
    {"name":"task1",
     "trays":[
        {"name":"Corsika",
         "modules":[
            {"name":"generate_corsika",
             "class":"generators.CorsikaIC"
            }
        ]}
    ]}
]}
"""
        tables = {
            'config':[
                {'dataset_id':'d1','config_data':config_data,'difplus_data':''},
            ],
        }

        yield self.set_tables(tables)
        ret = yield self.db['queue_get_cfg_for_dataset']('d1')
        self.assertEqual(ret, config_data)

        # sql error
        yield self.set_tables(tables)
        self.set_failures([True])

        try:
            yield self.db['queue_get_cfg_for_dataset']('d1')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # bad dataset
        self.set_failures(None)
        try:
            yield self.db['queue_get_cfg_for_dataset']('d32')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # bad dataset_id
        try:
            yield self.db['queue_get_cfg_for_dataset'](None)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_500_queue_add_task_lookup(self):
        """Test queue_add_task_lookup"""
        r = {'cpu': 1, 'gpu': 0, 'memory': 1.0, 'disk': 10.0, 'time': 1.0}
        tasks = {'t1':r}
        yield self.set_tables({'task_lookup':[]})
        yield self.db['queue_add_task_lookup'](tasks)
        ret = (yield self.get_tables(['task_lookup']))['task_lookup']
        self.assertEqual(ret[0]['task_id'], 't1')
        for k in r:
            self.assertEqual(ret[0]['req_'+k], r[k])

        # replacement
        r[k] = 12345
        yield self.db['queue_add_task_lookup'](tasks)
        ret = (yield self.get_tables(['task_lookup']))['task_lookup']
        self.assertEqual(ret[0]['task_id'], 't1')
        for k in r:
            self.assertEqual(ret[0]['req_'+k], r[k])

        # query error
        self.set_failures([True])
        try:
            yield self.db['queue_add_task_lookup'](tasks)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_510_queue_get_task_lookup(self):
        """Test queue_get_task_lookup"""
        r = {'cpu': 1, 'gpu': 0, 'memory': 1.0, 'disk': 10.0, 'time': 1.0}
        tasks = {'t1':r}

        tables = {
            'search':[
                {'task_id':'t1', 'job_id': 'j1', 'dataset_id': 'd1',
                 'gridspec': 'grid', 'name': '1', 'task_status': 'queued'},
            ],'task_lookup':[],
        }
        yield self.set_tables(tables)
        yield self.db['queue_add_task_lookup'](tasks)

        ret = yield self.db['queue_get_task_lookup']()
        self.assertEqual(ret['t1'], r)
        ret = (yield self.get_tables(['task_lookup']))['task_lookup']
        if not ret:
            raise Exception('task_lookup deleted')

        # now insert a bad task
        tasks = {'t2':r}
        yield self.set_tables(tables)
        yield self.db['queue_add_task_lookup'](tasks)
        
        ret = yield self.db['queue_get_task_lookup']()
        self.assertEqual(ret, {})
        ret = (yield self.get_tables(['task_lookup']))['task_lookup']
        if ret:
            raise Exception('task_lookup not deleted')

        # query error
        for i in range(3):
            yield self.set_tables(tables)
            yield self.db['queue_add_task_lookup'](tasks)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['queue_get_task_lookup'](tasks)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            self.set_failures(False)
            ret = (yield self.get_tables(['task_lookup']))['task_lookup']
            if not ret:
                raise Exception('task_lookup deleted')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_queue_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_queue_test))
    return suite
