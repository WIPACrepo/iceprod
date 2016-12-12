"""
Test script for dbmethods.rpc
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
import StringIO
from itertools import izip
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable
import unittest

import tornado.escape
from iceprod.core import functions
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods, GlobalID
from .dbmethods_test import dbmethods_base


now = dbmethods.nowstr()
gridspec = 'nsd89n3'
task_id = 'asdf'
def get_tables(test_dir):
    tables = {
        'task':[
                {'task_id':task_id, 'status':'queued', 'prev_status':'waiting',
                 'status_changed':now,
                 'walltime': 0., 'walltime_err': 0., 'walltime_err_n': 0,
                 'submit_dir':test_dir, 'grid_queue_id':'lkn',
                 'failures':0, 'evictions':0, 'task_rel_id':'a'},
                ],
        'task_rel':[
                    {'task_rel_id': 'a', 'dataset_id': 'd1', 'task_index': 0,
                     'name': '0', 'depends': '', 'requirements': ''}
                   ],
        'task_lookup':[
                       {'task_id':task_id, 'req_cpu':1, 'req_gpu':0,
                        'req_memory':1.0, 'req_disk':1.0}
                      ],
        'search':[
                  {'task_id':task_id, 'job_id':'bfsd', 'dataset_id':'d1',
                   'gridspec':gridspec, 'name':'0', 'task_status':'queued'},
                 ],
        'job':[
               {'job_id':'bfsd', 'status':'processing', 'job_index':0,
                'status_changed':now},
              ],
        'config': [{'dataset_id':'d1', 'config_data': '{"name":"value"}', 'difplus_data':'' }],
        'task_stat': [{'task_stat_id': 0, 'task_id': task_id}],
        'dataset': [{'dataset_id':'d1', 'jobs_submitted': 2,
                     'tasks_submitted': 2, 'debug': True}],
    }
    return tables

class dbmethods_rpc_test(dbmethods_base):
    @unittest_reporter
    def test_000_rpc_new_task(self):
        """Test rpc_new_task"""
        tables = get_tables(self.test_dir)
        yield self.set_tables(tables)

        # everything working
        ret = yield self.db['rpc_new_task'](gridspec=gridspec,
                platform='platform', hostname=self.hostname, ifaces=None)

        ret_should_be = {'name':'value','options':{'task_id':task_id,
                                                   'task':'0', 'job': 0,
                                                   'jobs_submitted': 2,
                                                   'dataset_id': 'd1',
                                                   'debug':True,
                                                   'resources':{'cpu':1,'gpu':0,'memory':1.0,'disk':1.0}}}
        self.assertEqual(ret, ret_should_be)

        # no queued jobs
        ret = yield self.db['rpc_new_task'](gridspec=gridspec,
                platform='platform', hostname=self.hostname, ifaces=None)
        if ret:
            raise Exception('returned task when there should be none')

        # db errors
        for i in range(5):
            yield self.set_tables(tables)
            starttables = yield self.get_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['rpc_new_task'](gridspec=gridspec,
                        platform='platform', hostname=self.hostname, ifaces=None)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(tables)
            self.assertEqual(endtables, starttables)

    @unittest_reporter
    def test_010_rpc_finish_task(self):
        """Test rpc_finish_task"""
        self.cfg['queue'] = {'site_temp': self.test_dir}

        tables = get_tables(self.test_dir)
        yield self.set_tables(tables)

        # everything working
        stats = {'name1':123123,'name2':968343}
        
        yield self.db['rpc_finish_task'](task_id, stats)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['search'][0]['task_status'], 'complete')

        # db error
        for i in range(6):
            yield self.set_tables(tables)
            starttables = yield self.get_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['rpc_finish_task'](task_id, stats)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            if i < 1:
                endtables = yield self.get_tables(tables)
                self.assertEqual(endtables, starttables)

        # update stats
        stats = {'name3': 54322}
        yield self.db['rpc_finish_task'](task_id, stats)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['task_stat'][-1]['stat'], json_encode(stats))

    @unittest_reporter
    def test_020_rpc_task_error(self):
        """Test rpc_task_error"""
        self.cfg['queue'] = {'max_resets': 10}

        tables = get_tables(self.test_dir)
        tables['dataset'][0]['debug'] = False
        yield self.set_tables(tables)

        # everything working
        yield self.db['rpc_task_error'](task_id)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['search'][0]['task_status'], 'reset')

        # error info
        yield self.db['rpc_task_error'](task_id, error_info={'a':1})
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['search'][0]['task_status'], 'reset')
        stat = json_decode(endtables['task_stat'][-1]['stat'])
        if 'error' not in stat:
            raise Exception('bad stat name')
        self.assertEqual(stat['a'], 1)

        # update requirements
        tables['task_rel'][0]['requirements'] = '{"cpu":1}'
        reqs = {'cpu': 1, 'memory':3.45644, 'test': 'blah'}

        yield self.set_tables(tables)
        yield self.db['rpc_task_error'](task_id, error_info={'resources':reqs})
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['search'][0]['task_status'], 'reset')
        stat = json_decode(endtables['task_stat'][-1]['stat'])
        self.assertIn('resources', stat)
        self.assertIsNotNone(endtables['task'][0]['requirements'])
        end_taskreq = json_decode(endtables['task'][0]['requirements'])
        self.assertEqual(end_taskreq.keys(), ['cpu','memory'])

        # failure
        tables['task'][0]['failures'] = 9
        yield self.set_tables(tables)
        yield self.db['rpc_task_error'](task_id)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['search'][0]['task_status'], 'failed')

        # suspend
        tables['task'][0]['failures'] = 0
        tables['dataset'][0]['debug'] = True
        yield self.set_tables(tables)
        yield self.db['rpc_task_error'](task_id)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['search'][0]['task_status'], 'suspended')
        
        for i in range(5):
            yield self.set_tables(tables)
            starttables = yield self.get_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['rpc_task_error'](task_id, error_info={'resources':reqs})
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(tables)
            self.assertEqual(endtables, starttables)


    @unittest_reporter
    def test_030_rpc_upload_logfile(self):
        """Test rpc_upload_logfile"""
        tables = {'task_log':[]}
        yield self.set_tables(tables)

        # everything working
        name = 'logfile'
        data = 'thelogfiledata'
        yield self.db['rpc_upload_logfile'](task_id, name, data)
        endtables = yield self.get_tables(['task_log'])
        self.assertEqual(endtables['task_log'][0]['name'], name)
        self.assertEqual(endtables['task_log'][0]['data'], data)

        # update
        data = 'secondary data'
        yield self.db['rpc_upload_logfile'](task_id, name, data)
        endtables = yield self.get_tables(['task_log'])
        self.assertEqual(endtables['task_log'][0]['data'], data)

        for i in range(2):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['rpc_upload_logfile'](task_id, name, data)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(tables)
            self.assertEqual(endtables, tables)

    @unittest_reporter
    def test_040_rpc_stillrunning(self):
        """Test rpc_stillrunning"""
        tables = get_tables(self.test_dir)

        # queued / processing
        for status in ('queued','processing'):
            tables['task'][0]['status'] = status
            yield self.set_tables(tables)
            ret = yield self.db['rpc_stillrunning'](task_id)
            self.assertTrue(ret)

        # non-processing states
        for status in ('reset','resume','suspended','failed','waiting','idle','complete'):
            tables['task'][0]['status'] = status
            yield self.set_tables(tables)
            ret = yield self.db['rpc_stillrunning'](task_id)
            self.assertFalse(ret)

        # sql error
        self.set_failures([True])
        try:
            yield self.db['rpc_stillrunning'](task_id)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_050_rpc_update_pilot(self):
        """Test rpc_update_pilot"""
        pilot_id = 'p1'
        tables = {'pilot':[{'pilot_id':pilot_id,'grid_queue_id':'g1',
                           'submit_time':dbmethods.nowstr(),'submit_dir':'',
                           'tasks':''}]
        }
        yield self.set_tables(tables)

        tasks = 't1,t2'
        yield self.db['rpc_update_pilot'](pilot_id, tasks=tasks)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['pilot'][0]['tasks'], tasks)

        # no update
        yield self.set_tables(tables)
        try:
            yield self.db['rpc_update_pilot'](pilot_id)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(tables, endtables)

        # sql error
        self.set_failures([True])
        try:
            yield self.db['rpc_update_pilot'](pilot_id, tasks=tasks)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(tables, endtables)

    @unittest_reporter
    def test_100_rpc_submit_dataset(self):
        """Test rpc_submit_dataset"""
        # try giving dict
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)

        tables = {'dataset':[], 'config':[], 'task_rel': []}
        yield self.set_tables(tables)
        yield self.db['rpc_submit_dataset'](config_dict)

        endtables = yield self.get_tables(tables)
        dataset_id = endtables['dataset'][0]['dataset_id']
        config_job['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
        config_json = serialization.serialize_json.dumps(config_job)
        self.assertEqual(endtables['config'][0]['config_data'], config_json)
        self.assertEqual(len(endtables['task_rel']), 1, "task_rel table")

        # try giving job
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)

        yield self.set_tables(tables)
        yield self.db['rpc_submit_dataset'](config_job)

        endtables = yield self.get_tables(tables)
        dataset_id = endtables['dataset'][0]['dataset_id']
        config_job['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
        config_json = serialization.serialize_json.dumps(config_job)
        self.assertEqual(endtables['config'][0]['config_data'], config_json)
        self.assertEqual(len(endtables['task_rel']), 1, "task_rel table")

        # try giving json
        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        yield self.set_tables(tables)
        yield self.db['rpc_submit_dataset'](config_json)

        endtables = yield self.get_tables(tables)
        dataset_id = endtables['dataset'][0]['dataset_id']
        config_job['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
        job = serialization.serialize_json.loads(endtables['config'][0]['config_data'])
        self.assertEqual(job, config_job)
        self.assertEqual(len(endtables['task_rel']), 1, "task_rel table")

        # try giving bad json
        yield self.set_tables(tables)
        try:
            yield self.db['rpc_submit_dataset']("<asf>")
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(tables, endtables)

        # number dependency
        config_dict = {'tasks':[
            {'trays':[{'modules':[]}]},
            {'depends':['0'],'trays':[{'modules':[]}]}
        ],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)

        yield self.set_tables(tables)
        yield self.db['rpc_submit_dataset'](config_dict)

        endtables = yield self.get_tables(tables)
        dataset_id = endtables['dataset'][0]['dataset_id']
        config_job['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
        job = serialization.serialize_json.loads(endtables['config'][0]['config_data'])
        self.assertEqual(job, config_job)
        self.assertEqual(len(endtables['task_rel']), 2, "task_rel table")
        self.assertEqual(endtables['task_rel'][1]['depends'],
                         endtables['task_rel'][0]['task_rel_id'],
                         "missing dependency")

        # named dependency
        config_dict = {'tasks':[
            {'name':'first','trays':[{'modules':[]}]},
            {'name':'second','depends':['first'],'trays':[{'modules':[]}]}
        ],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)

        yield self.set_tables(tables)
        yield self.db['rpc_submit_dataset'](config_dict)

        endtables = yield self.get_tables(tables)
        dataset_id = endtables['dataset'][0]['dataset_id']
        config_job['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
        job = serialization.serialize_json.loads(endtables['config'][0]['config_data'])
        self.assertEqual(job, config_job)
        self.assertEqual(len(endtables['task_rel']), 2, "task_rel table")
        self.assertEqual(endtables['task_rel'][1]['depends'],
                         endtables['task_rel'][0]['task_rel_id'],
                         "missing dependency")

        # missing named dependency
        config_dict2 = {'tasks':[
            {'name':'first','trays':[{'modules':[]}]},
            {'name':'second','depends':['third'],'trays':[{'modules':[]}]}
        ],'steering':{}}

        yield self.set_tables(tables)
        try:
            yield self.db['rpc_submit_dataset'](config_dict2)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(tables, endtables)

        # dataset dependency
        config_dict2 = {'tasks':[
            {'depends':['d1.0'],'trays':[{'modules':[]}]},
        ],'steering':{}}
        config_job2 = serialization.dict_to_dataclasses(config_dict2)

        tables2 = {'dataset':[], 'config':[],
            'task_rel': [
                {'task_rel_id':'tr1', 'dataset_id':'d1', 'task_index': 0,
                 'name':'0', 'depends':'', 'requirements':''},
            ],
        }
        yield self.set_tables(tables2)
        yield self.db['rpc_submit_dataset'](config_dict2)

        endtables = yield self.get_tables(tables)
        dataset_id = endtables['dataset'][0]['dataset_id']
        config_job2['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
        job2 = serialization.serialize_json.loads(endtables['config'][0]['config_data'])
        self.assertEqual(job2, config_job2)
        self.assertEqual(len(endtables['task_rel']), 2, "task_rel table")
        self.assertEqual(endtables['task_rel'][1]['depends'],
                         endtables['task_rel'][0]['task_rel_id'],
                         "missing dependency")

        # dataset dependency missing
        yield self.set_tables(tables)
        try:
            yield self.db['rpc_submit_dataset'](config_dict2)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(tables, endtables)

        # named dataset dependency
        config_dict2 = {'tasks':[
            {'depends':['d1.first'],'trays':[{'modules':[]}]},
        ],'steering':{}}
        config_job2 = serialization.dict_to_dataclasses(config_dict2)

        tables2 = {'dataset':[], 'config':[],
            'task_rel': [
                {'task_rel_id':'tr1', 'dataset_id':'d1', 'task_index': 0,
                 'name':'first', 'depends':'', 'requirements':''},
            ],
        }
        yield self.set_tables(tables2)
        yield self.db['rpc_submit_dataset'](config_dict2)

        endtables = yield self.get_tables(tables)
        dataset_id = endtables['dataset'][0]['dataset_id']
        config_job2['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
        job2 = serialization.serialize_json.loads(endtables['config'][0]['config_data'])
        self.assertEqual(job2, config_job2)
        self.assertEqual(len(endtables['task_rel']), 2, "task_rel table")
        self.assertEqual(endtables['task_rel'][1]['depends'],
                         endtables['task_rel'][0]['task_rel_id'],
                         "missing dependency")

        # test sql errors
        for i in range(2):
            yield self.set_tables(tables2)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['rpc_submit_dataset'](config_dict2)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(tables2)
            self.assertEqual(endtables, tables2)

    @unittest_reporter
    def test_110_rpc_update_dataset_config(self):
        tables = {'config':[{'dataset_id':'d1','config_data':'blah',
                             'difplus_data':''}]
        }

        config_dict = {'tasks':[{'trays':[{'modules':[]}]}],'steering':{}}
        config_job = serialization.dict_to_dataclasses(config_dict)
        config_json = serialization.serialize_json.dumps(config_job)

        # give dict
        yield self.set_tables(tables)
        yield self.db['rpc_update_dataset_config']('d1', config_dict)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['config'][0]['config_data'], config_json)

        # give job
        yield self.set_tables(tables)
        yield self.db['rpc_update_dataset_config']('d1', config_job)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['config'][0]['config_data'], config_json)

        # give json
        yield self.set_tables(tables)
        yield self.db['rpc_update_dataset_config']('d1', config_json)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['config'][0]['config_data'], config_json)

        # query error
        yield self.set_tables(tables)
        self.set_failures([True])
        try:
            yield self.db['rpc_update_dataset_config']('d1', config_json)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables, tables)

    @unittest_reporter
    def test_200_rpc_get_groups(self):
        tables = {
            'groups':[
                {'groups_id':'a','name':'/foo','description':'bar','priority':1.3},
                {'groups_id':'b','name':'/foo/bar','description':'bar2','priority':0.3}
            ],
        }

        # correct
        yield self.set_tables(tables)
        ret = yield self.db['rpc_get_groups']()

        ret_should_be = {row['groups_id']:row for row in tables['groups']}
        self.assertEqual(ret, ret_should_be)
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables, tables)

        # query failure
        self.set_failures([True])
        try:
            yield self.db['rpc_get_groups']()
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables, tables)
        
    @unittest_reporter
    def test_210_rpc_set_groups(self):
        user = 'user'
        tables = {
            'groups':[
                {'groups_id':'a','name':'/foo','description':'bar','priority':1.3},
                {'groups_id':'b','name':'/foo/bar','description':'bar2','priority':0.3}
            ],
        }

        # delete everything
        newgroups = {}
        yield self.set_tables(tables)
        yield self.db['rpc_set_groups'](user=user, groups=newgroups)

        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables, {'groups':[]})

        # update one
        newgroups = {row['groups_id']:row.copy() for row in tables['groups']}
        newgroups['a']['priority'] = 4.5
        yield self.set_tables(tables)
        yield self.db['rpc_set_groups'](user=user, groups=newgroups)

        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables, {'groups':[newgroups[k] for k in newgroups]})

        # delete and update
        newgroups = {row['groups_id']:row.copy() for row in tables['groups']}
        newgroups['a']['priority'] = 4.5
        newgroups['a']['name'] = '/fee'
        del newgroups['b']
        yield self.set_tables(tables)
        yield self.db['rpc_set_groups'](user=user, groups=newgroups)

        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables, {'groups':[newgroups[k] for k in newgroups]})

        # add one
        newgroups = {row['groups_id']:row.copy() for row in tables['groups']}
        newgroups['-1'] = {'name':'/baz','description':'foobar','priority':3.2}
        yield self.set_tables(tables)
        yield self.db['rpc_set_groups'](user=user, groups=newgroups)

        endtables = yield self.get_tables(tables)
        self.assertEqual(endtables['groups'][-1]['name'], '/baz')
        newgroups['-1']['groups_id'] = endtables['groups'][-1]['groups_id']
        self.assertEqual(endtables, {'groups':[newgroups[k] for k in newgroups]})

        # test sql errors
        for i in range(2):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['rpc_set_groups'](user=user, groups=newgroups)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(tables)
            self.assertEqual(endtables, tables)

    @unittest_reporter
    def test_220_rpc_get_user_roles(self):
        tables = {
            'roles':[
                {'roles_id':'a','role_name':'foo','groups_prefix':'/foo'},
                {'roles_id':'b','role_name':'bar','groups_prefix':'/foo/bar'}
            ],
            'user':[
                {'user_id':'a','username':'bob','roles':'a,b'},
                {'user_id':'b','username':'john','roles':''},
            ],
        }

        # correct
        yield self.set_tables(tables)
        starttables = yield self.get_tables(tables)
        ret = yield self.db['rpc_get_user_roles']('bob')

        ret_should_be = {row['roles_id']:row for row in tables['roles']}
        self.assertEqual(ret, ret_should_be)
        endtables = yield self.get_tables(tables)
        self.assertTrue(cmp_dict(tables, endtables), 'tables are modified')

        # try for no roles
        ret = yield self.db['rpc_get_user_roles']('john')
        self.assertEqual(ret, {})
        endtables = yield self.get_tables(tables)
        self.assertEqual(starttables, endtables)
        
        # try for invalid user
        try:
            yield self.db['rpc_get_user_roles']('foo')
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(starttables, endtables)
        
        # try for bad arguments
        try:
            yield self.db['rpc_get_user_roles'](None)
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(starttables, endtables)

        # test sql errors
        for i in range(2):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['rpc_get_user_roles']('bob')
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(tables)
            self.assertEqual(starttables, endtables)

    @unittest_reporter
    def test_230_rpc_set_user_roles(self):
        tables = {
            'roles':[
                {'roles_id':'a','role_name':'foo','groups_prefix':'/foo'},
                {'roles_id':'b','role_name':'bar','groups_prefix':'/foo/bar'}
            ],
            'user':[
                {'user_id':'a','username':'bob','roles':'a,b'},
                {'user_id':'b','username':'john','roles':''},
            ],
        }

        # correct
        yield self.set_tables(tables)
        starttables = yield self.get_tables(tables)
        yield self.db['rpc_set_user_roles'](user='c', username='bob', roles=['a'])

        endtables = yield self.get_tables(['user'])
        ret_should_be = {'user':[row.copy() for row in starttables['user']]}
        ret_should_be['user'][0]['roles'] = 'a'
        self.assertEqual(endtables, ret_should_be)

        # try for no groups
        yield self.db['rpc_set_user_roles'](user='c', username='bob', roles=[])
        
        endtables = yield self.get_tables(['user'])
        ret_should_be = {'user':[row.copy() for row in starttables['user']]}
        ret_should_be['user'][0]['roles'] = ''
        self.assertEqual(endtables, ret_should_be)
        
        # try for several groups
        yield self.db['rpc_set_user_roles'](user='c', username='bob', roles=['a','b'])
        endtables = yield self.get_tables(tables)
        self.assertEqual(starttables, endtables)
        
        # test sql errors
        self.set_failures([True])
        try:
            yield self.db['rpc_set_user_roles'](user='c', username='bob', roles=['a','b'])
        except:
            pass
        else:
            raise Exception('did not raise Exception')
        endtables = yield self.get_tables(tables)
        self.assertEqual(starttables, endtables)

    @unittest_reporter
    def test_300_rpc_queue_master(self):
        """Test rpc_queue_master"""
        # single dataset
        tables = get_tables(self.test_dir)
        yield self.set_tables(tables)
        ret = yield self.db['rpc_queue_master']()
        self.assertEqual(ret, {})

        # no tasks
        tables2 = tables.copy()
        tables2['task'] = []
        yield self.set_tables(tables2)
        ret = yield self.db['rpc_queue_master']()
        self.assertEqual(ret, {})

        # no tasks sql error
        yield self.set_tables(tables)
        self.set_failures([True])
        try:
            yield self.db['rpc_queue_master']()
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_400_rpc_public_get_graphs(self):
        now = datetime.utcnow()
        tables = {'graph':
            [{'graph_id':'gid0', 'name':'name1', 'value':'{"t":0}',
              'timestamp':dbmethods.datetime2str(now-timedelta(minutes=3))},
             {'graph_id':'gid1','name':'name1', 'value':'{"t":1}',
              'timestamp':dbmethods.datetime2str(now-timedelta(minutes=2))},
             {'graph_id':'gid2', 'name':'name1', 'value':'{"t":0}',
              'timestamp':dbmethods.datetime2str(now-timedelta(minutes=1))},
            ],
        }
        
        yield self.set_tables(tables)
        ret = yield self.db['rpc_public_get_graphs'](5)

        ret_should_be = []
        for row in tables['graph']:
            row = dict(row)
            del row['graph_id']
            row['value'] = json_decode(row['value'])
            ret_should_be.append(row)
        self.assertEqual(ret, ret_should_be)

        ret = yield self.db['rpc_public_get_graphs'](2)

        ret_should_be = []
        for row in tables['graph'][-1:]:
            row = dict(row)
            del row['graph_id']
            row['value'] = json_decode(row['value'])
            ret_should_be.append(row)
        self.assertEqual(ret, ret_should_be)

        self.set_failures([True])
        try:
            ret = yield self.db['rpc_public_get_graphs'](5)
        except:
            pass
        else:
            raise Exception('did not raise Exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_rpc_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_rpc_test))
    return suite
