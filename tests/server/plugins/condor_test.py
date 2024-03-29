"""
Test script for condor plugin
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('plugins_condor_test')

import os
import sys
import time
import random
import getpass
from datetime import datetime,timedelta
from contextlib import contextmanager
import shutil
import subprocess
from multiprocessing import Queue,Pipe

try:
    import cPickle as pickle
except:
    import pickle

import asyncio
import unittest

from unittest.mock import patch,MagicMock
from rest_tools.client import RestClient

import iceprod.server
from iceprod.server import module
from iceprod.server.plugins.condor import condor
from iceprod.core import dataclasses

from tests.server import grid_test

class condor_test(grid_test.grid_test):
    @unittest_reporter
    async def test_100_generate_submit_file(self):
        """Test generate_submit_file"""
        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        credentials_dir = os.path.join(self.test_dir,'credentials_dir')
        os.mkdir(submit_dir)
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        'credentials_dir':credentials_dir,
                        name:{'batchopts':{},
                              }},
               'download':{'http_username':None,'http_password':None},
               'db':{'address':None,'ssl':False},
               'rest_api':{'url':'foo'},
               }

        # init
        client = MagicMock(spec=RestClient)
        g = condor(gridspec, cfg['queue'][name], cfg, self.services,
                 self.executor, module.FakeStatsClient(),
                 client, client)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        g.tasks_queued = 0

        task = {'task_id':'thetaskid', 'submit_dir':submit_dir}
        await asyncio.ensure_future(g.generate_submit_file(task))
        if not os.path.isfile(os.path.join(submit_dir,'condor.submit')):
            raise Exception('submit file not written')
        for l in open(os.path.join(submit_dir,'condor.submit')):
            if l.startswith('arguments'):
                logger.info('args: %s',l)
                if '--passkey' in l:
                    raise Exception('passkey present when not supposed to be')
                if '--url' not in l:
                    raise Exception('download address not in args')
        os.unlink(os.path.join(submit_dir,'condor.submit'))

        # add passkey
        task = {'task_id':'thetaskid','submit_dir':submit_dir}
        passkey = 'aklsdfj'
        await asyncio.ensure_future(g.generate_submit_file(task,passkey=passkey))
        if not os.path.isfile(os.path.join(submit_dir,'condor.submit')):
            raise Exception('submit file not written')
        for l in open(os.path.join(submit_dir,'condor.submit')):
            if l.startswith('arguments'):
                if '--passkey' not in l:
                    raise Exception('passkey missing')
        os.unlink(os.path.join(submit_dir,'condor.submit'))

        # add batch opt
        cfg = dataclasses.Job()
        cfg['steering'] = dataclasses.Steering()
        cfg['steering']['batchsys'] = dataclasses.Batchsys()
        cfg['steering']['batchsys']['condor'] = {'+GPU_JOB':'true',
                'Requirements':'Target.Has_GPU == True'}
        await asyncio.ensure_future(g.generate_submit_file(task,cfg=cfg))
        if not os.path.isfile(os.path.join(submit_dir,'condor.submit')):
            raise Exception('submit file not written')
        gpu_job = False
        has_gpu = False
        for l in open(os.path.join(submit_dir,'condor.submit')):
            if (l.startswith('+GPU_JOB') and
                l.split('=')[-1].strip() == 'true'):
                gpu_job = True
            if (l.startswith('requirements') and
                'Target.Has_GPU == True' in l):
                has_gpu = True
        if not gpu_job:
            raise Exception('steering +GPU_JOB batchopt failed')
        if not has_gpu:
            raise Exception('steering requirements failed')
        os.unlink(os.path.join(submit_dir,'condor.submit'))

        # add batch opt
        cfg = dataclasses.Job()
        cfg['tasks'].append(dataclasses.Task())
        cfg['tasks'][0]['batchsys'] = dataclasses.Batchsys()
        cfg['tasks'][0]['batchsys']['condor'] = {'+GPU_JOB':'true',
                'Requirements':'Target.Has_GPU == True'}
        await asyncio.ensure_future(g.generate_submit_file(task,cfg=cfg))
        if not os.path.isfile(os.path.join(submit_dir,'condor.submit')):
            raise Exception('submit file not written')
        gpu_job = False
        has_gpu = False
        for l in open(os.path.join(submit_dir,'condor.submit')):
            if (l.startswith('+GPU_JOB') and
                l.split('=')[-1].strip() == 'true'):
                gpu_job = True
            if (l.startswith('requirements') and
                'Target.Has_GPU == True' in l):
                has_gpu = True
        if not gpu_job:
            raise Exception('task +GPU_JOB batchopt failed')
        if not has_gpu:
            raise Exception('task requirements failed')
        os.unlink(os.path.join(submit_dir,'condor.submit'))

    @patch('subprocess.check_output')
    @unittest_reporter
    async def test_101_submit(self, check_output):
        def caller(*args,**kwargs):
            caller.called = True
            caller.args = args
            caller.kwargs = kwargs
            if isinstance(caller.ret,Exception):
                raise caller.ret
            return caller.ret
        check_output.side_effect = caller

        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        credentials_dir = os.path.join(self.test_dir,'credentials_dir')
        os.mkdir(submit_dir)
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        'credentials_dir':credentials_dir,
                        name:{'batchopts':{},
                              }},
               'download':{'http_username':None,'http_password':None},
               'db':{'address':None,'ssl':False}}

        # init
        client = MagicMock(spec=RestClient)
        g = condor(gridspec, cfg['queue'][name], cfg, self.services,
                 self.executor, module.FakeStatsClient(),
                 client, client)
        if not g:
            raise Exception('init did not return grid object')

        # call normally
        caller.called = False
        caller.ret = ''
        g.tasks_queued = 0

        task = {'task_id':'thetaskid','submit_dir':submit_dir}
        await asyncio.ensure_future(g.submit(task))
        if not caller.called:
            raise Exception('subprocess.call not called')
        if caller.args[0][0] != 'condor_submit':
            logger.info('args: %r',caller.args)
            raise Exception('does not start with condor_submit')
        if 'cwd' not in caller.kwargs or caller.kwargs['cwd'] != submit_dir:
            raise Exception('did not change to submit dir')

        # call failed
        caller.called = False
        caller.ret = Exception('bad call')
        g.tasks_queued = 0

        task = {'task_id':'thetaskid','submit_dir':submit_dir}
        with self.assertRaises(Exception):
            await asyncio.ensure_future(g.submit(task))

    @patch('subprocess.check_output')
    @unittest_reporter
    async def test_102_get_grid_status(self, check_output):
        def caller(*args,**kwargs):
            caller.called = True
            caller.args = args
            caller.kwargs = kwargs
            if isinstance(caller.ret,Exception):
                raise caller.ret
            return caller.ret
        check_output.side_effect = caller

        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        credentials_dir = os.path.join(self.test_dir,'credentials_dir')
        os.mkdir(submit_dir)
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        'credentials_dir':credentials_dir,
                        name:{'batchopts':{},
                              'monitor_address':None,
                              }},
               'download':{'http_username':None,'http_password':None},
               'db':{'address':None,'ssl':False}}

        # init
        client = MagicMock(spec=RestClient)
        g = condor(gridspec, cfg['queue'][name], cfg, self.services,
                 self.executor, module.FakeStatsClient(),
                 client, client)
        if not g:
            raise Exception('init did not return grid object')

        # call empty queue
        caller.called = False
        caller.ret = ''

        ret = await asyncio.ensure_future(g.get_grid_status())
        if not caller.called:
            raise Exception('subprocess.call not called')
        if caller.args[0][0] != 'condor_q':
            logger.info('args: %r',caller.args)
            raise Exception('does not start with condor_q')
        if ret != {}:
            raise Exception('did not return empty dict')

        # call with queued job
        caller.called = False
        caller.ret = '1234.0 1 '+os.path.join(submit_dir,'loader.sh')

        ret = await asyncio.ensure_future(g.get_grid_status())
        if not caller.called:
            raise Exception('subprocess.call not called')
        if caller.args[0][0] != 'condor_q':
            logger.info('args: %r',caller.args)
            raise Exception('does not start with condor_q')
        if ret != {'1234.0':{'status':'queued','submit_dir':submit_dir}}:
            logger.info('ret: %r',ret)
            raise Exception('did not return queued job')

        # call with processing job
        caller.called = False
        caller.ret = '1234.0 2 '+os.path.join(submit_dir,'loader.sh')

        ret = await asyncio.ensure_future(g.get_grid_status())
        if not caller.called:
            raise Exception('subprocess.call not called')
        if caller.args[0][0] != 'condor_q':
            logger.info('args: %r',caller.args)
            raise Exception('does not start with condor_q')
        if ret != {'1234.0':{'status':'processing','submit_dir':submit_dir}}:
            logger.info('ret: %r',ret)
            raise Exception('did not return processing job')

        # call with completed job
        caller.called = False
        caller.ret = '1234.0 4 '+os.path.join(submit_dir,'loader.sh')

        ret = await asyncio.ensure_future(g.get_grid_status())
        if not caller.called:
            raise Exception('subprocess.call not called')
        if caller.args[0][0] != 'condor_q':
            logger.info('args: %r',caller.args)
            raise Exception('does not start with condor_q')
        if ret != {'1234.0':{'status':'completed','submit_dir':submit_dir}}:
            logger.info('ret: %r',ret)
            raise Exception('did not return completed job')

        # call with error job
        for s in ('3','5','6'):
            caller.called = False
            caller.ret = '1234.0 '+s+' '+os.path.join(submit_dir,'loader.sh')

            ret = await asyncio.ensure_future(g.get_grid_status())
            if not caller.called:
                raise Exception('subprocess.call not called')
            if caller.args[0][0] != 'condor_q':
                logger.info('args: %r',caller.args)
                raise Exception('does not start with condor_q')
            if ret != {'1234.0':{'status':'error','submit_dir':submit_dir}}:
                logger.info('ret: %r',ret)
                raise Exception('did not return error job')

        # call with unknown job
        caller.called = False
        caller.ret = '1234.0 blah '+os.path.join(submit_dir,'loader.sh')

        ret = await asyncio.ensure_future(g.get_grid_status())
        if not caller.called:
            raise Exception('subprocess.call not called')
        if caller.args[0][0] != 'condor_q':
            logger.info('args: %r',caller.args)
            raise Exception('does not start with condor_q')
        if ret != {'1234.0':{'status':'unknown','submit_dir':submit_dir}}:
            logger.info('ret: %r',ret)
            raise Exception('did not return unknown job')

        # call failed
        caller.called = False
        caller.ret = Exception('bad call')
        with self.assertRaises(Exception):
            await asyncio.ensure_future(g.get_grid_status())

        # call with junk output
        caller.called = False
        caller.ret = 'blah\nfoo bar'
        with self.assertRaises(Exception):
            await asyncio.ensure_future(g.get_grid_status())

    @patch('subprocess.check_call')
    @unittest_reporter
    async def test_103_remove(self, check_call):
        def caller(*args,**kwargs):
            caller.called = True
            caller.args = args
            caller.kwargs = kwargs
            if isinstance(caller.ret,Exception):
                raise caller.ret
            return caller.ret
        check_call.side_effect = caller

        site = 'thesite'
        name = 'grid1'
        gridspec = site+'.'+name
        submit_dir = os.path.join(self.test_dir,'submit_dir')
        credentials_dir = os.path.join(self.test_dir,'credentials_dir')
        os.mkdir(submit_dir)
        cfg = {'site_id':site,
               'queue':{'max_resets':5,
                        'submit_dir':submit_dir,
                        'credentials_dir':credentials_dir,
                        name:{'batchopts':{},
                              'monitor_address':None,
                              }},
               'download':{'http_username':None,'http_password':None},
               'db':{'address':None,'ssl':False}}

        # init
        client = MagicMock(spec=RestClient)
        g = condor(gridspec, cfg['queue'][name], cfg, self.services,
                 self.executor, module.FakeStatsClient(),
                 client, client)
        if not g:
            raise Exception('init did not return grid object')

        # remove task
        caller.called = False
        caller.ret = ''

        await asyncio.ensure_future(g.remove(['1','2']))
        if not caller.called:
            raise Exception('subprocess.call not called')
        if caller.args[0][0] != 'condor_rm':
            logger.info('args: %r',caller.args)
            raise Exception('does not start with condor_rm')
        if caller.args[0][1] != '1':
            logger.info('args: %r',caller.args)
            raise Exception('does not remove 1')
        if caller.args[0][2] != '2':
            logger.info('args: %r',caller.args)
            raise Exception('does not remove 2')

        # no tasks
        caller.called = False
        caller.ret = ''

        await asyncio.ensure_future(g.remove([]))
        if caller.called:
            raise Exception('subprocess.call when no tasks')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(condor_test))
    suite.addTests(loader.loadTestsFromNames(alltests,condor_test))
    return suite
