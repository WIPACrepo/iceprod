"""
Test script for core exe_json
"""

# from __future__ import absolute_import, division, print_function

# from tests.util import unittest_reporter, glob_tests, cmp_dict

# import logging
# logger = logging.getLogger('exe')

# import os
# import sys
# import time
# import shutil
# import tempfile
# import random
# import string
# import subprocess
# from functools import partial

# try:
    # import cPickle as pickle
# except:
    # import pickle

# import unittest
# from unittest.mock import patch

# from tornado.testing import AsyncTestCase

# from iceprod.core import to_log,constants
# import iceprod.core.dataclasses
# import iceprod.core.functions
# import iceprod.core.exe
# import iceprod.core.exe_json
# from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor


# class exe_json_test(AsyncTestCase):
    # def setUp(self):
        # super(exe_json_test,self).setUp()

        # curdir = os.getcwd()
        # self.test_dir = tempfile.mkdtemp(dir=curdir)
        # os.symlink(os.path.join(curdir, 'iceprod'),
                   # os.path.join(self.test_dir, 'iceprod'))
        # os.chdir(self.test_dir)
        # def cleanup():
            # os.chdir(curdir)
            # shutil.rmtree(self.test_dir)
        # self.addCleanup(cleanup)

        # # set offline mode
        # self.config = iceprod.core.exe.Config()
        # self.config.config['options']['offline'] = True

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # def test_01_ServerComms(self, Client):
        # """Test ServerComms"""
        # address = 'http://test'
        # passkey = 'ksdf8n4'
        # Client.return_value.request.return_value = 'e'

        # iceprod.core.exe_json.ServerComms(address, passkey, config=self.config)
        # self.assertTrue(Client.called)
        # logger.info('%r',Client.call_args[1])
        # self.assertEqual({'address':address,'token':passkey}, Client.call_args[1])

        # Client.reset_mock()
        # kwargs = {'ssl_cert':'cert','ssl_key':'key','cacert':'ca'}
        # iceprod.core.exe_json.ServerComms(address, passkey,
                                          # config=self.config, **kwargs)
        # self.assertTrue(Client.called)
        # expected = {'address':address}
        # expected.update(kwargs)
        # self.assertTrue(cmp_dict(expected, Client.call_args[1]))

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_10_download_task(self, Client):
        # """Test download_task"""
        # task = {'dataset':10}
        # c = Client.return_value
        # async def req(method, path, args=None):
            # if path.startswith('/task_actions/process'):
                # return {'task':'foo', 'task_id':'foo', 'job_id':'bar', 'dataset_id':'bar', 'task_index':1}
            # elif path.startswith('/config'):
                # return {'dataset_id':'bar'}
            # elif path.startswith('/jobs'):
                # return {'job_index':1}
            # elif path.startswith('/datasets'):
                # return {'dataset':1, 'jobs_submitted':10, 'tasks_submitted':1, 'debug':True}
        # c.request.side_effect = req

        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        # ret = (await rpc.download_task('gridspec'))[0]
        # self.assertIn('options', ret)
        # self.assertIn('task', ret['options'])
        # self.assertEqual(ret['options']['task'], 1)
        # self.assertIn('jobs_submitted', ret['options'])
        # self.assertEqual(ret['options']['jobs_submitted'], 10)

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_12_task_files(self, Client):
        # """Test task_files"""
        # c = Client.return_value
        # async def req(*args,**kwargs):
            # d = iceprod.core.dataclasses.Data()
            # d['remote'] = 'foo'
            # return {'files': [d]}
        # c.request.side_effect = req

        # self.config.config['options']['dataset_id'] = 'd'
        # self.config.config['options']['task_id'] = 't'

        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        # files = await rpc.task_files('d','t')

        # self.assertTrue(c.request.called)
        # logger.info(c.request.call_args[0])
        # logger.info('files: %r', files)
        # self.assertEqual(len(files), 1)
        # self.assertEqual(files[0]['remote'], 'foo')

        # async def req(*args,**kwargs):
            # return {'files': [{'movement':'blah'}]}
        # c.request.side_effect = req
        # with self.assertRaises(Exception):
            # await rpc.task_files('d','t')

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_15_processing(self, Client):
        # """Test processing"""
        # c = Client.return_value
        # async def req(*args,**kwargs):
            # return {}
        # c.request.side_effect = req

        # self.config.config['options']['task_id'] = 'task'

        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        # await rpc.processing('task')

        # self.assertTrue(c.request.called)
        # logger.info(c.request.call_args[0])
        # self.assertTrue({'status'}.issubset(
                        # c.request.call_args[0][-1]))

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_20_finish_task(self, Client):
        # """Test finish_task"""
        # c = Client.return_value
        # async def req(*args,**kwargs):
            # return {}
        # c.request.side_effect = req

        # self.config.config['options']['task_id'] = 'task'
        # stats = {'test':True}

        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        # await rpc.finish_task('task', stats=stats)

        # self.assertEqual(c.request.call_count, 2)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('task_stats', c.request.call_args_list[0][0][-1])

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_30_still_running(self, Client):
        # """Test still_running"""
        # c = Client.return_value
        
        # self.config.config['options']['task_id'] = 'task'

        # async def req(*args,**kwargs):
            # return {'status':'processing'}
        # c.request.side_effect = req
        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        # await rpc.still_running('task')
        # self.assertTrue(c.request.called)

        # async def req(*args,**kwargs):
            # return {'status':'reset'}
        # c.request.side_effect = req
        # with self.assertRaises(Exception):
            # await rpc.still_running('task')

        # c.request.side_effect = Exception('request error')
        # with self.assertRaises(Exception):
            # await rpc.still_running('task')

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_40_task_error(self, Client):
        # """Test task_error"""
        # c = Client.return_value
        # self.config.config['options']['task_id'] = 'task'
        
        # async def req(*args,**kwargs):
            # return {}
        # c.request.side_effect = req

        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        # await rpc.task_error('task')

        # self.assertEqual(c.request.call_count, 2)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('task_stats', c.request.call_args_list[0][0][-1])
        # self.assertIn('error_summary', c.request.call_args_list[0][0][-1])

        # c.request.reset_mock()
        # data = ''.join(random.choice(string.ascii_letters) for _ in range(10000))
        # await rpc.task_error('task', reason=data, start_time=time.time()-200)

        # self.assertEqual(c.request.call_count, 2)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('task_stats', c.request.call_args_list[0][0][-1])
        # self.assertIn('error_summary', c.request.call_args_list[0][0][-1])
        # self.assertEqual(data, c.request.call_args_list[0][0][-1]['error_summary'])
        # self.assertGreaterEqual(c.request.call_args_list[0][0][-1]['time_used'], 200)
        
        # c.request.side_effect = Exception('request error')
        # with self.assertRaises(Exception):
            # await rpc.task_error('task')

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_41_task_kill(self, Client):
        # """Test task_kill"""
        # c = Client.return_value
        # task_id = 'task'

        # async def req(*args,**kwargs):
            # return {}
        # c.request.side_effect = req

        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=None)
        # await rpc.task_kill(task_id)

        # self.assertEqual(c.request.call_count, 5)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('error_summary', c.request.call_args_list[0][0][-1])

        # c.request.reset_mock()
        # resources = {'cpu': 1, 'memory': 3.4, 'disk': 0.2}
        # await rpc.task_kill(task_id, resources=resources)
        
        # self.assertEqual(c.request.call_count, 5)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('error_summary', c.request.call_args_list[0][0][-1])
        # self.assertIn('resources', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['resources'], resources)

        # c.request.reset_mock()
        # resources = {'time': 34.2}
        # reason = 'testing'
        # await rpc.task_kill(task_id, resources=resources, reason=reason)
        
        # self.assertEqual(c.request.call_count, 5)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('error_summary', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['error_summary'], reason)
        # self.assertIn('resources', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['resources'], resources)

        # c.request.side_effect = Exception('request error')
        # with self.assertRaises(Exception):
            # await rpc.task_kill(task_id)

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_42_task_kill_sync(self, Client):
        # """Test task_kill"""
        # c = Client.return_value
        # task_id = 'task'

        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=None)
        # rpc.task_kill_sync(task_id)

        # self.assertEqual(c.request_seq.call_count, 5)
        # logger.info(c.request_seq.call_args_list[0][0])
        # self.assertIn('error_summary', c.request_seq.call_args_list[0][0][-1])

        # c.request_seq.reset_mock()
        # resources = {'cpu': 1, 'memory': 3.4, 'disk': 0.2}
        # rpc.task_kill_sync(task_id, resources=resources)
        
        # self.assertEqual(c.request_seq.call_count, 5)
        # logger.info(c.request_seq.call_args_list[0][0])
        # self.assertIn('error_summary', c.request_seq.call_args_list[0][0][-1])
        # self.assertIn('resources', c.request_seq.call_args_list[0][0][-1])
        # self.assertEqual(c.request_seq.call_args_list[0][0][-1]['resources'], resources)

        # c.request_seq.reset_mock()
        # resources = {'time': 34.2}
        # reason = 'testing'
        # rpc.task_kill_sync(task_id, resources=resources, reason=reason)
        
        # self.assertEqual(c.request_seq.call_count, 5)
        # logger.info(c.request_seq.call_args_list[0][0])
        # self.assertIn('error_summary', c.request_seq.call_args_list[0][0][-1])
        # self.assertEqual(c.request_seq.call_args_list[0][0][-1]['error_summary'], reason)
        # self.assertIn('resources', c.request_seq.call_args_list[0][0][-1])
        # self.assertEqual(c.request_seq.call_args_list[0][0][-1]['resources'], resources)

        # c.request_seq.side_effect = Exception('request error')
        # with self.assertRaises(Exception):
            # rpc.task_kill_sync(task_id)

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_50_uploadLogging(self, Client):
        # """Test uploading logfiles"""
        # c = Client.return_value
        # self.config.config['options']['task_id'] = 'task'

        # async def req(*args,**kwargs):
            # return {}
        # c.request.side_effect = req

        # data = ''.join([str(random.randint(0,10000)) for _ in range(100)])

        # filename = os.path.join(self.test_dir,str(random.randint(0,10000)))
        # with open(filename,'w') as f:
            # f.write(data)
        # name = 'testing'
        
        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        # await rpc._upload_logfile(name, filename)
        
        # self.assertTrue(c.request.called)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('name', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['name'], name)
        # self.assertIn('data', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['data'],
                         # data)

        # for f in constants.keys():
            # if f in ('stderr','stdout','stdlog'):
                # with open(constants[f],'w') as f:
                    # f.write(''.join([str(random.randint(0,10000))
                                     # for _ in range(100)]))

        # c.request.reset_mock()
        # await rpc.uploadLog()
        
        # self.assertTrue(c.request.called)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('name', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['name'], 'stdlog')
        # self.assertIn('data', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['data'],
                         # open(constants['stdlog']).read())

        # c.request.reset_mock()
        # await rpc.uploadErr()
        
        # self.assertTrue(c.request.called)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('name', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['name'], 'stderr')
        # self.assertIn('data', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['data'],
                         # open(constants['stderr']).read())

        # c.request.reset_mock()
        # await rpc.uploadOut()
        
        # self.assertTrue(c.request.called)
        # logger.info(c.request.call_args_list[0][0])
        # self.assertIn('name', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['name'], 'stdout')
        # self.assertIn('data', c.request.call_args_list[0][0][-1])
        # self.assertEqual(c.request.call_args_list[0][0][-1]['data'],
                        # open(constants['stdout']).read())


        # c.request.side_effect = Exception('request error')
        # with self.assertRaises(Exception):
            # await rpc._upload_logfile(name, filename)
        # with self.assertRaises(Exception):
            # await rpc.uploadLog()
        # with self.assertRaises(Exception):
            # await rpc.uploadErr()
        # with self.assertRaises(Exception):
            # await rpc.uploadOut()

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # async def test_60_update_pilot(self, Client):
        # """Test update_pilot"""
        # c = Client.return_value
        # pilot_id = 'pilot'
        # args = {'a': 1, 'b': 2}

        # async def req(*args,**kwargs):
            # return {}
        # c.request.side_effect = req
        
        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=None)
        # await rpc.update_pilot(pilot_id, **args)

        # self.assertTrue(c.request.called)
        # logger.info(c.request.call_args[0])
        # self.assertTrue({'a','b'}.issubset(c.request.call_args[0][-1]))

        # c.request.side_effect = Exception('request error')
        # with self.assertRaises(Exception):
            # await rpc.update_pilot(pilot_id, **args)

    # @patch('iceprod.core.exe_json.RestClient')
    # @unittest_reporter
    # def test_61_update_pilot_sync(self, Client):
        # """Test update_pilot"""
        # c = Client.return_value
        # pilot_id = 'pilot'
        # args = {'a': 1, 'b': 2}
        
        # rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=None)
        # rpc.update_pilot_sync(pilot_id, **args)

        # self.assertTrue(c.request_seq.called)
        # logger.info(c.request_seq.call_args[0])
        # self.assertTrue({'a','b'}.issubset(c.request_seq.call_args[0][-1]))

        # c.request_seq.side_effect = Exception('request error')
        # with self.assertRaises(Exception):
            # rpc.update_pilot_sync(pilot_id, **args)

# def load_tests(loader, tests, pattern):
    # suite = unittest.TestSuite()
    # alltests = glob_tests(loader.getTestCaseNames(exe_json_test))
    # suite.addTests(loader.loadTestsFromNames(alltests,exe_json_test))
    # return suite
