"""
Test script for core exe_json
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, cmp_dict

import logging
logger = logging.getLogger('exe')

import os
import sys
import time
import shutil
import tempfile
import random
import string
import subprocess
from functools import partial

try:
    import cPickle as pickle
except:
    import pickle

import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from iceprod.core import to_log,constants
import iceprod.core.functions
import iceprod.core.exe
import iceprod.core.exe_json
import iceprod.core.jsonRPCclient
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor


class exe_json_test(unittest.TestCase):
    def setUp(self):
        super(exe_json_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.symlink(os.path.join(curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        # set offline mode
        self.config = iceprod.core.exe.Config()
        self.config.config['options']['offline'] = True

    @patch('iceprod.core.jsonRPCclient.Client')
    @unittest_reporter
    def test_01_setupjsonRPC(self, RPC):
        """Test setupjsonRPC"""
        address = 'http://test:9080'
        passkey = 'ksdf8n4'
        rpc_instance = RPC.return_value
        rpc_instance.request.return_value = 'e'

        iceprod.core.exe_json.ServerComms(address, passkey, config=self.config)
        self.assertTrue(RPC.called)
        logger.info('%r',RPC.call_args[1])
        self.assertTrue(cmp_dict({'address':address}, RPC.call_args[1]))

        RPC.reset_mock()
        kwargs = {'ssl_cert':'cert','ssl_key':'key','cacert':'ca'}
        iceprod.core.exe_json.ServerComms(address, passkey,
                                          config=self.config, **kwargs)
        self.assertTrue(RPC.called)
        expected = {'address':address}
        expected.update(kwargs)
        self.assertTrue(cmp_dict(expected, RPC.call_args[1]))

    @patch('iceprod.core.exe_json.JSONRPC')
    @unittest_reporter
    def test_10_download_task(self, RPC):
        """Test download_task"""
        task = {'dataset':10}
        RPCinstance = RPC.return_value
        RPCinstance.new_task.return_value = [task]

        rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        rpc.download_task('gridspec')

        self.assertTrue(RPCinstance.new_task.called)
        logger.info(RPCinstance.new_task.call_args[1])
        self.assertTrue({'gridspec','hostname','ifaces'}.issubset(
                            RPCinstance.new_task.call_args[1]))

    @patch('iceprod.core.exe_json.JSONRPC')
    @unittest_reporter
    def test_20_finish_task(self, RPC):
        """Test finish_task"""
        RPCinstance = RPC.return_value

        self.config.config['options']['task_id'] = 'task'
        stats = {'test':True}

        rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        rpc.finish_task(stats)

        self.assertTrue(RPCinstance.finish_task.called)
        logger.info(RPCinstance.finish_task.call_args[1])
        self.assertTrue({'task_id','stats'}.issubset(
                            RPCinstance.finish_task.call_args[1]))

    @patch('iceprod.core.exe_json.JSONRPC')
    @unittest_reporter
    def test_30_still_running(self, RPC):
        """Test still_running"""
        RPCinstance = RPC.return_value
        
        self.config.config['options']['task_id'] = 'task'

        RPCinstance.stillrunning.return_value = True
        rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        rpc.still_running()
        self.assertTrue(RPCinstance.stillrunning.called)

        RPCinstance.stillrunning.return_value = False
        with self.assertRaises(Exception):
            rpc.still_running()
        self.assertIn('DBkill', self.config.config['options'])

        RPCinstance.stillrunning.side_effect = Exception('sql error')
        with self.assertRaises(Exception):
            rpc.still_running()

    @patch('iceprod.core.exe_json.JSONRPC')
    @unittest_reporter
    def test_40_task_error(self, RPC):
        """Test task_error"""
        RPCinstance = RPC.return_value
        self.config.config['options']['task_id'] = 'task'

        rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        rpc.task_error()

        self.assertTrue(RPCinstance.task_error.called)
        logger.info(RPCinstance.task_error.call_args[1])
        self.assertTrue({'task_id','error_info'}.issubset(
                            RPCinstance.task_error.call_args[1]))

        RPCinstance.task_error.reset_mock()
        data = ''.join(random.choice(string.ascii_letters) for _ in range(10000))
        with open('stderr','w') as fh:
            fh.write(data)
        rpc.task_error(start_time=time.time()-200)
        
        self.assertTrue(RPCinstance.task_error.called)
        self.assertTrue({'task_id','error_info'}.issubset(
                            RPCinstance.task_error.call_args[1]))
        error_info = RPCinstance.task_error.call_args[1]['error_info']
        self.assertGreaterEqual(error_info['time_used'], 200)

    @patch('iceprod.core.exe_json.JSONRPC')
    @unittest_reporter
    def test_41_task_kill(self, RPC):
        """Test task_kill"""
        RPCinstance = RPC.return_value
        task_id = 'task'

        rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=None)
        rpc.task_kill(task_id)

        self.assertTrue(RPCinstance.task_error.called)
        logger.info(RPCinstance.task_error.call_args[1])
        self.assertTrue({'task_id','error_info'}.issubset(
                            RPCinstance.task_error.call_args[1]))

        RPCinstance.task_error.reset_mock()
        resources = {'cpu': 1, 'memory': 3.4, 'disk': 0.2}
        rpc.task_kill(task_id, resources)
        
        self.assertTrue(RPCinstance.task_error.called)
        logger.info(RPCinstance.task_error.call_args[1])
        self.assertTrue({'task_id','error_info'}.issubset(
                            RPCinstance.task_error.call_args[1]))
        error_info = RPCinstance.task_error.call_args[1]['error_info']
        self.assertEqual(error_info['resources'], resources)

        RPCinstance.task_error.reset_mock()
        resources = {'time': 34.2}
        reason = 'testing'
        rpc.task_kill(task_id, resources, reason=reason)
        
        self.assertTrue(RPCinstance.task_error.called)
        logger.info(RPCinstance.task_error.call_args[1])
        self.assertTrue({'task_id','error_info'}.issubset(
                            RPCinstance.task_error.call_args[1]))
        error_info = RPCinstance.task_error.call_args[1]['error_info']
        self.assertEqual(error_info['resources'], resources)
        self.assertEqual(error_info['error_summary'], reason)

    @patch('iceprod.core.exe_json.JSONRPC')
    @unittest_reporter
    def test_50_uploadLogging(self, RPC):
        """Test uploading logfiles"""
        RPCinstance = RPC.return_value
        self.config.config['options']['task_id'] = 'task'

        data = ''.join([str(random.randint(0,10000)) for _ in range(100)])

        filename = os.path.join(self.test_dir,str(random.randint(0,10000)))
        with open(filename,'w') as f:
            f.write(data)
        name = 'testing'
        
        rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=self.config)
        rpc._upload_logfile(name, filename)
        
        self.assertTrue(RPCinstance.upload_logfile.called)
        logger.info(RPCinstance.upload_logfile.call_args[1])
        self.assertTrue({'task','name','data'}.issubset(
                            RPCinstance.upload_logfile.call_args[1]))
        self.assertEqual(RPCinstance.upload_logfile.call_args[1]['data'],
                         json_compressor.compress(data.encode('utf-8')))

        for f in constants.keys():
            if f in ('stderr','stdout','stdlog'):
                with open(constants[f],'w') as f:
                    f.write(''.join([str(random.randint(0,10000))
                                     for _ in range(100)]))

        RPCinstance.task_error.reset_mock()
        rpc.uploadLog()
        
        self.assertTrue(RPCinstance.upload_logfile.called)
        self.assertEqual('stdlog', RPCinstance.upload_logfile.call_args[1]['name'])
        self.assertEqual(RPCinstance.upload_logfile.call_args[1]['data'],
                         json_compressor.compress(open(constants['stdlog'],'rb').read()))

        RPCinstance.task_error.reset_mock()
        rpc.uploadErr()
        
        self.assertTrue(RPCinstance.upload_logfile.called)
        self.assertEqual('stderr', RPCinstance.upload_logfile.call_args[1]['name'])
        self.assertEqual(RPCinstance.upload_logfile.call_args[1]['data'],
                         json_compressor.compress(open(constants['stderr'],'rb').read()))

        RPCinstance.task_error.reset_mock()
        rpc.uploadOut()
        
        self.assertTrue(RPCinstance.upload_logfile.called)
        self.assertEqual('stdout', RPCinstance.upload_logfile.call_args[1]['name'])
        self.assertEqual(RPCinstance.upload_logfile.call_args[1]['data'],
                        json_compressor.compress( open(constants['stdout'],'rb').read()))

    @patch('iceprod.core.exe_json.JSONRPC')
    @unittest_reporter
    def test_60_update_pilot(self, RPC):
        """Test update_pilot"""
        RPCinstance = RPC.return_value
        pilot_id = 'pilot'
        args = {'a': 1, 'b': 2}
        
        rpc = iceprod.core.exe_json.ServerComms('a', 'p', config=None)
        rpc.update_pilot(pilot_id, **args)

        self.assertTrue(RPCinstance.update_pilot.called)
        logger.info(RPCinstance.update_pilot.call_args[1])
        self.assertTrue({'pilot_id','a','b'}.issubset(
                            RPCinstance.update_pilot.call_args[1]))

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(exe_json_test))
    suite.addTests(loader.loadTestsFromNames(alltests,exe_json_test))
    return suite
