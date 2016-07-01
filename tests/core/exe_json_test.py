"""
Test script for core exe_json
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

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
from iceprod.core import to_log,constants
import iceprod.core.functions
import iceprod.core.exe
import iceprod.core.exe_json
import iceprod.core.jsonRPCclient
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from flexmock import flexmock


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

    @unittest_reporter
    def test_90_setupjsonRPC(self):
        """Test setupjsonRPC"""
        # mock the JSONRPC class
        def start(*args,**kwargs):
            start.args = args
            start.kwargs = kwargs
        start.args = None
        start.kwargs = None
        jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
        jsonrpc.should_receive('start').replace_with(start)
        def f(*args,**kwargs):
            if kwargs['func_name'] in f.returns:
                ret = f.returns[kwargs['func_name']]
            else:
                ret = Exception('jsonrpc error')
            logger.debug('f(func_name=%s) returns %r',kwargs['func_name'],ret)
            if 'callback' in kwargs:
                kwargs['callback'](ret)
            else:
                return ret
        jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))

        address = 'http://test:9080'
        passkey = 'ksdf8n4'
        f.returns = {'echo':'e'}
        try:
            iceprod.core.exe_json.setupjsonRPC(address, passkey)
        except:
            logger.error('running setupjsonRPC failed')
            raise
        if (('address' in start.kwargs and start.kwargs['address'] != address) or
            ('address' not in start.kwargs and
             (len(start.args) < 1 or start.args[0] != address))):
            raise Exception('JSONRPC.start() does not have address')
        if 'address' not in start.kwargs:
            start.args = start.args[1:]
        if (('passkey' in start.kwargs and start.kwargs['passkey'] != passkey) or
            ('passkey' not in start.kwargs and
             (len(start.args) < 1 or start.args[0] != passkey))):
            raise Exception('JSONRPC.start() does not have passkey')

        kwargs = {'ssl_cert':'cert','ssl_key':'key','cacert':'ca'}
        try:
            iceprod.core.exe_json.setupjsonRPC(address, passkey, **kwargs)
        except:
            logger.error('running setupjsonRPC SSL failed')
            raise
        if 'ssl_cert' in start.kwargs and start.kwargs['ssl_cert'] != 'cert':
            raise Exception('JSONRPC.start() does not have ssl_cert')
        if 'ssl_key' in start.kwargs and start.kwargs['ssl_key'] != 'key':
            raise Exception('JSONRPC.start() does not have ssl_key')
        if 'cacert' in start.kwargs and start.kwargs['cacert'] != 'ca':
            raise Exception('JSONRPC.start() does not have cacert')


    @unittest_reporter
    def test_91_downloadtask(self):
        """Test downloadtask"""
        # mock the JSONRPC class
        task = {'dataset':10}
        def new_task(platform=None, hostname=None, ifaces=None,
                     gridspec=None, **kwargs):
            new_task.called = True
            new_task.platform = platform
            new_task.hostname = hostname
            new_task.ifaces = ifaces
            new_task.gridspec = gridspec
            new_task.kwargs = kwargs
            return task
        new_task.called = False
        def f(*args,**kwargs):
            name = kwargs.pop('func_name')
            if 'callback' in kwargs:
                cb = kwargs.pop('callback')
            else:
                cb = None
            if name == 'new_task':
                ret = new_task(*args,**kwargs)
            else:
                ret = Exception()
            if cb:
                cb(ret)
            else:
                return ret
        jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
        jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))

        if 'PLATFORM' not in os.environ:
            os.environ['PLATFORM'] = 'other'
        platform = os.environ['PLATFORM']
        hostname = iceprod.core.functions.gethostname()
        ifaces = iceprod.core.functions.getInterfaces()
        gridspec = 'thegrid'
        try:
            iceprod.core.exe_json.downloadtask(gridspec)
        except:
            logger.error('running downloadtask failed')
            raise
        if not new_task.called:
            raise Exception('JSONRPC.new_task() not called')
        if new_task.platform != platform:
            raise Exception('JSONRPC.new_task() platform !=')
        if new_task.hostname != hostname:
            raise Exception('JSONRPC.new_task() hostname !=')
        if new_task.ifaces != ifaces:
            raise Exception('JSONRPC.new_task() ifaces !=')
        if new_task.gridspec != gridspec:
            raise Exception('JSONRPC.new_task() gridspec !=')

    @unittest_reporter
    def test_92_finishtask(self):
        """Test finishtask"""
        # mock the JSONRPC class
        task_id = 'a task'
        stats = {'test':True}
        def finish_task(task,stats={}):
            finish_task.called = True
            finish_task.task_id = task
            finish_task.stats = stats
            return None
        finish_task.called = False
        def f(*args,**kwargs):
            name = kwargs.pop('func_name')
            if 'callback' in kwargs:
                cb = kwargs.pop('callback')
            else:
                cb = None
            if name == 'finish_task':
                ret = finish_task(*args,**kwargs)
            else:
                ret = Exception()
            if cb:
                cb(ret)
            else:
                return ret
        jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
        jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
        self.config.config['options']['task_id'] = task_id

        try:
            iceprod.core.exe_json.finishtask(self.config, stats)
        except:
            logger.error('running finishtask failed')
            raise
        if not finish_task.called:
            raise Exception('JSONRPC.finish_task() not called')
        if finish_task.task_id != task_id:
            raise Exception('JSONRPC.finish_task() task_id !=')
        if finish_task.stats != stats:
            raise Exception('JSONRPC.finish_task() stats !=')

    @unittest_reporter
    def test_93_stillrunning(self):
        """Test stillrunning"""
        try:
            # mock the JSONRPC class
            task_id = 'a task'
            def stillrunning(task):
                stillrunning.called = True
                stillrunning.task_id = task
                return stillrunning.ret
            stillrunning.called = False
            def f(*args,**kwargs):
                name = kwargs.pop('func_name')
                if 'callback' in kwargs:
                    cb = kwargs.pop('callback')
                else:
                    cb = None
                if name == 'stillrunning':
                    ret = stillrunning(*args,**kwargs)
                else:
                    ret = Exception()
                if cb:
                    cb(ret)
                else:
                    return ret
            jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
            jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
            self.config.config['options']['task_id'] = task_id

            stillrunning.ret = True
            try:
                iceprod.core.exe_json.stillrunning(self.config)
            except:
                logger.error('exception when not supposed to')
                raise
            if not stillrunning.called:
                raise Exception('JSONRPC.stillrunning() not called')
            if stillrunning.task_id != task_id:
                raise Exception('JSONRPC.stillrunning() task_id !=')

            stillrunning.ret = False
            try:
                iceprod.core.exe_json.stillrunning(self.config)
            except:
                pass
            else:
                raise Exception('exception not thrown')
                raise
            if 'DBkill' not in self.config.config['options']:
                raise Exception('DBkill not in config["options"]')

            stillrunning.ret = Exception('sql error')
            try:
                iceprod.core.exe_json.stillrunning(self.config)
            except:
                pass
            else:
                raise Exception('exception not thrown2')
                raise

        finally:
            if 'DBkill' in self.config.config['options']:
                del self.config.config['options']['DBkill']

    @unittest_reporter
    def test_94_taskerror(self):
        """Test taskerror"""
        # mock the JSONRPC class
        task_id = 'a task'
        def task_error(task, error_info=None):
            task_error.called = True
            task_error.task_id = task
            task_error.error_info = error_info
            return None
        task_error.called = False
        def f(*args,**kwargs):
            name = kwargs.pop('func_name')
            if 'callback' in kwargs:
                cb = kwargs.pop('callback')
            else:
                cb = None
            if name == 'task_error':
                ret = task_error(*args,**kwargs)
            else:
                ret = Exception()
            if cb:
                cb(ret)
            else:
                return ret
        jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
        jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
        self.config.config['options']['task_id'] = task_id

        try:
            iceprod.core.exe_json.taskerror(self.config)
        except:
            logger.error('running taskerror failed')
            raise
        if not task_error.called:
            raise Exception('JSONRPC.task_error() not called')
        if task_error.task_id != task_id:
            raise Exception('JSONRPC.task_error() task_id !=')

        data = ''.join(random.choice(string.letters) for _ in range(10000))
        with open('stderr','w') as fh:
            fh.write(data)
        try:
            iceprod.core.exe_json.taskerror(self.config, start_time=time.time()-200)
        except:
            logger.error('running taskerror failed')
            raise
        if not task_error.called:
            raise Exception('JSONRPC.task_error() not called')
        if task_error.task_id != task_id:
            raise Exception('JSONRPC.task_error() task_id !=')
        if ((not task_error.error_info) or
            task_error.error_info['time_used'] < 200 or
            json_compressor.uncompress(task_error.error_info['error_summary'])[-100:] != data[-100:]):
            logger.info('error_info: %r', task_error.error_info)
            raise Exception('error_info incorrect')

    @unittest_reporter
    def test_95_uploadLogging(self):
        """Test uploading logfiles"""
        # mock the JSONRPC class
        task_id = 'a task'
        def uploader(task,name,data):
            uploader.called = True
            uploader.task_id = task
            uploader.data[name] = json_compressor.uncompress(data)
            return None
        def fun(*args,**kwargs):
            name = kwargs.pop('func_name')
            if 'callback' in kwargs:
                cb = kwargs.pop('callback')
            else:
                cb = None
            if name == 'upload_logfile':
                ret = uploader(*args,**kwargs)
            else:
                ret = Exception()
            if cb:
                cb(ret)
            else:
                return ret
        jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
        jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(fun,func_name=a))
        self.config.config['options']['task_id'] = task_id

        data = ''.join([str(random.randint(0,10000)) for _ in xrange(100)])

        filename = os.path.join(self.test_dir,str(random.randint(0,10000)))
        with open(filename,'w') as f:
            f.write(data)

        uploader.called = False
        uploader.task_id = None
        uploader.data = {}
        name = 'testing'
        try:
            iceprod.core.exe_json._upload_logfile(self.config, task_id,
                                             name, filename)
        except:
            logger.error('running _upload_logfile failed')
            raise
        if not uploader.called:
            raise Exception('JSONRPC._upload_logfile() not called')
        if uploader.task_id != task_id:
            raise Exception('JSONRPC._upload_logfile() task_id !=')
        if name not in uploader.data:
            raise Exception('JSONRPC._upload_logfile() invalid name: %r'%
                            uploader.data.keys())
        if uploader.data[name] != data:
            raise Exception('JSONRPC._upload_logfile() data !=')

        uploader.called = False
        uploader.task_id = None
        uploader.data = {}
        for f in constants.keys():
            if f in ('stderr','stdout','stdlog'):
                with open(constants[f],'w') as f:
                    f.write(''.join([str(random.randint(0,10000))
                                     for _ in xrange(100)]))
        try:
            iceprod.core.exe_json.uploadLogging(self.config)
        except:
            logger.error('running uploadLogging failed')
            raise
        if not uploader.called:
            raise Exception('JSONRPC.uploadLogging() not called')
        if uploader.task_id != task_id:
            raise Exception('JSONRPC.uploadLogging() task_id !=')
        for name in ('stdlog','stderr','stdout'):
            if name not in uploader.data:
                raise Exception('JSONRPC.uploadLogging(%s) invalid name: %r'%
                                (name,uploader.data.keys()))
            if uploader.data[name] != open(constants[name]).read():
                raise Exception('JSONRPC.uploadLogging(%s) data !='%name)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(exe_json_test))
    suite.addTests(loader.loadTestsFromNames(alltests,exe_json_test))
    return suite
