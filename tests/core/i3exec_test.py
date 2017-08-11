"""
Test script for i3exec
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('i3exec_test')

import os, sys, time
import shutil
import tempfile
import random
import string
import subprocess
import threading
import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from iceprod.core import to_log
import iceprod.core.dataclasses
import iceprod.core.functions
import iceprod.core.serialization
import iceprod.core.logger
from iceprod.core import jsonUtil

# mock the logger methods so we don't overwrite the root logger
def log2(*args,**kwargs):
    pass
iceprod.core.logger.set_logger = log2
iceprod.core.logger.remove_stdout = log2
from iceprod.core import i3exec


from .exe_test import DownloadTestCase

class i3exec_test(DownloadTestCase):
    def setUp(self):
        super(i3exec_test,self).setUp()

    def make_config(self):
        # create basic config file
        config = iceprod.core.dataclasses.Job()
        config['options']['job_temp'] = os.path.join(self.test_dir,'job_temp')
        config['options']['local_temp'] = os.path.join(self.test_dir,'local_temp')
        config['options']['data_directory'] = os.path.join(self.test_dir,'data')
        config['options']['loglevel'] = 'info'
        config['options']['task_id'] = 'a'
        config['options']['dataset_id'] = 'a'
        config['options']['job'] = 0
        config['steering'] = iceprod.core.dataclasses.Steering()
        return config

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='main() basic')
    def test_01(self, download):
        # create basic config file
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        def create(*args, **kwargs):
            path = os.path.join(config['options']['local_temp'], mod['src'])
            self.mk_files(path, """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 0
""", ext=True)
            return path
        download.side_effect = create

        # write configuration to file
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        # set some default values
        logfile = logging.getLogger().handlers[0].stream.name
        url = 'http://foo/'
        debug = False
        passkey = 'pass'
        offline = True

        # try to run the config
        i3exec.main(cfgfile, logfile=logfile, url=url, debug=debug,
                    passkey=passkey, offline=offline)

    @unittest_reporter(name='main() bad config')
    def test_02(self):
        """Test not providing a steering file"""
        # set some default values
        cfgfile = None
        logfile = logging.getLogger().handlers[0].stream.name
        url = 'http://foo/'
        debug = True
        passkey = 'pass'
        offline = True

        # try to run the config
        with self.assertRaises(Exception):
            i3exec.main(cfgfile, logfile=logfile, url=url, debug=debug,
                        passkey=passkey, offline=offline)

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='main() debug')
    def test_03(self, download):
        # create basic config file
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        def create(*args, **kwargs):
            path = os.path.join(config['options']['local_temp'], mod['src'])
            self.mk_files(path, """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 0
""", ext=True)
            return path
        download.side_effect = create

        # write configuration to file
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        # set some default values
        logfile = logging.getLogger().handlers[0].stream.name
        url = 'http://foo/'
        debug = True
        passkey = 'pass'
        offline = True

        # try to run the config
        i3exec.main(cfgfile, logfile=logfile, url=url, debug=debug,
                    passkey=passkey, offline=offline)

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='main() specific task')
    def test_10(self, download):
        # create basic config file
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        def create(*args, **kwargs):
            path = os.path.join(config['options']['local_temp'], mod['src'])
            self.mk_files(path, """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 0
""", ext=True)
            return path
        download.side_effect = create

        # write configuration to file
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        # set some default values
        logfile = logging.getLogger().handlers[0].stream.name
        url = 'http://foo/'
        debug = False
        passkey = 'pass'
        offline = True

        # try to run the config
        i3exec.main(cfgfile, logfile=logfile, url=url, debug=debug,
                    passkey=passkey, offline=offline)

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='main() .so lib')
    def test_11(self, download):
        """Test multiple tasks"""
        # create basic config file
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        config = self.make_config()

        # create the task object
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'

        config['tasks'].append(task)

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'mytest.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray2'
        tray['iterations'] = 3

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'mytest.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

        # make .so file
        so = self.make_shared_lib()

        def create(url, *args, **kwargs):
            if url.endswith(c['src']):
                path = os.path.join(config['options']['local_temp'], c['src'])
                self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            else:
                path = os.path.join(config['options']['local_temp'], module['src'])
                self.mk_files(path, """
def Test():
    return 'Tester2'
""", ext=True)
            return path
        download.side_effect = create

        # write configuration to file
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)


        # set some default values
        logfile = logging.getLogger().handlers[0].stream.name
        url = 'http://foo'
        debug = False
        passkey = 'pass'
        offline = True

        # try to run the config
        i3exec.main(cfgfile, logfile=logfile, url=url, debug=debug,
                    passkey=passkey, offline=offline)

    @patch('iceprod.core.functions.download')
    @unittest_reporter(name='main() failing task')
    def test_20(self, download):
        # create basic config file
        cfgfile = os.path.join(self.test_dir,'test_steering.json')
        config = self.make_config()
        task = iceprod.core.dataclasses.Task()
        task['name'] = 'task'
        config['tasks'].append(task)
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        task['trays'].append(tray)
        mod = iceprod.core.dataclasses.Module()
        mod['name'] = 'mod'
        mod['running_class'] = 'MyTest'
        mod['src'] = 'mytest.py'
        tray['modules'].append(mod)

        def create(*args, **kwargs):
            path = os.path.join(config['options']['local_temp'], mod['src'])
            self.mk_files(path, """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        raise Exception()
""", ext=True)
            return path
        download.side_effect = create

        # write configuration to file
        iceprod.core.serialization.serialize_json.dump(config,cfgfile)

        # set some default values
        logfile = logging.getLogger().handlers[0].stream.name
        url = 'http://foo/'
        debug = True
        passkey = 'pass'
        offline = True

        # try to run the config
        with self.assertRaises(Exception):
            i3exec.main(cfgfile, logfile=logfile, url=url, debug=debug,
                        passkey=passkey, offline=offline)



def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(i3exec_test))
    suite.addTests(loader.loadTestsFromNames(alltests,i3exec_test))
    return suite