"""
Test script for core exe_helper
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('exe_helper')

import os
import sys
import time
import shutil
import tempfile
import glob
import subprocess

try:
    import cPickle as pickle
except:
    import pickle

import unittest
from iceprod.core import to_log,constants
from iceprod.core.jsonUtil import json_encode

class exe_helper_test(unittest.TestCase):
    def setUp(self):
        super(exe_helper_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
            if os.path.exists(constants['args']):
                os.remove(constants['args'])
        self.addCleanup(cleanup)

    def call(self, classname, src=None, args=None):
        helper = os.path.join(self.test_dir,'coverage_exe_helper.py')
        with open(helper,'w') as f:
            f.write("""
import coverage
cov = coverage.coverage(data_suffix=True,include=['*exe_helper.py'],branch=True)
cov.start()
from iceprod.core import exe_helper
exe_helper.main()
cov.stop()
cov.save()
""")
        cmd = ['python', helper, '--classname', classname, '--debug']
        if src:
            cmd.extend(['--filename',src])
        if args:
            with open(constants['args'],'w') as f:
                f.write(json_encode(args))
            cmd.extend(['--args'])
        logger.info('cmd=%r',cmd)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        comm = [x.decode('utf-8') if x else x for x in p.communicate()]
        logger.info('out,err:\n%s\n%s',*comm)
        if p.returncode:
            raise Exception('call failed')
        return comm

    @unittest_reporter
    def test_01_direct_import(self):
        test_script = os.path.join(self.test_dir,'test.py')
        with open(test_script,'w') as f:
            f.write("""
from __future__ import print_function
class A:
    def __init__(self):
        print('hello world')
""")
        if 'PYTHONPATH' in os.environ:
            os.environ['PYTHONPATH'] = self.test_dir+':'+os.environ['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = self.test_dir
        out,err = self.call('test.A')
        if 'hello world' not in out:
            raise Exception('did not call __init__')

    @unittest_reporter
    def test_02_direct_import_with_arg(self):
        test_script = os.path.join(self.test_dir,'test.py')
        with open(test_script,'w') as f:
            f.write("""
from __future__ import print_function
class A:
    def __init__(self,blah=None):
        print('hello world')
        print('blah=',blah,sep='')
""")
        if 'PYTHONPATH' in os.environ:
            os.environ['PYTHONPATH'] = self.test_dir+':'+os.environ['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = self.test_dir
        out,err = self.call('test.A',args={'args':[2],'kwargs':{}})
        if 'hello world' not in out:
            raise Exception('did not call __init__')
        if 'blah=2' not in out:
            raise Exception('argument not passed')
            
        out,err = self.call('test.A',args={'args':[],'kwargs':{'blah':2}})
        if 'hello world' not in out:
            raise Exception('did not call __init__')
        if 'blah=2' not in out:
            raise Exception('argument not passed')

    @unittest_reporter
    def test_03_direct_IPBaseClass(self):
        test_script = os.path.join(self.test_dir,'test.py')
        with open(test_script,'w') as f:
            f.write("""
from __future__ import print_function
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class A(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
        self.AddParameter('blah','the blah',None)
    def Execute(self,stats):
        print('hello world')
        print('blah=',self.GetParameter('blah'),sep='')
        stats['foo'] = 'bar'
        return 0
""")
        if 'PYTHONPATH' in os.environ:
            os.environ['PYTHONPATH'] = self.test_dir+':'+os.environ['PYTHONPATH']
        else:
            os.environ['PYTHONPATH'] = self.test_dir
        out,err = self.call('test.A',args={'args':[],'kwargs':{'blah':2}})
        if 'hello world' not in out:
            raise Exception('did not call __init__')
        if 'blah=2' not in out:
            raise Exception('argument not passed')
        if not os.path.exists(constants['stats']):
            raise Exception('stats file does not exist')
        stats = pickle.load(open(constants['stats'],'rb'))
        if stats != {'foo':'bar'}:
            raise Exception('bad stats')

    @unittest_reporter
    def test_10_file_import(self):
        test_script = os.path.join(self.test_dir,'test.py')
        with open(test_script,'w') as f:
            f.write("""
from __future__ import print_function
class A:
    def __init__(self):
        print('hello world')
""")
        out,err = self.call('A', src=test_script)
        if 'hello world' not in out:
            raise Exception('did not call __init__')

    @unittest_reporter
    def test_11_function_call(self):
        test_script = os.path.join(self.test_dir,'test.py')
        with open(test_script,'w') as f:
            f.write("""
from __future__ import print_function
def a(blah):
    print('hello world')
""")
        out,err = self.call('a', src=test_script, args={'args':[],'kwargs':{'blah':2}})
        if 'hello world' not in out:
            raise Exception('did not call __init__')

    @unittest_reporter
    def test_20_exception(self):
        test_script = os.path.join(self.test_dir,'test.py')
        with open(test_script,'w') as f:
            f.write("""
from __future__ import print_function
def a():
    raise Exception()
""")
        try:
            out,err = self.call('a', src=test_script)
        except Exception:
            pass
        else:
            raise Exception('failed to raise exception')
        

    @unittest_reporter
    def test_21_exception_IPBaseClass(self):
        test_script = os.path.join(self.test_dir,'test.py')
        with open(test_script,'w') as f:
            f.write("""
from __future__ import print_function
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class A(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        raise Exception()
        return 0
""")
        try:
            out,err = self.call('test.A', src=test_script)
        except Exception:
            pass
        else:
            raise Exception('failed to raise exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(exe_helper_test))
    suite.addTests(loader.loadTestsFromNames(alltests,exe_helper_test))
    return suite
