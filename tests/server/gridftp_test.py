"""
Test script for gridftp tornado integration
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('gridftp')

import os, sys, time
import shutil
import random
import string
import subprocess
import tempfile
from threading import Event

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import tornado.ioloop

import iceprod.server.gridftp

class gridftp_test(unittest.TestCase):
    
    def setUp(self):
        self._timeout = 1
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        self.server_test_dir = os.path.join('gsiftp://gridftp.icecube.wisc.edu/data/sim/sim-new/tmp/test',
                                            str(random.randint(0,2**32)))
        try:
            iceprod.core.gridftp.GridFTP.mkdir(self.server_test_dir,
                                               parents=True,
                                               request_timeout=self._timeout)
        except:
            pass
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        super(gridftp_test,self).setUp()
    
    def tearDown(self):
        try:
            iceprod.core.gridftp.GridFTP.rmtree(self.server_test_dir,
                                                request_timeout=self._timeout)
        except:
            pass
        shutil.rmtree(self.test_dir)
        super(gridftp_test,self).tearDown()
    
    def test_callback(self):
        """Test async callback"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                # put str
                iceprod.server.gridftp.GridFTP.put(address,data=filecontents,callback=cb,
                                                   request_timeout=self._timeout)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address,
                                                          request_timeout=self._timeout)
                except:
                    pass
            
        except Exception as e:
            logger.error('Error running callback test: %s',str(e))
            printer('Test gridftp async callback',False)
            raise
        else:
            printer('Test gridftp async callback')
    
    def test_streaming_callback(self):
        """Test async streaming callback"""
        try:
            address = os.path.join(self.server_test_dir,'test')
            filecontents = 'this is a test'
            def contents():
                # give every 10 chars
                for i in xrange(0,len(filecontents),10):
                    yield filecontents[i:i+10]
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.ret = False
            
            try:
                # put from function
                iceprod.server.gridftp.GridFTP.put(address,
                                                   streaming_callback=contents().next,
                                                   callback=cb,
                                                   request_timeout=self._timeout)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                if ret is not True:
                    raise Exception('put failed: ret=%r'%ret)
            finally:
                try:
                    iceprod.server.gridftp.GridFTP.delete(address,
                                                          request_timeout=self._timeout)
                except:
                    pass
            
        except Exception as e:
            logger.error('Error running streaming callback test: %s',str(e))
            printer('Test gridftp async streaming callback',False)
            raise
        else:
            printer('Test gridftp async streaming callback')
    
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(gridftp_test))
    suite.addTests(loader.loadTestsFromNames(alltests,gridftp_test))
    return suite
