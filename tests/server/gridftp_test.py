"""
Test script for gridftp tornado integration
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

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

import unittest
import tornado.ioloop

from iceprod.core import to_log
import iceprod.server.gridftp

skip_tests = False
if (subprocess.call(['which','uberftp']) or
    subprocess.call(['which','globus-url-copy'])):
    skip_tests = True

skip_tests2 = False
if subprocess.call(['which','grid-proxy-init']):
    skip_tests2 = True

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

    @unittest_reporter(skip=skip_tests)
    def test_01_callback(self):
        """Test async callback"""
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

    @unittest_reporter(skip=skip_tests)
    def test_02_streaming_callback(self):
        """Test async streaming callback"""
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

class siteglobusproxy_test(unittest.TestCase):
    def setUp(self):
        self._timeout = 1
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        # clear any proxies
        FNULL = open(os.devnull, 'w')
        subprocess.call(['grid-proxy-destroy'],stdout=FNULL,stderr=FNULL)
        super(siteglobusproxy_test,self).setUp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(siteglobusproxy_test,self).tearDown()

    @unittest_reporter(skip=skip_tests2, module='gridftp.SiteGlobusProxy')
    def test_01_init(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = iceprod.server.gridftp.SiteGlobusProxy(cfgfile=cfgfile)
        if not os.path.exists(cfgfile):
            raise Exception('cfgfile does not exist')

    @unittest_reporter(skip=skip_tests2, module='gridftp.SiteGlobusProxy')
    def test_02_init_duration(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = iceprod.server.gridftp.SiteGlobusProxy(cfgfile=cfgfile,
                                                   duration=10)
        if not os.path.exists(cfgfile):
            raise Exception('cfgfile does not exist')

    @unittest_reporter(skip=skip_tests2, module='gridftp.SiteGlobusProxy')
    def test_10_update_proxy(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = iceprod.server.gridftp.SiteGlobusProxy(cfgfile=cfgfile)
        try:
            p.update_proxy()
        except Exception as e:
            pass
        else:
            raise Exception('did not raise Exception')

        p.set_passphrase('gibberish')
        try:
            with to_log(sys.stderr), to_log(sys.stdout):
                p.update_proxy()
        except Exception as e2:
            if str(e) == str(e2):
                logger.info('%r\n%r',e,e2)
                raise Exception('Exception is the same')
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter(skip=skip_tests2, module='gridftp.SiteGlobusProxy')
    def test_11_get_proxy(self):
        cfgfile = os.path.join(self.test_dir,'cfg')
        p = iceprod.server.gridftp.SiteGlobusProxy(cfgfile=cfgfile)
        try:
            p.get_proxy()
        except Exception as e:
            pass
        else:
            raise Exception('did not raise Exception')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(gridftp_test))
    suite.addTests(loader.loadTestsFromNames(alltests,gridftp_test))
    alltests = glob_tests(loader.getTestCaseNames(siteglobusproxy_test))
    suite.addTests(loader.loadTestsFromNames(alltests,siteglobusproxy_test))
    return suite
