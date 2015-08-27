"""
Test script for loader.sh
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('loader_test')

import os, sys, time
import shutil
import tempfile
import random
import string
import subprocess
import threading

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

from iceprod.core import to_log

# a simple server for testing the external process
def server(port,cb):
    import BaseHTTPServer
    import SocketServer
    class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-type", "text")
            self.end_headers()
        def do_GET(self):
            logging.warn('got GET request %s'%self.path)
            self.send_response(200)
            self.end_headers()
            ret = cb(self.path)
            self.wfile.write(ret)
            self.wfile.close()
        def do_POST(self):
            logging.warn('got POST request %s'%self.path)
            self.send_response(200)
            self.end_headers()
            input = None
            varLen = 0
            try:
                varLen = int(self.headers['Content-Length'])
            except Exception as e:
                logger.info('error getting content-length: %r',e)
                pass
            if varLen:
                try:
                    input = self.rfile.read(varLen)
                except Exception as e:
                    logger.info('error getting input: %r',e)
                    pass
            logger.info('input: %r',input)
            try:
                if input:
                    ret = cb(self.path,input=input)
                else:
                    ret = cb(self.path)
            except Exception as e:
                logger.error('Error running callback function: %r',e)
                ret = ''
            logger.info('ret: %r',ret)
            self.wfile.write(ret)
            self.wfile.close()

    httpd = SocketServer.TCPServer(("localhost", port), Handler)
    def run():
        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
            httpd.serve_forever()
    threading.Thread(target=run).start()
    time.sleep(1)
    logging.info('test server started at localhost:%d'%port)
    return httpd

class loader_test(unittest.TestCase):
    def setUp(self):
        super(loader_test,self).setUp()

        self.orig_dir = os.path.abspath(os.path.expandvars(os.getcwd()))

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

        self.real_loader = os.path.join(self.orig_dir,'bin','loader.sh')

        self.chdir = os.path.join(self.test_dir,'ch')
        if not os.path.exists(self.chdir):
            os.mkdir(self.chdir)
        os.chdir(self.chdir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)
        super(loader_test,self).tearDown()

    @unittest_reporter(name=' ')
    def test_01(self):
        """Test basic loader functionality"""
        loader_lines = open(self.real_loader).readlines()
        test_loader = os.path.join(self.chdir,'loader.sh')
        with open(test_loader,'w') as f:
            for line in loader_lines:
                line = line.strip('\n')
                if (line.startswith('exec $cmd') or
                    line.startswith('rm -Rf')):
                    continue
                f.write(line+'\n')

        # make test tarball
        env = os.path.join(self.test_dir,'env')
        os.mkdir(env)
        os.mkdir(os.path.join(env,'bin'))
        for i in range(10):
            with open(os.path.join(env,str(i)),'w') as f:
                f.write(str(i))
        subprocess.check_output('tar -zc -C '+self.test_dir+' -f '+env+'.tar.gz env',
                                shell=True,stderr=subprocess.STDOUT)

        def down(url,input=''):
            if 'env' in url:
                e = open(env+'.tar.gz','rb').read()
                logger.info('env: %r',e)
                return e

        # make server to host env tarball
        port = random.randint(16000,32000)
        http = server(port,down)

        try:
            # call with no args
            cmd = '/bin/sh %s'%test_loader
            try:
                out = subprocess.check_output(cmd,shell=True,
                                              stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError:
                pass
            else:
                raise Exception('no args: should have failed')

            # call help
            cmd = '/bin/sh %s -h'%test_loader
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if 'Iceprod core framework starter script.' not in out:
                logger.info(out)
                raise Exception('help: did not display usage info')

            # call main
            cmd = '/bin/sh %s -d localhost:%d'%(test_loader,port)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('main: error raised')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('main: did not echo cmd')

            # test cvmfs
            cvmfs_dir = os.path.join(self.test_dir,'cvmfs')
            os.mkdir(cvmfs_dir)
            os.mkdir(os.path.join(cvmfs_dir,'standard'))
            with open(os.path.join(cvmfs_dir,'standard/setup.sh'),'w') as f:
                f.write('#!/bin/sh\necho "export PYTHOPATH=$PWD/lib/python2.7/site-pacakges"\n')

            cmd = '/bin/sh %s -d localhost:%d -s %s'%(test_loader,port+1,cvmfs_dir)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('main: error raised')
            if 'cvmfs' not in out:
                logger.info(out)
                raise Exception('main: did not echo CVMFS')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('env cache: did not echo cmd')

            # test env caching
            cmd = '/bin/sh %s -d localhost:%d'%(test_loader,port)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('env cache: error raised')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('env cache: did not echo cmd')

            # explicitly set platform
            platform = 'test'
            env_name = 'env.'+platform+'.tar.gz'
            cmd = '/bin/sh %s -d localhost:%d -m %s'
            cmd = cmd%(test_loader,port,platform)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('platform: error raised')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('platform: did not echo cmd')
            if not os.path.exists(env_name):
                logger.info(out)
                raise Exception('platform: did not set correctly')

            # explicitly set env filename
            env_name = 'env.test.tar.gz'
            cmd = '/bin/sh %s -d localhost:%d -e %s'
            cmd = cmd%(test_loader,port,env_name)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('env filename: error raised')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('env filename: did not echo cmd')
            if not os.path.exists(env_name):
                logger.info(out)
                raise Exception('env filename: did not set correctly')

            # test username,password
            username = 'user'
            password = 'password'
            cmd = '/bin/sh %s -d localhost:%d -u %s -p %s'
            cmd = cmd%(test_loader,port,username,password)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('username,password: error raised')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('username,password: did not echo cmd')

            # test x509
            x509 = 'x509_file'
            cmd = '/bin/sh %s -d localhost:%d -x %s'
            cmd = cmd%(test_loader,port,x509)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('x509: error raised')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('x509: did not echo cmd')

            # test bad arg
            cmd = '/bin/sh %s -d localhost:%d -x'
            cmd = cmd%(test_loader,port)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if not proc.returncode:
                logger.info(out)
                raise Exception('bad arg: did not raise error')

            # test argument passthrough
            arg = '--passkey kn23sdf9 --arg 3'
            cmd = '/bin/sh %s -d localhost:%d %s'
            cmd = cmd%(test_loader,port,arg)
            proc = subprocess.Popen(cmd,shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            out = proc.communicate()[0]
            if proc.returncode:
                logger.info(out)
                raise Exception('argument passthrough: error raised')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('argument passthrough: did not echo cmd')
            if arg not in out:
                logger.info(out)
                raise Exception('argument passthrough: did not echo arg')

        finally:
            http.shutdown()
            time.sleep(0.5)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(loader_test))
    suite.addTests(loader.loadTestsFromNames(alltests,loader_test))
    return suite
