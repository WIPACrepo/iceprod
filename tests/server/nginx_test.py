"""
Test script for nginx
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('nginx_test')

import os, sys, time
import shutil
import tempfile
import random
import threading
import glob
import subprocess
import unittest

from threading import Thread

import requests

from iceprod.core import to_log
from iceprod.core import functions
from iceprod.core import util
from iceprod.server import nginx

from iceprod.server import ssl_cert

skip_tests = False
with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
    if subprocess.call(['which','nginx']):
        skip_tests = True

# a simple server for testing the proxy
def server(port,cb):
    import BaseHTTPServer
    import SocketServer
    import Queue
    class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(405)
            self.send_header("Content-type", "text")
            self.end_headers()
        def do_GET(self):
            logging.warn('got GET request %s'%self.path)
            ret = cb(self.path)
            if isinstance(ret,(tuple,list)):
                self.send_response(ret[1])
                self.end_headers()
                ret = ret[0]
            else:
                self.send_response(200)
                self.end_headers()
            self.wfile.write(ret)
            self.wfile.close()
        def do_POST(self):
            logging.warn('got POST request %s'%self.path)
            input = None
            varLen = 0
            try:
                varLen = int(self.headers['Content-Length'])
            except Exception:
                logger.info('error getting content-length',exc_info=True)
                pass
            if varLen:
                try:
                    input = self.rfile.read(varLen)
                except Exception:
                    logger.info('error getting input',exc_info=True)
                    pass
            logger.info('input: %r',input)
            try:
                if input:
                    ret = cb(self.path,input=input)
                else:
                    ret = cb(self.path)
            except Exception:
                logger.error('Error running callback function',exc_info=True)
                ret = ''
            logger.info('ret: %r',ret)
            if isinstance(ret,(tuple,list)):
                self.send_response(ret[1])
                self.end_headers()
                ret = ret[0]
            else:
                self.send_response(200)
                self.end_headers()
            self.wfile.write(ret)
            self.wfile.close()

    SocketServer.TCPServer.allow_reuse_address = True
    httpd = SocketServer.TCPServer(("localhost", port), Handler)
    def noop(*args,**kwargs):
        pass
    httpd.handle_error = noop
    t = threading.Thread(target=httpd.serve_forever)
    t.start()
    time.sleep(1)
    logger.info('test server started at localhost:%d'%port)
    class http:
        @staticmethod
        def shutdown():
            httpd.shutdown()
            t.join()
            logger.info('test server stopped at localhost:%d'%port)
            time.sleep(1)
    return http

class nginx_test(unittest.TestCase):
    def setUp(self):
        super(nginx_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

        self.ssl_key = os.path.join(self.test_dir,'test.key')
        self.ssl_cert = os.path.join(self.test_dir,'test.crt')

        # get hostname
        hostname = functions.gethostname()
        if hostname is None:
            hostname = 'localhost'
        elif isinstance(hostname,set):
            hostname = hostname.pop()
        self.hostname = hostname

        # make cert
        ssl_cert.create_cert(self.ssl_cert,self.ssl_key,days=1,hostname=hostname)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(nginx_test,self).tearDown()

    @unittest_reporter(skip=skip_tests)
    def test_01_init(self):
        """Test __init__"""
        # default setup
        n = nginx.Nginx(cfg_file=os.path.join(self.test_dir,'nginx.conf'),
                        access_log=os.path.join(self.test_dir,'access.log'),
                        error_log=os.path.join(self.test_dir,'nginx_error.log'))

        # test config file
        filename = os.path.expandvars(os.path.join(self.test_dir,'nginx.conf'))
        if not os.path.exists(filename):
            raise Exception('Basic config does not produce nginx.conf')
        for i,line in enumerate(open(filename)):
            line = line.strip()
            if not line.endswith(';') and '{' not in line and '}' not in line:
                raise Exception('Basic config missing semicolon at line %d'%i)

        # test ssl and auth_basic
        if n.ssl is True:
            raise Exception('Basic config has ssl enabled')
        if n.auth_basic is True:
            raise Exception('Basic config has auth_basic enabled')


        # auth_basic setup
        n = nginx.Nginx(cfg_file=os.path.join(self.test_dir,'nginx.conf'),
                        access_log=os.path.join(self.test_dir,'access.log'),
                        error_log=os.path.join(self.test_dir,'nginx_error.log'),
                        username='user',
                        password='pass')

        # test config file
        filename = os.path.expandvars(os.path.join(self.test_dir,'nginx.conf'))
        if not os.path.exists(filename):
            raise Exception('auth_basic config does not produce nginx.conf')
        for i,line in enumerate(open(filename)):
            line = line.strip()
            if not line.endswith(';') and '{' not in line and '}' not in line:
                raise Exception('auth_basic config missing semicolon at line %d'%i)

        # test ssl and auth_basic
        if n.ssl is True:
            raise Exception('auth_basic config has ssl enabled')
        if n.auth_basic is False:
            raise Exception('auth_basic config has auth_basic disabled')
        if not os.path.exists(os.path.expandvars('$PWD/authbasic.htpasswd')):
            raise Exception('auth_basic config missing authbasic.htpasswd file')

        # ssl setup
        n = nginx.Nginx(cfg_file=os.path.join(self.test_dir,'nginx.conf'),
                        access_log=os.path.join(self.test_dir,'access.log'),
                        error_log=os.path.join(self.test_dir,'nginx_error.log'),
                        sslkey=self.ssl_key,
                        sslcert=self.ssl_cert)

        # test config file
        filename = os.path.expandvars(os.path.join(self.test_dir,'nginx.conf'))
        if not os.path.exists(filename):
            raise Exception('ssl config does not produce nginx.conf')
        for i,line in enumerate(open(filename)):
            line = line.strip()
            if not line.endswith(';') and '{' not in line and '}' not in line:
                raise Exception('ssl config missing semicolon at line %d'%i)

        # test ssl and auth_basic
        if n.ssl is False:
            raise Exception('ssl config has ssl disabled')
        if n.auth_basic is True:
            raise Exception('ssl config has auth_basic enabled')


        # authbasic and ssl setup
        n = nginx.Nginx(cfg_file=os.path.join(self.test_dir,'nginx.conf'),
                        access_log=os.path.join(self.test_dir,'access.log'),
                        error_log=os.path.join(self.test_dir,'nginx_error.log'),
                        sslkey=self.ssl_key,
                        sslcert=self.ssl_cert,
                        username='user',
                        password='pass')

        # test config file
        filename = os.path.expandvars(os.path.join(self.test_dir,'nginx.conf'))
        if not os.path.exists(filename):
            raise Exception('auth_basic+ssl config does not produce nginx.conf')
        for i,line in enumerate(open(filename)):
            line = line.strip()
            if not line.endswith(';') and '{' not in line and '}' not in line:
                raise Exception('auth_basic+ssl config missing semicolon at line %d'%i)

        # test ssl and auth_basic
        if n.ssl is False:
            raise Exception('auth_basic+ssl config has ssl disabled')
        if n.auth_basic is False:
            raise Exception('auth_basic+ssl config has auth_basic disabled')
        if not os.path.exists(os.path.expandvars('$PWD/authbasic.htpasswd')):
            raise Exception('auth_basic+ssl config missing authbasic.htpasswd file')

    @unittest_reporter(skip=skip_tests)
    def test_02_start_stop(self):
        """Test start/stop"""
        # common kwargs
        common = {
            'prefix': self.test_dir,
            'pid_file': os.path.join(self.test_dir,'nginx.pid'),
            'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
            'access_log': os.path.join(self.test_dir,'access.log'),
            'error_log': os.path.join(self.test_dir,'nginx_error.log'),
            }

        instances = {}
        # default setup
        instances['default'] = {
            }
        # auth_basic setup
        instances['auth_basic'] = {
            'username': 'user',
            'password': 'pass',
            }
        # ssl setup
        instances['ssl'] = {
            'sslkey': self.ssl_key,
            'sslcert': self.ssl_cert,
            }
        # authbasic and ssl setup
        instances['auth_basic+ssl'] = {
            'sslkey': self.ssl_key,
            'sslcert': self.ssl_cert,
            'username': 'user',
            'password': 'pass',
            }

        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
            for desc in instances:
                kwargs = common.copy()
                kwargs.update(instances[desc])
                n = nginx.Nginx(**kwargs)
                try:
                    n.start()
                except Exception:
                    logger.warn('start %s failed',desc,exc_info=True)
                    raise
                try:
                    n.stop()
                except Exception:
                    logger.warn('stop %s failed',desc,exc_info=True)
                    raise

    @unittest_reporter(skip=skip_tests)
    def test_03_start_kill(self):
        """Test start/kill"""
        # common kwargs
        common = {
            'prefix': self.test_dir,
            'pid_file': os.path.join(self.test_dir,'nginx.pid'),
            'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
            'access_log': os.path.join(self.test_dir,'access.log'),
            'error_log': os.path.join(self.test_dir,'nginx_error.log'),
            }

        instances = {}
        # default setup
        instances['default'] = {
            }

        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
            for desc in instances:
                kwargs = common.copy()
                kwargs.update(instances[desc])
                n = nginx.Nginx(**kwargs)
                try:
                    n.start()
                except Exception:
                    logger.warn('start %s failed',desc,exc_info=True)
                    raise
                try:
                    n.kill()
                except Exception:
                    logger.warn('kill %s failed',desc,exc_info=True)
                    raise

    @unittest_reporter(skip=skip_tests)
    def test_04_logrotate(self):
        """Test logrotate"""
        # common kwargs
        common = {
            'prefix': self.test_dir,
            'pid_file': os.path.join(self.test_dir,'nginx.pid'),
            'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
            'access_log': os.path.join(self.test_dir,'access.log'),
            'error_log': os.path.join(self.test_dir,'nginx_error.log'),
            }

        instances = {}
        # default setup
        instances['default'] = {
            }

        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
            for desc in instances:
                kwargs = common.copy()
                kwargs.update(instances[desc])
                n = nginx.Nginx(**kwargs)
                try:
                    n.start()
                except Exception:
                    logger.warn('start %s failed',desc,exc_info=True)
                    raise
                n.logrotate()
                log_path = os.path.join(self.test_dir,'nginx_error.log_*')
                files = glob.glob(log_path)
                if not files:
                    raise Exception('no rotated file')
                for f in files:
                    os.unlink(f)
                try:
                    n.stop()
                except Exception:
                    logger.warn('stop %s failed',desc,exc_info=True)
                    raise

    @unittest_reporter(skip=skip_tests)
    def test_09_static_download(self):
        """Test static download"""
        static_dir = os.path.join(self.test_dir,'static')
        os.mkdir(static_dir)

        # common kwargs
        common = {
            'prefix': self.test_dir,
            'pid_file': os.path.join(self.test_dir,'nginx.pid'),
            'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
            'access_log': os.path.join(self.test_dir,'access.log'),
            'error_log': os.path.join(self.test_dir,'nginx_error.log'),
            'static_dir': static_dir,
            'port': 58080,
            }

        instances = {}
        # default setup
        instances['default'] = {
            }
        # auth_basic setup
        instances['auth_basic'] = {
            'username': 'user',
            'password': 'pass',
            }
        # ssl setup
        instances['ssl'] = {
            'sslkey': self.ssl_key,
            'sslcert': self.ssl_cert,
            }
        # authbasic and ssl setup
        instances['auth_basic+ssl'] = {
            'sslkey': self.ssl_key,
            'sslcert': self.ssl_cert,
            'username': 'user',
            'password': 'pass',
            }

        dest_path = os.path.join(self.test_dir,'download')
        url = 'http://'+self.hostname+':58080/static'
        logger.info('url:%r',url)
        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout),requests.Session() as s:
            for desc in instances:
                kwargs = common.copy()
                kwargs.update(instances[desc])
                n = nginx.Nginx(**kwargs)
                try:
                    n.start()
                except Exception:
                    logger.warn('start %s failed',desc,exc_info=True)
                    raise

                try:
                    for _ in range(10):
                        # try to download from static dir
                        filename = str(random.randint(0,10000))
                        filecontents = str(random.randint(0,10000))
                        dest_path = os.path.join(self.test_dir,filename)
                        with open(os.path.join(static_dir,filename),'w') as f:
                            f.write(filecontents)

                        # static dir should not require username or password, so leave them blank
                        r = s.get(os.path.join(url,filename),
                                  verify=self.ssl_cert)
                        r.raise_for_status()

                        if r.content != filecontents:
                            logger.info('correct contents: %r',filecontents)
                            logger.info('downloaded contents: %r',r.content)
                            raise Exception('contents not equal')
                finally:
                    try:
                        n.stop()
                    except Exception:
                        logger.warn('stop %s failed',desc,exc_info=True)
                        raise

    @unittest_reporter(skip=skip_tests)
    def test_10_proxy(self):
        """Test proxy"""
        static_dir = os.path.join(self.test_dir,'static')
        os.mkdir(static_dir)

        # common kwargs
        common = {
            'prefix': self.test_dir,
            'pid_file': os.path.join(self.test_dir,'nginx.pid'),
            'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
            'access_log': os.path.join(self.test_dir,'access.log'),
            'error_log': os.path.join(self.test_dir,'nginx_error.log'),
            'static_dir': static_dir,
            'port': 58080,
            'proxy_port': 58081,
            }

        instances = {}
        # default setup
        instances['default'] = {
            }
        # auth_basic setup
        instances['auth_basic'] = {
            'username': 'user',
            'password': 'pass',
            }
        # ssl setup
        instances['ssl'] = {
            'sslkey': self.ssl_key,
            'sslcert': self.ssl_cert,
            }
        # authbasic and ssl setup
        instances['auth_basic+ssl'] = {
            'sslkey': self.ssl_key,
            'sslcert': self.ssl_cert,
            'username': 'user',
            'password': 'pass',
            }

        filecontents = str(random.randint(0,10000))
        def proxy(url,input=None):
            # get a request
            proxy.url = url
            proxy.input = input
            if proxy.success:
                return filecontents
            else:
                return ('',404)

        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout),requests.Session() as s:
            try:
                http = server(common['proxy_port'],proxy)
            except Exception:
                logger.error('initializing server failed',exc_info=True)
                raise Exception('failed to start proxy server')

            try:
                dest_path = os.path.join(self.test_dir,'download')
                url = 'http://'+self.hostname+':58080/'
                for desc in instances:
                    kwargs = common.copy()
                    kwargs.update(instances[desc])
                    n = nginx.Nginx(**kwargs)
                    try:
                        n.start()
                    except Exception:
                        logger.warn('start %s failed',desc,exc_info=True)
                        raise

                    try:
                        for _ in range(10):
                            # try to open main page
                            proxy.success = True
                            r = s.get(url, verify=self.ssl_cert)
                            r.raise_for_status()

                            self.assertEqual(r.content, filecontents)

                            # see what happens when it errors
                            proxy.success = False
                            try:
                                r = s.get(url, verify=self.ssl_cert)
                                r.raise_for_status()
                            except Exception:
                                pass
                            else:
                                raise Exception('did not raise Exception')

                    finally:
                        try:
                            n.stop()
                        except Exception:
                            logger.warn('stop %s failed',desc,exc_info=True)
                            raise
            finally:
                try:
                    http.shutdown()
                    time.sleep(0.5)
                except:
                    logger.error('http server failed to stop')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(nginx_test))
    suite.addTests(loader.loadTestsFromNames(alltests,nginx_test))
    return suite
