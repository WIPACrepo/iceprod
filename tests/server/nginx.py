"""
  Test script for nginx

  copyright (c) 2013 the icecube collaboration
"""

from __future__ import print_function,absolute_import
try:
    from server_tester import printer, glob_tests
    import logging
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    import logging
    logging.basicConfig()
logger = logging.getLogger('nginx_test')

import os, sys, time
import shutil
import random
import threading

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from threading import Thread

from iceprod.core import to_log
from iceprod.core import functions,dataclasses
from iceprod.server import nginx

from iceprod.server import openssl


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
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
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
        openssl.create_cert(self.ssl_cert,self.ssl_key,days=1,hostname=hostname)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(nginx_test,self).tearDown()
    
    def test_01_init(self):
        """Test __init__"""
        try:
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
            
        except Exception, e:
            logger.error('Error running nginx.__init__ test - %s',str(e))
            printer('Test nginx.__init__',False)
            raise
        else:
            printer('Test nginx.__init__')

            
    def test_02_start_stop(self):
        """Test start/stop"""
        try:
            # common kwargs
            common = {
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
            
            for desc in instances:
                kwargs = common.copy()
                kwargs.update(instances[desc])
                n = nginx.Nginx(**kwargs)
                try:
                    n.start()
                except Exception, e:
                    raise Exception('start %s failed: %r'%(desc,e))
                try:
                    n.stop()
                except Exception, e:
                    raise Exception('stop %s failed: %r'%(desc,e))
        
        except Exception, e:
            logger.error('Error running nginx.start/stop test - %s',str(e))
            printer('Test nginx.start/stop',False)
            raise
        else:
            printer('Test nginx.start/stop')
    
    def test_03_static_download(self):
        """Test start/stop"""
        try:
            static_dir = os.path.join(self.test_dir,'static')
            os.mkdir(static_dir)
            upload_dir = os.path.join(self.test_dir,'upload')
            os.mkdir(upload_dir)
            
            # common kwargs
            common = {
                'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
                'access_log': os.path.join(self.test_dir,'access.log'),
                'error_log': os.path.join(self.test_dir,'nginx_error.log'),
                'static_dir': static_dir,
                'upload_dir': upload_dir,
                'port': 8080,
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
            
            pycurl_handle = dataclasses.PycURL()
            dest_path = os.path.join(self.test_dir,'download')
            url = 'http://'+self.hostname+':8080/static'
            for desc in instances:
                kwargs = common.copy()
                kwargs.update(instances[desc])
                n = nginx.Nginx(**kwargs)
                try:
                    n.start()
                except Exception, e:
                    raise Exception('start %s failed: %r'%(desc,e))
                
                try:
                    for _ in xrange(10):
                        # try to download from static dir
                        filename = str(random.randint(0,10000))
                        filecontents = str(random.randint(0,10000))
                        dest_path = os.path.join(self.test_dir,filename)
                        with open(os.path.join(static_dir,filename),'w') as f:
                            f.write(filecontents)
                        
                        try:
                            # static dir should not require username or password, so leave them blank
                            pycurl_handle.fetch(os.path.join(url,filename),dest_path,
                                                cacert=self.ssl_cert)
                        except Exception as e:
                            raise Exception('pycurl failed to download file: %r',e)
                        
                        if not os.path.exists(dest_path):
                            raise Exception('downloaded file not found')
                        newcontents = ''
                        with open(dest_path) as f:
                            newcontents = f.read(1000)
                        if newcontents != filecontents:
                            logger.info('correct contents: %r',filecontents)
                            logger.info('downloaded contents: %r',newcontents)
                            raise Exception('contents not equal')
                    
                        os.remove(dest_path)
                finally:
                    try:
                        n.stop()
                    except Exception, e:
                        raise Exception('stop %s failed: %r'%(desc,e))
        
        except Exception, e:
            logger.error('Error running nginx.start/stop test - %s',str(e))
            printer('Test nginx static download',False)
            raise
        else:
            printer('Test nginx static download')
    
    def test_10_proxy(self):
        """Test proxy"""
        try:
            static_dir = os.path.join(self.test_dir,'static')
            os.mkdir(static_dir)
            upload_dir = os.path.join(self.test_dir,'upload')
            os.mkdir(upload_dir)
            
            # common kwargs
            common = {
                'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
                'access_log': os.path.join(self.test_dir,'access.log'),
                'error_log': os.path.join(self.test_dir,'nginx_error.log'),
                'static_dir': static_dir,
                'upload_dir': upload_dir,
                'port': 8080,
                'proxy_port': 8081,
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
            
            with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
                try:
                    http = server(common['proxy_port'],proxy)
                except Exception as e:
                    logger.error('%r',e,exc_info=True)
                    raise Exception('failed to start proxy server')
                
                try:
                    pycurl_handle = dataclasses.PycURL()
                    dest_path = os.path.join(self.test_dir,'download')
                    url = 'http://'+self.hostname+':8080/'
                    for desc in instances:
                        kwargs = common.copy()
                        kwargs.update(instances[desc])
                        n = nginx.Nginx(**kwargs)
                        try:
                            n.start()
                        except Exception as e:
                            raise Exception('start %s failed: %r'%(desc,e))
                        
                        try:
                            for _ in xrange(10):
                                # try to open main page
                                proxy.success = True
                                try:
                                    # static dir should not require username or password, so leave them blank
                                    pycurl_handle.fetch(url,dest_path,
                                                        cacert=self.ssl_cert)
                                except Exception as e:
                                    raise Exception('pycurl failed to download file: %r',e)
                                
                                if not os.path.exists(dest_path):
                                    raise Exception('downloaded file not found')
                                newcontents = ''
                                with open(dest_path) as f:
                                    newcontents = f.read(1000)
                                if newcontents != filecontents:
                                    logger.info('correct contents: %r',filecontents)
                                    logger.info('downloaded contents: %r',newcontents)
                                    raise Exception('contents not equal')
                                os.remove(dest_path)
                                
                                # see what happens when it errors
                                proxy.success = False
                                try:
                                    # static dir should not require username or password, so leave them blank
                                    pycurl_handle.fetch(url,dest_path,
                                                        cacert=self.ssl_cert)
                                except Exception as e:
                                    pass
                                else:
                                    raise Exception('pycurl succeeds when it should fail')
                                
                        finally:
                            try:
                                n.stop()
                            except Exception, e:
                                raise Exception('stop %s failed: %r'%(desc,e))
                finally:
                    try:
                        http.shutdown()
                        time.sleep(0.5)
                    except:
                        logger.error('http server failed to stop')
        
        except Exception, e:
            logger.error('Error running nginx.proxy test - %s',str(e))
            printer('Test nginx proxy',False)
            raise
        else:
            printer('Test nginx proxy')
    
    def test_11_proxy(self):
        """Test proxy upload"""
        try:
            static_dir = os.path.join(self.test_dir,'static')
            os.mkdir(static_dir)
            upload_dir = os.path.join(self.test_dir,'upload')
            os.mkdir(upload_dir)
            
            # common kwargs
            common = {
                'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
                'access_log': os.path.join(self.test_dir,'access.log'),
                'error_log': os.path.join(self.test_dir,'nginx_error.log'),
                'static_dir': static_dir,
                'upload_dir': upload_dir,
                'port': 8080,
                'proxy_port': 8081,
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
            
            with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
                try:
                    http = server(common['proxy_port'],proxy)
                except Exception as e:
                    logger.error('%r',e,exc_info=True)
                    raise Exception('failed to start proxy server')
                
                try:
                    pycurl_handle = dataclasses.PycURL()
                    dest_path = os.path.join(self.test_dir,'download')
                    url = 'http://'+self.hostname+':8080/upload'
                    for desc in instances:
                        kwargs = common.copy()
                        kwargs.update(instances[desc])
                        n = nginx.Nginx(**kwargs)
                        try:
                            n.start()
                        except Exception as e:
                            raise Exception('start %s failed: %r'%(desc,e))
                        
                        try:
                            for _ in xrange(10):
                                # try to open main page
                                proxy.success = True
                                try:
                                    pycurl_handle.fetch(url,dest_path,
                                                        cacert=self.ssl_cert,
                                                        **instances[desc])
                                except Exception as e:
                                    raise Exception('pycurl failed to download file: %r',e)
                                
                                if not os.path.exists(dest_path):
                                    raise Exception('downloaded file not found')
                                newcontents = ''
                                with open(dest_path) as f:
                                    newcontents = f.read(1000)
                                if newcontents != filecontents:
                                    logger.info('correct contents: %r',filecontents)
                                    logger.info('downloaded contents: %r',newcontents)
                                    raise Exception('contents not equal')
                                os.remove(dest_path)
                                
                                # see what happens when it errors
                                proxy.success = False
                                try:
                                    # static dir should not require username or password, so leave them blank
                                    pycurl_handle.fetch(url,dest_path,
                                                        cacert=self.ssl_cert,
                                                        **instances[desc])
                                except Exception as e:
                                    pass
                                else:
                                    raise Exception('pycurl succeeds when it should fail')
                                
                        finally:
                            try:
                                n.stop()
                            except Exception, e:
                                raise Exception('stop %s failed: %r'%(desc,e))
                finally:
                    try:
                        http.shutdown()
                        time.sleep(0.5)
                    except:
                        logger.error('http server failed to stop')
        
        except Exception, e:
            logger.error('Error running nginx.proxy upload test - %s',str(e))
            printer('Test nginx proxy upload',False)
            raise
        else:
            printer('Test nginx proxy upload')
    
    def test_12_proxy_auth(self):
        """Test proxy upload auth"""
        try:
            static_dir = os.path.join(self.test_dir,'static')
            os.mkdir(static_dir)
            upload_dir = os.path.join(self.test_dir,'upload')
            os.mkdir(upload_dir)
            
            # common kwargs
            common = {
                'cfg_file': os.path.join(self.test_dir,'nginx.conf'),
                'access_log': os.path.join(self.test_dir,'access.log'),
                'error_log': os.path.join(self.test_dir,'nginx_error.log'),
                'static_dir': static_dir,
                'upload_dir': upload_dir,
                'port': 8080,
                'proxy_port': 8081,
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
            
            def proxy(url,input=None):
                # get a request
                proxy.url = url
                proxy.input = input
                if url == '/uploadauth':
                    proxy.auth = (url,input)
                    if proxy.successauth:
                        return ('',200)
                    else:
                        return ('',403)
                if input:
                    try:
                        input = input.replace('\x93','"').replace('\x94','"')
                        tmpname = ''
                        proxy.dict = {}
                        for x in input.split('\r\n'):
                            if x and x[0] == 'C':
                                tmpname = x.split('=')[-1].replace('"','')
                            elif tmpname and x:
                                proxy.dict[tmpname] = x.replace('"','')
                                tmpname = ''
                    except Exception as e:
                        logger.info('proxy dict exception %r',e,exc_info=True)
                        proxy.dict = None
                if proxy.success:
                    return ('',200)
                else:
                    return ('',404)
            
            # make 10M test data file
            filename = str(random.randint(0,10000))
            filecontents = os.urandom(10**7)
            dest_path = os.path.join(self.test_dir,filename)
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
                try:
                    http = server(common['proxy_port'],proxy)
                except Exception as e:
                    logger.error('%r',e,exc_info=True)
                    raise Exception('failed to start proxy server')
                
                try:
                    pycurl_handle = dataclasses.PycURL()
                    url = 'http://'+self.hostname+':8080/upload/test'
                    for desc in instances:
                        kwargs = common.copy()
                        kwargs.update(instances[desc])
                        n = nginx.Nginx(**kwargs)
                        try:
                            n.start()
                        except Exception as e:
                            raise Exception('start %s failed: %r'%(desc,e))
                        
                        try:
                            for _ in xrange(2):
                                # try to open main page
                                proxy.success = True
                                proxy.successauth = True
                                proxy.auth = None
                                proxy.input = None
                                try:
                                    pycurl_handle.put(url,dest_path,
                                                      cacert=self.ssl_cert,
                                                      **instances[desc])
                                except Exception as e:
                                    raise Exception('pycurl failed to upload file: %r',e)
                                
                                if not proxy.auth:
                                    raise Exception('proxy.auth not called')
                                if proxy.auth[1] and len(proxy.auth[1]) > 100:
                                    raise Exception('proxy.auth has a large body')
                                if not proxy.dict or 'path' not in proxy.dict:
                                    logger.info('uploaded contents: %r',proxy.input)
                                    logger.info('uploaded dict: %r',proxy.dict)
                                    raise Exception('request POST dict is empty')
                                if open(proxy.dict['path']).read() != filecontents:
                                    logger.info('uploaded dict: %r',proxy.dict)
                                    raise Exception('file contents not equal')
                                
                                # see what happens when it errors
                                proxy.successauth = True
                                proxy.success = False
                                proxy.input = None
                                proxy.auth = None
                                try:
                                    # static dir should not require username or password, so leave them blank
                                    pycurl_handle.put(url,dest_path,
                                                      cacert=self.ssl_cert,
                                                      **instances[desc])
                                except Exception as e:
                                    pass
                                else:
                                    raise Exception('pycurl succeeds when it should fail')
                                
                                # see what happens when auth errors
                                proxy.successauth = False
                                proxy.success = True
                                proxy.input = None
                                proxy.auth = None
                                try:
                                    # static dir should not require username or password, so leave them blank
                                    pycurl_handle.put(url,dest_path,
                                                      cacert=self.ssl_cert,
                                                      **instances[desc])
                                except Exception as e:
                                    pass
                                else:
                                    raise Exception('pycurl auth succeeds when it should fail')
                                
                                if not proxy.auth:
                                    raise Exception('proxy.auth3 not called')
                                if proxy.auth[1] and len(proxy.auth[1]) > 100:
                                    raise Exception('proxy.auth3 has a large body')
                                if proxy.input and len(proxy.input) > 100:
                                    raise Exception('proxy3 has a large input')
                                
                        finally:
                            try:
                                n.stop()
                            except Exception, e:
                                raise Exception('stop %s failed: %r'%(desc,e))
                finally:
                    del pycurl_handle
                    try:
                        http.shutdown()
                    except:
                        logger.error('http server failed to stop')
        
        except Exception, e:
            logger.error('Error running nginx.proxy upload test - %s',str(e))
            printer('Test nginx proxy upload',False)
            raise
        else:
            printer('Test nginx proxy upload')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(nginx_test))
    suite.addTests(loader.loadTestsFromNames(alltests,nginx_test))
    return suite
