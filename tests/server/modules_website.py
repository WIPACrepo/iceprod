"""
  Test script for website

  copyright (c) 2013 the icecube collaboration  
"""

from __future__ import print_function
import logging
try:
    from server_tester import printer, glob_tests
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    logging.basicConfig()
logger = logging.getLogger('website_test')

import os, sys, time, random
import shutil
import random
import threading
import signal
from datetime import datetime,timedelta
from multiprocessing import Queue,Pipe
from functools import partial

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import tornado.httpserver
import tornado.ioloop
from flexmock import flexmock


from iceprod.core import functions
from iceprod.core import dataclasses
import iceprod.core.jsonRPCclient
from iceprod.core.jsonUtil import json_encode,json_decode
import iceprod.server
import iceprod.core.logger
from iceprod.server import module
from iceprod.server.modules.website import website
from iceprod.server.dbclient import MetaDB,DB
from iceprod.server.nginx import Nginx
from iceprod.server import openssl
try:
    import iceprod.procname
except ImportError:
    pass


class website_test(unittest.TestCase):
    def setUp(self):
        super(website_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        self.ca_cert = os.path.join(self.test_dir,'ca.crt')
        self.ca_key = os.path.join(self.test_dir,'ca.key')
        self.ssl_key = os.path.join(self.test_dir,'test.key')
        self.ssl_cert = os.path.join(self.test_dir,'test.crt')
        
        # set hostname
        self.hostname = 'localhost'
        
        # make certs
        openssl.create_ca(self.ca_cert,self.ca_key,days=1,hostname=self.hostname)
        openssl.create_cert(self.ssl_cert,self.ssl_key,days=1,
                            cacert=self.ca_cert,cakey=self.ca_key,
                            hostname=self.hostname)
        
        # make cfg
        self.cfg = {'webserver':{'tornado_port':random.randint(10000,32000),
                                 'port':random.randint(10000,32000),
                                 'numcpus':1,
                                 'tmp_upload_dir':os.path.join(self.test_dir,'tmpupload'),
                                 'upload_dir':os.path.join(self.test_dir,'upload'),
                                 'static_dir':os.path.join(self.test_dir,'static'),
                                 'template_dir':os.path.join(self.test_dir,'template'),
                                 'lib_dir':os.path.join(self.test_dir,'lib'),
                                 'proxycache_dir':os.path.join(self.test_dir,'proxy'),
                                 'proxy_request_timeout':10,
                                },
                    'db':{'address':'localhost',
                          'ssl':True,
                         },
                    'system':{'ssl_cert':self.ssl_cert,
                              'ssl_key':self.ssl_key,
                              #'ssl_cacert':self.ssl_cert,
                              'ssl_cacert':self.ca_cert,
                             },
                    'download':{'http_username':None,
                                'http_password':None,
                               },
                   }
        
        def sig(*args):
            sig.args = args
        flexmock(signal).should_receive('signal').replace_with(sig)
        def basicConfig(*args,**kwargs):
            pass
        flexmock(logging).should_receive('basicConfig').replace_with(basicConfig)
        def setLogger(*args,**kwargs):
            pass
        flexmock(iceprod.core.logger).should_receive('setlogger').replace_with(setLogger)
        def removestdout(*args,**kwargs):
            pass
        flexmock(iceprod.core.logger).should_receive('removestdout').replace_with(removestdout)
        def setprocname(*args,**kwargs):
            pass
        try:
            flexmock(iceprod.procname).should_receive('setprocname').replace_with(setprocname)
        except:
            pass
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(website_test,self).tearDown()

    
    def test_01_init(self):
        """Test init"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(website).should_receive('start').replace_with(start)
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(website).should_receive('message_handling_loop').replace_with(message_handling_loop)
            
            start.called = False
            message_handling_loop.called = False
            
            cfg = {'test':1}
            args = [cfg,Queue(),Pipe()[1],Queue()]
            web = website(args)
            if not web:
                raise Exception('did not return website object')
            if start.called != True:
                raise Exception('init did not call start')
            if message_handling_loop.called != True:
                raise Exception('init did not call message_handling_loop')
            if not web.cfg or 'test' not in web.cfg or web.cfg['test'] != 1:
                raise Exception('init did not copy cfg properly')
            
        except Exception as e:
            logger.error('Error running website init test - %s',str(e))
            printer('Test website init',False)
            raise
        else:
            printer('Test website init')
    
    def test_02_start_stop(self):
        """Test start_stop"""
        try:
            # test that objects are called on startup
            def db_start(*args,**kwargs):
                db_start.called = True
            db_start.called = False
            flexmock(MetaDB).should_receive('start').replace_with(db_start)
            def db_stop(*args,**kwargs):
                db_stop.called = True
            db_stop.called = False
            flexmock(MetaDB).should_receive('stop').replace_with(db_stop)
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(website).should_receive('message_handling_loop').replace_with(message_handling_loop)
            
            # test start
            args = [self.cfg,Queue(),Pipe()[1],Queue()]
            web = website(args)
            if not web:
                raise Exception('did not return website object')
            try:
                for _ in xrange(50): # try for 5 seconds
                    if (tornado.ioloop.IOLoop.initialized() and 
                        tornado.ioloop.IOLoop.instance()._running):
                        break
                    time.sleep(0.1)
                if not (tornado.ioloop.IOLoop.initialized() and 
                        tornado.ioloop.IOLoop.instance()._running):
                    raise Exception('did not start tornado')
                if not hasattr(web,'nginx') or not web.nginx:
                    raise Exception('did not start nginx')
                if not hasattr(web,'http_server') or not web.http_server:
                    raise Exception('did not start http_server')
                if not db_start.called:
                    raise Exception('did not start DB')
            except:
                web.kill()
                raise
            else:
                # test stop
                web.stop()
                if not db_stop.called:
                    raise Exception('did not stop DB')
            
        except Exception as e:
            logger.error('Error running website start_stop test - %s',str(e))
            printer('Test website start_stop',False)
            raise
        else:
            printer('Test website start_stop')
    
    def test_10_JSONRPCHandler(self):
        """Test JSONRPCHandler"""
        try:
            # test that objects are called on startup
            passkey = 'passkey'
            def db_start(*args,**kwargs):
                db_start.called = True
            db_start.called = False
            flexmock(MetaDB).should_receive('start').replace_with(db_start)
            def db_stop(*args,**kwargs):
                db_stop.called = True
            db_stop.called = False
            flexmock(MetaDB).should_receive('stop').replace_with(db_stop)
            def f(*args,**kwargs):
                ret = f.returns.pop(0)
                logger.info('f() returns %r',ret)
                if 'callback' in kwargs:
                    kwargs['callback'](ret)
                else:
                    return ret
            flexmock(MetaDB).should_receive('__getattr__').replace_with(lambda a:f)
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(website).should_receive('message_handling_loop').replace_with(message_handling_loop)
            
            args = [self.cfg,Queue(),Pipe()[1],Queue()]
            web = website(args)
            try:
                for _ in xrange(50): # try for 5 seconds
                    if (tornado.ioloop.IOLoop.initialized() and 
                        tornado.ioloop.IOLoop.instance()._running):
                        break
                    time.sleep(0.1)
                if not (tornado.ioloop.IOLoop.initialized() and 
                        tornado.ioloop.IOLoop.instance()._running):
                    raise Exception('did not start tornado')
                
                address = 'localhost:%d/jsonrpc'%(
                          self.cfg['webserver']['tornado_port'])
                logger.info('try connecting directly to tornado at %s',address)
                
                ssl_opts = {'ca_certs': self.cfg['system']['ssl_cacert'],
                            'username': self.cfg['download']['http_username'],
                            'password': self.cfg['download']['http_password'],
                           }
                
                iceprod.core.jsonRPCclient.JSONRPC.start(address=address,
                                                         passkey=passkey,
                                                         ssl_options=ssl_opts)
                try:
                    f.returns = [True,'testing']
                    ret = iceprod.core.jsonRPCclient.JSONRPC.test()
                    if ret != 'testing':
                        raise Exception('bad ret from JSONRPC.test()')
                    
                    f.returns = [True,Exception()]
                    try:
                        iceprod.core.jsonRPCclient.JSONRPC.test()
                    except:
                        pass
                    else:
                        raise Exception('JSONRPC.test() did not raise Exception')
                finally:
                    iceprod.core.jsonRPCclient.JSONRPC.stop()
                
                time.sleep(0.1)
                
                address = '%s:%d/jsonrpc'%(self.hostname,
                                   self.cfg['webserver']['port'])
                logger.info('try connecting to nginx->tornado at %s',address)
                
                iceprod.core.jsonRPCclient.JSONRPC.start(address=address,
                                                         passkey=passkey,
                                                         ssl_options=ssl_opts)
                try:
                    f.returns = [True,'testing']
                    ret = iceprod.core.jsonRPCclient.JSONRPC.test()
                    if ret != 'testing':
                        raise Exception('bad ret from JSONRPC.test()')
                    
                    f.returns = [True,Exception()]
                    try:
                        iceprod.core.jsonRPCclient.JSONRPC.test()
                    except:
                        pass
                    else:
                        raise Exception('JSONRPC.test() did not raise Exception')
                finally:
                    iceprod.core.jsonRPCclient.JSONRPC.stop()
                
            finally:
                web.stop()
            
        except Exception as e:
            logger.error('Error running website JSONRPCHandler test - %s',str(e))
            printer('Test website JSONRPCHandler',False)
            raise
        else:
            printer('Test website JSONRPCHandler')
    
    def test_20_LibHandler(self):
        """Test LibHandler"""
        try:
            passkey = 'passkey'
            def db_start(*args,**kwargs):
                db_start.called = True
            db_start.called = False
            flexmock(MetaDB).should_receive('start').replace_with(db_start)
            def db_stop(*args,**kwargs):
                db_stop.called = True
            db_stop.called = False
            flexmock(MetaDB).should_receive('stop').replace_with(db_stop)
            def message_handling_loop():
                message_handling_loop.called = True
            flexmock(website).should_receive('message_handling_loop').replace_with(message_handling_loop)
            
            pycurl_handle = dataclasses.PycURL()
            args = [self.cfg,Queue(),Pipe()[1],Queue()]
            web = website(args)
            try:
                for _ in xrange(50): # try for 5 seconds
                    if (tornado.ioloop.IOLoop.initialized() and 
                        tornado.ioloop.IOLoop.instance()._running):
                        break
                    time.sleep(0.1)
                if not (tornado.ioloop.IOLoop.initialized() and 
                        tornado.ioloop.IOLoop.instance()._running):
                    raise Exception('did not start tornado')
                
                address = 'localhost:%d/lib/'%(
                          self.cfg['webserver']['tornado_port'])
                logger.info('try connecting directly to tornado at %s',address)
                
                ssl_opts = {}
                
                # run normally
                extras = 'extras_%d.tar.gz'%(random.randint(0,10000))
                extras_path = os.path.join(self.cfg['webserver']['lib_dir'],
                                           extras)
                extras_data = os.urandom(10**7)
                with open(extras_path,'w') as f:
                    f.write(extras_data)
                outfile = os.path.join(self.test_dir,
                                       str(random.randint(0,10000)))
                pycurl_handle.fetch(address+extras,outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('file not fetched')
                if open(outfile).read() != extras_data:
                    raise Exception('fetched file data incorrect')
                os.unlink(outfile)
                
                # test for browsability
                try:
                    pycurl_handle.fetch(address,outfile,**ssl_opts)
                except:
                    pass
                else:
                    raise Exception('did not raise exception when testing browsability')
                
                # test for bad file
                extras = 'extras_%d.tar.gz'%(random.randint(0,10000))
                try:
                    pycurl_handle.fetch(address+extras,outfile,**ssl_opts)
                except:
                    pass
                else:
                    raise Exception('did not raise exception when testing bad file')
                
                
                address = 'localhost:%d/lib/'%(
                          self.cfg['webserver']['port'])
                logger.info('try connecting through nginx at %s',address)
                
                ssl_opts = {'cacert': self.cfg['system']['ssl_cacert'],
                            'username': self.cfg['download']['http_username'],
                            'password': self.cfg['download']['http_password'],
                           }
                
                # run normally
                extras = 'extras_%d.tar.gz'%(random.randint(0,10000))
                extras_path = os.path.join(self.cfg['webserver']['lib_dir'],
                                           extras)
                extras_data = os.urandom(10**7)
                with open(extras_path,'w') as f:
                    f.write(extras_data)
                outfile = os.path.join(self.test_dir,
                                       str(random.randint(0,10000)))
                pycurl_handle.fetch(address+extras,outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('nginx: file not fetched')
                if open(outfile).read() != extras_data:
                    raise Exception('nginx: fetched file data incorrect')
                os.unlink(outfile)
                
                # test for browsability
                try:
                    pycurl_handle.fetch(address,outfile,**ssl_opts)
                except:
                    pass
                else:
                    raise Exception('nginx: did not raise exception when testing browsability')
                
                # test for bad file
                extras = 'extras_%d.tar.gz'%(random.randint(0,10000))
                try:
                    pycurl_handle.fetch(address+extras,outfile,**ssl_opts)
                except:
                    pass
                else:
                    raise Exception('nginx: did not raise exception when testing bad file')
                
            finally:
                web.stop()
            
        except Exception as e:
            logger.error('Error running website LibHandler test - %s',str(e))
            printer('Test website LibHandler',False)
            raise
        else:
            printer('Test website LibHandler')
    
    #def test_21_DownloadHandler(self):
    #    """Test DownloadHandler"""
    #    try:
    #        # test that objects are called on startup
    #        passkey = 'passkey'
    #        def db_start(*args,**kwargs):
    #            db_start.called = True
    #        db_start.called = False
    #        flexmock(MetaDB).should_receive('start').replace_with(db_start)
    #        def db_stop(*args,**kwargs):
    #            db_stop.called = True
    #        db_stop.called = False
    #        flexmock(MetaDB).should_receive('stop').replace_with(db_stop)
    #        def f(*args,**kwargs):
    #            ret = f.returns.pop(0)
    #            logger.info('f() returns %r',ret)
    #            if 'callback' in kwargs:
    #                kwargs['callback'](ret)
    #            else:
    #                return ret
    #        flexmock(MetaDB).should_receive('__getattr__').replace_with(lambda a:f)
    #        def message_handling_loop():
    #            message_handling_loop.called = True
    #        flexmock(website).should_receive('message_handling_loop').replace_with(message_handling_loop)
    #        
    #        def proxy_func(name,*args,**kwargs):
    #            proxy_func.called = name
    #            if proxy_func.ret:
    #                kwargs['writer'](proxy_func.ret)
    #                kwargs['callback']()
    #            else:
    #                kwargs['error']('error')
    #        proxy_func.ret = 'OK'
    #        flexmock(Proxy).should_receive('size_request').replace_with(
    #            partial(proxy_func,'size_request'))
    #        flexmock(Proxy).should_receive('checksum_request').replace_with(
    #            partial(proxy_func,'checksum_request'))
    #        flexmock(Proxy).should_receive('download_request').replace_with(
    #            partial(proxy_func,'download_request'))
    #        
    #        pycurl_handle = dataclasses.PycURL()
    #        def writer(data):
    #            writer.data += data
    #        writer.data = ''
    #        
    #        args = [self.cfg,Queue(),Pipe()[1],Queue()]
    #        web = website(args)
    #        try:
    #            for _ in xrange(50): # try for 5 seconds
    #                if (tornado.ioloop.IOLoop.initialized() and 
    #                    tornado.ioloop.IOLoop.instance()._running):
    #                    break
    #                time.sleep(0.1)
    #            if not (tornado.ioloop.IOLoop.initialized() and 
    #                    tornado.ioloop.IOLoop.instance()._running):
    #                raise Exception('did not start tornado')
    #            
    #            address = 'localhost:%d/download'%(
    #                      self.cfg['webserver']['tornado_port'])
    #            logger.info('try connecting directly to tornado at %s',address)
    #            
    #            kwargs = {'cacert': self.cfg['system']['ssl_cacert'],
    #                      'username': self.cfg['download']['http_username'],
    #                      'password': self.cfg['download']['http_password']
    #                     }
    #            
    #            body = json_encode({'type':'download',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True]
    #            writer.data = ''
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            if proxy_func.called != 'download_request':
    #                raise Exception('download_request not called')
    #            
    #            f.returns = [True]
    #            proxy_func.ret = False
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raiseException('error expected for download_request')
    #            if proxy_func.called != 'download_request':
    #                raise Exception('download_request not called')
    #            
    #            body = json_encode({'type':'checksum',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True]
    #            writer.data = ''
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            if proxy_func.called != 'checksum_request':
    #                raise Exception('checksum_request not called')
    #            
    #            f.returns = [True]
    #            proxy_func.ret = False
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raise Exception('error expected for checksum_request')
    #            if proxy_func.called != 'checksum_request':
    #                raise Exception('checksum_request not called')
    #            
    #            body = json_encode({'type':'size',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True]
    #            writer.data = ''
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            if proxy_func.called != 'size_request':
    #                raise Exception('size_request not called')
    #            
    #            f.returns = [True]
    #            proxy_func.ret = False
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raise Exception('error expected for size_request')
    #            if proxy_func.called != 'size_request':
    #                raise Exception('size_request not called')
    #            
    #            body = json_encode({'type':'tests',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            f.returns = [True]
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raise Exception('error expected for bad request type')
    #            
    #            
    #            
    #            time.sleep(0.1)
    #            
    #            address = '%s:%d/download'%(self.hostname,
    #                               self.cfg['webserver']['port'])
    #            logger.info('try connecting to nginx->tornado at %s',address)
    #            
    #            
    #            body = json_encode({'type':'download',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True]
    #            writer.data = ''
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            if proxy_func.called != 'download_request':
    #                raise Exception('download_request not called')
    #            
    #            f.returns = [True]
    #            proxy_func.ret = False
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raiseException('error expected for download_request')
    #            if proxy_func.called != 'download_request':
    #                raise Exception('download_request not called')
    #            
    #            body = json_encode({'type':'checksum',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True]
    #            writer.data = ''
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            if proxy_func.called != 'checksum_request':
    #                raise Exception('checksum_request not called')
    #            
    #            f.returns = [True]
    #            proxy_func.ret = False
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raise Exception('error expected for checksum_request')
    #            if proxy_func.called != 'checksum_request':
    #                raise Exception('checksum_request not called')
    #            
    #            body = json_encode({'type':'size',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True]
    #            writer.data = ''
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            if proxy_func.called != 'size_request':
    #                raise Exception('size_request not called')
    #            
    #            f.returns = [True]
    #            proxy_func.ret = False
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raise Exception('error expected for size_request')
    #            if proxy_func.called != 'size_request':
    #                raise Exception('size_request not called')
    #            
    #            body = json_encode({'type':'tests',
    #                                'url':address+'/test',
    #                                'key':passkey,
    #                               });
    #            f.returns = [True]
    #            proxy_func.ret = 'OK'
    #            proxy_func.called = None
    #            try:
    #                ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            except:
    #                pass
    #            else:
    #                raise Exception('error expected for bad request type')
    #            
    #        finally:
    #            web.stop()
    #        
    #    except Exception as e:
    #        logger.error('Error running website DownloadHandler test - %s',str(e))
    #        printer('Test website DownloadHandler',False)
    #        raise
    #    else:
    #        printer('Test website DownloadHandler')
    #
    #def test_30_upload(self):
    #    """Test upload"""
    #    try:
    #        # test that objects are called on startup
    #        passkey = 'passkey'
    #        def db_start(*args,**kwargs):
    #            db_start.called = True
    #        db_start.called = False
    #        flexmock(MetaDB).should_receive('start').replace_with(db_start)
    #        def db_stop(*args,**kwargs):
    #            db_stop.called = True
    #        db_stop.called = False
    #        flexmock(MetaDB).should_receive('stop').replace_with(db_stop)
    #        def f(*args,**kwargs):
    #            f.names.append(kwargs['func_name'])
    #            ret = f.returns.pop(0)
    #            logger.info('f(func_name=%s) returns %r',kwargs['func_name'],ret)
    #            if 'callback' in kwargs:
    #                kwargs['callback'](ret)
    #            else:
    #                return ret
    #        flexmock(MetaDB).should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
    #        def message_handling_loop():
    #            message_handling_loop.called = True
    #        flexmock(website).should_receive('message_handling_loop').replace_with(message_handling_loop)
    #        
    #        pycurl_handle = dataclasses.PycURL()
    #        def writer(data):
    #            writer.data += data
    #        writer.data = ''
    #        
    #        # make 10M test data file
    #        filename = str(random.randint(0,10000))
    #        filecontents = os.urandom(10**7)
    #        dest_path = os.path.join(self.test_dir,filename)
    #        with open(dest_path,'w') as file:
    #            file.write(filecontents)
    #        size = os.path.getsize(dest_path)
    #        chksum = functions.sha512sum(dest_path)
    #        
    #        args = [self.cfg,Queue(),Pipe()[1],Queue()]
    #        web = website(args)
    #        try:
    #            for _ in xrange(50): # try for 5 seconds
    #                if (tornado.ioloop.IOLoop.initialized() and 
    #                    tornado.ioloop.IOLoop.instance()._running):
    #                    break
    #                time.sleep(0.1)
    #            if not (tornado.ioloop.IOLoop.initialized() and 
    #                    tornado.ioloop.IOLoop.instance()._running):
    #                raise Exception('did not start tornado')
    #            
    #            address = '%s:%d/upload'%(self.hostname,
    #                               self.cfg['webserver']['port'])
    #            logger.info('try connecting to nginx->tornado at %s',address)
    #            
    #            kwargs = {'cacert': self.cfg['system']['ssl_cacert'],
    #                      'username': self.cfg['download']['http_username'],
    #                      'password': self.cfg['download']['http_password']
    #                     }
    #            
    #            # do pre-upload
    #            body = json_encode({'type':'upload',
    #                                'url':address,
    #                                'size':size,
    #                                'checksum':chksum,
    #                                'checksum_type':'sha512',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True,'testurl']
    #            f.names = []
    #            writer.data = ''
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            try:
    #                ret = json_decode(writer.data)
    #            except:
    #                raise Exception('initial request: ret is not json')
    #            if 'type' not in ret or ret['type'] != 'upload':
    #                raise Exception('initial request: type incorrect')
    #            if 'url' not in ret or ret['url'] != address:
    #                raise Exception('initial request: url incorrect')
    #            if 'upload' not in ret or ret['upload'] != '/upload/testurl':
    #                raise Exception('initial request: upload incorrect')
    #            if (len(f.names) < 2 or f.names[0] != 'authorize_task' or
    #                f.names[1] != 'new_upload'):
    #                logger.info('DB funcs: %r',f.names)
    #                raise Exception('initial request: DB funcs incorrect')
    #            
    #            # move to actual upload
    #            f.returns = [True,True]
    #            f.names = []
    #            ret = pycurl_handle.put(address+'/testurl',dest_path,**kwargs)
    #            if (len(f.names) < 2 or f.names[0] != 'is_upload_addr' or
    #                f.names[1] != 'handle_upload'):
    #                logger.info('DB funcs: %r',f.names)
    #                raise Exception('upload request: DB funcs incorrect')
    #            
    #            # do check-upload
    #            body = json_encode({'type':'check',
    #                                'url':address+'/testurl',
    #                                'key':passkey,
    #                               });
    #            
    #            f.returns = [True,True]
    #            f.names = []
    #            writer.data = ''
    #            ret = pycurl_handle.post(address,writer,postbody=body,**kwargs)
    #            try:
    #                ret = json_decode(writer.data)
    #            except:
    #                raise Exception('check request: ret is not json')
    #            if 'type' not in ret or ret['type'] != 'check':
    #                raise Exception('check request: type incorrect')
    #            if 'url' not in ret or ret['url'] != address+'/testurl':
    #                raise Exception('check request: url incorrect')
    #            if 'result' not in ret or ret['result'] != True:
    #                raise Exception('check request: result incorrect')
    #            if (len(f.names) < 2 or f.names[0] != 'authorize_task' or
    #                f.names[1] != 'check_upload'):
    #                logger.info('DB funcs: %r',f.names)
    #                raise Exception('check request: DB funcs incorrect')
    #            
    #        finally:
    #            web.stop()
    #        
    #    except Exception as e:
    #        logger.error('Error running website upload test - %s',str(e))
    #        printer('Test website upload',False)
    #        raise
    #    else:
    #        printer('Test website upload')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()    
    alltests = glob_tests(loader.getTestCaseNames(website_test))
    suite.addTests(loader.loadTestsFromNames(alltests,website_test))
    return suite
