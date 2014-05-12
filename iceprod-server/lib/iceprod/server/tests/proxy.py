#! /usr/bin/env python
"""
  Test script for proxy

  copyright (c) 2012 the icecube collaboration
"""

from __future__ import print_function
try:
    from server_tester import printer, glob_tests, logger
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
    logger = logging.getLogger('server_tester')

import os, sys, time
import shutil
import random
import stat
import StringIO
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from threading import Thread

import tornado.escape
import tornado.httpclient
import tornado.httpserver
import tornado.ioloop

from pyuv_tornado import fs

from flexmock import flexmock

from iceprod.core import functions,dataclasses
from iceprod.server.dbclient import MetaDB,DB
from iceprod.server import proxy


class proxy_test(unittest.TestCase):
    def setUp(self):
        super(proxy_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
        # mock DB
        @classmethod
        def start(cls):
            cls.__db = True
        @classmethod
        def stop(cls):
            cls.__db = None
        MetaDB.start = start
        MetaDB.stop = stop
        def f(callback=None):
            if callback:
                callback()
            return 'f'
        flexmock(MetaDB).should_receive('__getattr__').and_return(f)
        DB.start()
        
        # get hostname
        hostname = functions.gethostname()
        if hostname is None:
            raise Exception('Cannot get hostname')
        elif isinstance(hostname,set):
            hostname = hostname.pop()
        self.hostname = hostname
        
    
    def tearDown(self):
        DB.stop()
        shutil.rmtree(self.test_dir)
        super(proxy_test,self).tearDown()
    
    def test_01_calc_checksum(self):
        """Test calc_checksum"""
        try:
            # create file
            filename = os.path.join(self.test_dir,'cksm_test')
            with open(filename,'w') as f:
                f.write('testing the checksum functions')
            
            def cb(ret):
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()   
            cb.ret = False
            
            for type in ('md5','sha1','sha256','sha512'):
                # calc checksum of file with proxy (async) method
                proxy.calc_checksum(filename,type=type,callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                ret = cb.ret
                
                # calc checksum of file with regular method
                cksm = functions.cksm(filename,type)
                
                if ret != cksm:
                    raise Exception('Checksums not equal:  ret=%r and cksm=%r'%(ret,cksm))
        
        except Exception, e:
            logger.error('Error running proxy calc_checksum test - %s',str(e))
            printer('Test proxy calc_checksum',False)
            raise
        else:
            printer('Test proxy calc_checksum')

    def test_02_configure_username(self):
        """Test Proxy.configure username"""
        try:
            
            cfg = proxy.Proxy._cfg
            try:
                proxy.Proxy.configure(username='usr',password='pwd')
                if proxy.Proxy._cfg['username'] != 'usr':
                    raise Exception('username not set')
                if proxy.Proxy._cfg['password'] != 'pwd':
                    raise Exception('password not set')
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy configure test - %s',str(e))
            printer('Test proxy configure user/pass',False)
            raise
        else:
            printer('Test proxy configure user/pass')
    
    def test_03_configure_ssl(self):
        """Test Proxy.configure ssl"""
        try:
            # create file
            filename = os.path.join(self.test_dir,'sslcert')
            with open(filename,'w') as f:
                f.write('sslcert')
            
            cfg = proxy.Proxy._cfg
            try:
                proxy.Proxy.configure(sslcert=filename,
                                sslkey=filename,
                                cacert=filename)
                if proxy.Proxy._cfg['sslcert'] != filename:
                    raise Exception('sslcert not set')
                if proxy.Proxy._cfg['sslkey'] != filename:
                    raise Exception('sslkey not set')
                if proxy.Proxy._cfg['cacert'] != filename:
                    raise Exception('cacert not set')
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy configure test - %s',str(e))
            printer('Test proxy configure ssl',False)
            raise
        else:
            printer('Test proxy configure ssl')
    
    def test_04_configure_misc(self):
        """Test Proxy.configure misc options"""
        import stat
        try:
            cfg = proxy.Proxy._cfg
            try:
                proxy.Proxy.configure(request_timeout=1,
                                download_dir=self.test_dir,
                                cache_file_stat=764)
                st = stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP
                st |= stat.S_IWGRP|stat.S_IROTH
                if proxy.Proxy._cfg['request_timeout'] != 1:
                    raise Exception('request_timeout not set')
                if proxy.Proxy._cfg['download_dir'] != self.test_dir:
                    raise Exception('download_dir not set')
                if proxy.Proxy._cfg['cache_file_stat'] != st:
                    raise Exception('cache_file_stat not set')
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy configure test - %s',str(e))
            printer('Test proxy configure misc',False)
            raise
        else:
            printer('Test proxy configure misc')
    
    def test_05_getprefix(self):
        """Test Proxy.getprefix"""
        try:
            cfg = proxy.Proxy._cfg
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                ret = proxy.Proxy.getprefix('http://www.test.com/testing/s')
                if ret != 'http':
                    raise Exception('should be http but returned %r'%ret)
                
                ret = proxy.Proxy.getprefix('https://www.test.com/testing/s')
                if ret != 'https':
                    raise Exception('should be https but returned %r'%ret)
                
                ret = proxy.Proxy.getprefix('ftp://www.test.com/testing/s')
                if ret != 'ftp':
                    raise Exception('should be ftp but returned %r'%ret)
                
                ret = proxy.Proxy.getprefix('gsiftp://www.test.com/testing/s')
                if ret != 'gsiftp':
                    raise Exception('should be gsiftp but returned %r'%ret)
                
                ret = proxy.Proxy.getprefix('test//www.test.com/testing/s')
                if ret is not None:
                    raise Exception('should be None but returned %r'%ret)
                
                ret = proxy.Proxy.getprefix('http://www.test.com/testing/s?sd{f:3&ns]d:sdfn')
                if ret != 'http':
                    raise Exception('should be http but returned %r'%ret)
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy getprefix test - %s',str(e))
            printer('Test proxy getprefix',False)
            raise
        else:
            printer('Test proxy getprefix')
    
    def test_06_newfilename(self):
        """Test Proxy.newfilename"""
        try:
            cfg = proxy.Proxy._cfg
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                ret = proxy.Proxy.newfilename()
                if os.path.dirname(ret) != self.test_dir:
                    raise Exception('returned %r'%ret)
                
                ret2 = proxy.Proxy.newfilename()
                if os.path.dirname(ret2) != self.test_dir:
                    raise Exception('returned %r'%ret2)
                if ret == ret2:
                    raise Exception('duplicate filename')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy newfilename test - %s',str(e))
            printer('Test proxy newfilename',False)
            raise
        else:
            printer('Test proxy newfilename')
    
    def test_10_cache_stream(self):
        """Test Proxy.cache_stream"""
        try:
            data = 'this is a test'
            cfg = proxy.Proxy._cfg
            
            filename = os.path.join(self.test_dir,'testout')
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                callback()
            flusher.called = False
            def cb(error=None):
                cb.called = True
                cb.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.error = None
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                fh = open(filename,'w')
                try:
                    proxy.Proxy.cache_stream(data,
                                             fh=fh.fileno(),
                                             writer=writer,
                                             flusher=flusher,
                                             error=cb,
                                             callback=cb)
                    tornado.ioloop.IOLoop.instance().start()
                finally:
                    fh.close()
                
                filedata = open(filename).read()
                if writer.data != data:
                    raise Exception('writer.data = %r, but supposed to be %r'%(writer.data,data))
                if filedata != data:
                    raise Exception('file data = %r, but supposed to be %r'%(filedata,data))
                if flusher.called is not True:
                    raise Exception('flusher not called')
                if cb.called is not True:
                    raise Exception('callback not called')
                if cb.error:
                    raise Exception('error called: %r',cb.error)
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy cache_stream test - %s',str(e))
            printer('Test proxy cache_stream',False)
            raise
        else:
            printer('Test proxy cache_stream')
    
    def test_11_passthrough_stream(self):
        """Test Proxy.passthrough_stream"""
        try:
            data = 'this is a test'
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                callback()
            flusher.called = False
            def cb(error=None):
                cb.called = True
                cb.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.error = None
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                proxy.Proxy.passthrough_stream(data,
                                               writer=writer,
                                               flusher=flusher,
                                               error=cb,
                                               callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if writer.data != data:
                    raise Exception('writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('flusher not called')
                if cb.called is not True:
                    raise Exception('callback not called')
                if cb.error:
                    raise Exception('error called: %r',cb.error)
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy passthrough_stream test - %s',str(e))
            printer('Test proxy passthrough_stream',False)
            raise
        else:
            printer('Test proxy passthrough_stream')
    
    def test_12_cache_end(self):
        """Test Proxy.cache_end"""
        try:
            data = 'this is a test'
            
            filename = os.path.join(self.test_dir,'testout')
            
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            
            def cb(error=None):
                cb.called = True
                cb.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.error = None
            
            req = tornado.httpclient.HTTPRequest('http://test')
            resp = tornado.httpclient.HTTPResponse(req,200)
            
            cfg = proxy.Proxy._cfg
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                fh = fs.open(filename,os.O_WRONLY|os.O_CREAT,stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
                try:
                    proxy.Proxy.cache_end(resp,
                                          fh=fh,
                                          filename=filename,
                                          uid='n3wlk',
                                          error=cb,
                                          callback=cb)
                    tornado.ioloop.IOLoop.instance().start()
                except:
                    if fh:
                        try:
                            fs.close(fh)
                        except:
                            pass
                    raise
                
                if add_to_cache.called is not True:
                    raise Exception('DB.add_to_cache not called')
                if cb.called is not True:
                    raise Exception('callback not called')
                if cb.error:
                    raise Exception('error called: %r',cb.error)
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy cache_end test - %s',str(e))
            printer('Test proxy cache_end',False)
            raise
        else:
            printer('Test proxy cache_end')
    
    def test_13_passthrough_end(self):
        """Test Proxy.passthrough_end"""
        try:
            data = 'this is a test'
            cfg = proxy.Proxy._cfg
            
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            req = tornado.httpclient.HTTPRequest('http://test')
            resp = tornado.httpclient.HTTPResponse(req,200)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                try:
                    proxy.Proxy.passthrough_end(resp,
                                                error=cberr,
                                                callback=cb)
                    tornado.ioloop.IOLoop.instance().start()
                except:
                    try:
                        fh.close()
                    except:
                        pass
                    raise
                
                if cb.called is not True:
                    raise Exception('callback not called')
                if cberr.called is True:
                    raise Exception('err called')
                if cberr.error:
                    raise Exception('error called: %r',cberr.error)
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy passthrough_end test - %s',str(e))
            printer('Test proxy passthrough_end',False)
            raise
        else:
            printer('Test proxy passthrough_end')
    
    def test_14_cache_request(self):
        """Test Proxy.cache_request"""
        try:
            data = 'this is a test'
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                if callback:
                    callback()
            flusher.called = False
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                add_to_cache.uid = uid
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            add_to_cache.uid = None
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](data)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(data)
                    kwargs['callback'](resp)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                proxy.Proxy.cache_request('http://test',
                                          setheader=None,
                                          writer=writer,
                                          flusher=flusher,
                                          error=cberr,
                                          callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if writer.data != data:
                    raise Exception('http: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('http: flusher not called')
                if add_to_cache.called is not True:
                    raise Exception('http: DB.add_to_cache not called')
                
                
                writer.data = None
                flusher.called = False
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                add_to_cache.uid = None
                proxy.Proxy.cache_request('gsiftp://test',
                                          setheader=None,
                                          writer=writer,
                                          flusher=flusher,
                                          error=cberr,
                                          callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if writer.data != data:
                    raise Exception('gsiftp: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('gsiftp: flusher not called')
                if add_to_cache.called is not True:
                    raise Exception('gsiftp: DB.add_to_cache not called')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy cache_request test - %s',str(e))
            printer('Test proxy cache_request',False)
            raise
        else:
            printer('Test proxy cache_request')
    
    def test_15_passthrough_request(self):
        """Test Proxy.passthrough_request"""
        try:
            data = 'this is a test'
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                if callback:
                    callback()
            flusher.called = False
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](data)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(data)
                    kwargs['callback'](resp)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                proxy.Proxy.passthrough_request('http://test',
                                                setheader=None,
                                                writer=writer,
                                                flusher=flusher,
                                                error=cberr,
                                                callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if writer.data != data:
                    raise Exception('http: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('http: flusher not called')                
                
                writer.data = None
                flusher.called = False
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                proxy.Proxy.passthrough_request('gsiftp://test',
                                                setheader=None,
                                                writer=writer,
                                                flusher=flusher,
                                                error=cberr,
                                                callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if writer.data != data:
                    raise Exception('gsiftp: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('gsiftp: flusher not called')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy passthrough_request test - %s',str(e))
            printer('Test proxy passthrough_request',False)
            raise
        else:
            printer('Test proxy passthrough_request')
    
    def test_16_passthrough_size_request(self):
        """Test Proxy.passthrough_size_request"""
        try:
            data = 125
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](data)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(data)
                    kwargs['callback'](resp)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(get)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                proxy.Proxy.passthrough_size_request('http://test',
                                                setheader=None,
                                                writer=writer,
                                                error=cberr,
                                                callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if writer.data != data:
                    raise Exception('http: writer.data = %r, but supposed to be %r'%(writer.data,data))
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                proxy.Proxy.passthrough_size_request('gsiftp://test',
                                                setheader=None,
                                                writer=writer,
                                                error=cberr,
                                                callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if writer.data != data:
                    raise Exception('gsiftp: writer.data = %r, but supposed to be %r'%(writer.data,data))
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy passthrough_size_request test - %s',str(e))
            printer('Test proxy passthrough_size_request',False)
            raise
        else:
            printer('Test proxy passthrough_size_request')
    
    def test_17_passthrough_checksum_request(self):
        """Test Proxy.passthrough_checksum_request"""
        try:
            data = '23klj4oasijfoi34nakln4'
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](data)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(data)
                    kwargs['callback'](resp)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(get)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                proxy.Proxy.passthrough_checksum_request('http://test',
                                                         setheader=None,
                                                         writer=writer,
                                                         error=cberr,
                                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if writer.data != data:
                    raise Exception('http: writer.data = %r, but supposed to be %r'%(writer.data,data))
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                proxy.Proxy.passthrough_checksum_request('gsiftp://test',
                                                         setheader=None,
                                                         writer=writer,
                                                         error=cberr,
                                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if writer.data != data:
                    raise Exception('gsiftp: writer.data = %r, but supposed to be %r'%(writer.data,data))
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy passthrough_checksum_request test - %s',str(e))
            printer('Test proxy passthrough_checksum_request',False)
            raise
        else:
            printer('Test proxy passthrough_checksum_request')
    
    def test_18_send_from_cache(self):
        """Test Proxy.send_from_cache"""
        try:
            data = 'this is a test'
            filename = os.path.join(self.test_dir,'testout')
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                if callback:
                    callback()
            flusher.called = False
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            with open(filename,'w') as f:
                f.write(data)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                proxy.Proxy.send_from_cache(filename,
                                            writer=writer,
                                            flusher=flusher,
                                            error=cberr,
                                            callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('err called')
                if cb.called is not True:
                    raise Exception('callback not called')
                if writer.data != data:
                    raise Exception('writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('flusher not called')                
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy send_from_cache test - %s',str(e))
            printer('Test proxy send_from_cache',False)
            raise
        else:
            printer('Test proxy send_from_cache')
    
    def test_20_download_request(self):
        """Test Proxy.download_request"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                if callback:
                    callback()
            flusher.called = False
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True,file_uid)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':'asdfasdf'})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':'asdfasdf'}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':'asdfasdf'}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.download_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             flusher=flusher,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if writer.data != data:
                    raise Exception('http: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('http: flusher not called')  
                if add_to_cache.called is True:
                    raise Exception('http: DB.add_to_cache called, but was already in cache')
                
                writer.data = None
                flusher.called = False
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.download_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             flusher=flusher,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if writer.data != data:
                    raise Exception('gsiftp: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('gsiftp: flusher not called')
                if add_to_cache.called is True:
                    raise Exception('gsiftp: DB.add_to_cache called, but was already in cache')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy download_request test - %s',str(e))
            printer('Test proxy download_request',False)
            raise
        else:
            printer('Test proxy download_request')
    
    def test_21_download_request(self):
        """Test Proxy.download_request incache=False"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                if callback:
                    callback()
            flusher.called = False
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.download_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             flusher=flusher,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error is None:
                    raise Exception('http: error not called')
                if cberr.called is not True:
                    raise Exception('http: err not called')
                
                writer.data = None
                flusher.called = False
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.download_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             flusher=flusher,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error is None:
                    raise Exception('gsiftp: error not called')
                if cberr.called is not True:
                    raise Exception('gsiftp: err not called')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy download_request test - %s',str(e))
            printer('Test proxy download_request incache=False',False)
            raise
        else:
            printer('Test proxy download_request incache=False')
    
    def test_22_download_request(self):
        """Test Proxy.download_request incache=False url!=host"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def flusher(callback=None):
                flusher.called = True
                if callback:
                    callback()
            flusher.called = False
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.download_request(req,
                                             host='other',
                                             setheader=None,
                                             writer=writer,
                                             flusher=flusher,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if writer.data != data:
                    raise Exception('http: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('http: flusher not called')
                if add_to_cache.called is not True:
                    raise Exception('http: DB.add_to_cache not called')
                
                writer.data = None
                flusher.called = False
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.download_request(req,
                                             host='other',
                                             setheader=None,
                                             writer=writer,
                                             flusher=flusher,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if writer.data != data:
                    raise Exception('gsiftp: writer.data = %r, but supposed to be %r'%(writer.data,data))
                if flusher.called is not True:
                    raise Exception('gsiftp: flusher not called')
                if add_to_cache.called is not True:
                    raise Exception('gsiftp: DB.add_to_cache not called')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy download_request test - %s',str(e))
            printer('Test proxy download_request incache=False url!=host',False)
            raise
        else:
            printer('Test proxy download_request incache=False url!=host')
    
    def test_25_size_request(self):
        """Test Proxy.size_request"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True,file_uid)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def get_cache_size(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True,len(data))
            flexmock(DB).should_receive('get_cache_size').replace_with(get_cache_size)
            def get_cache_checksum(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True,checksum)
            flexmock(DB).should_receive('get_cache_checksum').replace_with(get_cache_checksum)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.size_request(req,
                                         host='test.ing',
                                         setheader=None,
                                         writer=writer,
                                         error=cberr,
                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if add_to_cache.called is True:
                    raise Exception('http: DB.add_to_cache called, but was already in cache')
                if not isinstance(writer.data,dict):
                    raise Exception('http: writer.data is not json: %r'%writer.data)
                if 'size' not in writer.data:
                    raise Exception('http: writer.data does not contain size')
                if writer.data['size'] != len(data):
                    raise Exception('http: size= %r, but supposed to be %r'%(writer.data['size'],len(data)))
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.size_request(req,
                                         host='test.ing',
                                         setheader=None,
                                         writer=writer,
                                         error=cberr,
                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if add_to_cache.called is True:
                    raise Exception('gsiftp: DB.add_to_cache called, but was already in cache')
                if not isinstance(writer.data,dict):
                    raise Exception('gsiftp: writer.data is not json: %r'%writer.data)
                if 'size' not in writer.data:
                    raise Exception('gsiftp: writer.data does not contain size')
                if writer.data['size'] != len(data):
                    raise Exception('gsiftp: size= %r, but supposed to be %r'%(writer.data['size'],len(data)))
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy size_request test - %s',str(e))
            printer('Test proxy size_request',False)
            raise
        else:
            printer('Test proxy size_request')
    
    def test_26_size_request(self):
        """Test Proxy.size_request incache=False"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,file_uid)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def get_cache_size(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,len(data))
            flexmock(DB).should_receive('get_cache_size').replace_with(get_cache_size)
            def get_cache_checksum(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,checksum)
            flexmock(DB).should_receive('get_cache_checksum').replace_with(get_cache_checksum)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.size_request(req,
                                         host='test.ing',
                                         setheader=None,
                                         writer=writer,
                                         error=cberr,
                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error is None:
                    raise Exception('http: error not called')
                if cberr.called is not True:
                    raise Exception('http: err not called')
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.size_request(req,
                                         host='test.ing',
                                         setheader=None,
                                         writer=writer,
                                         error=cberr,
                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error is None:
                    raise Exception('gsiftp: error not called')
                if cberr.called is not True:
                    raise Exception('gsiftp: err not called')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy size_request test - %s',str(e))
            printer('Test proxy size_request incache=False',False)
            raise
        else:
            printer('Test proxy size_request incache=False')
    
    def test_27_size_request(self):
        """Test Proxy.size_request incache=False url!=host"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,file_uid)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def get_cache_size(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,len(data))
            flexmock(DB).should_receive('get_cache_size').replace_with(get_cache_size)
            def get_cache_checksum(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,checksum)
            flexmock(DB).should_receive('get_cache_checksum').replace_with(get_cache_checksum)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.size_request(req,
                                         host='other',
                                         setheader=None,
                                         writer=writer,
                                         error=cberr,
                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if add_to_cache.called is True:
                    raise Exception('http: DB.add_to_cache called, but was already in cache')
                if not isinstance(writer.data,dict):
                    try:
                        writer.data = tornado.escape.json_decode(writer.data)
                    except:
                        raise Exception('http: writer.data is not json: %r'%writer.data)
                if 'size' not in writer.data:
                    raise Exception('http: writer.data does not contain size')
                if writer.data['size'] != len(data):
                    raise Exception('http: size= %r, but supposed to be %r'%(writer.data['size'],len(data)))
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.size_request(req,
                                         host='other',
                                         setheader=None,
                                         writer=writer,
                                         error=cberr,
                                         callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if add_to_cache.called is True:
                    raise Exception('gsiftp: DB.add_to_cache called, but was already in cache')
                if not isinstance(writer.data,dict):
                    try:
                        writer.data = tornado.escape.json_decode(writer.data)
                    except:
                        raise Exception('gsiftp: writer.data is not json: %r'%writer.data)
                if 'size' not in writer.data:
                    raise Exception('gsiftp: writer.data does not contain size')
                if writer.data['size'] != len(data):
                    raise Exception('gsiftp: size= %r, but supposed to be %r'%(writer.data['size'],len(data)))
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy size_request test - %s',str(e))
            printer('Test proxy size_request incache=False url!=host',False)
            raise
        else:
            printer('Test proxy size_request incache=False url!=host')
    
    def test_30_checksum_request(self):
        """Test Proxy.checksum_request"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True,file_uid)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def get_cache_size(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True,len(data))
            flexmock(DB).should_receive('get_cache_size').replace_with(get_cache_size)
            def get_cache_checksum(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True,checksum)
            flexmock(DB).should_receive('get_cache_checksum').replace_with(get_cache_checksum)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.checksum_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called')
                if add_to_cache.called is True:
                    raise Exception('http: DB.add_to_cache called, but was already in cache')   
                if not isinstance(writer.data,dict):
                    raise Exception('http: writer.data is not json: %r'%writer.data)
                if 'checksum' not in writer.data:
                    raise Exception('http: writer.data does not contain checksum')
                if writer.data['checksum'] != checksum:
                    raise Exception('http: size= %r, but supposed to be %r'%(writer.data['checksum'],checksum))     
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.checksum_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if add_to_cache.called is True:
                    raise Exception('gsiftp: DB.add_to_cache called, but was already in cache')
                if not isinstance(writer.data,dict):
                    raise Exception('gsiftp: writer.data is not json: %r'%writer.data)
                if 'checksum' not in writer.data:
                    raise Exception('gsiftp: writer.data does not contain checksum')
                if writer.data['checksum'] != checksum:
                    raise Exception('gsiftp: size= %r, but supposed to be %r'%(writer.data['checksum'],checksum))
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy checksum_request test - %s',str(e))
            printer('Test proxy checksum_request',False)
            raise
        else:
            printer('Test proxy checksum_request')    
    
    def test_31_checksum_request(self):
        """Test Proxy.checksum_request incache=False"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,file_uid)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def get_cache_size(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,len(data))
            flexmock(DB).should_receive('get_cache_size').replace_with(get_cache_size)
            def get_cache_checksum(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,checksum)
            flexmock(DB).should_receive('get_cache_checksum').replace_with(get_cache_checksum)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.checksum_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error is None:
                    raise Exception('http: error not called')
                if cberr.called is not True:
                    raise Exception('http: err not called')  
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.checksum_request(req,
                                             host='test.ing',
                                             setheader=None,
                                             writer=writer,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error is None:
                    raise Exception('gsiftp: error not called')
                if cberr.called is not True:
                    raise Exception('gsiftp: err not called')
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy checksum_request test - %s',str(e))
            printer('Test proxy checksum_request incache=False',False)
            raise
        else:
            printer('Test proxy checksum_request incache=False')
    
    def test_32_checksum_request(self):
        """Test Proxy.checksum_request incache=False"""
        try:
            data = 'this is a test'
            file_uid = 'lksjdlkfs'
            filename = os.path.join(self.test_dir,file_uid)
            with open(filename,'w') as f:
                f.write(data)
            checksum = 'asdfasdf'
            
            cfg = proxy.Proxy._cfg
            
            def writer(data):
                writer.data = data
            writer.data = None
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
                tornado.ioloop.IOLoop.instance().stop()
            cb.called = False
            cb.ret = None
            def cberr(error=None,*args):
                cberr.called = True
                cberr.error = error
                tornado.ioloop.IOLoop.instance().stop()
            cberr.called = False
            cberr.error = None
            
            flexmock(DB).should_receive('get_site_auth').replace_with(lambda callback:callback(True,'key'))
            def add_to_cache(url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
                add_to_cache.called = True
                try:
                    callback()
                except:
                    pass
            add_to_cache.called = False
            flexmock(DB).should_receive('add_to_cache').replace_with(add_to_cache)
            def check_cache_space(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](True)
            flexmock(DB).should_receive('check_cache_space').replace_with(check_cache_space)
            def in_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,file_uid)
            flexmock(DB).should_receive('in_cache').replace_with(in_cache)
            def get_cache_size(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,len(data))
            flexmock(DB).should_receive('get_cache_size').replace_with(get_cache_size)
            def get_cache_checksum(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,checksum)
            flexmock(DB).should_receive('get_cache_checksum').replace_with(get_cache_checksum)
            def remove_from_cache(*args,**kwargs):
                if 'callback' in kwargs:
                    kwargs['callback'](False,None)
            flexmock(DB).should_receive('remove_from_cache').replace_with(remove_from_cache)
            
            def fetch(url,**kwargs):
                fetch.url = url
                fetch.stream = False
                
                body = tornado.escape.json_decode(kwargs['body'])
                try:
                    type = body['type']
                except:
                    type = 'data'
                if type == 'size':
                    ret = tornado.escape.json_encode({'url':url,'size':len(data)})
                elif type == 'checksum':
                    ret = tornado.escape.json_encode({'url':url,'checksum':checksum})
                else:
                    ret = data
                
                if 'streaming_callback' in kwargs:
                    fetch.stream = True
                    kwargs['streaming_callback'](ret)
                    
                req = tornado.httpclient.HTTPRequest('http://test')
                resp = tornado.httpclient.HTTPResponse(req,200)
                if 'callback' in kwargs:
                    if not fetch.stream:
                        resp.buffer = StringIO.StringIO(ret)
                    kwargs['callback'](resp)
            flexmock(proxy.Proxy._httpclient).should_receive('fetch').replace_with(fetch)
            def get(url,**kwargs):
                get.url = url
                get.stream = False
                if 'streaming_callback' in kwargs:
                    get.stream = True
                    kwargs['streaming_callback'](data)
                    
                if 'callback' in kwargs:
                    if get.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](data)
            flexmock(proxy.Proxy._gridftpclient).should_receive('get').replace_with(get)
            def size(url,**kwargs):
                size.url = url
                size.stream = False
                if 'streaming_callback' in kwargs:
                    size.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
                    
                if 'callback' in kwargs:
                    if size.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'size':len(data)}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('size').replace_with(size)
            def sha512sum(url,**kwargs):
                sha512sum.url = url
                sha512sum.stream = False
                if 'streaming_callback' in kwargs:
                    sha512sum.stream = True
                    kwargs['streaming_callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
                    
                if 'callback' in kwargs:
                    if sha512sum.stream:
                        kwargs['callback'](True)
                    else:
                        kwargs['callback'](tornado.escape.json_encode({'url':url,'checksum':checksum}))
            flexmock(proxy.Proxy._gridftpclient).should_receive('sha512sum').replace_with(sha512sum)
            
            try:
                proxy.Proxy.configure(download_dir=self.test_dir)
                
                req = tornado.httpserver.HTTPRequest('GET','http://test.ing/tester',host='test.ing')
                proxy.Proxy.checksum_request(req,
                                             host='other',
                                             setheader=None,
                                             writer=writer,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('http: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('http: err called')
                if cb.called is not True:
                    raise Exception('http: callback not called') 
                if not isinstance(writer.data,dict):
                    try:
                        writer.data = tornado.escape.json_decode(writer.data)
                    except:
                        raise Exception('http: writer.data is not json: %r'%writer.data)
                if 'checksum' not in writer.data:
                    raise Exception('http: writer.data does not contain checksum')
                if writer.data['checksum'] != checksum:
                    raise Exception('http: size= %r, but supposed to be %r'%(writer.data['checksum'],checksum))     
                
                writer.data = None
                cb.called = False
                cb.ret = None
                cberr.called = False
                cberr.error = None
                add_to_cache.called = False
                req = tornado.httpserver.HTTPRequest('GET','gsiftp://test.ing/tester',host='test.ing')
                proxy.Proxy.checksum_request(req,
                                             host='other',
                                             setheader=None,
                                             writer=writer,
                                             error=cberr,
                                             callback=cb)
                tornado.ioloop.IOLoop.instance().start()
                
                if cberr.error:
                    raise Exception('gsiftp: error called: %r',cberr.error)
                if cberr.called is True:
                    raise Exception('gsiftp: err called')
                if cb.called is not True:
                    raise Exception('gsiftp: callback not called')
                if not isinstance(writer.data,dict):
                    try:
                        writer.data = tornado.escape.json_decode(writer.data)
                    except:
                        raise Exception('gsiftp: writer.data is not json: %r'%writer.data)
                if 'checksum' not in writer.data:
                    raise Exception('gsiftp: writer.data does not contain checksum')
                if writer.data['checksum'] != checksum:
                    raise Exception('gsiftp: size= %r, but supposed to be %r'%(writer.data['checksum'],checksum))
                
            finally:
                proxy.Proxy._cfg = cfg
        
        except Exception, e:
            logger.error('Error running proxy checksum_request test - %s',str(e))
            printer('Test proxy checksum_request incache=False url!=host',False)
            raise
        else:
            printer('Test proxy checksum_request incache=False url!=host')
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(proxy_test))
    suite.addTests(loader.loadTestsFromNames(alltests,proxy_test))
    return suite
