"""
Test script for the website module.
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests, _messaging

import logging
logger = logging.getLogger('modules_website_test')

import os, sys, time, random
import shutil
import tempfile
import random
import threading
import signal
from datetime import datetime,timedelta
from functools import partial

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock
import tornado.ioloop

from iceprod.core import functions
from iceprod.core import util
import iceprod.core.jsonRPCclient
from iceprod.core.jsonUtil import json_encode,json_decode
import iceprod.server
from iceprod.server.modules.website import website
try:
    from iceprod.server import openssl
except ImportError:
    openssl = None


class _Nginx(object):
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.killed = False
    def start(self):
        self.started = True
    def stop(self):
        self.stopped = True
    def kill(self):
        self.killed = True

class modules_website_test(unittest.TestCase):
    def setUp(self):
        super(modules_website_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        
        self.ca_cert = os.path.join(self.test_dir,'ca.crt')
        self.ca_key = os.path.join(self.test_dir,'ca.key')
        self.ssl_key = os.path.join(self.test_dir,'test.key')
        self.ssl_cert = os.path.join(self.test_dir,'test.crt')
        
        # set hostname
        self.hostname = 'localhost'
        
        # make certs
        if openssl:
            openssl.create_ca(self.ca_cert,self.ca_key,days=1,
                              hostname=self.hostname)
            openssl.create_cert(self.ssl_cert,self.ssl_key,days=1,
                                cacert=self.ca_cert,cakey=self.ca_key,
                                hostname=self.hostname)
        
        # make cfg
        self.cfg = {'webserver':{'tornado_port':random.randint(10000,32000),
                                 'port':random.randint(10000,32000),
                                 'numcpus':1,
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
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(modules_website_test,self).tearDown()

    def test_01_init(self):
        """Test init"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(website).should_receive('start').replace_with(start)
            start.called = False
            
            url = 'localhost'
            q = website(url)
            q.messaging = _messaging()
            if not q:
                raise Exception('did not return website object')
            if start.called != True:
                raise Exception('init did not call start')
            
            new_cfg = {'new':1}
            q.messaging.BROADCAST.reload(cfg=new_cfg)
            if not q.messaging.called:
                raise Exception('init did not call messaging')
            if q.messaging.called != ['BROADCAST','reload',(),{'cfg':new_cfg}]:
                raise Exception('init did not call correct message')
            
        except Exception as e:
            logger.error('Error running website init test - %s',str(e))
            printer('Test website init',False)
            raise
        else:
            printer('Test website init')
    
    def test_02_start_stop(self):
        """Test start_stop"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(website).should_receive('start').replace_with(start)
            start.called = False
            
            url = 'localhost'
            q = website(url)
            q.messaging = _messaging()
            ngx = _Nginx()
            q.nginx = ngx
            
            q.start()
            if start.called is not True:
                raise Exception('did not start')
            
            q.nginx.start()
            if ngx.started is not True:
                raise Exeption('did not start Nginx')
            
            q.stop()
            if ngx.stopped is not True:
                raise Exception('did not stop Nginx')
            
            ngx.finished = False
            q.nginx = ngx
            q.kill()
            if ngx.killed is not True:
                raise Exception('did not kill Nginx')
            
            q.nginx = None
            try:
                q.stop()
                q.kill()
            except Exception:
                logger.info('exception raised',exc_info=True)
                raise Exception('website = None and exception raised')
            
        except Exception as e:
            logger.error('Error running modules.website start_stop test - %s',str(e))
            printer('Test modules.website start_stop',False)
            raise
        else:
            printer('Test modules.website start_stop')
    
    def test_10_JSONRPCHandler(self):
        """Test JSONRPCHandler"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(website).should_receive('start').replace_with(start)
            start.called = False
            
            passkey = 'key'
            
            url = 'localhost'
            web = website(url)
            web.messaging = _messaging()
            web.messaging.ret = {'db':{'authorize_task':True,
                                       'echo':'e',
                                       'rpc_test':'testing'}}
            web.cfg = self.cfg
            web._start()
            
            # actual start
            ioloop = tornado.ioloop.IOLoop.instance()
            t = threading.Thread(target=ioloop.start)
            t.start()
            try:
                
                address = 'localhost:%d/jsonrpc'%(
                          self.cfg['webserver']['port'])
                logger.info('try connecting directly to tornado at %s',address)
                
                ssl_opts = {'ca_certs': self.cfg['system']['ssl_cacert'],
                            'username': self.cfg['download']['http_username'],
                            'password': self.cfg['download']['http_password'],
                           }
                
                iceprod.core.jsonRPCclient.JSONRPC.start(address=address,
                                                         passkey=passkey,
                                                         ssl_options=ssl_opts)
                try:
                    ret = iceprod.core.jsonRPCclient.JSONRPC.test()
                    if ret != 'testing':
                        raise Exception('bad ret from JSONRPC.test()')
                    
                    web.messaging.ret = {'db':{'authorize_task':True,
                                               'rpc_test':Exception()}}
                    try:
                        iceprod.core.jsonRPCclient.JSONRPC.test()
                    except Exception:
                        pass
                    else:
                        raise Exception('JSONRPC.test() did not raise Exception')
                finally:
                    iceprod.core.jsonRPCclient.JSONRPC.stop()
                
                time.sleep(0.1)
                
            finally:
                ioloop.stop()
            
        except Exception as e:
            logger.error('Error running website JSONRPCHandler test - %s',str(e))
            printer('Test website JSONRPCHandler',False)
            raise
        else:
            printer('Test website JSONRPCHandler')
    
    def test_20_LibHandler(self):
        """Test LibHandler"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(website).should_receive('start').replace_with(start)
            start.called = False
            
            passkey = 'key'
            
            url = 'localhost'
            web = website(url)
            web.messaging = _messaging()
            web.cfg = self.cfg
            web._start()
            
            # actual start
            ioloop = tornado.ioloop.IOLoop.instance()
            t = threading.Thread(target=ioloop.start)
            t.start()
            
            
            pycurl_handle = util.PycURL()
            try:
                
                address = 'localhost:%d/lib/'%(
                          self.cfg['webserver']['port'])
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
                
                time.sleep(0.1)
                
            finally:
                ioloop.stop()
            
        except Exception as e:
            logger.error('Error running modules.website LibHandler test - %s',str(e))
            printer('Test modules.website LibHandler',False)
            raise
        else:
            printer('Test modules.website LibHandler')
    
    def test_30_MainHandler(self):
        """Test MainHandler"""
        try:
            # mock some functions so we don't go too far
            def start():
                start.called = True
            flexmock(website).should_receive('start').replace_with(start)
            start.called = False
            
            gridspec = 'thegrid'
            passkey = 'key'
            datasets = {'d1':1,'d2':2}
            dataset_details = {'a':1,'b':2}
            tasks = {'task_1':3,'task_2':4}
            task_details = {'waiting':{'c':1,'d':2}}
            tasks_status = {'waiting':10,'queued':30}
            
            url = 'localhost'
            web = website(url)
            web.messaging = _messaging()
            web.messaging.ret = {'db':{'new_passkey':passkey,
                                       'get_gridspec':gridspec,
                                       'get_datasets_details':dataset_details,
                                       'get_tasks_by_status':tasks,
                                       'get_datasets_by_status':datasets,
                                       'get_tasks_details':task_details,
                                       'get_tasks_by_status':tasks_status,
                                }}
            web.cfg = self.cfg
            web._start()
            
            # actual start
            ioloop = tornado.ioloop.IOLoop.instance()
            t = threading.Thread(target=ioloop.start)
            t.start()
            
            
            pycurl_handle = util.PycURL()
            try:
                
                address = 'localhost:%d/'%(
                          self.cfg['webserver']['port'])
                logger.info('try connecting directly to tornado at %s',address)
                
                ssl_opts = {}
                outfile = os.path.join(self.test_dir,
                                       str(random.randint(0,10000)))
                
                # main site
                pycurl_handle.fetch(address,outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('main: file not fetched')
                data = open(outfile).read()
                if any(k not in data for k in datasets):
                    raise Exception('main: fetched file data incorrect')
                os.unlink(outfile)
                
                # submit
                pycurl_handle.fetch(address+'submit',outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('submit: file not fetched')
                data = open(outfile).read()
                if gridspec not in data:
                    raise Exception('submit: fetched file data incorrect')
                os.unlink(outfile)
                
                # dataset
                pycurl_handle.fetch(address+'dataset',outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('dataset: file not fetched')
                data = open(outfile).read()
                if any(k not in data for k in datasets):
                    raise Exception('dataset: fetched file data incorrect')
                os.unlink(outfile)
                
                # dataset by status
                dataset_browse = {'waiting':{'d1':1,'d2':2}}
                web.messaging.ret['db']['get_datasets_details'] = dataset_browse
                pycurl_handle.fetch(address+'dataset?status=waiting',outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('dataset by status: file not fetched')
                data = open(outfile).read()
                if any(k not in data for k in dataset_browse['waiting']):
                    raise Exception('dataset by status: fetched file data incorrect')
                os.unlink(outfile)
                
                # dataset by dataset_id
                dataset_details = {'waiting':{'d1':1,'d2':2}}
                web.messaging.ret['db']['get_datasets_details'] = dataset_details
                pycurl_handle.fetch(address+'dataset/1234',outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('dataset by id: file not fetched')
                data = open(outfile).read()
                if any(k not in data for k in dataset_details['waiting']):
                    raise Exception('dataset by id: fetched file data incorrect')
                if any(k not in data for k in tasks_status):
                    logger.info(data)
                    raise Exception('dataset by id: fetched file data incorrect')
                os.unlink(outfile)
                
                # task
                pycurl_handle.fetch(address+'task',outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('task: file not fetched')
                data = open(outfile).read()
                if any(k not in data for k in tasks_status):
                    raise Exception('task: fetched file data incorrect')
                os.unlink(outfile)
                
                # task by status
                task_browse = {'waiting':{'d1':1,'d2':2}}
                web.messaging.ret['db']['get_tasks_details'] = task_browse
                pycurl_handle.fetch(address+'task?status=waiting',outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('task by status: file not fetched')
                data = open(outfile).read()
                if any(k not in data for k in task_browse['waiting']):
                    logger.info(data)
                    raise Exception('task by status: fetched file data incorrect')
                os.unlink(outfile)
                
                # task by task_id
                task_details = {'waiting':{'d1':1,'d2':2}}
                logs = {'err':'this is a log','out':'output'}
                web.messaging.ret['db']['get_tasks_details'] = task_details
                web.messaging.ret['db']['get_logs'] = logs
                pycurl_handle.fetch(address+'task/1234',outfile,**ssl_opts)
                if not os.path.exists(outfile):
                    raise Exception('task by id: file not fetched')
                data = open(outfile).read()
                if any(k not in data for k in task_details['waiting']):
                    logger.info(data)
                    raise Exception('task by id: fetched task_details')
                if any(logs[k] not in data for k in logs):
                    raise Exception('task by id: no log in file data')
                os.unlink(outfile)
                
                # test for bad page
                try:
                    pycurl_handle.fetch(address+'bad_page',outfile,**ssl_opts)
                except:
                    pass
                else:
                    raise Exception('did not raise exception when testing bad page')
                
                # test internal error
                task_browse = None
                web.messaging.ret['db']['get_tasks_details'] = task_browse
                try:
                    pycurl_handle.fetch(address+'task?status=waiting',outfile,**ssl_opts)
                except:
                    pass
                else:
                    raise Exception('did not raise exception when testing internal error')
                
                time.sleep(0.1)
                
            finally:
                ioloop.stop()
            
        except Exception as e:
            logger.error('Error running modules.website MainHandler test - %s',str(e))
            printer('Test modules.website MainHandler',False)
            raise
        else:
            printer('Test modules.website MainHandler')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_website_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_website_test))
    return suite
