"""
Test script for the website module.
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

import logging
logger = logging.getLogger('modules_website_test')

import os, sys, time, random
import shutil
import tempfile
import random
import threading
import subprocess
from datetime import datetime,timedelta
from functools import partial
import unittest
from contextlib import contextmanager

try:
    from unittest.mock import patch, MagicMock
except ImportError:
    from mock import patch, MagicMock

import tornado.ioloop
import requests

import iceprod.core.logger
from iceprod.core import to_log
import iceprod.core.jsonRPCclient
from iceprod.core.jsonUtil import json_encode,json_decode
import iceprod.server
from iceprod.server.modules.website import website
try:
    from iceprod.server import ssl_cert
except ImportError:
    ssl_cert = None

from .module_test import module_test

# check for javascript testing
try:
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    with to_log(sys.stdout):
        subprocess.check_call(['which','phantomjs'])
    testjs = True
except Exception:
    logger.info('skipping javascript tests', exc_info=True)
    testjs = False


class modules_website_test(module_test):
    def setUp(self):
        super(modules_website_test,self).setUp()
        os.environ['I3PROD'] = self.test_dir
        try:
            # set hostname
            self.hostname = 'localhost'
            
            self.ca_cert = os.path.join(self.test_dir,'ca.crt')
            self.ca_key = os.path.join(self.test_dir,'ca.key')
            self.ssl_key = os.path.join(self.test_dir,'test.key')
            self.ssl_cert = os.path.join(self.test_dir,'test.crt')

            self.cfg = {'webserver':{'tornado_port':random.randint(10000,32000),
                                     'port':random.randint(10000,32000),
                                     'numcpus':1,
                                     'lib_dir':os.path.join(self.test_dir,'lib'),
                                     'proxycache_dir':os.path.join(self.test_dir,'proxy'),
                                     'proxy_request_timeout':10,
                                    },
                        'db':{'name':'test'},
                        'site_id':'abc',
                        'system':{'ssl':False},
                        'download':{'http_username':None,
                                    'http_password':None,
                                   },
                       }
            self.executor = {}
            self.modules = services_mock()
            
            self.website = website(self.cfg, self.io_loop, self.executor, self.modules)
        except:
            logger.warn('error setting up modules_website', exc_info=True)
            raise

    @contextmanager
    def start(self):
        self.website.start()
        t = threading.Thread(target=self.wait)
        t.start()
        try:
            yield
        finally:
            self.stop()
            t.join()
            self.website.stop()

    @patch('iceprod.server.modules.website.Nginx')
    @unittest_reporter(name='start/stop/kill')
    def test_10_start_stop_kill(self, nginx):
        self.website.start()
        self.assertTrue(nginx.called)
        self.assertTrue(nginx.return_value.start.called)

        self.website.stop()
        self.assertTrue(nginx.return_value.stop.called)

        self.website.start()
        self.website.kill()
        self.assertTrue(nginx.return_value.kill.called)

    @unittest_reporter(name='start() no ssl')
    def test_11_start_no_ssl(self):
        with self.start():
            datasets_status = {'processing':3}
            self.modules.ret['db']['web_get_datasets'] = datasets_status

            auth = (self.cfg['download']['http_username'],
                    self.cfg['download']['http_password'])

            address = 'http://localhost:%d'%(self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            r = requests.get(address, auth=auth)
            r.raise_for_status()

    @unittest_reporter(skip=not ssl_cert, name='start() with ssl')
    def test_12_start_ssl(self):
        # self-signed cert
        ssl_cert.create_cert(self.ssl_cert,self.ssl_key,days=1,
                             hostname=self.hostname)
        self.cfg['system']['ssl'] = {
            'cert':self.ssl_cert,
            'key':self.ssl_key,
        }

        with self.start():
            datasets_status = {'processing':3}
            self.modules.ret['db']['web_get_datasets'] = datasets_status

            auth = (self.cfg['download']['http_username'],
                    self.cfg['download']['http_password'])

            address = 'http://localhost:%d'%(self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            r = requests.get(address, auth=auth, verify=self.ssl_cert)
            r.raise_for_status()

    @unittest_reporter(skip=not ssl_cert, name='start() with ssl, ca cert')
    def test_13_start_ssl_ca(self):
        # make certs
        ssl_cert.create_ca(self.ca_cert,self.ca_key,days=1,
                          hostname=self.hostname)
        ssl_cert.create_cert(self.ssl_cert,self.ssl_key,days=1,
                            cacert=self.ca_cert,cakey=self.ca_key,
                            hostname=self.hostname)
        self.cfg['system']['ssl'] = {
            'cert':self.ssl_cert,
            'key':self.ssl_key,
            'cacert':self.ca_cert,
        }

        with self.start():
            datasets_status = {'processing':3}
            self.modules.ret['db']['web_get_datasets'] = datasets_status

            auth = (self.cfg['download']['http_username'],
                    self.cfg['download']['http_password'])

            address = 'http://localhost:%d'%(self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            r = requests.get(address, auth=auth, verify=self.ca_cert)
            r.raise_for_status()

    @unittest_reporter
    def test_20_JSONRPCHandler(self):
        with self.start():
            self.modules.ret['db']['auth_authorize_task'] = True
            self.modules.ret['db']['rpc_echo'] = 'e'
            self.modules.ret['db']['rpc_test'] = 'testing'

            passkey = 'key'
            address = 'http://localhost:%d/jsonrpc'%(
                      self.cfg['webserver']['port'])
            logger.info('try connecting to %s',address)

            ssl_opts = {'username': self.cfg['download']['http_username'],
                        'password': self.cfg['download']['http_password'],
                       }

            iceprod.core.jsonRPCclient.JSONRPC.start(address=address,
                                                     passkey=passkey,
                                                     ssl_options=ssl_opts)
            try:
                ret = iceprod.core.jsonRPCclient.JSONRPC.test()
                self.assertEqual(ret, 'testing')
                
                ret = iceprod.core.jsonRPCclient.JSONRPC.test(1,2,3)
                self.assertEqual(self.modules.called[-1][2], (1,2,3))
                
                ret = iceprod.core.jsonRPCclient.JSONRPC.test(a=1,b=2)
                self.assertEqual(self.modules.called[-1][3], {'a':1,'b':2})
                
                ret = iceprod.core.jsonRPCclient.JSONRPC.test(1,2,c=3)
                self.assertEqual(self.modules.called[-1][2], (1,2))
                self.assertEqual(self.modules.called[-1][3], {'c':3})

                self.modules.ret['db']['rpc_test'] = Exception()
                try:
                    iceprod.core.jsonRPCclient.JSONRPC.test()
                except:
                    pass
                else:
                    raise Exception('did not raise Exception')
            finally:
                iceprod.core.jsonRPCclient.JSONRPC.stop()

    @unittest_reporter
    def test_30_MainHandler(self):
        with self.start():
            gridspec = 'thegrid'
            passkey = 'key'
            datasets = {'d1':1,'d2':2}
            datasets_status = {'processing':3}
            datasets_full = [{'dataset_id':'d1','name':'dataset 1','status':'processing','description':'desc','gridspec':gridspec}]
            dataset_details = datasets_full
            tasks = {'task_1':3,'task_2':4}
            task_details = {'waiting':{'c':1,'d':2}}
            tasks_status = {'waiting':10,'queued':30}
            
            self.modules.ret['db']['auth_new_passkey'] = passkey
            self.modules.ret['db']['web_get_gridspec'] = gridspec
            self.modules.ret['db']['web_get_datasets_details'] = dataset_details
            self.modules.ret['db']['web_get_datasets'] = datasets_status
            self.modules.ret['db']['web_get_tasks_by_status'] = tasks_status
            self.modules.ret['db']['web_get_datasets_by_status'] = datasets
            self.modules.ret['db']['web_get_tasks_details'] = task_details

            address = 'http://localhost:%d'%(self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            outfile = os.path.join(self.test_dir,
                                   str(random.randint(0,10000)))

            # main site
            logger.info('url: /')
            r = requests.get(address)
            r.raise_for_status()
            if any(k not in r.content for k in datasets_status):
                raise Exception('main: fetched file data incorrect')

            # test for bad page
            logger.info('url: %s','/bad_page')
            try:
                r = requests.get(address+'bad_page')
                r.raise_for_status()
            except:
                pass
            else:
                raise Exception('did not raise Exception')

            # test internal error
            logger.info('url: /task?status=waiting  internal error')
            self.modules.ret['db']['web_get_tasks_details'] = None
            try:
                r = requests.get(address+'task?status=waiting')
                r.raise_for_status()
            except:
                pass
            else:
                raise Exception('did not raise Exception')

    @unittest_reporter(skip=not testjs)
    def test_40_groups(self):
        with self.start():
            passkey = 'key'
            groups = {
                'a': {'name':'/Sim','priority':0.3,'description':'blah'},
                'b': {'name':'/Filt','priority':0.4,'description':'blah2'},
            }

            self.modules.ret['db']['auth_new_passkey'] = passkey
            self.modules.ret['db']['rpc_get_groups'] = groups

            driver = webdriver.PhantomJS()
            driver.get('http://localhost:%d/groups'%self.cfg['webserver']['port'])
            web_groups = {}
            for row in driver.find_elements_by_css_selector("#groups div.row:not(.header)"):
                logger.info('%r',row)
                gid = row.find_element_by_css_selector("input.id").get_attribute('value')
                web_groups[gid] = {
                    'name': row.find_element_by_class_name('name').text,
                    'priority': float(row.find_element_by_class_name('priority').text),
                    'description': row.find_element_by_class_name('description').text,
                }
            self.assertEqual(groups, web_groups)

    @unittest_reporter(skip=not testjs)
    def test_41_groups_edit(self):
        with self.start():

            passkey = 'key'
            groups = {
                'a': {'name':'/Sim','priority':0.3,'description':'blah'},
                'b': {'name':'/Filt','priority':0.4,'description':'blah2'},
            }

            self.modules.ret['db']['auth_new_passkey'] = passkey
            self.modules.ret['db']['auth_authorize_task'] = True
            self.modules.ret['db']['rpc_get_groups'] = groups
            self.modules.ret['db']['rpc_set_groups'] = True
            self.modules.ret['db']['web_get_datasets'] = []
            
            driver = webdriver.PhantomJS()
            #driver = webdriver.Firefox()
            driver.implicitly_wait(1)
            driver.get('http://localhost:%d/login'%self.cfg['webserver']['port'])
            driver.get('http://localhost:%d/groups'%self.cfg['webserver']['port'])
            row = driver.find_elements_by_css_selector("#groups div.row:not(.header)")[0]
            groups['a'] = {'name':'/SimProd','priority':0.4,'description':'blah3'}
            for name,value in groups['a'].items():
                e = row.find_element_by_class_name(name)
                i = e.find_element_by_tag_name('input')
                i.clear()
                i.send_keys(str(value))
                #WebDriverWait(driver, 1).until(EC.staleness_of(i))
            driver.find_element_by_id('submit').click()
            try:
                WebDriverWait(driver, 1).until(EC.text_to_be_present_in_element(
                    (webdriver.common.by.By.ID,'status'),'OK'))
            finally:
                logger.warn('status: %r',driver.find_element_by_id('status').text)
            c = self.modules.called[-1]
            logger.info('%r',c)
            self.assertEqual(c[:2], ('db','rpc_set_groups'))
            web_groups = c[-1]['groups']
            self.assertDictEqual(groups, web_groups)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_website_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_website_test))
    return suite
