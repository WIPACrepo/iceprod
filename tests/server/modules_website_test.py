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
import urllib

from unittest.mock import patch, MagicMock

import asyncio

import tornado.ioloop
from tornado.testing import AsyncTestCase
from tornado.httpclient import AsyncHTTPClient, HTTPError


from bs4 import BeautifulSoup

import iceprod.core.logger
from iceprod.core import to_log
import iceprod.core.jsonRPCclient
from iceprod.core.jsonUtil import json_encode,json_decode
import iceprod.server
from iceprod.server.modules.website import website
import iceprod.server.modules.website
import iceprod.server.module
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
except ImportError:
    logger.info('skipping javascript tests', exc_info=True)
    testjs = False


class modules_website_test(AsyncTestCase):
    def setUp(self):
        super(modules_website_test,self).setUp()

        orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp(dir=orig_dir)
        os.chdir(self.test_dir)
        def clean_dir():
            os.chdir(orig_dir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(clean_dir)

        os.environ['I3PROD'] = self.test_dir
        try:
            # set hostname
            self.hostname = b'localhost'

            self.cfg = {'webserver':{'port':random.randint(10000,32000)},
                        'db':{'name':'test'},
                        'site_id':'abc',
                        'download':{'http_username':None,
                                    'http_password':None,
                                   },
                        'rest_api':{'url':'foo','auth_key':'bar'},
                       }
            self.executor = {}
            self.modules = services_mock()
            
            self.website = website(self.cfg, self.io_loop, self.executor, self.modules)
        except:
            logger.warn('error setting up modules_website', exc_info=True)
            raise

        self.auth_cookie = None

    @contextmanager
    def start(self):
        try:
            self.website.start()
            yield
        finally:
            self.website.stop()

    async def get_auth_cookie(self):
        address = 'http://localhost:%d'%(self.cfg['webserver']['port'])

        # login site
        client = AsyncHTTPClient()
        r = await client.fetch(address+'/login')
        soup = BeautifulSoup(r.body, "html.parser")
        xsrf = None
        for e in soup.findAll("input"):
            logger.info('element: %r', e)
            if 'name' in e.attrs and e['name'] == '_xsrf':
                xsrf = e['value']
                break
        self.assertIsNotNone(xsrf)

        xsrf_cookie = None
        for text in r.headers['set-cookie'].split(','):
            for part in text.split(';'):
                logger.info('cookie: %r', part)
                if part.startswith('_xsrf'):
                    xsrf_cookie = part
                    break
            if xsrf_cookie:
                break
        self.assertIsNotNone(xsrf_cookie)

        data = {
            'username': 'foo',
            'password': 'bar',
            '_xsrf': xsrf,
        }
        body = urllib.parse.urlencode(data)
        headers = {
            'content-type': 'application/x-www-form-urlencoded',
            'cookie': xsrf_cookie,
        }
        logger.info('headers: %r', headers)
        logger.info('body: %r', body)
        
        async def rest(method, url, args=None):
            if url.startswith('/ldap'):
                rest.called = True
                return {
                    'token': 'thetoken',
                    'username': 'foo',
                    'roles': ['user'],
                    'current_role': 'user',
                }
            else:
                raise Exception()
        rest.called = False
        self.website.rest_client.request = rest
        r = await client.fetch(address+'/login',
            method='POST', body=body,
            headers=headers,
            raise_error=False, follow_redirects=False)
        self.assertTrue(rest.called)
        self.assertEqual(r.code, 302)

        auth_cookie = None
        for text in r.headers['set-cookie'].split(','):
            for part in text.split(';'):
                logger.info('cookie: %r', part)
                if part.startswith('user='):
                    auth_cookie = part
                    break
            if auth_cookie:
                break
        self.assertIsNotNone(auth_cookie)
        self.auth_cookie = auth_cookie

    def request(self, *args, **kwargs):
        if self.auth_cookie:
            if 'headers' in kwargs:
                kwargs['headers']['cookie'] = self.auth_cookie
            else:
                kwargs['headers'] = {'cookie': self.auth_cookie}
        if 'follow_redirects' not in kwargs:
            kwargs['follow_redirects'] = False
        client = AsyncHTTPClient()
        return client.fetch(*args, **kwargs)


    @unittest_reporter(name='start/stop/kill')
    async def test_010_start_stop_kill(self):
        self.website.start()

        self.website.stop()

        self.website.start()
        self.website.kill()

    @unittest_reporter(name=' /login')
    async def test_100_Login(self):
        with self.start():
            await self.get_auth_cookie()
            self.assertIsNotNone(self.auth_cookie)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /')
    async def test_110_Default(self, req):
        with self.start():
            datasets_status = {'processing':['d1','d2']}

            address = 'http://localhost:%d'%(self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                if url.startswith('/dataset_summaries/status'):
                    rest.called = True
                    return datasets_status
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            # main site
            r = await self.request(address)
            self.assertEqual(r.code, 200)
            body = r.body.decode('utf-8')
            if any(k not in body for k in datasets_status):
                raise Exception('main: fetched file data incorrect')
            self.assertTrue(rest.called)

            # test error
            req.request.side_effect = Exception()
            with self.assertRaises(Exception):
                r = await client.fetch(address)

"""
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
"""

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(modules_website_test))
    suite.addTests(loader.loadTestsFromNames(alltests,modules_website_test))
    return suite
