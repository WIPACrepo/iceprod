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
from iceprod.core.jsonUtil import json_encode,json_decode
import iceprod.server
from iceprod.server.modules.website import website
import iceprod.server.modules.website
import iceprod.server.module
try:
    from iceprod.server import ssl_cert
except ImportError:
    ssl_cert = None

from ..module_test import module_test

# check for javascript testing
try:
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    with to_log(sys.stdout):
        subprocess.check_call(['which','phantomjs'])
    testjs = True
except (ImportError, subprocess.CalledProcessError):
    logger.info('skipping javascript tests', exc_info=True)
    testjs = False


class website_test(AsyncTestCase):
    def setUp(self):
        super(website_test,self).setUp()

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

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /submit')
    async def test_200_Submit(self, req):
        with self.start():
            address = 'http://localhost:%d/submit'%(self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                if url.startswith('/create_token'):
                    rest.called = True
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /config')
    async def test_210_Config(self, req):
        with self.start():
            dataset_id = 'foo'
            address = 'http://localhost:{}/config?dataset_id={}'.format(
                self.cfg['webserver']['port'], dataset_id)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                if url.startswith('/datasets/{}'.format(dataset_id)):
                    return {'dataset_id':'foo'}
                elif url.startswith('/config/{}'.format(dataset_id)):
                    rest.called = True
                    return {'dataset_id':'foo'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)
            
            address = 'http://localhost:{}/config?dataset_id={}&edit=1'.format(
                self.cfg['webserver']['port'], dataset_id)
            logger.info('try connecting at %s',address)

            async def rest(method, url, args=None):
                if url.startswith('/create_token'):
                    rest.called = True
                    return {'result': 'token'}
                elif url.startswith('/datasets/{}'.format(dataset_id)):
                    return {'dataset_id':'foo'}
                elif url.startswith('/config/{}'.format(dataset_id)):
                    return {'dataset_id':'foo'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)
            
            address = 'http://localhost:{}/config'.format(
                self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            r = await self.request(address, raise_error=False)
            self.assertEqual(r.code, 400)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /dataset')
    async def test_300_Dataset(self, req):
        with self.start():
            dataset_id = 'foo'
            address = 'http://localhost:{}/dataset'.format(
                self.cfg['webserver']['port'])
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                if url.startswith('/datasets'):
                    rest.called = True
                    return {dataset_id:{'dataset_id':dataset_id, 'status':'processing',
                                        'dataset': 1234, 'description':'desc'}}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /dataset/<dataset_id>')
    async def test_301_Dataset(self, req):
        with self.start():
            dataset_id = 'foo'
            address = 'http://localhost:{}/dataset/{}'.format(
                self.cfg['webserver']['port'], dataset_id)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                if url == '/datasets/{}'.format(dataset_id):
                    rest.called = True
                    return {'dataset_id':dataset_id, 'status':'processing',
                            'dataset': 1234, 'description':'desc'}
                elif url.startswith('/datasets/{}/job_counts/status'.format(dataset_id)):
                    return {'processing':['foo','bar']}
                elif url.startswith('/datasets/{}/task_counts/status'.format(dataset_id)):
                    return {'processing':['1','2','3'], 'complete': ['4','5'], 'suspended':['6']}
                elif url.startswith('/datasets/{}/task_counts/name_status'.format(dataset_id)):
                    return {'generator':{'complete':2},
                            'hits':{'processing':1,'reset':1},
                            'detector':{'waiting':1,'suspended':1},
                           }
                elif url.startswith('/datasets/{}/task_stats'.format(dataset_id)):
                    return {'generator':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         'max_hrs': 1.1, 'efficiency': 0.54},
                            'hits':{'count': 2, 'gpu': 2, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         'max_hrs': 1.1, 'efficiency': 0.54},
                            'detector':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         'max_hrs': 1.1, 'efficiency': 0.54},
                           }
                elif url.startswith('/create_token'):
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)

            async def rest(method, url, args=None):
                raise Exception()
            req.return_value.request = rest
            r = await self.request(address, raise_error=False)
            self.assertEqual(r.code, 404)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @patch('iceprod.server.GlobalID.globalID_gen')
    @unittest_reporter(name=' /dataset/<dataset_num>')
    async def test_302_Dataset(self, globalid, req):
        with self.start():
            dataset_id = 'foo'
            dataset_num = 120
            address = 'http://localhost:{}/dataset/{}'.format(
                self.cfg['webserver']['port'], dataset_num)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            globalid.return_value = dataset_id

            async def rest(method, url, args=None):
                logger.info('REST %s %s', method, url)
                if url == '/datasets/{}'.format(dataset_id):
                    rest.called = True
                    return {'dataset_id':dataset_id, 'status':'processing',
                            'dataset': dataset_num, 'description':'desc'}
                elif url.startswith('/datasets/{}/job_counts/status'.format(dataset_id)):
                    return {'processing':['foo','bar']}
                elif url.startswith('/datasets/{}/task_counts/status'.format(dataset_id)):
                    return {'processing':['1','2','3'], 'complete': ['4','5'], 'suspended':['6']}
                elif url.startswith('/datasets/{}/task_counts/name_status'.format(dataset_id)):
                    return {'generator':{'complete':2},
                            'hits':{'processing':1,'reset':1},
                            'detector':{'waiting':1,'suspended':1},
                           }
                elif url.startswith('/datasets/{}/task_stats'.format(dataset_id)):
                    return {'generator':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         'max_hrs': 1.1, 'efficiency': 0.54},
                            'hits':{'count': 2, 'gpu': 2, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         'max_hrs': 1.1, 'efficiency': 0.54},
                            'detector':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         'max_hrs': 1.1, 'efficiency': 0.54},
                           }
                elif url.startswith('/create_token'):
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)

            async def rest(method, url, args=None):
                raise Exception()
            req.return_value.request = rest
            r = await self.request(address, raise_error=False)
            self.assertEqual(r.code, 404)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /dataset/<dataset_id>/task')
    async def test_400_Task(self, req):
        with self.start():
            dataset_id = 'foo'
            task_id = 'bar'
            address = 'http://localhost:{}/dataset/{}/task'.format(
                self.cfg['webserver']['port'], dataset_id)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                if url.startswith('/datasets/{}/task_counts/status'.format(dataset_id)):
                    rest.called = True
                    return {'processing':['1','2','3'], 'complete': ['4','5'], 'suspended':['6']}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)
            
            address = 'http://localhost:{}/dataset/{}/task?status=processing'.format(
                self.cfg['webserver']['port'], dataset_id)

            async def rest(method, url, args=None):
                logger.info('REST %s %s', method, url)
                if url.startswith('/datasets/{}/tasks'.format(dataset_id)):
                    rest.called = True
                    return {task_id:{'task_id':task_id,'dataset_id':dataset_id,
                                     'job_id':'j', 'status':'processing',
                                     'name':'generator', 'failures':0}}
                elif url.startswith('/datasets/{}/job'.format(dataset_id)):
                    return {'job_index':0}
                elif url.startswith('/create_token'):
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)
            soup = BeautifulSoup(r.body, "html.parser")
            for e in soup.findAll("td"):
                if e.text == 'bar':
                    break
            else:
                raise Exception('did not find task_id bar')

            async def rest(method, url, args=None):
                logger.info('REST %s %s', method, url)
                if url.startswith('/datasets/{}/tasks'.format(dataset_id)):
                    rest.called = True
                    return {}
                elif url.startswith('/create_token'):
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest
                
            address = 'http://localhost:{}/dataset/{}/task?status=waiting'.format(
                self.cfg['webserver']['port'], dataset_id)
            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)
            soup = BeautifulSoup(r.body, "html.parser")
            for e in soup.findAll("td"):
                self.assertNotEqual(e.text, 'bar')

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /dataset/<dataset_id>/task/<task_id>')
    async def test_401_Task(self, req):
        with self.start():
            dataset_id = 'foo'
            task_id = 'bar'
            address = 'http://localhost:{}/dataset/{}/task/{}'.format(
                self.cfg['webserver']['port'], dataset_id, task_id)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                logger.info('REST %s %s', method, url)
                if url == '/datasets/{}/tasks/{}'.format(dataset_id,task_id):
                    rest.called = True
                    return {'task_id':task_id,'dataset_id':dataset_id,
                            'job_id':'j', 'status':'processing',
                            'name':'generator', 'failures':0}
                elif url.startswith('/datasets/{}/tasks/{}/log'.format(dataset_id,task_id)):
                    return {'logs':[{'log_id':'baz','name':'stdout','data':'foo\nbar',
                                     'dataset_id':dataset_id,'task_id':task_id}]}
                elif url.startswith('/create_token'):
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /dataset/<dataset_id>/job')
    async def test_500_Job(self, req):
        with self.start():
            dataset_id = 'foo'
            job_id = 'bar'
            address = 'http://localhost:{}/dataset/{}/job'.format(
                self.cfg['webserver']['port'], dataset_id)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                logger.info('REST %s %s', method, url)
                if url.startswith('/datasets/{}/jobs'.format(dataset_id)):
                    rest.called = True
                    return {job_id:{'job_id':job_id,'dataset_id':dataset_id,
                                    'status':'processing', 'status_changed':'2018',
                                    'job_index':0}}
                elif url.startswith('/create_token'):
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)

            address = 'http://localhost:{}/dataset/{}/job?status=processing'.format(
                self.cfg['webserver']['port'], dataset_id)

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            soup = BeautifulSoup(r.body, "html.parser")
            for e in soup.findAll("td"):
                if e.text == 'bar':
                    break
            else:
                raise Exception('did not find task_id bar')
                
            address = 'http://localhost:{}/dataset/{}/job?status=waiting'.format(
                self.cfg['webserver']['port'], dataset_id)
            r = await self.request(address)
            self.assertEqual(r.code, 200)
            soup = BeautifulSoup(r.body, "html.parser")
            for e in soup.findAll("td"):
                self.assertNotEqual(e.text, 'bar')

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /dataset/<dataset_id>/job/<job_id>')
    async def test_501_Job(self, req):
        with self.start():
            dataset_id = 'foo'
            job_id = 'bar'
            address = 'http://localhost:{}/dataset/{}/job/{}'.format(
                self.cfg['webserver']['port'], dataset_id, job_id)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                logger.info('REST %s %s', method, url)
                if url.startswith('/datasets/{}/jobs/{}'.format(dataset_id,job_id)):
                    rest.called = True
                    return {'job_id':job_id,'dataset_id':dataset_id,
                            'status':'processing', 'status_changed':'2018',
                            'job_index':0}
                elif url.startswith('/datasets/{}/tasks'.format(dataset_id)):
                    return {'baz':{'task_id':'baz','dataset_id':dataset_id,
                                   'job_id':job_id,'name':'generate',
                                   'status':'queued', 'status_changed':'2018',
                                   'task_index':0, 'failures':0, 'requirements':{'cpu':1},
                                   'walltime':0.,'walltime_err':0.}}
                elif url.startswith('/create_token'):
                    return {'result': 'token'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)

    @patch('iceprod.server.modules.website.rest_client.Client')
    @unittest_reporter(name=' /dataset/<dataset_id/log/<log_id>')
    async def test_600_Log(self, req):
        with self.start():
            dataset_id = 'foo'
            log_id = 'bar'
            address = 'http://localhost:{}/dataset/{}/log/{}'.format(
                self.cfg['webserver']['port'], dataset_id, log_id)
            logger.info('try connecting at %s',address)

            await self.get_auth_cookie()

            async def rest(method, url, args=None):
                logger.info('REST %s %s', method, url)
                if url.startswith('/datasets/{}/logs/{}'.format(dataset_id,log_id)):
                    rest.called = True
                    return {'log_id':log_id,'dataset_id':dataset_id,
                            'name':'name', 'data': 'this is a log\nfoo bar'}
                else:
                    raise Exception()
            rest.called = False
            req.return_value.request = rest

            r = await self.request(address)
            self.assertEqual(r.code, 200)
            self.assertTrue(rest.called)
            self.assertIn(b'this is a log', r.body)

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
    alltests = glob_tests(loader.getTestCaseNames(website_test))
    suite.addTests(loader.loadTestsFromNames(alltests,website_test))
    return suite
