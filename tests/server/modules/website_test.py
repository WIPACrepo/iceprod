import asyncio
import binascii
import logging
import re
import secrets

import httpx
import pytest
import pytest_asyncio
from rest_tools.utils import Auth
from rest_tools.client import RestClient
import requests.exceptions
from tornado.web import create_signed_value

from iceprod.rest.auth import ROLES, GROUPS
from iceprod.server.modules import website

from ...util import services_mock


@pytest_asyncio.fixture
async def server(monkeypatch, port, requests_mock):
    monkeypatch.setenv('CI_TESTING', '1')

    # set hostname
    hostname = b'localhost'
    address = f'http://localhost:{port}'

    cfg = {
        'webserver': {
            'port': port,
            'cookie_secret': secrets.token_hex(16),
            'full_url': address,
        },
        'db': {'name': 'test'},
        'site_id': 'abc',
        'download': {
            'http_username': None,
            'http_password': None,
        },
        'rest_api': {
            'url': 'http://iceprod.test',
            'auth_key': 'bar'
        },
    }
    executor = {}
    modules = services_mock()

    s = website.website(cfg, executor, modules)
    s.rest_client = RestClient('http://iceprod.test', 'bar')
    s.start()

    auth = Auth('secret')
    cookie_secret = binascii.unhexlify(cfg['webserver']['cookie_secret'])

    def _add_to_data(data, attrs):
        logging.debug('attrs: %r', attrs)
        for item in attrs:
            key,value = item.split('=',1)
            d = data
            while '.' in key:
                k,key = key.split('.',1)
                if k not in d:
                    d[k] = {}
                d = d[k]
            if key not in d:
                d[key] = []
            d[key].append(value)

    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)

    class Request:
        def __init__(self, token_data, token, timeout=None):
            self.timeout = timeout
            requests_mock.get('http://iceprod.test/users/username/credentials', status_code=200, json={
                address: {
                    'url': address,
                    'type': 'oauth',
                    'access_token': token,
                }
            })
            requests_mock.get('http://iceprod.test/groups/simprod/credentials', status_code=200, json={})
            
            self.token_cookie = create_signed_value(cookie_secret, 'iceprod_username', token_data['preferred_username'])
            logging.debug('Request cookie_secret: %r', cfg['webserver']['cookie_secret'].encode())

        async def request(self, method, path, args=None):
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                kwargs = {'headers': {'Cookie': b'iceprod_username='+self.token_cookie}}
                if args:
                    kwargs['params'] = args
                ret = await client.request(method, address+path, **kwargs)
                ret.raise_for_status()
                return ret.text

    def client(timeout=1, username='user', roles=[], groups=[], exp=10):
        data = {'preferred_username': username}
        for r in roles:
            _add_to_data(data, ROLES[r])
        for g in groups:
            _add_to_data(data, GROUPS[g])
        token = auth.create_token('username', expiration=exp, payload=data)
        return Request(data, token, timeout=timeout)

    try:
        yield client
    finally:
        await s.stop()


async def test_website_root(server):
    client = server()
    await client.request('GET', '/')

async def test_website_submit(server, requests_mock):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    ret = await client.request('GET', '/submit')

async def test_website_config(server, requests_mock):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    requests_mock.register_uri('GET', re.compile('localhost'), real_http=True)
    requests_mock.get('http://iceprod.test/datasets/123', status_code=200, json={})
    requests_mock.get('http://iceprod.test/config/123', status_code=200, json={})

    ret = await client.request('GET', '/config', {'dataset_id': '123'})


async def test_website_profile(server, requests_mock):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    ret = await client.request('GET', '/profile')
    







# """
# Test script for the website module.
# """

# from __future__ import absolute_import, division, print_function

# from tests.util import unittest_reporter, glob_tests, services_mock

# import logging
# logger = logging.getLogger('modules_website_test')

# import os, sys, time, random
# import shutil
# import tempfile
# import random
# import threading
# import subprocess
# from datetime import datetime,timedelta
# from functools import partial
# import unittest
# from contextlib import contextmanager
# import urllib

# from unittest.mock import patch, MagicMock

# import asyncio

# import tornado.ioloop
# from tornado.testing import AsyncTestCase
# from tornado.httpclient import AsyncHTTPClient, HTTPError


# from bs4 import BeautifulSoup

# import iceprod.core.logger
# from iceprod.core import to_log
# from iceprod.core.jsonUtil import json_encode,json_decode
# import iceprod.server
# from iceprod.server.modules.website import website
# import iceprod.server.modules.website
# import iceprod.server.module
# try:
    # from iceprod.server import ssl_cert
# except ImportError:
    # ssl_cert = None

# from ..module_test import module_test

# # check for javascript testing
# try:
    # from selenium import webdriver
    # from selenium.webdriver.support.ui import WebDriverWait
    # from selenium.webdriver.support import expected_conditions as EC
    # with to_log(sys.stdout):
        # subprocess.check_call(['which','phantomjs'])
    # testjs = True
# except (ImportError, subprocess.CalledProcessError):
    # logger.info('skipping javascript tests', exc_info=True)
    # testjs = False


# class website_test(AsyncTestCase):
    # def setUp(self):
        # super(website_test,self).setUp()

        # orig_dir = os.getcwd()
        # self.test_dir = tempfile.mkdtemp(dir=orig_dir)
        # os.chdir(self.test_dir)
        # def clean_dir():
            # os.chdir(orig_dir)
            # shutil.rmtree(self.test_dir)
        # self.addCleanup(clean_dir)

        # try:
            # # set hostname
            # self.hostname = b'localhost'

            # self.cfg = {'webserver':{'port':random.randint(10000,32000)},
                        # 'db':{'name':'test'},
                        # 'site_id':'abc',
                        # 'download':{'http_username':None,
                                    # 'http_password':None,
                                   # },
                        # 'rest_api':{'url':'foo','auth_key':'bar'},
                       # }
            # self.executor = {}
            # self.modules = services_mock()
            
            # self.website = website(self.cfg, self.executor, self.modules)
        # except:
            # logger.warning('error setting up modules_website', exc_info=True)
            # raise

        # self.auth_cookie = None

    # @contextmanager
    # def start(self):
        # try:
            # self.website.start()
            # yield
        # finally:
            # self.website.stop()

    # def request(self, *args, **kwargs):
        # if self.auth_cookie:
            # if 'headers' in kwargs:
                # kwargs['headers']['cookie'] = self.auth_cookie
            # else:
                # kwargs['headers'] = {'cookie': self.auth_cookie}
        # if 'follow_redirects' not in kwargs:
            # kwargs['follow_redirects'] = False
        # client = AsyncHTTPClient()
        # return client.fetch(*args, **kwargs)


    # @unittest_reporter(name='start/stop/kill')
    # async def test_010_start_stop_kill(self):
        # self.website.start()

        # await self.website.stop()

        # self.website.start()
        # self.website.kill()

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /')
    # async def test_110_Default(self, req):
        # with self.start():
            # datasets_status = {'processing':['d1','d2']}

            # address = 'http://localhost:%d'%(self.cfg['webserver']['port'])
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # if url.startswith('/dataset_summaries/status'):
                    # rest.called = True
                    # return datasets_status
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # # main site
            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # body = r.body.decode('utf-8')
            # self.assertFalse(rest.called)

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /submit')
    # async def test_200_Submit(self, req):
        # with self.start():
            # address = 'http://localhost:%d/submit'%(self.cfg['webserver']['port'])
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # if url.startswith('/create_token'):
                    # rest.called = True
                    # return {'result': 'token'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /config')
    # async def test_210_Config(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # address = 'http://localhost:{}/config?dataset_id={}'.format(
                # self.cfg['webserver']['port'], dataset_id)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # if url.startswith('/datasets/{}'.format(dataset_id)):
                    # return {'dataset_id':'foo'}
                # elif url.startswith('/config/{}'.format(dataset_id)):
                    # rest.called = True
                    # return {'dataset_id':'foo'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)
            
            # address = 'http://localhost:{}/config?dataset_id={}&edit=1'.format(
                # self.cfg['webserver']['port'], dataset_id)
            # logger.info('try connecting at %s',address)

            # async def rest(method, url, args=None):
                # if url.startswith('/create_token'):
                    # rest.called = True
                    # return {'result': 'token'}
                # elif url.startswith('/datasets/{}'.format(dataset_id)):
                    # return {'dataset_id':'foo'}
                # elif url.startswith('/config/{}'.format(dataset_id)):
                    # return {'dataset_id':'foo'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)
            
            # address = 'http://localhost:{}/config'.format(
                # self.cfg['webserver']['port'])
            # logger.info('try connecting at %s',address)

            # r = await self.request(address, raise_error=False)
            # self.assertEqual(r.code, 400)

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /dataset')
    # async def test_300_Dataset(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # address = 'http://localhost:{}/dataset'.format(
                # self.cfg['webserver']['port'])
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # if url.startswith('/datasets'):
                    # rest.called = True
                    # return {dataset_id:{'dataset_id':dataset_id, 'status':'processing',
                                        # 'dataset': 1234, 'description':'desc'}}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /dataset/<dataset_id>')
    # async def test_301_Dataset(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # address = 'http://localhost:{}/dataset/{}'.format(
                # self.cfg['webserver']['port'], dataset_id)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # if url == '/datasets/{}'.format(dataset_id):
                    # rest.called = True
                    # return {'dataset_id':dataset_id, 'status':'processing',
                            # 'dataset': 1234, 'description':'desc'}
                # elif url.startswith('/datasets/{}/job_counts/status'.format(dataset_id)):
                    # return {'processing':['foo','bar']}
                # elif url.startswith('/datasets/{}/task_counts/status'.format(dataset_id)):
                    # return {'processing':['1','2','3'], 'complete': ['4','5'], 'suspended':['6']}
                # elif url.startswith('/datasets/{}/task_counts/name_status'.format(dataset_id)):
                    # return {'generator':{'complete':2},
                            # 'hits':{'processing':1,'reset':1},
                            # 'detector':{'waiting':1,'suspended':1},
                           # }
                # elif url.startswith('/datasets/{}/task_stats'.format(dataset_id)):
                    # return {'generator':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         # 'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         # 'max_hrs': 1.1, 'efficiency': 0.54},
                            # 'hits':{'count': 2, 'gpu': 2, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         # 'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         # 'max_hrs': 1.1, 'efficiency': 0.54},
                            # 'detector':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         # 'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         # 'max_hrs': 1.1, 'efficiency': 0.54},
                           # }
                # elif url.startswith('/create_token'):
                    # return {'result': 'token'}
                # elif url == f'/config/{dataset_id}':
                    # return {'tasks':[]}
                # else:
                    # logger.info(f'{method} {url}')
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)

            # async def rest(method, url, args=None):
                # raise Exception()
            # req.return_value.request = rest
            # r = await self.request(address, raise_error=False)
            # self.assertEqual(r.code, 404)

    # @patch('iceprod.server.modules.website.RestClient')
    # @patch('iceprod.server.GlobalID.globalID_gen')
    # @unittest_reporter(name=' /dataset/<dataset_num>')
    # async def test_302_Dataset(self, globalid, req):
        # with self.start():
            # dataset_id = 'foo'
            # dataset_num = 120
            # address = 'http://localhost:{}/dataset/{}'.format(
                # self.cfg['webserver']['port'], dataset_num)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # globalid.return_value = dataset_id

            # async def rest(method, url, args=None):
                # logger.info('REST %s %s', method, url)
                # if url == '/datasets/{}'.format(dataset_id):
                    # rest.called = True
                    # return {'dataset_id':dataset_id, 'status':'processing',
                            # 'dataset': dataset_num, 'description':'desc'}
                # elif url == '/datasets':
                    # return {dataset_id: {'dataset_id':dataset_id, 'status':'processing',
                            # 'dataset': dataset_num, 'description':'desc'}}
                # elif url.startswith('/datasets/{}/job_counts/status'.format(dataset_id)):
                    # return {'processing':['foo','bar']}
                # elif url.startswith('/datasets/{}/task_counts/status'.format(dataset_id)):
                    # return {'processing':['1','2','3'], 'complete': ['4','5'], 'suspended':['6']}
                # elif url.startswith('/datasets/{}/task_counts/name_status'.format(dataset_id)):
                    # return {'generator':{'complete':2},
                            # 'hits':{'processing':1,'reset':1},
                            # 'detector':{'waiting':1,'suspended':1},
                           # }
                # elif url.startswith('/datasets/{}/task_stats'.format(dataset_id)):
                    # return {'generator':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         # 'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         # 'max_hrs': 1.1, 'efficiency': 0.54},
                            # 'hits':{'count': 2, 'gpu': 2, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         # 'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         # 'max_hrs': 1.1, 'efficiency': 0.54},
                            # 'detector':{'count': 2, 'gpu': 0, 'total_hrs': 1.2, 'total_err_hrs': 3.4,
                                         # 'avg_hrs': 0.7, 'stddev_hrs': 0.3, 'min_hrs': 0.4,
                                         # 'max_hrs': 1.1, 'efficiency': 0.54},
                           # }
                # elif url.startswith('/create_token'):
                    # return {'result': 'token'}
                # elif url == f'/config/{dataset_id}':
                    # return {'tasks':[]}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)

            # async def rest(method, url, args=None):
                # raise Exception()
            # req.return_value.request = rest
            # r = await self.request(address, raise_error=False)
            # self.assertEqual(r.code, 404)

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /dataset/<dataset_id>/task')
    # async def test_400_Task(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # task_id = 'bar'
            # address = 'http://localhost:{}/dataset/{}/task'.format(
                # self.cfg['webserver']['port'], dataset_id)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # if url.startswith('/datasets/{}/task_counts/status'.format(dataset_id)):
                    # rest.called = True
                    # return {'processing':['1','2','3'], 'complete': ['4','5'], 'suspended':['6']}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)
            
            # address = 'http://localhost:{}/dataset/{}/task?status=processing'.format(
                # self.cfg['webserver']['port'], dataset_id)

            # async def rest(method, url, args=None):
                # logger.info('REST %s %s', method, url)
                # if url.startswith('/datasets/{}/tasks'.format(dataset_id)):
                    # rest.called = True
                    # return {task_id:{'task_id':task_id,'dataset_id':dataset_id,
                                     # 'task_index': 0, 'job_index': 0,
                                     # 'job_id':'j', 'status':'processing',
                                     # 'name':'generator', 'failures':0}}
                # elif url.startswith('/datasets/{}/job'.format(dataset_id)):
                    # return {'job_index':0}
                # elif url.startswith('/create_token'):
                    # return {'result': 'token'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)
            # soup = BeautifulSoup(r.body, "html.parser")
            # for e in soup.findAll("a"):
                # if e.get('href').endswith('/bar'):
                    # break
            # else:
                # raise Exception('did not find task_id bar')

            # async def rest(method, url, args=None):
                # logger.info('REST %s %s', method, url)
                # if url.startswith('/datasets/{}/tasks'.format(dataset_id)):
                    # rest.called = True
                    # return {}
                # elif url.startswith('/create_token'):
                    # return {'result': 'token'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest
                
            # address = 'http://localhost:{}/dataset/{}/task?status=waiting'.format(
                # self.cfg['webserver']['port'], dataset_id)
            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)
            # soup = BeautifulSoup(r.body, "html.parser")
            # for e in soup.findAll("a"):
                # self.assertFalse(e.get('href').endswith('/bar'))

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /dataset/<dataset_id>/task/<task_id>')
    # async def test_401_Task(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # task_id = 'bar'
            # address = 'http://localhost:{}/dataset/{}/task/{}'.format(
                # self.cfg['webserver']['port'], dataset_id, task_id)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # logger.info('REST %s %s', method, url)
                # if url == '/datasets/{}'.format(dataset_id):
                    # return {'dataset_id':dataset_id, 'status':'processing',
                            # 'dataset': 1000, 'description':'desc'}
                # elif url == '/datasets/{}/tasks/{}'.format(dataset_id,task_id):
                    # rest.called = True
                    # return {'task_id':task_id,'dataset_id':dataset_id,
                            # 'task_index': 0, 'job_index': 0,
                            # 'job_id':'j', 'status':'processing',
                            # 'name':'generator', 'failures':0}
                # elif url.startswith('/datasets/{}/tasks/{}/log'.format(dataset_id,task_id)):
                    # return {'logs':[{'log_id':'baz','name':'stdout','data':'foo\nbar',
                                     # 'dataset_id':dataset_id,'task_id':task_id}]}
                # elif url.startswith('/datasets/{}/tasks/{}/task_stats'.format(dataset_id,task_id)):
                    # return {}
                # elif url.startswith('/create_token'):
                    # return {'result': 'token'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /dataset/<dataset_id>/job')
    # async def test_500_Job(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # job_id = 'bar'
            # address = 'http://localhost:{}/dataset/{}/job'.format(
                # self.cfg['webserver']['port'], dataset_id)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # logger.info('REST %s %s', method, url)
                # if url.startswith('/datasets/{}/jobs'.format(dataset_id)):
                    # rest.called = True
                    # return {job_id:{'job_id':job_id,'dataset_id':dataset_id,
                                    # 'status':'processing', 'status_changed':'2018',
                                    # 'job_index':0}}
                # elif url.startswith('/create_token'):
                    # return {'result': 'token'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)

            # address = 'http://localhost:{}/dataset/{}/job?status=processing'.format(
                # self.cfg['webserver']['port'], dataset_id)

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # soup = BeautifulSoup(r.body, "html.parser")
            # for e in soup.findAll("a"):
                # if e.get('href').endswith('/bar'):
                    # break
            # else:
                # raise Exception('did not find task_id bar')
                
            # address = 'http://localhost:{}/dataset/{}/job?status=waiting'.format(
                # self.cfg['webserver']['port'], dataset_id)
            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # soup = BeautifulSoup(r.body, "html.parser")
            # for e in soup.findAll("a"):
                # self.assertFalse(e.get('href').endswith('/bar'))

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /dataset/<dataset_id>/job/<job_id>')
    # async def test_501_Job(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # job_id = 'bar'
            # address = 'http://localhost:{}/dataset/{}/job/{}'.format(
                # self.cfg['webserver']['port'], dataset_id, job_id)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # logger.info('REST %s %s', method, url)
                # if url == '/datasets/{}'.format(dataset_id):
                    # return {'dataset_id':dataset_id, 'status':'processing',
                            # 'dataset': 1000, 'description':'desc'}
                # elif url.startswith('/datasets/{}/jobs/{}'.format(dataset_id,job_id)):
                    # rest.called = True
                    # return {'job_id':job_id,'dataset_id':dataset_id,
                            # 'status':'processing', 'status_changed':'2018',
                            # 'job_index':0}
                # elif url.startswith('/datasets/{}/tasks'.format(dataset_id)):
                    # return {'baz':{'task_id':'baz','dataset_id':dataset_id,
                                   # 'job_id':job_id,'name':'generate',
                                   # 'status':'queued', 'status_changed':'2018',
                                   # 'task_index':0, 'failures':0, 'requirements':{'cpu':1},
                                   # 'walltime':0.,'walltime_err':0.}}
                # elif url.startswith('/create_token'):
                    # return {'result': 'token'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)

    # @patch('iceprod.server.modules.website.RestClient')
    # @unittest_reporter(name=' /dataset/<dataset_id/log/<log_id>')
    # async def test_600_Log(self, req):
        # with self.start():
            # dataset_id = 'foo'
            # log_id = 'bar'
            # address = 'http://localhost:{}/dataset/{}/log/{}'.format(
                # self.cfg['webserver']['port'], dataset_id, log_id)
            # logger.info('try connecting at %s',address)

            # await self.get_auth_cookie()

            # async def rest(method, url, args=None):
                # logger.info('REST %s %s', method, url)
                # if url.startswith('/datasets/{}/logs/{}'.format(dataset_id,log_id)):
                    # rest.called = True
                    # return {'log_id':log_id,'dataset_id':dataset_id,
                            # 'name':'name', 'data': 'this is a log\nfoo bar'}
                # else:
                    # raise Exception()
            # rest.called = False
            # req.return_value.request = rest

            # r = await self.request(address)
            # self.assertEqual(r.code, 200)
            # self.assertTrue(rest.called)
            # self.assertIn(b'this is a log', r.body)


# def load_tests(loader, tests, pattern):
    # suite = unittest.TestSuite()
    # alltests = glob_tests(loader.getTestCaseNames(website_test))
    # suite.addTests(loader.loadTestsFromNames(alltests,website_test))
    # return suite
