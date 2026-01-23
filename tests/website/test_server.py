import json
import logging
from unittest.mock import AsyncMock, MagicMock, PropertyMock
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
import pytest
import requests
import requests_mock

from iceprod.core.config import Config


async def test_website_root(server):
    client = server()
    await client.request('GET', '/')


async def test_website_schemas(server):
    client = server()
    ret = await client.request('GET', '/schemas/v3/dataset.schema.json')
    ret = json.loads(ret)
    assert ret['title'] == 'IceProd Dataset Config'
    
    ret = await client.request('GET', '/schemas/v3/dataset_v3.1.schema.json')
    ret = json.loads(ret)
    assert ret['title'] == 'IceProd Dataset Config'
    logging.info("schema:%r", ret)
    assert ret['properties']['version']['default'] == 3.1
    
    ret = await client.request('GET', '/schemas/v3/dataset_v3.2.schema.json')
    ret = json.loads(ret)
    assert ret['title'] == 'IceProd Dataset Config'
    logging.info("schema:%r", ret)
    assert ret['properties']['version']['default'] == 3.2

    ret = await client.request('GET', '/schemas/v3/config.schema.json')
    ret = json.loads(ret)
    assert ret['title'] == 'IceProd Server Config'


async def test_website_submit(server):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    description = 'Test dataset'

    config = Config({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }],
        'version': 3.2,
    })
    config.fill_defaults()
    config.validate()

    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/submit')
        ret.raise_for_status()
        #logging.info('ret: %r', ret.text)
        doc = BeautifulSoup(ret.text, 'html.parser')
        xsrf = doc.find('input', {'name': '_xsrf'}).get('value')  # type: ignore
        logging.info('xsrf: %r', xsrf)
        logging.info('cookies: %r', http_client.cookies)

        config_str = json.dumps(config.config, indent=2)

        submit_mock = client.req_mock.add_mock('/actions/submit', {'result': '123'})

        ret = await client.request_raw(http_client, 'POST', '/submit', form_data={
            '_xsrf': xsrf,
            'submit_box': config_str,
            'description': description,
            'number_jobs': 10,
            'group': 'users',
        })
        assert ret.status_code == 303
        assert ret.headers['location'].endswith('/submit/status/123')

        assert submit_mock.called
        assert json.loads(submit_mock.call_args[0][2]['config']) == config.config
        assert submit_mock.call_args[0][2]['description'] == description


def MockError(status_code=400, body=''):
    def _inner(*args, **kwargs):
        response = MagicMock()
        response.status_code = status_code
        if body and isinstance(body, dict):
            response.text = json.dumps(body)
            response.json = MagicMock(return_value=body)
        elif body:
            response.text = body
            response.json = MagicMock(return_value=None)
        raise requests.exceptions.HTTPError(response=response)
    return _inner


async def test_website_submit_invalid(server):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    description = 'Test dataset'

    config = {'tasks':[
        {
            'name': 'testing',
            'trays': []
        }
    ]}

    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/submit')
        ret.raise_for_status()
        #logging.info('ret: %r', ret.text)
        doc = BeautifulSoup(ret.text, 'html.parser')
        xsrf = doc.find('input', {'name': '_xsrf'}).get('value')  # type: ignore
        logging.info('xsrf: %r', xsrf)
        logging.info('cookies: %r', http_client.cookies)

        config_str = json.dumps(config)

        submit_mock = client.req_mock.add_mock('/actions/submit', MockError(400, '{"error": "Validation error"}'))

        ret = await client.request_raw(http_client, 'POST', '/submit', form_data={
            '_xsrf': xsrf,
            'submit_box': config_str,
            'description': description,
            'number_jobs': 10,
            'group': 'users',
        })
        assert ret.status_code == 400
        doc = BeautifulSoup(ret.text, 'html.parser')
        error = doc.find('div', {'id': 'error'}).text  # type: ignore
        assert 'Validation error' in error


async def test_website_submit_status_queued(server):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    description = 'Test dataset'

    config = Config({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }],
        'version': 3.2,
    })
    config.fill_defaults()
    config.validate()
    config_str = json.dumps(config.config)

    async with client.get_http_client() as http_client:
        submit_mock = client.req_mock.add_mock('/actions/submit/123', {
            'status': 'queued',
            'payload': {
                'config': config_str,
                'description': description,
                'jobs_submitted': 10,
                'group': 'users',
            },
        })

        ret = await client.request_raw(http_client, 'GET', '/submit/status/123')
        ret.raise_for_status()

        assert ret.status_code == 200


async def test_website_submit_status_error(server):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    description = 'Test dataset'

    config = Config({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }],
        'version': 3.2,
    })
    config.fill_defaults()
    config.validate()
    config_str = json.dumps(config.config)

    async with client.get_http_client() as http_client:
        submit_mock = client.req_mock.add_mock('/actions/submit/123', {
            'status': 'error',
            'payload': {
                'config': config_str,
                'description': description,
                'jobs_submitted': 10,
                'group': 'users',
            },
            'error_message': 'Validation error'
        })

        ret = await client.request_raw(http_client, 'GET', '/submit/status/123')
        ret.raise_for_status()

        assert ret.status_code == 200
        doc = BeautifulSoup(ret.text, 'html.parser')
        logging.info('doc: %s', ret.text)
        error = doc.find('div', {'id': 'error'}).text  # type: ignore
        assert error == 'Validation error'
        

async def test_website_submit_status_complete(server):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    description = 'Test dataset'

    config = Config({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }],
        'version': 3.2,
    })
    config.fill_defaults()
    config.validate()
    config_str = json.dumps(config.config)

    async with client.get_http_client() as http_client:
        submit_mock = client.req_mock.add_mock('/actions/submit/123', {
            'status': 'complete',
            'payload': {
                'config': config_str,
                'description': description,
                'jobs_submitted': 10,
                'group': 'users',
                'dataset_id': '456',
            },
        })

        ret = await client.request_raw(http_client, 'GET', '/submit/status/123')
        logging.info('headers %r', ret.headers)

        assert ret.status_code == 303
        assert ret.headers['location'].endswith('/dataset/456')


async def test_website_config(server):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    client.req_mock.add_mock('/datasets/123', {})
    client.req_mock.add_mock('/config/123', {})

    ret = await client.request('GET', '/config', {'dataset_id': '123'})


async def test_website_config_status_queued(server):
    client = server(username='username', roles=['user'], groups=['users', 'simprod'])

    dataset_id = 'd1'
    description = 'Test dataset'

    config = Config({
        'tasks':[{
            'name': 'testing',
            'trays': [{
                'modules': [{
                    'src': '/usr/bin/python3',
                    'args': ''
                }]
            }]
        }],
        'version': 3.2,
    })
    config.fill_defaults()
    config.validate()
    config_str = json.dumps(config.config)

    async with client.get_http_client() as http_client:
        client.req_mock.add_mock('/actions/edit_config/123', {
            'status': 'queued',
            'payload': {
                'dataset_id': dataset_id,
                'config': config_str,
                'description': description,
            },
        })

        ret = await client.request_raw(http_client, 'GET', '/config/status/123')
        ret.raise_for_status()

        assert ret.status_code == 200


async def test_website_profile(server):
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

