"""
Test script for master_communication
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('master_communication_test')

import os, sys, time
import shutil
import tempfile
import random
import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from tornado.testing import AsyncTestCase
from tornado.concurrent import Future

import requests_mock
from requests.exceptions import Timeout, SSLError

from iceprod.core.jsonUtil import json_encode, json_decode
import iceprod.server.master_communication


class master_communication_test(AsyncTestCase):
    def setUp(self):
        super(master_communication_test,self).setUp()
        orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp(dir=orig_dir)
        os.chdir(self.test_dir)
        def clean_dir():
            os.chdir(orig_dir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(clean_dir)

    @requests_mock.mock()
    @unittest_reporter(name='send_master()')
    def test_01_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['result'] = 'ok'
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response)

        site_id = 'thesite'
        cfg = {'site_id':site_id,'master':{'url':'http://localhost','passkey':'thekey'}}
        method = 'mymethod'

        ret = yield iceprod.server.master_communication.send_master(cfg, method)
        self.assertTrue(mock.call_count, 1)
        client_body = json_decode(mock.request_history[0].body)
        self.assertEqual(client_body['method'], method)
        expected = {'site_id':site_id,'passkey':cfg['master']['passkey']}
        self.assertEqual(client_body['params'], expected)
        self.assertEqual(ret, 'ok')

    @requests_mock.mock()
    @unittest_reporter(name='send_master() - fail')
    def test_02_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['error'] = 'fail'
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response)

        site_id = 'thesite'
        cfg = {'site_id':site_id,'master':{'url':'http://localhost','passkey':'thekey'}}
        method = 'mymethod'

        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @requests_mock.mock()
    @unittest_reporter(name='send_master() - bad request')
    def test_03_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['error'] = 'fail'
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response, status_code=400)

        site_id = 'thesite'
        cfg = {'site_id':site_id,'master':{'url':'http://localhost','passkey':'thekey'}}
        method = 'mymethod'

        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @requests_mock.mock()
    @unittest_reporter(name='send_master() - empty response')
    def test_04_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response)

        site_id = 'thesite'
        cfg = {'site_id':site_id,'master':{'url':'http://localhost','passkey':'thekey'}}
        method = 'mymethod'

        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @requests_mock.mock()
    @unittest_reporter(name='send_master() - bad cfg')
    def test_05_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['result'] = 'ok'
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response)

        cfg = {}
        method = 'mymethod'

        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @requests_mock.mock()
    @unittest_reporter(name='send_master() - missing passkey')
    def test_06_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['result'] = 'ok'
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response)

        cfg = {'master':{'url':'http://localhost'}}
        method = 'mymethod'
        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @requests_mock.mock()
    @unittest_reporter(name='send_master() - direct passkey')
    def test_07_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['result'] = 'ok'
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response)

        cfg = {'master':{'url':'http://localhost','passkey':'tmpkey'}}
        method = 'mymethod'
        ret = yield iceprod.server.master_communication.send_master(cfg,
                method, passkey='otherkey')
        self.assertTrue(mock.call_count, 1)
        client_body = json_decode(mock.request_history[0].body)
        self.assertEqual(client_body['method'], method)
        expected = {'passkey':'otherkey'}
        self.assertEqual(client_body['params'], expected)
        self.assertEqual(ret, 'ok')

    @requests_mock.mock()
    @unittest_reporter(name='send_master() - redirect')
    def test_08_send_master(self, mock):
        """Test master_communication.send_master()"""
        def response(req, ctx):
            body = json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['result'] = 'ok'
            return json_encode(ret).encode('utf-8')
        mock.post('/jsonrpc', content=response)

        # try with / on end of url
        cfg = {'master':{'url':'http://localhost/','passkey':'tmpkey'}}
        method = 'mymethod'
        ret = yield iceprod.server.master_communication.send_master(cfg, method)
        self.assertTrue(mock.call_count, 1)
        client_body = json_decode(mock.request_history[0].body)
        self.assertEqual(client_body['method'], method)
        expected = {'passkey':cfg['master']['passkey']}
        self.assertEqual(client_body['params'], expected)
        self.assertEqual(ret, 'ok')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(master_communication_test))
    suite.addTests(loader.loadTestsFromNames(alltests,master_communication_test))
    return suite
