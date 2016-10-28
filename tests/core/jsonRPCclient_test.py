"""
Test script for jsonRPCclient
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('jsonRPCclient')

import os, sys, time
import shutil
import random
import string
from threading import Thread
from multiprocessing import Queue

try:
    import cPickle as pickle
except:
    import pickle

import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import requests_mock
from requests.exceptions import Timeout

import iceprod.core.util
import iceprod.core.jsonUtil
import iceprod.core.jsonRPCclient


class jsonRPCclient_test(unittest.TestCase):
    def setUp(self):
        super(jsonRPCclient_test,self).setUp()
        self.test_dir = os.path.join(os.getcwd(),'test')
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(jsonRPCclient_test,self).tearDown()

    @unittest_reporter(name='Client.init()')
    def test_01_init(self):
        """Test init"""
        address = 'http://test/jsonrpc'
        rpc = iceprod.core.jsonRPCclient.Client(address=address)

    @unittest_reporter(name='Client.newid()')
    def test_02_newid(self):
        """Test newid"""
        def runner(q):
            ret = []
            for _ in range(1000):
                ret.append(iceprod.core.jsonRPCclient.Client.newid())
            q.put(ret)
        q = Queue()
        for _ in range(10):
            Thread(target=runner,args=(q,)).start()
        ret = []
        for _ in range(10):
            ret.extend(q.get())
        if len(ret) != len(set(ret)):
            raise Exception('duplicate ids')
        if len(ret) != 1000*10:
            raise Exception('incorrect number of ids')

    @requests_mock.mock()
    @unittest_reporter(name='Client.request()')
    def test_03_request(self, mock):
        """Test request"""
        address = 'http://test/jsonrpc'
        passkey = 'passkey'
        result = 'the result'
        rpc = iceprod.core.jsonRPCclient.Client(address=address)

        def response(req, ctx):
            body = iceprod.core.jsonUtil.json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['result'] = result
            return iceprod.core.jsonUtil.json_encode(ret)
        mock.post('/jsonrpc', content=response)

        kwargs = {
            'passkey':passkey,
        }
        ret = rpc.request('test',kwargs)

        self.assertTrue(mock.called)
        self.assertEqual(ret, result)

    @unittest_reporter(name='JSONRPC.start()')
    def test_10_start(self):
        """Test start"""
        address = 'http://test/jsonrpc'
        passkey = 'passkey'
        for _ in range(10):
            iceprod.core.jsonRPCclient.JSONRPC.start(address=address,passkey=passkey)

    @unittest_reporter(name='JSONRPC.stop()')
    def test_11_stop(self):
        """Test stop"""
        address = 'http://test/jsonrpc'
        passkey = 'passkey'
        for _ in range(10):
            iceprod.core.jsonRPCclient.JSONRPC.start(address=address,passkey=passkey)
            iceprod.core.jsonRPCclient.JSONRPC.stop()

    @unittest_reporter(name='JSONRPC.restart()')
    def test_12_restart(self):
        """Test restart"""
        address = 'http://test/jsonrpc'
        passkey = 'passkey'
        for _ in range(10):
            iceprod.core.jsonRPCclient.JSONRPC.start(address=address,passkey=passkey)
            iceprod.core.jsonRPCclient.JSONRPC.restart()

    @requests_mock.mock()
    @unittest_reporter(name='JSONRPC.rpc()')
    def test_20_rpc(self, mock):
        """Test actual rpc functions"""
        address = 'http://test/jsonrpc'
        passkey = 'passkey'
        result = 'the result'
        iceprod.core.jsonRPCclient.JSONRPC.start(address=address,passkey=passkey)

        def response(req, ctx):
            body = iceprod.core.jsonUtil.json_decode(req.body)
            ret = {}
            ret['id'] = body['id']
            ret['jsonrpc'] = body['jsonrpc']
            ret['result'] = result
            return iceprod.core.jsonUtil.json_encode(ret)
        mock.post('/jsonrpc', content=response)

        ret = iceprod.core.jsonRPCclient.JSONRPC.test()

        self.assertTrue(mock.called)
        self.assertEqual(ret, result)

    @requests_mock.mock()
    @unittest_reporter(name='JSONRPC timeout')
    def test_21_timeout(self, mock):
        """Test rpc timeout"""
        address = 'http://test/jsonrpc'
        passkey = 'passkey'
        result = 'the result'
        iceprod.core.jsonRPCclient.JSONRPC.start(timeout=2,address=address,passkey=passkey)

        def response(req, ctx):
            raise Timeout()
        mock.post('/jsonrpc', content=response)

        try:
            iceprod.core.jsonRPCclient.JSONRPC.test()
        except:
            pass
        else:
            raise Exception('timeout not raised')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(jsonRPCclient_test))
    suite.addTests(loader.loadTestsFromNames(alltests,jsonRPCclient_test))
    return suite
