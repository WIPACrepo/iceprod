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

from iceprod.core.jsonUtil import json_encode, json_decode
import iceprod.server.master_communication

class FakeResponse(object):
    def __init__(self, code, body, error=None):
        self.code = code
        self.body = body
        if (not error) and (code < 200 or code >= 300):
            self.error = Exception('http code error')
        else:
            self.error = error
    def rethrow(self):
        if self.error:
            raise self.error

def make_response(*args, **kwargs):
    f = Future()
    f.set_result(FakeResponse(*args,**kwargs))
    return f

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

    @patch('iceprod.server.master_communication.AsyncHTTPClient.fetch')
    @unittest_reporter
    def test_01_send_master(self, fetch):
        """Test master_communication.send_master()"""

        site_id = 'thesite'
        cfg = {'site_id':site_id,'master':{'url':'localhost','passkey':'thekey'}}
        method = 'mymethod'
        fetch.return_value = make_response(200,json_encode({'result':'ok'}))
        
        ret = yield iceprod.server.master_communication.send_master(cfg, method)
        self.assertTrue(fetch.call_count, 1)
        client_body = json_decode(fetch.call_args[1]['body'])
        self.assertEqual(client_body['method'], method)
        expected = {'site_id':site_id,'passkey':cfg['master']['passkey']}
        self.assertEqual(client_body['params'], expected)
        self.assertEqual(ret, 'ok')

        fetch.return_value = make_response(200,json_encode({'error':'fail'}))
        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        fetch.return_value = make_response(400,'','fail')
        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        fetch.return_value = make_response(200,json_encode({}))
        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        fetch.return_value = make_response(200,json_encode({'result':'ok'}))
        cfg = {}
        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # try without passkey
        cfg = {'master':{'url':'localhost'}}
        try:
            yield iceprod.server.master_communication.send_master(cfg, method)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # try giving passkey directly
        fetch.reset_mock()
        cfg = {'master':{'url':'localhost','passkey':'tmpkey'}}
        ret = yield iceprod.server.master_communication.send_master(cfg,
                method, passkey='otherkey')
        self.assertTrue(fetch.call_count, 1)
        client_body = json_decode(fetch.call_args[1]['body'])
        self.assertEqual(client_body['method'], method)
        expected = {'passkey':'otherkey'}
        self.assertEqual(client_body['params'], expected)
        self.assertEqual(ret, 'ok')

        # try with / on end of url
        cfg = {'master':{'url':'localhost/','passkey':'tmpkey'}}
        ret = yield iceprod.server.master_communication.send_master(cfg, method)
        self.assertTrue(fetch.call_count, 1)
        client_body = json_decode(fetch.call_args[1]['body'])
        self.assertEqual(client_body['method'], method)
        expected = {'passkey':cfg['master']['passkey']}
        self.assertEqual(client_body['params'], expected)
        self.assertEqual(ret, 'ok')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(master_communication_test))
    suite.addTests(loader.loadTestsFromNames(alltests,master_communication_test))
    return suite
