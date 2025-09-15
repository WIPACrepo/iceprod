"""
Test script for file_catalog
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('file_catalog')


try:
    pass
except:
    pass

import unittest
try:
    from unittest.mock import patch
except ImportError:
    pass
import requests_mock
import requests

import iceprod.core.file_catalog
from iceprod.core.jsonUtil import json_encode,json_decode


class file_catalog_test(unittest.TestCase):
    def setUp(self):
        super(file_catalog_test,self).setUp()

    def tearDown(self):
        super(file_catalog_test,self).tearDown()

    @unittest_reporter
    def test_001_FileCatalogLowLevel_init(self):
        url = 'http://foo.bar'
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        self.assertIn(url, fc.url)

        t = 12.32
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url, timeout=t)
        self.assertEqual(fc.timeout, t)

    @requests_mock.mock()
    @unittest_reporter
    def test_010_FileCatalogLowLevel_setitem(self, http_mock):
        url = 'http://foo.bar'
        data = url+'/api/file/12345'
        http_mock.get(requests_mock.ANY, exc=KeyError)
        http_mock.post(requests_mock.ANY, content=data.encode('utf-8'))
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        meta = {'checksum':'thecheck', 'locations':['/path/to/file']}
        fc['blah'] = meta
        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'POST')
        meta_ret = json_decode(req.body)
        self.assertEqual(meta_ret.pop('uid'), 'blah')
        self.assertEqual(meta_ret, meta)

    @requests_mock.mock()
    @unittest_reporter(name='FileCatalogLowLevel_setitem() - update')
    def test_011_FileCatalogLowLevel_setitem(self, http_mock):
        url = 'http://foo.bar'
        data = json_encode({'files':['/api/file/12345']})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        data = url+'/api/file/12345'
        http_mock.put(requests_mock.ANY, content=data.encode('utf-8'))

        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        meta = {'checksum':'thecheck', 'locations':['/path/to/file']}
        fc['blah'] = meta
        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'PUT')
        meta_ret = json_decode(req.body)
        self.assertEqual(meta_ret.pop('uid'), 'blah')
        self.assertEqual(meta_ret, meta)

    @requests_mock.mock()
    @unittest_reporter(name='FileCatalogLowLevel_setitem() - error')
    def test_012_FileCatalogLowLevel_setitem_error(self, http_mock):
        url = 'http://foo.bar'
        http_mock.get(requests_mock.ANY, exc=KeyError)
        http_mock.post(requests_mock.ANY, exc=requests.exceptions.HTTPError)
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        meta = {'checksum':'thecheck', 'locations':['/path/to/file']}
        try:
            fc['blah'] = meta
        except:
            pass
        else:
            raise Exception('should have raised Exception for non-existance')

        http_mock.post(requests_mock.ANY, exc=requests.exceptions.Timeout)
        try:
            fc['blah'] = meta
        except:
            pass
        else:
            raise Exception('should have raised Exception')

    @requests_mock.mock()
    @unittest_reporter
    def test_020_FileCatalogLowLevel_getitem(self, http_mock):
        data = json_encode({'files':['/api/file/12345']})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        meta = {'uid':'foo','checksum':'thecheck', 'locations':['/path/to/file']}
        data = json_encode(meta)
        http_mock.get('/api/file/12345', content=data.encode('utf-8'))
        url = 'http://foo.bar'
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        meta_ret = fc['foo']
        self.assertEqual(meta_ret, meta)

    @requests_mock.mock()
    @unittest_reporter(name='FileCatalogLowLevel_getitem() - error')
    def test_021_FileCatalogLowLevel_getitem_error(self, http_mock):
        data = json_encode({'files':[]})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        url = 'http://foo.bar'
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        try:
            meta = fc['blah']
        except KeyError:
            pass
        else:
            raise Exception('should have raised Exception')

        data = json_encode({'files':['/api/file/12345']})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        http_mock.get('/api/file/12345', exc=requests.exceptions.Timeout)
        try:
            meta = fc['blah']
        except:
            pass
        else:
            raise Exception('should have raised Exception')

        http_mock.get('/api/files', exc=requests.exceptions.HTTPError)
        try:
            meta = fc['blah']
        except:
            pass
        else:
            raise Exception('should have raised Exception')

        http_mock.get('/api/files', exc=requests.exceptions.Timeout)
        try:
            meta = fc['blah']
        except:
            pass
        else:
            raise Exception('should have raised Exception')

    @requests_mock.mock()
    @unittest_reporter
    def test_030_FileCatalogLowLevel_delitem(self, http_mock):
        data = json_encode({'files':['/api/file/12345']})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        http_mock.delete(requests_mock.ANY, content=b'')
        url = 'http://foo.bar'
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        del fc['foo']
        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'DELETE')

    @requests_mock.mock()
    @unittest_reporter(name='FileCatalogLowLevel_delitem() - error')
    def test_031_FileCatalogLowLevel_delitem_error(self, http_mock):
        data = json_encode({'files':['/api/file/12345']})
        http_mock.get(requests_mock.ANY, content=data.encode('utf-8'))
        http_mock.delete(requests_mock.ANY, exc=requests.exceptions.HTTPError)
        url = 'http://foo.bar'
        fc = iceprod.core.file_catalog.FileCatalogLowLevel(url)
        try:
            del fc['blah']
        except:
            pass
        else:
            raise Exception('should have raised Exception')

        http_mock.get(requests_mock.ANY, content=data.encode('utf-8'))
        http_mock.delete(requests_mock.ANY, exc=requests.exceptions.Timeout)
        try:
            del fc['blah']
        except:
            pass
        else:
            raise Exception('should have raised Exception')

        http_mock.get(requests_mock.ANY, exc=KeyError)
        try:
            del fc['blah']
        except KeyError:
            pass
        else:
            raise Exception('should have raised KeyError')

    @unittest_reporter
    def test_100_FileCatalog_init(self):
        url = 'http://foo.bar'
        fc = iceprod.core.file_catalog.FileCatalog(url)

    @requests_mock.mock()
    @unittest_reporter
    def test_110_FileCatalog_add(self, http_mock):
        url = 'http://foo.bar'
        http_mock.get(requests_mock.ANY, exc=KeyError)
        http_mock.post(requests_mock.ANY, content=b'')
        fc = iceprod.core.file_catalog.FileCatalog(url)

        name = 'foo'
        path = '/path/to/file'
        checksum = 'thechecksum'
        fc.add(name, path, checksum)

        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'POST')
        meta_ret = json_decode(req.body)
        expected = {'uid':name,'locations':[path],'checksum':checksum}
        self.assertEqual(meta_ret, expected)

        metadata = {'foo':'bar'}
        fc.add(name, path, checksum, metadata=metadata)

        self.assertTrue(http_mock.called)
        req = http_mock.request_history[2]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[3]
        self.assertEqual(req.method, 'POST')
        meta_ret = json_decode(req.body)
        expected = {'uid':name,'locations':[path],'checksum':checksum,
                    'foo':'bar'}
        self.assertEqual(meta_ret, expected)

    @requests_mock.mock()
    @unittest_reporter
    def test_120_FileCatalog_get(self, http_mock):
        data = json_encode({'files':['/api/file/12345']})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        url = 'http://foo.bar'
        meta = {'mongo_id':'bar','uid':'foo','checksum':'thecheck', 'locations':['/path/to/file']}
        data = json_encode(meta)
        http_mock.get('/api/file/12345', content=data.encode('utf-8'))
        fc = iceprod.core.file_catalog.FileCatalog(url)

        name = 'foo'
        path, checksum = fc.get(name)

        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'GET')
        self.assertEqual(path, meta['locations'][0])
        self.assertEqual(checksum, meta['checksum'])

    @requests_mock.mock()
    @unittest_reporter
    def test_121_FileCatalog_get_metadata(self, http_mock):
        data = json_encode({'files':['/api/file/12345']})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        url = 'http://foo.bar'
        meta = {'mongo_id':'bar','uid':'foo','checksum':'thecheck', 'locations':['/path/to/file']}
        data = json_encode(meta)
        http_mock.get('/api/file/12345', content=data.encode('utf-8'))
        fc = iceprod.core.file_catalog.FileCatalog(url)

        name = 'foo'
        meta_ret = fc.get_metadata(name)

        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'GET')
        self.assertEqual(meta, meta_ret)

    @requests_mock.mock()
    @unittest_reporter
    def test_130_FileCatalog_delete(self, http_mock):
        url = 'http://foo.bar'
        data = json_encode({'files':['/api/file/12345']})
        http_mock.get('/api/files', content=data.encode('utf-8'))
        http_mock.delete('/api/file/12345', content=b'')
        fc = iceprod.core.file_catalog.FileCatalog(url)

        fc.delete('foo')

        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'GET')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'DELETE')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(file_catalog_test))
    suite.addTests(loader.loadTestsFromNames(alltests,file_catalog_test))
    return suite
