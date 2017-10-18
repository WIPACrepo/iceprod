"""
Test script for file_io
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('file_io_test')

import os, sys, time
import shutil
import tempfile
import random
import unittest
from io import IOBase

import iceprod.server.file_io


class file_io_test(unittest.TestCase):
    def setUp(self):
        super(file_io_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(file_io_test,self).tearDown()

    @unittest_reporter
    def test_01_open(self):
        """Test file_io open"""
        filename = os.path.join(self.test_dir,'test')
        with open(filename,'w') as f:
            f.write('test')
        fileio = iceprod.server.file_io.AsyncFileIO()

        fut = fileio.open(filename)
        ret = None
        try:
            ret = fut.result(timeout=1)
            if not isinstance(ret,IOBase):
                raise Exception('did not return a file object')
            if ret.mode != 'r':
                raise Exception('file did not open in read mode')
        finally:
            if ret is not None:
                try:
                    ret.close()
                except Exception:
                    pass

        for mode in ('r','w','a','rb','wb','ab','rb+','wb+','ab+'):
            fut = fileio.open(filename,mode)
            ret = None
            try:
                ret = fut.result(timeout=1)
                self.assertIsInstance(ret,IOBase)
                if 'r' in mode or '+' in mode:
                    self.assertTrue(ret.readable())
                if 'w' in mode or 'a' in mode:
                    self.assertTrue(ret.writable())
                if 'b' in mode:
                    self.assertIn('b', ret.mode)
            finally:
                if ret is not None:
                    try:
                        ret.close()
                    except Exception:
                        pass

    @unittest_reporter
    def test_02_close(self):
        """Test file_io close"""
        filename = os.path.join(self.test_dir,'test')
        with open(filename,'w') as f:
            f.write('test')
        fileio = iceprod.server.file_io.AsyncFileIO()
        f = open(filename)
        try:
            fut = fileio.close(f)
            fut.result(timeout=1)
            if not f.closed:
                raise Exception('did not close file')
        finally:
            if not f.closed:
                f.close

    @unittest_reporter
    def test_03_read(self):
        """Test file_io read"""
        filename = os.path.join(self.test_dir,'test')
        data = 'test'
        with open(filename,'w') as f:
            f.write(data)
        fileio = iceprod.server.file_io.AsyncFileIO()
        f = open(filename)
        try:
            fut = fileio.read(f)
            ret = fut.result(timeout=1)
            if ret != data:
                raise Exception('did not read data')
        finally:
            if not f.closed:
                f.close()

        data = ''.join(chr(i) for i in range(256)).encode('utf-8')
        with open(filename,'wb') as f:
            f.write(data)
        f = open(filename,'rb')
        try:
            fut = fileio.read(f,150)
            ret = fut.result(timeout=1)
            if ret != data[:150]:
                raise Exception('did not read 150 chars of data')
            fut = fileio.read(f)
            ret = fut.result(timeout=1)
            if ret != data[150:]:
                raise Exception('did not read rest of data')
        finally:
            if not f.closed:
                f.close()

    @unittest_reporter
    def test_04_readline(self):
        """Test file_io readline"""
        filename = os.path.join(self.test_dir,'test')
        data = 'test\ndata'
        with open(filename,'w') as f:
            f.write(data)
        fileio = iceprod.server.file_io.AsyncFileIO()
        f = open(filename)
        try:
            fut = fileio.readline(f)
            ret = fut.result(timeout=1)
            if ret != data.split('\n')[0]+'\n':
                logger.info('ret = %r',ret)
                logger.info('first line = %r',data.split('\n')[0]+'\n')
                raise Exception('did not read first line')
            fut = fileio.readline(f)
            ret = fut.result(timeout=1)
            if ret != data.split('\n')[1]:
                logger.info('ret = %r',ret)
                logger.info('2nd line = %r',data.split('\n')[1])
                raise Exception('did not read second line')
        finally:
            if not f.closed:
                f.close()

    @unittest_reporter
    def test_05_write(self):
        """Test file_io write"""
        filename = os.path.join(self.test_dir,'test')
        data = 'test\ndata'
        fileio = iceprod.server.file_io.AsyncFileIO()

        f = open(filename,'w')
        try:
            fut = fileio.write(f,data)
            fut.result(timeout=1)
        finally:
            if not f.closed:
                f.close()
        if not os.path.exists(filename):
            raise Exception('file does not exist')
        ret = open(filename).read()
        if ret != data:
            logger.info('ret = %r',ret)
            logger.info('data = %r',data)
            raise Exception('did not write data')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(file_io_test))
    suite.addTests(loader.loadTestsFromNames(alltests,file_io_test))
    return suite
