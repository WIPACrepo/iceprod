"""
Test script for file_io
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('file_io_test')

import os, sys, time
import shutil
import tempfile
import random

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import iceprod.server.file_io


class file_io_test(unittest.TestCase):
    def setUp(self):
        super(file_io_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(file_io_test,self).tearDown()
    
    def test_01_open(self):
        """Test file_io open"""
        try:
            filename = os.path.join(self.test_dir,'test')
            with open(filename,'w') as f: 
                f.write('test')
            fileio = iceprod.server.file_io.AsyncFileIO()
            
            fut = fileio.open(filename)
            ret = None
            try:
                ret = fut.result(timeout=1)
                if not isinstance(ret,file):
                    raise Exception('did not return a file object')
                if ret.mode != 'r':
                    raise Exception('file did not open in read mode')
            finally:
                if ret is not None:
                    try:
                        ret.close()
                    except Exception:
                        pass
            
            for mode in ('r','w','a','rb','wb','ab','r+b','w+b','a+b'):
                fut = fileio.open(filename,mode)
                ret = None
                try:
                    ret = fut.result(timeout=1)
                    if not isinstance(ret,file):
                        raise Exception('did not return a file object for mode %s'%(mode,))
                    if ret.mode != mode:
                        raise Exception('file did not open in mode %s'%(mode,))
                finally:
                    if ret is not None:
                        try:
                            ret.close()
                        except Exception:
                            pass
            
        except Exception as e:
            logger.error('Error running file_io open test - %s',str(e))
            printer('Test file_io open',False)
            raise
        else:
            printer('Test file_io open')
    
    def test_02_close(self):
        """Test file_io close"""
        try:
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
                    f.close()
            
        except Exception as e:
            logger.error('Error running file_io close test - %s',str(e))
            printer('Test file_io close',False)
            raise
        else:
            printer('Test file_io close')
    
    def test_03_read(self):
        """Test file_io read"""
        try:
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
            
            data = ''.join(chr(i) for i in range(256))
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
            
        except Exception as e:
            logger.error('Error running file_io read test - %s',str(e))
            printer('Test file_io read',False)
            raise
        else:
            printer('Test file_io read')
    
    def test_04_readline(self):
        """Test file_io readline"""
        try:
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
            
        except Exception as e:
            logger.error('Error running file_io readline test - %s',str(e))
            printer('Test file_io readline',False)
            raise
        else:
            printer('Test file_io readline')
    
    def test_05_write(self):
        """Test file_io write"""
        try:
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
            
        except Exception as e:
            logger.error('Error running file_io write test - %s',str(e))
            printer('Test file_io write',False)
            raise
        else:
            printer('Test file_io write')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(file_io_test))
    suite.addTests(loader.loadTestsFromNames(alltests,file_io_test))
    return suite
