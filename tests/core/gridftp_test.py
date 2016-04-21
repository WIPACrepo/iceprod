"""
Test script for gridftp
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('gridftp')

import os, sys, time
import shutil
import random
import string
import subprocess
import tempfile
from threading import Event

try:
    import cPickle as pickle
except:
    import pickle

try:
    import unittest2 as unittest
except ImportError:
    import unittest


import iceprod.core.gridftp

skip_tests = False
if (subprocess.call(['which','uberftp']) or
    subprocess.call(['which','globus-url-copy'])):
    skip_tests = True

class gridftp_test(unittest.TestCase):
    def setUp(self):
        self._timeout = 30
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        self.server_test_dir = os.path.join('gsiftp://gridftp.icecube.wisc.edu/data/sim/sim-new/tmp/test',
                                            str(random.randint(0,2**32)))
        try:
            iceprod.core.gridftp.GridFTP.mkdir(self.server_test_dir,
                                               parents=True,
                                               request_timeout=self._timeout)
        except:
            pass
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        super(gridftp_test,self).setUp()

    def tearDown(self):
        try:
            iceprod.core.gridftp.GridFTP.rmtree(self.server_test_dir,
                                                request_timeout=self._timeout)
        except:
            pass
        shutil.rmtree(self.test_dir)
        super(gridftp_test,self).tearDown()

    @unittest_reporter(skip=skip_tests)
    def test_01_supported_address(self):
        """Test supported_address"""
        bad_addresses = ['test','file:/test','gsiftp:test','gsiftp:/test',
                         'ftp:test','http://x2100.icecube.wisc.edu',
                         'ftp:/test']
        good_addresses = ['gsiftp://data.icecube.wisc.edu','ftp://gnu.org',
                          'gsiftp://gridftp-rr.icecube.wisc.edu/data/sim/sim-new']

        for i in xrange(0,10):
            for a in bad_addresses:
                ret = iceprod.core.gridftp.GridFTP.supported_address(a)
                if ret is True:
                    raise Exception('Bad address %s was called good'%a)
            for a in good_addresses:
                ret = iceprod.core.gridftp.GridFTP.supported_address(a)
                if ret is not True:
                    raise Exception('Good address %s was called bad'%a)

    @unittest_reporter(skip=skip_tests)
    def test_02_address_split(self):
        """Test address_split"""
        good_addresses = {'gsiftp://data.icecube.wisc.edu':('gsiftp://data.icecube.wisc.edu','/'),
                      'ftp://gnu.org':('ftp://gnu.org','/'),
                      'gsiftp://gridftp-rr.icecube.wisc.edu/data/sim/sim-new':('gsiftp://gridftp-rr.icecube.wisc.edu','/data/sim/sim-new')}

        for i in xrange(0,10):
            for a in good_addresses:
                pieces = iceprod.core.gridftp.GridFTP.address_split(a)
                if pieces != good_addresses[a]:
                    raise Exception('Address %s was not split properly'%a)

    @unittest_reporter(skip=skip_tests,name='put() with str')
    def test_100_put_str(self):
        """Test put with a str - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='put() with file')
    def test_101_put_file(self):
        """Test put with a file - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        # make temp file
        filename = os.path.join(self.test_dir,'test')
        filecontents = 'this is a test'
        with open(filename,'w') as f:
            f.write(filecontents)

        try:
            # put file
            ret = iceprod.core.gridftp.GridFTP.put(address,filename=filename,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                    pass

    @unittest_reporter(skip=skip_tests,name='put() with func')
    def test_102_put_func(self):
        """Test put with a function - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'
        def contents():
            # give every 10 chars
            for i in xrange(0,len(filecontents),10):
                yield filecontents[i:i+10]

        try:
            # put from function
            ret = iceprod.core.gridftp.GridFTP.put(address,streaming_callback=contents().next,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='get() with str')
    def test_110_get_str(self):
        """Test get with a str - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            # get str
            ret = iceprod.core.gridftp.GridFTP.get(address,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('get failed: ret=%r'%ret)
            if ret != filecontents:
                logger.warning('contents should be: %s',filecontents)
                logger.warning('contents is actually: %s',ret)
                raise Exception('contents is incorrect')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='get() with file')
    def test_111_get_file(self):
        """Test get with a file - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        # make temp file
        filename = os.path.join(self.test_dir,'test')
        filename2 = os.path.join(self.test_dir,'test2')
        filecontents = 'this is a test'
        with open(filename,'w') as f:
            f.write(filecontents)

        try:
            # put file
            ret = iceprod.core.gridftp.GridFTP.put(address,filename=filename,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            # get file
            ret = iceprod.core.gridftp.GridFTP.get(address,filename=filename2,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('get failed: ret=%r'%ret)
            if not os.path.exists(filename2):
                raise Exception('dest file does not exist')
            with open(filename2) as f:
                newcontents = f.read()
                if filecontents != newcontents:
                    logger.warning('contents should be: %s',filecontents)
                    logger.warning('contents is actually: %s',newcontents)
                    raise Exception('file contents is incorrect')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='get() with func')
    def test_112_get_func(self):
        """Test get with a function - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'
        def contents():
            # give every 10 chars
            for i in xrange(0,len(filecontents),10):
                yield filecontents[i:i+10]
        def contents2(data):
            contents2.data += data
        contents2.data = ''

        try:
            # put from function
            ret = iceprod.core.gridftp.GridFTP.put(address,streaming_callback=contents().next,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            # get strGridFTP
            ret = iceprod.core.gridftp.GridFTP.get(address,streaming_callback=contents2,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('get failed: ret=%r'%ret)
            if contents2.data != filecontents:
                logger.warning('contents should be: %s',filecontents)
                logger.warning('contents is actually: %s',contents2.data)
                raise Exception('contents is incorrect')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir)')
    def test_120_list(self):
        """Test list of directory - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != []:
                logger.info('expected: []')
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file)')
    def test_121_list(self):
        """Test list of file - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address,
                                                    request_timeout=self._timeout)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != ['test_file']:
                logger.info("expected: ['test_file']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir,dotfiles)')
    def test_122_list(self):
        """Test list of dir with dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address,dotfiles=True,
                                                    request_timeout=self._timeout)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != ['.','..']:
                logger.info("expected: ['.','..']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file,dotfiles)')
    def test_123_list(self):
        """Test list of file with dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address,dotfiles=True,
                                                    request_timeout=self._timeout)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != ['test_file']:
                logger.info('expected: [\'test_file\']')
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir,details)')
    def test_124_list(self):
        """Test list of dir with details - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                    request_timeout=self._timeout)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != []:
                logger.info('expected: []')
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file,details)')
    def test_125_list(self):
        """Test list of file with details - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                    request_timeout=self._timeout)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if len(ret) != 1 or ret[0].directory:
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir,details,dotfiles)')
    def test_126_list(self):
        """Test list of dir with details and dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                    dotfiles=True,
                                                    request_timeout=self._timeout)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if (len(ret) != 2 or not any([x.name == '.' for x in ret])
                or not any([x.name == '..' for x in ret])):
                logger.info("expected: ['..','.']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file,details,dotfiles)')
    def test_127_list(self):
        """Test list of file with details and dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                    dotfiles=True,
                                                    request_timeout=self._timeout)
            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if len(ret) != 1 or ret[0].name != 'test_file':
                logger.info("expected: ['test_file']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests)
    def test_130_delete(self):
        """Test delete - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        ret = iceprod.core.gridftp.GridFTP.delete(address,
                                                  request_timeout=self._timeout)
        if ret is not True:
            raise Exception('delete failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree(file)')
    def test_140_rmtree(self):
        """Test rmtree of a file - synchronous"""
        address = os.path.join(self.server_test_dir,'file_test')
        filecontents = 'this is a test'

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        ret = iceprod.core.gridftp.GridFTP.rmtree(address,
                                                  request_timeout=self._timeout)
        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree(empty dir)')
    def test_141_rmtree(self):
        """Test rmtree of an empty dir - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        # mkdir
        ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        ret = iceprod.core.gridftp.GridFTP.rmtree(address,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree(dir + file)')
    def test_142_rmtree(self):
        """Test rmtree of a directory with a file - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        # mkdir
        ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        address2 = os.path.join(self.server_test_dir,'test','file_test')
        filecontents = 'this is a test'

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address2,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        ret = iceprod.core.gridftp.GridFTP.rmtree(address,
                                                  request_timeout=self._timeout)
        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree(dir + dir + file)')
    def test_143_rmtree(self):
        """Test rmtree of dir with subdir and subfile - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        # mkdir
        ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        # mkdir
        address2 = os.path.join(self.server_test_dir,'test','test2')
        ret = iceprod.core.gridftp.GridFTP.mkdir(address2,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        address3 = os.path.join(self.server_test_dir,'test','test2','file_test')
        filecontents = 'this is a test'

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address3,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        ret = iceprod.core.gridftp.GridFTP.rmtree(address,
                                                  request_timeout=self._timeout)
        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests)
    def test_160_exists(self):
        """Test exists - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        try:
            ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                      request_timeout=self._timeout)
            if ret is True:
                raise Exception('exists succeeded when it should have failed')

            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                      request_timeout=self._timeout)
            if ret is not True:
                raise Exception('exists failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests)
    def test_170_move(self):
        """Test move - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        address2 = os.path.join(self.server_test_dir,'test2')
        filecontents = 'this is a test'

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            ret = iceprod.core.gridftp.GridFTP.exists(address2,
                                                      request_timeout=self._timeout)
            if ret is True:
                raise Exception('exists succeeded before move')

            ret = iceprod.core.gridftp.GridFTP.move(address,address2,
                                                    request_timeout=self._timeout)
            if ret is not True:
                raise Exception('move failed: ret=%r'%ret)

            ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                      request_timeout=self._timeout)
            if ret is True:
                raise Exception('exists succeeded on old address')
            ret = iceprod.core.gridftp.GridFTP.exists(address2,
                                                      request_timeout=self._timeout)
            if ret is not True:
                raise Exception('exists failed on new address')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address2,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests)
    def test_180_checksum(self):
        """Test checksums - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        import hashlib

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            ret = iceprod.core.gridftp.GridFTP.md5sum(address,
                                                      request_timeout=self._timeout)
            correct = hashlib.md5(filecontents).hexdigest()
            if ret != correct:
                raise Exception('md5sum failed: ret=%r and correct=%r'%(ret,correct))

            ret = iceprod.core.gridftp.GridFTP.sha1sum(address,
                                                       request_timeout=self._timeout)
            correct = hashlib.sha1(filecontents).hexdigest()
            if ret != correct:
                raise Exception('sha1sum failed: ret=%r and correct=%r'%(ret,correct))

            ret = iceprod.core.gridftp.GridFTP.sha256sum(address,
                                                         request_timeout=self._timeout)
            correct = hashlib.sha256(filecontents).hexdigest()
            if ret != correct:
                raise Exception('sha256sum failed: ret=%r and correct=%r'%(ret,correct))

            ret = iceprod.core.gridftp.GridFTP.sha512sum(address,
                                                         request_timeout=self._timeout)
            correct = hashlib.sha512(filecontents).hexdigest()
            if ret != correct:
                raise Exception('sha512sum failed: ret=%r and correct=%r'%(ret,correct))
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests)
    def test_190_size(self):
        """Test size - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            ret = iceprod.core.gridftp.GridFTP.size(address,
                                                    request_timeout=self._timeout)
            correct = len(filecontents)
            if ret != correct:
                raise Exception('size failed: ret=%r and correct=%r'%(ret,correct))
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='put() with str - async')
    def test_200_put_str(self):
        """Test put with a str - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put str
            iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                             callback=cb,
                                             request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='put() with file - async')
    def test_201_put_file(self):
        """Test put with a file - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        # make temp file
        filename = os.path.join(self.test_dir,'test')
        filecontents = 'this is a test'
        with open(filename,'w') as f:
            f.write(filecontents)

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put file
            iceprod.core.gridftp.GridFTP.put(address,filename=filename,
                                             callback=cb,
                                             request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='put() with func - async')
    def test_202_put_func(self):
        """Test put with a function - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'
        def contents():
            # give every 10 chars
            for i in xrange(0,len(filecontents),10):
                yield filecontents[i:i+10]

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put from function
            iceprod.core.gridftp.GridFTP.put(address,
                                             streaming_callback=contents().next,
                                             callback=cb,
                                             request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='get() with str - async')
    def test_210_get_str(self):
        """Test get with a str - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            # get str
            iceprod.core.gridftp.GridFTP.get(address,callback=cb,
                                             request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('get failed: ret=%r'%ret)
            if ret != filecontents:
                logger.warning('contents should be: %s',filecontents)
                logger.warning('contents is actually: %s',ret)
                raise Exception('contents is incorrect')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='get() with file - async')
    def test_211_get_file(self):
        """Test get with a file - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        # make temp file
        filename = os.path.join(self.test_dir,'test')
        filename2 = os.path.join(self.test_dir,'test2')
        filecontents = 'this is a test'
        with open(filename,'w') as f:
            f.write(filecontents)
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put file
            ret = iceprod.core.gridftp.GridFTP.put(address,filename=filename,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            # get file
            iceprod.core.gridftp.GridFTP.get(address,filename=filename2,
                                             callback=cb,
                                             request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('get failed: ret=%r'%ret)
            if not os.path.exists(filename2):
                raise Exception('dest file does not exist')
            with open(filename2) as f:
                newcontents = f.read()
                if filecontents != newcontents:
                    logger.warning('contents should be: %s',filecontents)
                    logger.warning('contents is actually: %s',newcontents)
                    raise Exception('file contents is incorrect')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='get() with func - async')
    def test_212_get_func(self):
        """Test get with a function - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'
        def contents():
            # give every 10 chars
            for i in xrange(0,len(filecontents),10):
                yield filecontents[i:i+10]
        def contents2(data):
            contents2.data += data
        contents2.data = ''

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put from function
            ret = iceprod.core.gridftp.GridFTP.put(address,
                                                   streaming_callback=contents().next,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            # get strGridFTP
            iceprod.core.gridftp.GridFTP.get(address,streaming_callback=contents2,
                                             callback=cb,
                                             request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('get failed: ret=%r'%ret)
            if contents2.data != filecontents:
                logger.warning('contents should be: %s',filecontents)
                logger.warning('contents is actually: %s',contents2.data)
                raise Exception('contents is incorrect')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir) - async')
    def test_220_list(self):
        """Test list of dir - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != []:
                logger.info('expected: []')
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file) - async')
    def test_221_list(self):
        """Test list with file - asynchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != ['test_file']:
                logger.info("expected: ['test_file']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir,dotfiles) - async')
    def test_222_list(self):
        """Test list with dir and dotfiles - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,dotfiles=True,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != ['.','..']:
                logger.info("expected: ['.','..']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file,dotfiles) - async')
    def test_223_list(self):
        """Test list with file and dotfiles - asynchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,dotfiles=True,callback=cb,
                                                   request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != ['test_file']:
                logger.info('expected: [\'test_file\']')
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir,details) - async')
    def test_224_list(self):
        """Test list with dir and details - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,details=True,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if ret != []:
                logger.info('expected: []')
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file,details) - async')
    def test_225_list(self):
        """Test list with file and details - asynchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,details=True,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if len(ret) != 1 or ret[0].directory:
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(dir,details,dotfiles) - async')
    def test_226_list(self):
        """Test list with dir and details and dotfiles - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                     request_timeout=self._timeout)
            if ret is False:
                raise Exception('mkdir failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,details=True,
                                              dotfiles=True,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if (len(ret) != 2 or not any([x.name == '.' for x in ret])
                or not any([x.name == '..' for x in ret])):
                logger.info("expected: ['..','.']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.rmdir(address,
                                                   request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='list(file,details,dotfiles) - async')
    def test_227_list(self):
        """Test list with file and details and dotfiles - asynchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.put(address,data=data,
                                                   request_timeout=self._timeout)
            if ret is False:
                raise Exception('put failed')

            # get listing
            iceprod.core.gridftp.GridFTP.list(address,details=True,
                                              dotfiles=True,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is False:
                raise Exception('list failed: ret=%r'%ret)

            if len(ret) != 1 or ret[0].name != 'test_file':
                logger.info("expected: ['test_file']")
                logger.info('actual: %r',ret)
                raise Exception('list did not return expected results')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='delete() - async')
    def test_230_delete(self):
        """Test delete - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        iceprod.core.gridftp.GridFTP.delete(address,callback=cb,
                                            request_timeout=self._timeout)

        if cb.event.wait(self._timeout) is False:
            # timeout
            raise Exception('Request timed out: %s'%str(address))
        ret = cb.ret

        if ret is not True:
            raise Exception('delete failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree() - async')
    def test_240_rmtree(self):
        """Test rmtree - asynchronous"""
        address = os.path.join(self.server_test_dir,'file_test')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        iceprod.core.gridftp.GridFTP.rmtree(address,callback=cb,
                                            request_timeout=self._timeout)

        if cb.event.wait(self._timeout) is False:
            # timeout
            raise Exception('Request timed out: %s'%str(address))
        ret = cb.ret

        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree(empty dir) - async')
    def test_241_rmtree(self):
        """Test rmtree empty dir - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        # mkdir
        ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        iceprod.core.gridftp.GridFTP.rmtree(address,callback=cb,
                                            request_timeout=self._timeout)

        if cb.event.wait(self._timeout) is False:
            # timeout
            raise Exception('Request timed out: %s'%str(address))
        ret = cb.ret

        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree(dir + file) - async')
    def test_242_rmtree(self):
        """Test rmtree dir and file - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        # mkdir
        ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        address2 = os.path.join(self.server_test_dir,'test','file_test')
        filecontents = 'this is a test'

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address2,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        iceprod.core.gridftp.GridFTP.rmtree(address,callback=cb,
                                            request_timeout=self._timeout)

        if cb.event.wait(self._timeout) is False:
            # timeout
            raise Exception('Request timed out: %s'%str(address))
        ret = cb.ret

        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='rmtree(dir + dir + file) - async')
    def test_243_rmtree(self):
        """Test rmtree dir and subdir and subfile - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        # mkdir
        ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        # mkdir
        address2 = os.path.join(self.server_test_dir,'test','test2')
        ret = iceprod.core.gridftp.GridFTP.mkdir(address2,
                                                 request_timeout=self._timeout)
        if ret is not True:
            raise Exception('mkdir failed: ret=%r'%ret)

        address3 = os.path.join(self.server_test_dir,'test','test2','file_test')
        filecontents = 'this is a test'

        # put str
        ret = iceprod.core.gridftp.GridFTP.put(address3,data=filecontents,
                                               request_timeout=self._timeout)
        if ret is not True:
            raise Exception('put failed: ret=%r'%ret)

        iceprod.core.gridftp.GridFTP.rmtree(address,callback=cb,
                                            request_timeout=self._timeout)

        if cb.event.wait(self._timeout) is False:
            # timeout
            raise Exception('Request timed out: %s'%str(address))
        ret = cb.ret

        if ret is not True:
            raise Exception('rmtree failed: ret=%r'%ret)

    @unittest_reporter(skip=skip_tests,name='exists() - async')
    def test_260_exists(self):
        """Test exists - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                      request_timeout=self._timeout)
            if ret is True:
                raise Exception('exists succeeded when it should have failed')

            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            iceprod.core.gridftp.GridFTP.exists(address,callback=cb,
                                                request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is not True:
                raise Exception('exists failed: ret=%r'%ret)
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='move() - async')
    def test_270_move(self):
        """Test move - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        address2 = os.path.join(self.server_test_dir,'test2')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            ret = iceprod.core.gridftp.GridFTP.exists(address2,
                                                      request_timeout=self._timeout)
            if ret is True:
                raise Exception('exists succeeded before move')

            iceprod.core.gridftp.GridFTP.move(address,address2,callback=cb,
                                              request_timeout=self._timeout)

            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret

            if ret is not True:
                raise Exception('move failed: ret=%r'%ret)

            ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                      request_timeout=self._timeout)
            if ret is True:
                raise Exception('exists succeeded on old address')
            ret = iceprod.core.gridftp.GridFTP.exists(address2,
                                                      request_timeout=self._timeout)
            if ret is not True:
                raise Exception('exists failed on new address')
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address2,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='checksum() - async')
    def test_280_checksum(self):
        """Test checksums - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()


        import hashlib

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            iceprod.core.gridftp.GridFTP.md5sum(address,callback=cb,
                                                request_timeout=self._timeout)
            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret
            correct = hashlib.md5(filecontents).hexdigest()
            if ret != correct:
                raise Exception('md5sum failed: ret=%r and correct=%r'%(ret,correct))

            cb.event.clear()
            iceprod.core.gridftp.GridFTP.sha1sum(address,callback=cb,
                                                 request_timeout=self._timeout)
            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret
            correct = hashlib.sha1(filecontents).hexdigest()
            if ret != correct:
                raise Exception('sha1sum failed: ret=%r and correct=%r'%(ret,correct))

            cb.event.clear()
            iceprod.core.gridftp.GridFTP.sha256sum(address,callback=cb,
                                                   request_timeout=self._timeout)
            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret
            correct = hashlib.sha256(filecontents).hexdigest()
            if ret != correct:
                raise Exception('sha256sum failed: ret=%r and correct=%r'%(ret,correct))

            cb.event.clear()
            iceprod.core.gridftp.GridFTP.sha512sum(address,callback=cb,
                                                   request_timeout=self._timeout)
            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret
            correct = hashlib.sha512(filecontents).hexdigest()
            if ret != correct:
                raise Exception('sha512sum failed: ret=%r and correct=%r'%(ret,correct))
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass

    @unittest_reporter(skip=skip_tests,name='size() - async')
    def test_290_size(self):
        """Test size - asynchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        def cb(ret):
            cb.ret = ret
            cb.event.set()
        cb.ret = False
        cb.event = Event()
        cb.event.clear()

        try:
            # put str
            ret = iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                                   request_timeout=self._timeout)
            if ret is not True:
                raise Exception('put failed: ret=%r'%ret)

            iceprod.core.gridftp.GridFTP.size(address,callback=cb,
                                              request_timeout=self._timeout)
            if cb.event.wait(self._timeout) is False:
                # timeout
                raise Exception('Request timed out: %s'%str(address))
            ret = cb.ret
            correct = len(filecontents)
            if ret != correct:
                raise Exception('size failed: ret=%r and correct=%r'%(ret,correct))
        finally:
            try:
                iceprod.core.gridftp.GridFTP.delete(address,
                                                    request_timeout=self._timeout)
            except:
                pass


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(gridftp_test))
    suite.addTests(loader.loadTestsFromNames(alltests,gridftp_test))
    return suite
