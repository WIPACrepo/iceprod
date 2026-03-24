"""
Test script for gridftp
"""

import logging

from tests.util import glob_tests

logger = logging.getLogger('gridftp')

import os
import random
import shutil
import subprocess
import tempfile
import unittest

import pytest

import iceprod.core.gridftp

skip_tests = False
if (subprocess.call(['which','grid-proxy-init']) or
    subprocess.call(['grid-proxy-info','-e','-valid','1:0'])):
    skip_tests = True
    
skip_uberftp_tests = skip_tests
if (subprocess.call(['which','uberftp']) or
    subprocess.call(['which','globus-url-copy'])):
    skip_uberftp_tests = True


class gridftp_test(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self._timeout = 1
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        self.server_test_dir = os.path.join('gsiftp://gridftp.icecube.wisc.edu/data/sim/sim-new/tmp/test',
                                            str(random.randint(0,2**32)))

        if not skip_tests:
            try:
                iceprod.core.gridftp.GridFTP.mkdir(self.server_test_dir,
                                                   parents=True,
                                                   request_timeout=self._timeout)
            except:
                pass
            if not os.path.exists(self.test_dir):
                os.mkdir(self.test_dir)

            def cleanup():
                try:
                    iceprod.core.gridftp.GridFTP.rmtree(self.server_test_dir,
                                                        request_timeout=self._timeout)
                except:
                    pass
                shutil.rmtree(self.test_dir)
            self.addCleanup(cleanup)

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_01_supported_address(self):
        """Test supported_address"""
        bad_addresses = ['test','file:/test','gsiftp:test','gsiftp:/test',
                         'ftp:test','http://x2100.icecube.wisc.edu',
                         'ftp:/test']
        good_addresses = ['gsiftp://data.icecube.wisc.edu','ftp://gnu.org',
                          'gsiftp://gridftp-rr.icecube.wisc.edu/data/sim/sim-new']

        for i in range(0,10):
            for a in bad_addresses:
                ret = iceprod.core.gridftp.GridFTP.supported_address(a)
                if ret is True:
                    raise Exception(f'Bad address {a} was called good')
            for a in good_addresses:
                ret = iceprod.core.gridftp.GridFTP.supported_address(a)
                if ret is not True:
                    raise Exception(f'Good address {a} was called bad')

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_02_address_split(self):
        """Test address_split"""
        good_addresses = {'gsiftp://data.icecube.wisc.edu':('gsiftp://data.icecube.wisc.edu','/'),
                      'ftp://gnu.org':('ftp://gnu.org','/'),
                      'gsiftp://gridftp-rr.icecube.wisc.edu/data/sim/sim-new':('gsiftp://gridftp-rr.icecube.wisc.edu','/data/sim/sim-new')}

        for i in range(0,10):
            for a in good_addresses:
                pieces = iceprod.core.gridftp.GridFTP.address_split(a)
                if pieces != good_addresses[a]:
                    raise Exception(f'Address {a} was not split properly')

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_100_put_str(self):
        """Test put with a str - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_101_put_file(self):
        """Test put with a file - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        # make temp file
        filename = os.path.join(self.test_dir,'test')
        filecontents = 'this is a test'
        with open(filename,'w') as f:
            f.write(filecontents)

        iceprod.core.gridftp.GridFTP.put(address,filename=filename,
                                         request_timeout=self._timeout)

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_110_get_str(self):
        """Test get with a str - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        # put str
        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

        # get str
        ret = iceprod.core.gridftp.GridFTP.get(address,
                                               request_timeout=self._timeout)
        self.assertEqual(ret, filecontents)

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_111_get_file(self):
        """Test get with a file - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        # make temp file
        filename = os.path.join(self.test_dir,'test')
        filename2 = os.path.join(self.test_dir,'test2')
        filecontents = 'this is a test'
        with open(filename,'w') as f:
            f.write(filecontents)

        # put file
        iceprod.core.gridftp.GridFTP.put(address,filename=filename,
                                         request_timeout=self._timeout)

        # get file
        ret = iceprod.core.gridftp.GridFTP.get(address,filename=filename2,
                                               request_timeout=self._timeout)
        if not os.path.exists(filename2):
            raise Exception('dest file does not exist')
        newcontents = open(filename2).read()
        self.assertEqual(filecontents, newcontents)

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_120_list(self):
        """Test list of directory - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        iceprod.core.gridftp.GridFTP.mkdir(address,
                                           request_timeout=self._timeout)

        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address)
        self.assertEqual(ret, [])

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_121_list(self):
        """Test list of file - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        iceprod.core.gridftp.GridFTP.put(address,data=data,
                                               request_timeout=self._timeout)

        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address,
                                                request_timeout=self._timeout)
        self.assertEqual(ret, ['test_file'])

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_122_list(self):
        """Test list of dir with dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        iceprod.core.gridftp.GridFTP.mkdir(address,
                                           request_timeout=self._timeout)


        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address,dotfiles=True,
                                                request_timeout=self._timeout)
        self.assertEqual(ret, ['.','..'])

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_123_list(self):
        """Test list of file with dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        iceprod.core.gridftp.GridFTP.put(address,data=data,
                                         request_timeout=self._timeout)

        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address,dotfiles=True,
                                                request_timeout=self._timeout)
        self.assertEqual(ret, ['test_file'])

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_124_list(self):
        """Test list of dir with details - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        iceprod.core.gridftp.GridFTP.mkdir(address,
                                           request_timeout=self._timeout)

        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                request_timeout=self._timeout)
        self.assertEqual(ret, [])

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_125_list(self):
        """Test list of file with details - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        iceprod.core.gridftp.GridFTP.put(address,data=data,
                                         request_timeout=self._timeout)

        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                request_timeout=self._timeout)
        if len(ret) != 1 or ret[0].directory:
            logger.info('actual: %r',ret)
            raise Exception('list did not return expected results')

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_126_list(self):
        """Test list of dir with details and dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        ret = iceprod.core.gridftp.GridFTP.mkdir(address,
                                                 request_timeout=self._timeout)

        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                dotfiles=True,
                                                request_timeout=self._timeout)

        if (len(ret) != 2 or not any([x.name == '.' for x in ret])
            or not any([x.name == '..' for x in ret])):
            logger.info("expected: ['..','.']")
            logger.info('actual: %r',ret)
            raise Exception('list did not return expected results')

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_127_list(self):
        """Test list of file with details and dotfiles - synchronous"""
        address = os.path.join(self.server_test_dir,'test_file')
        data = 'this is a test'

        iceprod.core.gridftp.GridFTP.put(address,data=data,
                                         request_timeout=self._timeout)

        # get listing
        ret = iceprod.core.gridftp.GridFTP.list(address,details=True,
                                                dotfiles=True,
                                                request_timeout=self._timeout)

        if len(ret) != 1 or ret[0].name != 'test_file':
            logger.info("expected: ['test_file']")
            logger.info('actual: %r',ret)
            raise Exception('list did not return expected results')

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_130_delete(self):
        """Test delete - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        # put str
        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

        iceprod.core.gridftp.GridFTP.delete(address,
                                            request_timeout=self._timeout)

    @pytest.mark.skipif(skip_uberftp_tests, reason='not installed')
    def test_140_rmtree(self):
        """Test rmtree of a file - synchronous"""
        address = os.path.join(self.server_test_dir,'file_test')
        filecontents = 'this is a test'

        # put str
        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

        iceprod.core.gridftp.GridFTP.rmtree(address,
                                            request_timeout=self._timeout)

    @pytest.mark.skipif(skip_uberftp_tests, reason='not installed')
    def test_141_rmtree(self):
        """Test rmtree of an empty dir - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        # mkdir
        iceprod.core.gridftp.GridFTP.mkdir(address,
                                           request_timeout=self._timeout)

        iceprod.core.gridftp.GridFTP.rmtree(address,
                                            request_timeout=self._timeout)

    @pytest.mark.skipif(skip_uberftp_tests, reason='not installed')
    def test_142_rmtree(self):
        """Test rmtree of a directory with a file - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        # mkdir
        iceprod.core.gridftp.GridFTP.mkdir(address,
                                           request_timeout=self._timeout)

        address2 = os.path.join(self.server_test_dir,'test','file_test')
        filecontents = 'this is a test'

        # put str
        iceprod.core.gridftp.GridFTP.put(address2,data=filecontents,
                                         request_timeout=self._timeout)

        iceprod.core.gridftp.GridFTP.rmtree(address,
                                            request_timeout=self._timeout)

    @pytest.mark.skipif(skip_uberftp_tests, reason='not installed')
    def test_143_rmtree(self):
        """Test rmtree of dir with subdir and subfile - synchronous"""
        address = os.path.join(self.server_test_dir,'test')

        # mkdir
        iceprod.core.gridftp.GridFTP.mkdir(address,
                                           request_timeout=self._timeout)

        # mkdir
        address2 = os.path.join(self.server_test_dir,'test','test2')
        iceprod.core.gridftp.GridFTP.mkdir(address2,
                                          request_timeout=self._timeout)

        address3 = os.path.join(self.server_test_dir,'test','test2','file_test')
        filecontents = 'this is a test'

        # put str
        iceprod.core.gridftp.GridFTP.put(address3,data=filecontents,
                                         request_timeout=self._timeout)

        iceprod.core.gridftp.GridFTP.rmtree(address,
                                            request_timeout=self._timeout)

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_160_exists(self):
        """Test exists - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                  request_timeout=self._timeout)
        if ret is True:
            raise Exception('exists succeeded when it should have failed')

        # put str
        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

        ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                  request_timeout=self._timeout)
        if ret is not True:
            raise Exception(f'exists failed: ret={ret}')

    @pytest.mark.skipif(skip_tests, reason='not installed')
    def test_170_move(self):
        """Test move - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        address2 = os.path.join(self.server_test_dir,'test2')
        filecontents = 'this is a test'

        # put str
        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

        ret = iceprod.core.gridftp.GridFTP.exists(address2,
                                                  request_timeout=self._timeout)
        if ret is True:
            raise Exception('exists succeeded before move')

        iceprod.core.gridftp.GridFTP.move(address,address2,
                                          request_timeout=self._timeout)
        
        ret = iceprod.core.gridftp.GridFTP.exists(address,
                                                  request_timeout=self._timeout)
        if ret is True:
            raise Exception('exists succeeded on old address')
        ret = iceprod.core.gridftp.GridFTP.exists(address2,
                                                  request_timeout=self._timeout)
        if ret is not True:
            raise Exception('exists failed on new address')

    @pytest.mark.skipif(skip_uberftp_tests, reason='not installed')
    def test_180_checksum(self):
        """Test checksums - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = b'this is a test'

        import hashlib

        # put str
        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

        ret = iceprod.core.gridftp.GridFTP.md5sum(address,
                                                  request_timeout=self._timeout)
        correct = hashlib.md5(filecontents).hexdigest()
        if ret != correct:
            raise Exception(f'md5sum failed: ret={ret} and correct={correct}')

        ret = iceprod.core.gridftp.GridFTP.sha1sum(address,
                                                   request_timeout=self._timeout)
        correct = hashlib.sha1(filecontents).hexdigest()
        if ret != correct:
            raise Exception(f'sha1sum failed: ret={ret} and correct={correct}')

        ret = iceprod.core.gridftp.GridFTP.sha256sum(address,
                                                     request_timeout=self._timeout)
        correct = hashlib.sha256(filecontents).hexdigest()
        if ret != correct:
            raise Exception(f'sha256sum failed: ret={ret} and correct={correct}')

        ret = iceprod.core.gridftp.GridFTP.sha512sum(address,
                                                     request_timeout=self._timeout)
        correct = hashlib.sha512(filecontents).hexdigest()
        if ret != correct:
            raise Exception(f'sha512sum failed: ret={ret} and correct={correct}')

    @pytest.mark.skipif(skip_uberftp_tests, reason='not installed')
    def test_190_size(self):
        """Test size - synchronous"""
        address = os.path.join(self.server_test_dir,'test')
        filecontents = 'this is a test'

        # put str
        iceprod.core.gridftp.GridFTP.put(address,data=filecontents,
                                         request_timeout=self._timeout)

        ret = iceprod.core.gridftp.GridFTP.size(address,
                                                request_timeout=self._timeout)
        correct = len(filecontents)
        self.assertEqual(ret, correct)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(gridftp_test))
    suite.addTests(loader.loadTestsFromNames(alltests,gridftp_test))
    return suite
