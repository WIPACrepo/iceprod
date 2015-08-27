"""
Test script for common functions
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('functions')

import os, sys, time
import shutil
import filecmp
import random
import string
import subprocess
import tempfile
import socket

try:
    import cPickle as pickle
except:
    import pickle

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

from iceprod.core import to_log
import iceprod.core.dataclasses
import iceprod.core.util
import iceprod.core.gridftp
import iceprod.core.functions
from iceprod.core.jsonUtil import json_encode,json_decode


class functions_test(unittest.TestCase):
    def setUp(self):
        super(functions_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

        # mock the PycURL interface
        self.put_error = None
        self.put_called = False
        self.put_args = ([],{})
        flexmock(iceprod.core.util.PycURL).should_receive('put').replace_with(self.put)
        self.fetch_error = None
        self.fetch_body = ''
        self.fetch_called = False
        self.fetch_args = ([],{})
        flexmock(iceprod.core.util.PycURL).should_receive('fetch').replace_with(self.fetch)
        self.post_error = None
        self.post_headers = []
        self.post_body = ''
        self.post_called = False
        self.post_args = ([],{})
        self.post_response = None
        flexmock(iceprod.core.util.PycURL).should_receive('post').replace_with(self.post)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(functions_test,self).tearDown()

    # the mocked functions of PycURL
    def put(self,*args,**kwargs):
        self.put_called = True
        self.put_args = (args,kwargs)
        if self.put_error is not None:
            raise self.put_error
    def fetch(self, *args,**kwargs):
        if self.fetch_error is not None:
            raise self.fetch_error
        self.fetch_args = (args,kwargs)
        with open(args[1],'w') as f:
            if callable(self.fetch_body):
                f.write(self.fetch_body())
            else:
                f.write(self.fetch_body)
        self.fetch_called = True
    def post(self, *args,**kwargs):
        self.post_called = True
        self.post_args = (args,kwargs)
        if self.post_error is not None:
            raise self.post_error
        url = args[0]
        writefunc = args[1]
        if 'headerfunc' in kwargs:
            headerfunc = kwargs['headerfunc']
            for h in self.post_headers:
                headerfunc(h)
        if 'postbody' in kwargs:
            self.post_body = kwargs['postbody']
        writefunc(self.post_response())

    @unittest_reporter(name='uncompress() with .gz')
    def test_001_uncompress_gz(self):
        """Test uncompressing a file with .gz extension"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # compress with unix utility
            if subprocess.call('gzip %s'%filename,shell=True):
                raise Exception, 'gzip of test file failed'
            if not os.path.isfile(filename+'.gz'):
                raise Exception, 'gzip did not write to the expected filename of %s.gz'%filename

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # uncompress
            logger.info('compressed file is %s.gz'%filename)
            files = iceprod.core.functions.uncompress(filename+'.gz')
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,str):
                raise Exception, 'uncompress did not return a string of the new file name'
            if not os.path.isfile(files):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files):
                logger.warning('file names are different')
            with open(files,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

    @unittest_reporter(name='uncompress() with .tar.gz')
    def test_002_uncompress_tar(self):
        """Test uncompressing a file with .tar.gz extension"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # compress with unix utility
            dir,file = os.path.split(filename)
            if subprocess.call('tar -zcf %s.tar.gz --directory %s %s'%(filename,dir,file),shell=True):
                raise Exception, 'tar.gz of test file failed'
            if not os.path.isfile(filename+'.tar.gz'):
                raise Exception, 'tar.gz  did not write to the expected filename of %s.tar.gz'%filename

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # uncompress
            logger.info('compressed file is %s.tar.gz'%filename)
            files = iceprod.core.functions.uncompress(filename+'.tar.gz')
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,str):
                raise Exception, 'uncompress did not return a str of the new files'
            if not os.path.isfile(files):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files):
                logger.warning('file names are different')
            with open(files,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

        for i in range(0,10):
            # create test files
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)
            filename2 = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename2,'w') as f:
                file_contents2 = ''
                for x in range(0,1000):
                    file_contents2 += str(random.choice(string.ascii_letters))
                f.write(file_contents2)

            # compress with unix utility
            dir,file = os.path.split(filename)
            if subprocess.call('tar -zcf %s.tar.gz --directory %s %s %s'%(filename,dir,file,os.path.basename(filename2)),shell=True):
                raise Exception, 'tar.gz of test file failed'
            if not os.path.isfile(filename+'.tar.gz'):
                raise Exception, 'tar.gz  did not write to the expected filename of %s.tar.gz'%filename

            # remove original files
            if os.path.exists(filename):
                os.remove(filename)
            if os.path.exists(filename2):
                os.remove(filename2)

            # uncompress
            logger.info('compressed file is %s.tar.gz'%filename)
            files = iceprod.core.functions.uncompress(filename+'.tar.gz')
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,list):
                raise Exception, 'uncompress did not return a list of the new files'
            if len(files) != 2:
                raise Exception, 'uncompress gave too many or too few files in the list'
            if not os.path.isfile(files[0]):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files[0]):
                logger.warning('file names1 are different')
            if not os.path.samefile(filename2,files[1]):
                logger.warning('file names2 are different')
            with open(files[0],'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents1 not the same'
            with open(files[1],'r') as f:
                results = f.read(len(file_contents2)*10)
                if file_contents2 != results:
                    raise Exception, 'contents2 not the same'

    @unittest_reporter(name='uncompress() with .tgz')
    def test_003_uncompress_tgz(self):
        """Test uncompressing a file with .tgz extension"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # compress with unix utility
            dir,file = os.path.split(filename)
            if subprocess.call('tar -zcf %s.tgz --directory %s %s'%(filename,dir,file),shell=True):
                raise Exception, 'tgz of test file failed'
            if not os.path.isfile(filename+'.tgz'):
                raise Exception, 'tgz  did not write to the expected filename of %s.tgz'%filename

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # uncompress
            logger.info('compressed file is %s.tgz'%filename)
            files = iceprod.core.functions.uncompress(filename+'.tgz')
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,str):
                raise Exception, 'uncompress did not return a str of the new files'
            if not os.path.isfile(files):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files):
                logger.warning('file names are different')
            with open(files,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

    @unittest_reporter(name='uncompress() with .tar.gz special')
    def test_004_uncompress_tgz_special(self):
        """Test uncompressing a file with .tgz extension (special creation)"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # compress with in-house tar + compress
            dir,file = os.path.split(filename)
            iceprod.core.functions.tar(filename+'.tar',filename,workdir=dir)
            os.rename(filename+'.tar',filename)
            iceprod.core.functions.compress(filename,'tgz')
            if not os.path.isfile(filename+'.tgz'):
                raise Exception, 'tgz  did not write to the expected filename of %s.tgz'%filename

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # uncompress
            logger.info('compressed file is %s.tgz'%filename)
            files = iceprod.core.functions.uncompress(filename+'.tgz')
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,str):
                raise Exception, 'uncompress did not return a str of the new files'
            if not os.path.isfile(files):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files):
                logger.warning('file names are different')
            with open(files,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

    @unittest_reporter(name='uncompress() with .bz2')
    def test_005_uncompress_bz2(self):
        """Test uncompressing a file with .bz2 extension"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # compress with unix utility
            if subprocess.call('bzip2 %s'%filename,shell=True):
                raise Exception, 'bzip of test file failed'
            if not os.path.isfile(filename+'.bz2'):
                raise Exception, 'bzip did not write to the expected filename of %s.bz2'%filename

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # uncompress
            logger.info('compressed file is %s.bz2'%filename)
            files = iceprod.core.functions.uncompress(filename+'.bz2')
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,str):
                raise Exception, 'uncompress did not return a string of the new file name'
            if not os.path.isfile(files):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files):
                logger.warning('file names are different')
            f = open(files,'r')
            results = f.read(len(file_contents)*10)
            if file_contents != results:
                raise Exception, 'contents not the same'

    @unittest_reporter(name='uncompress() with .tar.bz2')
    def test_006_uncompress_tar_bz2(self):
        """Test uncompressing a file with .tar.bz2 extension"""
        for i in range(0,10):
            # create 2 test files
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)
            filename2 = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            with open(filename2,'w') as f:
                file_contents2 = ''
                for x in range(0,1000):
                    file_contents2 += str(random.choice(string.ascii_letters))
                f.write(file_contents2)

            # compress with unix utility
            dir,file = os.path.split(filename)
            dir,file2 = os.path.split(filename2)
            if subprocess.call('tar cjf %s.tar.bz2 --directory %s %s %s'%(filename,dir,file,file2),shell=True):
                raise Exception, 'bzip of test file failed'
            if not os.path.isfile('%s.tar.bz2'%filename):
                raise Exception, 'bzip did not write to the expected filename of %s.tar.bz2'%filename

            # remove original files
            if os.path.exists(filename):
                os.remove(filename)
            if os.path.exists(filename2):
                os.remove(filename2)

            # uncompress
            logger.info('compressed file is %s.tar.bz2',filename)
            files = iceprod.core.functions.uncompress('%s.tar.bz2'%filename)
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,list):
                raise Exception, 'uncompress did not return a list of the new file names'
            if not os.path.isfile(files[0]):
                raise Exception, 'uncompress returned an invalid file name'
            if not os.path.isfile(files[1]):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if os.path.basename(filename) == os.path.basename(files[0]):
                filecmp = files[0]
                filecmp2 = files[1]
            else:
                filecmp = files[1]
                filecmp2 = files[0]
            if not os.path.samefile(filename,filecmp):
                logger.warning('file names are different')
            with open(filecmp,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'
            if not os.path.samefile(filename,filecmp2):
                logger.warning('file names2 are different')
            with open(filecmp2,'r') as f:
                results = f.read(len(file_contents2)*10)
                if file_contents2 != results:
                    raise Exception, 'contents2 not the same'

    @unittest_reporter(name='compress() with .gz')
    def test_010_compress_gz(self):
        """Test compressing a file with .gz extension"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_compress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # compress
            file = iceprod.core.functions.compress(filename,'gz')
            if not os.path.isfile(file):
                raise Exception, 'compress did not return a valid filename: %s'%file

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # uncompress
            logger.info('compressed file is %s'%file)
            if subprocess.call('gzip -d -f %s'%file,shell=True):
                raise Exception, 'gzip failed'
            if not os.path.isfile(filename):
                raise Exception, 'gzip filename is invalid'

            # check file
            with open(filename,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

    @unittest_reporter(name='compress() with .lzma')
    def test_011_compress_lzma(self):
        """Test compressing a file with .lzma extension"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_compress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # compress
            file = iceprod.core.functions.compress(filename,'lzma')
            if not os.path.isfile(file):
                raise Exception, 'compress did not return a valid filename: %s'%file

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # uncompress
            logger.info('compressed file is %s'%file)
            files = iceprod.core.functions.uncompress(file)
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,str):
                raise Exception, 'uncompress did not return a str of the new file'
            if not os.path.isfile(files):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files):
                logger.warning('file names are different')
            with open(files,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

    @unittest_reporter(name='compress() with .tar.gz')
    def test_012_compress_tar_gz(self):
        """Test compressing a file with .tar.gz extension"""
        for i in range(0,5):
            # create test file
            filename = os.path.join(self.test_dir,'test_compress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # tar file
            tfile = iceprod.core.functions.tar(filename+'.tar',filename,os.path.dirname(filename))
            if not os.path.isfile(tfile):
                raise Exception, 'tar did not return a valid filename: %s'%tfile

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # compress
            file = iceprod.core.functions.compress(tfile,'gz')
            if not os.path.isfile(file):
                raise Exception, 'compress did not return a valid filename: %s'%file

            # uncompress
            logger.info('compressed file is %s'%file)

            if subprocess.call('tar zxf %s --directory %s'%(file,os.path.dirname(file)),shell=True):
                raise Exception, 'untar failed'
            if not os.path.isfile(filename):
                raise Exception, 'untar filename is invalid'

            # check file
            with open(filename,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

        for i in range(0,5):
            # create 2 test files
            filename = os.path.join(self.test_dir,'test_compress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)
            filename2 = os.path.join(self.test_dir,'test_compress'+str(random.randint(0,100000)))
            with open(filename2,'w') as f:
                file_contents2 = ''
                for x in range(0,1000):
                    file_contents2 += str(random.choice(string.ascii_letters))
                f.write(file_contents2)

            # tar files
            with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
                tfile = iceprod.core.functions.tar(filename+'.tar',[filename,filename2],os.path.dirname(filename))
            if not os.path.isfile(tfile):
                raise Exception, 'tar did not return a valid filename: %s'%tfile

            # remove original files
            if os.path.exists(filename):
                os.remove(filename)
            if os.path.exists(filename2):
                os.remove(filename2)

            # compress
            file = iceprod.core.functions.compress(tfile,'gz')
            if not os.path.isfile(file):
                raise Exception, 'compress did not return a valid filename: %s'%file

            # uncompress
            logger.info('compressed file is %s'%file)
            if subprocess.call('tar zxf %s --directory %s'%(file,os.path.dirname(file)),shell=True):
                raise Exception, 'untar failed'
            if not os.path.isfile(filename):
                raise Exception, 'untar filename is invalid'

            # check files
            with open(filename,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'
            with open(filename2,'r') as f:
                results = f.read(len(file_contents2)*10)
                if file_contents2 != results:
                    raise Exception, 'contents2 not the same'

    @unittest_reporter(name='compress() with .tar.lzma')
    def test_013_compress_tar_lzma(self):
        """Test compressing a file with .tar.lzma extension"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_compress'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # tar file
            tfile = iceprod.core.functions.tar(filename+'.tar',filename,os.path.dirname(filename))
            if not os.path.isfile(tfile):
                raise Exception, 'tar did not return a valid filename: %s'%tfile

            # remove original file
            if os.path.exists(filename):
                os.remove(filename)

            # compress
            file = iceprod.core.functions.compress(tfile,'lzma')
            if not os.path.isfile(file):
                raise Exception, 'compress did not return a valid filename: %s'%file

            # uncompress
            logger.info('compressed file is %s'%file)
            files = iceprod.core.functions.uncompress(file)
            logger.info('uncompress returned %s',str(files))
            if not isinstance(files,str):
                raise Exception, 'uncompress did not return a str of the new file'
            if not os.path.isfile(files):
                raise Exception, 'uncompress returned an invalid file name'

            # check file
            if not os.path.samefile(filename,files):
                logger.warning('file names are different')
            with open(files,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

    @unittest_reporter
    def test_020_iscompressed(self):
        """Test the iscompressed function with various extensions"""
        for i in range(0,10):
            if not iceprod.core.functions.iscompressed('test.gz'):
                raise Exception, 'failed on .gz'
            if not iceprod.core.functions.iscompressed('test.tar.gz'):
                raise Exception, 'failed on .tar.gz'
            if not iceprod.core.functions.iscompressed('test.tgz'):
                raise Exception, 'failed on .tgz'
            if not iceprod.core.functions.iscompressed('test.tar.bz2'):
                raise Exception, 'failed on .tar.bz2'
            if not iceprod.core.functions.iscompressed('test.lzma'):
                raise Exception, 'failed on .lzma'
            if not iceprod.core.functions.iscompressed('test.xz'):
                raise Exception, 'failed on .xz'
            if not iceprod.core.functions.iscompressed('test.tar.lzma'):
                raise Exception, 'failed on .tar.lzma'
            if not iceprod.core.functions.iscompressed('test.7z'):
                raise Exception, 'failed on .7z'
            if iceprod.core.functions.iscompressed('test'):
                raise Exception, 'failed on (no ext)'
            if iceprod.core.functions.iscompressed('test.doc'):
                raise Exception, 'failed on .doc'
            if iceprod.core.functions.iscompressed('test.xml'):
                raise Exception, 'failed on .xml'
            if iceprod.core.functions.iscompressed('test.gzhelp'):
                raise Exception, 'failed on .gzhelp'

    @unittest_reporter
    def test_021_istarred(self):
        """Test the istarred function with various extensions"""
        for i in range(0,10):
            if not iceprod.core.functions.istarred('test.tar.gz'):
                raise Exception, 'failed on .tar.gz'
            if not iceprod.core.functions.istarred('test.tgz'):
                raise Exception, 'failed on .tgz'
            if not iceprod.core.functions.istarred('test.tar.bz2'):
                raise Exception, 'failed on .tar.bz2'
            if not iceprod.core.functions.istarred('test.tar.lzma'):
                raise Exception, 'failed on .tar.lzma'
            if iceprod.core.functions.istarred('test'):
                raise Exception, 'failed on (no ext)'
            if iceprod.core.functions.istarred('test.doc'):
                raise Exception, 'failed on .doc'
            if iceprod.core.functions.istarred('test.xml'):
                raise Exception, 'failed on .xml'
            if iceprod.core.functions.istarred('test.gzhelp'):
                raise Exception, 'failed on .gzhelp'

    @unittest_reporter
    def test_100_md5sum(self):
        """Test the creation of md5sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_md5sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get md5sum from functions
            internal = iceprod.core.functions.md5sum(filename)
            out = subprocess.Popen('md5sum %s'%filename,shell=True,stdout=subprocess.PIPE).communicate()[0]
            try:
                external, file = out.split()
            except Exception:
                raise Exception, 'failed to get external md5sum'

            if internal != external:
                raise Exception, 'failed md5sum check'

            os.remove(filename)

    @unittest_reporter
    def test_101_check_md5sum(self):
        """Test the checking of md5sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_md5sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get md5sum from functions
            internal = iceprod.core.functions.md5sum(filename)

            # check md5sum
            if not iceprod.core.functions.check_md5sum(filename,internal):
                raise Exception, 'md5sum as str failed'

            os.remove(filename)

        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_md5sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get md5sum from functions
            if subprocess.call('md5sum %s > %s.md5sum'%(filename,filename),shell=True):
                raise Exception, 'failed to generate md5sum'

            # check md5sum
            if not iceprod.core.functions.check_md5sum(filename,filename+'.md5sum'):
                raise Exception, 'md5sum as list failed'

            os.remove(filename)
            os.remove(filename+'.md5sum')

    @unittest_reporter
    def test_102_sha1sum(self):
        """Test the creation of sha1sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha1sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha1sum from functions
            internal = iceprod.core.functions.sha1sum(filename)
            out = subprocess.Popen('sha1sum %s'%filename,shell=True,stdout=subprocess.PIPE).communicate()[0]
            try:
                external, file = out.split()
            except Exception:
                raise Exception, 'failed to get external sha1sum'

            if internal != external:
                raise Exception, 'failed sha1sum check'

            os.remove(filename)

    @unittest_reporter
    def test_103_check_sha1sum(self):
        """Test the checking of sha1sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha1sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha1sum from functions
            internal = iceprod.core.functions.sha1sum(filename)

            # check sha1sum
            if not iceprod.core.functions.check_sha1sum(filename,internal):
                raise Exception, 'sha1sum as str failed'

            os.remove(filename)

        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha1sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha1sum from functions
            if subprocess.call('sha1sum %s > %s.sha1sum'%(filename,filename),shell=True):
                raise Exception, 'failed to generate sha1sum'

            # check sha1sum
            if not iceprod.core.functions.check_sha1sum(filename,filename+'.sha1sum'):
                raise Exception, 'sha1sum as list failed'

            os.remove(filename)
            os.remove(filename+'.sha1sum')

    @unittest_reporter
    def test_104_sha256sum(self):
        """Test the creation of sha256sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha256sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha256sum from functions
            internal = iceprod.core.functions.sha256sum(filename)
            out = subprocess.Popen('sha256sum %s'%filename,shell=True,stdout=subprocess.PIPE).communicate()[0]
            try:
                external, file = out.split()
            except Exception:
                raise Exception, 'failed to get external sha256sum'

            if internal != external:
                raise Exception, 'failed sha256sum check'

            os.remove(filename)

    @unittest_reporter
    def test_105_check_sha256sum(self):
        """Test the checking of sha256sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha256sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha256sum from functions
            internal = iceprod.core.functions.sha256sum(filename)

            # check sha256sum
            if not iceprod.core.functions.check_sha256sum(filename,internal):
                raise Exception, 'sha256sum as str failed'

            os.remove(filename)

        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha256sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha256sum from functions
            if subprocess.call('sha256sum %s > %s.sha256sum'%(filename,filename),shell=True):
                raise Exception, 'failed to generate sha256sum'

            # check sha256sum
            if not iceprod.core.functions.check_sha256sum(filename,filename+'.sha256sum'):
                raise Exception, 'sha256sum as list failed'

            os.remove(filename)
            os.remove(filename+'.sha256sum')

    @unittest_reporter
    def test_106_sha512sum(self):
        """Test the creation of sha512sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha512sum from functions
            internal = iceprod.core.functions.sha512sum(filename)
            out = subprocess.Popen('sha512sum %s'%filename,shell=True,stdout=subprocess.PIPE).communicate()[0]
            try:
                external, file = out.split()
            except Exception:
                raise Exception, 'failed to get external sha512sum'

            if internal != external:
                raise Exception, 'failed sha512sum check'

            os.remove(filename)

    @unittest_reporter
    def test_107_check_sha512sum(self):
        """Test the checking of sha512sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha512sum from functions
            internal = iceprod.core.functions.sha512sum(filename)

            # check sha512sum
            if not iceprod.core.functions.check_sha512sum(filename,internal):
                raise Exception, 'sha512sum as str failed'

            os.remove(filename)

        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha512sum from functions
            if subprocess.call('sha512sum %s > %s.sha512sum'%(filename,filename),shell=True):
                raise Exception, 'failed to generate sha512sum'

            # check sha512sum
            if not iceprod.core.functions.check_sha512sum(filename,filename+'.sha512sum'):
                raise Exception, 'sha512sum as list failed'

            os.remove(filename)
            os.remove(filename+'.sha512sum')

    @unittest_reporter
    def test_200_removedirs(self):
        """Test removing files and directories"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_removedirs'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # remove file
            iceprod.core.functions.removedirs(filename)

            # check file
            if os.path.exists(filename):
                raise Exception, 'removedirs failed to remove %s'%filename

        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_removedirs'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # remove directory
            iceprod.core.functions.removedirs(dir)

            # check file
            if os.path.exists(dir):
                raise Exception, 'removedirs failed to remove %s'%dir

    @unittest_reporter
    def test_201_copy(self):
        """Test copying files and directories"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_copy'+str(random.randint(0,100000)))
            filename2 = os.path.join(self.test_dir,'test_copy'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # copy file
            iceprod.core.functions.copy(filename,filename2)

            # check file
            if not os.path.isfile(filename2):
                raise Exception, 'copy failed to copy %s to %s'%(filename,filename2)
            with open(filename2,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            dir2 = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            rand = str(random.randint(0,100000))
            filename = os.path.join(dir,'test_copy'+rand)
            filename2 = os.path.join(dir2,'test_copy'+rand)
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # copy directory
            iceprod.core.functions.copy(dir,dir2)

            # check directory
            if not os.path.isdir(dir2):
                raise Exception, 'copy failed to copy %s to %s'%(dir,dir2)
            # check file
            if not os.path.isfile(filename2):
                raise Exception, 'copy failed to copy %s to %s'%(filename,filename2)
            with open(filename2,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception, 'contents not the same'

    @unittest_reporter
    def test_202_find_regex(self):
        """Test using find_regex"""
        # find a file
        logging.info('find file')
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find file
            matches = iceprod.core.functions.find_regex(self.test_dir,'test','file')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif len(matches) > 1:
                raise Exception, 'too many matches'
            elif filename not in matches:
                raise Exception, 'failed to find %s'%filename

            # delete file
            os.remove(filename)

        # find a directory
        logging.info('find directory')
        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find directory
            matches = iceprod.core.functions.find_regex(self.test_dir,os.path.basename(dir),'dir')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif len(matches) > 1:
                raise Exception, 'too many matches'
            elif dir not in matches:
                raise Exception, 'failed to find %s'%dir

            # delete directory
            iceprod.core.functions.removedirs(dir)

        # find either
        logging.info('find either')
        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,'test_d'+str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find directory
            matches = iceprod.core.functions.find_regex(self.test_dir,'test')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif dir not in matches:
                raise Exception, 'failed to find %s'%dir
            elif filename not in matches:
                raise Exception, 'failed to find %s'%filename

            # delete directory
            iceprod.core.functions.removedirs(dir)

    @unittest_reporter
    def test_203_find_unix(self):
        """Test using find_unix"""
        # find a file
        logging.info('find file')
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find file
            matches = iceprod.core.functions.find_unix(self.test_dir,'test*','file')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif len(matches) > 1:
                raise Exception, 'too many matches'
            elif filename not in matches:
                raise Exception, 'failed to find %s'%filename

            # delete file
            os.remove(filename)

        # find a directory
        logging.info('find directory')
        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find directory
            matches = iceprod.core.functions.find_unix(self.test_dir,os.path.basename(dir),'dir')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif len(matches) > 1:
                raise Exception, 'too many matches'
            elif dir not in matches:
                raise Exception, 'failed to find %s'%dir

            # delete directory
            iceprod.core.functions.removedirs(dir)

        # find either
        logging.info('find either')
        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find directory
            matches = iceprod.core.functions.find_unix(self.test_dir,'*')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif dir not in matches:
                raise Exception, 'failed to find %s'%dir
            elif filename not in matches:
                raise Exception, 'failed to find %s'%filename

            # delete directory
            iceprod.core.functions.removedirs(dir)

    @unittest_reporter
    def test_204_find_glob(self):
        """Test using find_glob"""
        # find a file
        logging.info('find file')
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find file
            matches = iceprod.core.functions.find_glob(self.test_dir,'test*','file')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif len(matches) > 1:
                raise Exception, 'too many matches'
            elif filename not in matches:
                raise Exception, 'failed to find %s'%filename

            # delete file
            os.remove(filename)

        # find a directory
        logging.info('find directory')
        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find directory
            matches = iceprod.core.functions.find_glob(self.test_dir,os.path.basename(dir),'dir')
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif len(matches) > 1:
                raise Exception, 'too many matches'
            elif dir not in matches:
                raise Exception, 'failed to find %s'%dir

            # delete directory
            iceprod.core.functions.removedirs(dir)

        # find file in directory
        logging.info('find file in directory')
        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_find'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # find file
            matches = iceprod.core.functions.find_glob(self.test_dir,os.path.join(os.path.basename(dir),'test*'))
            logging.info('matching against %s',os.path.join(os.path.basename(dir),'test*'))
            logging.info('matches = %s',str(matches))
            if not matches:
                raise Exception, 'failed to return any matches'
            elif len(matches) > 2:
                raise Exception, 'too many matches'
            elif filename not in matches:
                raise Exception, 'failed to find %s'%filename

            # delete directory
            iceprod.core.functions.removedirs(dir)

    @unittest_reporter
    def test_205_tail(self):
        """Test using tail"""
        # tail of small file
        logger.info('tail of small file')
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_find'+str(random.randint(0,100000)))
            file_contents = ''
            with open(filename,'w') as f:
                for x in range(0,10):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get tail
            tail = iceprod.core.functions.tail(filename)
            logging.info('tail = %s',str(tail))
            if not tail:
                raise Exception, 'failed to return a tail'
            if file_contents != tail:
                raise Exception, 'contents not the same'

            # delete file
            os.remove(filename)

        # tail of large file
        logger.info('tail of large file')
        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            os.mkdir(dir)

            # create a file in the directory
            filename = os.path.join(dir,'test_find'+str(random.randint(0,100000)))
            file_contents = ''
            with open(filename,'w') as f:
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get tail
            tail = iceprod.core.functions.tail(filename)
            logging.info('tail = %s',str(tail))
            if not tail:
                raise Exception, 'failed to return a tail'
            if not file_contents.endswith(tail):
                raise Exception, 'contents not the same'

            # delete file
            os.remove(filename)

    @unittest_reporter
    def test_300_getInterfaces(self):
        """Test the getInterfaces function"""
        # get interfaces
        ifaces = iceprod.core.functions.getInterfaces()
        if not ifaces:
            raise Exception('getInterfaces() returned None')

        loop = None
        eth = []
        for iface in ifaces:
            for link in iface.link:
                if link['type'] == 'ipv4' and link['ip'] == '127.0.0.1':
                    loop = iface
            if iface.encap.lower() in ('ether','ethernet'):
                eth.append(iface)

        # check that we can see the loopback interface
        if not loop:
            raise Exception('No loop interface')
        if loop.name != 'lo':
            raise Exception('loop interface name error: expected \'lo\' but got \'%s\''%loop.name)
        if loop.encap.lower() not in ('local','loopback'):
            raise Exception('loop interface type error: expected \'local\' or \'loopback\' but got \'%s\''%loop.type)

        # check that we can see an ethernet interface
        if len(eth) < 1:
            raise Exception('No ethernet interfaces')
        for e in eth:
            logging.info('%s',e)

        # get interfaces (newkernel=True)
        ifaces = iceprod.core.functions.getInterfaces(newkernel=True)
        if not ifaces:
            raise Exception('getInterfaces(newkernel=True) returned None')

        loop = None
        eth = []
        for iface in ifaces:
            for link in iface.link:
                if link['type'] == 'ipv4' and link['ip'] == '127.0.0.1':
                    loop = iface
            if iface.encap.lower() in ('ether','ethernet'):
                eth.append(iface)

        # check that we can see the loopback interface
        if not loop:
            raise Exception('newkernel - No loop interface')
        if loop.name != 'lo':
            raise Exception('newkernel - loop interface name error: expected \'lo\' but got \'%s\''%loop.name)
        if loop.encap.lower() not in ('local','loopback'):
            raise Exception('newkernel - loop interface type error: expected \'local\' or \'loopback\' but got \'%s\''%loop.type)

        # check that we can see an ethernet interface
        if len(eth) < 1:
            raise Exception('newkernel - No ethernet interfaces')
        for e in eth:
            logging.info('%s',e)

        # get interfaces (legacy)
        ifaces = iceprod.core.functions.getInterfaces(legacy=True)
        if not ifaces:
            raise Exception('getInterfaces(legacy=True) returned None')

        loop = None
        eth = []
        for iface in ifaces:
            for link in iface.link:
                if link['type'] == 'ipv4' and link['ip'] == '127.0.0.1':
                    loop = iface
            if iface.encap.lower() in ('ether','ethernet'):
                eth.append(iface)

        # check that we can see the loopback interface
        if not loop:
            raise Exception('Legacy - No loop interface')
        if loop.name != 'lo':
            raise Exception('Legacy - loop interface name error: expected \'lo\' but got \'%s\''%loop.name)
        if loop.encap.lower() not in ('local','loopback'):
            raise Exception('Legacy - loop interface type error: expected \'local\' or \'loopback\' but got \'%s\''%loop.type)

        # check that we can see an ethernet interface
        if len(eth) < 1:
            raise Exception('Legacy - No ethernet interfaces')
        for e in eth:
            logging.info('Legacy - %s',e)

    @unittest_reporter
    def test_301_gethostname(self):
        """Test the gethostname function"""
        # get hostnames
        host = iceprod.core.functions.gethostname()
        logging.info('hostname = %s',str(host))

        # get external hostname
        ext_host = socket.getfqdn()

        if not host and len(ext_host) > 1:
            raise Exception('no hostname returned. host is %s'%str(ext_host))
        if isinstance(host,str):
            if host not in ext_host.strip() and ext_host.strip() not in host:
                raise Exception('hostnames not equal.  expected %s and got %s'%(ext_host.strip(),host))
        elif isinstance(host,list):
            present = False
            for h in host:
                if host == ext_host.strip():
                    present = True
            if not present:
                raise Exception('multiple hostnames, but correct one not present.  expected %s and got %r'%(ext_host.strip(),host))

    @unittest_reporter
    def test_302_isurl(self):
        """Test the isurl function"""
        good_urls = ['http://www.google.com',
                     'https://skua.icecube.wisc.edu:9080',
                     'gsiftp://gridftp-rr.icecube.wisc.edu',
                     'ftp://x2100.icecube.wisc.edu',
                     'file:/data/exp',
                     'lfn://test',
                     'srm://test2']
        bad_urls = ['slkdjf:/sldfjlksd',
                    'rpc://test']
        for i in range(0,10):
            for url in good_urls:
                if not iceprod.core.functions.isurl(url):
                    raise Exception, 'isurl thought %s was not a valid url'%url
            for url in bad_urls:
                if iceprod.core.functions.isurl(url):
                    raise Exception, 'isurl thought %s was a valid url'%url

    @unittest_reporter
    def test_303_wget(self):
        """Test the wget function"""
        download_options = {'http_username':'icecube',
                            'http_password':'skua',
                            'key':'key'}

        data = 'the data'

        def response():
            return data
        self.fetch_body = data
        self.post_response = response
        self.post_headers = ['http/1.1 200']

        # download file from resources
        if not iceprod.core.functions.wget('http://x2100.icecube.wisc.edu/downloads/globus.tar.gz',self.test_dir,options=download_options):
            raise Exception('simple http: wget failed')
        if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
            raise Exception('simple http: downloaded file does not exist')
        data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
        if data2 != data:
            raise Exception('simple http: data not equal.  expected %r and got %r'%(data,data2))

        # try download from iceprod server (POST only)
        self.fetch_error = iceprod.core.util.NoncriticalError('HTTP error code: %d'%405)
        download_options = {'key':'abcd'}

        # download file from resources
        if not iceprod.core.functions.wget('http://x2100.icecube.wisc.edu/downloads/globus.tar.gz',self.test_dir,options=download_options):
            raise Exception('iceprod http: wget failed')
        if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
            raise Exception('iceprod http: downloaded file does not exist')
        data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
        if data2 != data:
            raise Exception('iceprod http: data not equal.  expected %r and got %r'%(data,data2))

        self.fetch_error = None

        # download file from svn
        if not iceprod.core.functions.wget('http://code.icecube.wisc.edu/svn/projects/simprod-scripts/trunk/simulation/generators.py',self.test_dir,options=download_options):
            raise Exception('svn http: wget failed')
        if not os.path.isfile(os.path.join(self.test_dir,'generators.py')):
            raise Exception('svn http: downloaded file does not exist')
        data2 = open(os.path.join(self.test_dir,'generators.py')).read()
        if data2 != data:
            raise Exception('svn http: data not equal.  expected %r and got %r'%(data,data2))

        # download file from local file system
        if not iceprod.core.functions.wget(os.path.join(self.test_dir,'generators.py'),os.path.join(self.test_dir,'generators2.py'),options=download_options):
            raise Exception('local cp: wget failed')
        if not os.path.isfile(os.path.join(self.test_dir,'generators2.py')):
            raise Exception('local cp: copied file does not exist')
        data2 = open(os.path.join(self.test_dir,'generators2.py')).read()
        if data2 != data:
            raise Exception('local cp: data not equal.  expected %r and got %r'%(data,data2))

        # download file from gsiftp
        def get(url,filename=None):
            with open(filename,'w') as f:
                f.write(data)
            get.url = url
            return True
        get.url = None
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('get').replace_with(get)

        if not iceprod.core.functions.wget('gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz',self.test_dir,options=download_options):
            raise Exception('gsiftp: wget failed')
        if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
            raise Exception('gsiftp: downloaded file does not exist')
        data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
        if get.url != 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz':
            raise Exception('gsiftp: url is incorrect: %r'%get.url)
        if data2 != data:
            raise Exception('gsiftp: data not equal.  expected %r and got %r'%(data,data2))

    @unittest_reporter
    def test_304_download(self):
        """Test the download function"""
        download_options = {'http_username':'icecube',
                            'http_password':'skua',
                            'key':'key'}

        data = 'the data'
        md5sum = '3d5f3303ed6ce28c2d5ac1192118f0e2'
        def response():
            return data
        def fetcher():
            if self.fetch_args[0][0].endswith('md5sum'):
                return (md5sum,'md5sum')
            else:
                return data
        self.fetch_body = fetcher
        self.post_response = response
        self.post_headers = ['http/1.1 200']

        # download file from resources
        if not iceprod.core.functions.download('http://x2100.icecube.wisc.edu/downloads/globus.tar.gz',self.test_dir,options=download_options):
            raise Exception, 'simple http: download failed'
        if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
            raise Exception, 'simple http: downloaded file does not exist'
        data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
        if data2 != data:
            raise Exception('simple http: data not equal.  expected %r and got %r'%(data,data2))

        # download file from svn
        if not iceprod.core.functions.download('http://code.icecube.wisc.edu/svn/projects/simprod-scripts/trunk/simulation/generators.py',self.test_dir,options=download_options):
            raise Exception, 'svn http: download failed'
        if not os.path.isfile(os.path.join(self.test_dir,'generators.py')):
            raise Exception, 'svn http: downloaded file does not exist'
        data2 = open(os.path.join(self.test_dir,'generators.py')).read()
        if data2 != data:
            raise Exception('svn http: data not equal.  expected %r and got %r'%(data,data2))

        # download file from local file system
        if not iceprod.core.functions.download(os.path.join(self.test_dir,'generators.py'),os.path.join(self.test_dir,'generators2.py'),options=download_options):
            raise Exception, 'local cp: download failed'
        if not os.path.isfile(os.path.join(self.test_dir,'generators2.py')):
            raise Exception, 'local cp: copied file does not exist'
        data2 = open(os.path.join(self.test_dir,'generators2.py')).read()
        if data2 != data:
            raise Exception('local cp: data not equal.  expected %r and got %r'%(data,data2))

        # download file from gsiftp
        def get(url,filename=None):
            with open(filename,'w') as f:
                f.write(data)
            get.url = url
            return True
        get.url = None
        def sha512sum(url):
            return '8580e83fc859a2786430406fd41c7c6a0d3ac77b7eff07bc94c880f5b6e86b87320ea25cb3f3c5a3881236cf8bda92cb8f61c2a813881fee1d8f8331565ce98a'
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('get').replace_with(get)
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('sha512sum').replace_with(sha512sum)

        if not iceprod.core.functions.download('gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz',self.test_dir,options=download_options):
            raise Exception('gsiftp: wget failed')
        if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
            raise Exception('gsiftp: downloaded file does not exist')
        data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
        if get.url != 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz':
            raise Exception('gsiftp: url is incorrect: %r'%get.url)
        if data2 != data:
            raise Exception('gsiftp: data not equal.  expected %r and got %r'%(data,data2))

    @unittest_reporter
    def test_305_download_cached(self):
        """Test the download function with caching"""
        # download file from resources
        download_options = {'http_username':'icecube',
                            'http_password':'skua',
                            'cache_dir':os.path.join(self.test_dir,'cache_test'),
                            'key':'key'}

        data = 'the data'
        md5sum = '3d5f3303ed6ce28c2d5ac1192118f0e2'
        def response():
            return data
        def fetcher():
            if self.fetch_args[0][0].endswith('md5sum'):
                return (md5sum,'md5sum')
            else:
                return data
        self.fetch_body = fetcher
        self.post_response = response
        self.post_headers = ['http/1.1 200']

        def get(url,filename=None):
            with open(filename,'w') as f:
                f.write(data)
            get.url = url
            return True
        get.url = None
        def sha512sum(url):
            return '8580e83fc859a2786430406fd41c7c6a0d3ac77b7eff07bc94c880f5b6e86b87320ea25cb3f3c5a3881236cf8bda92cb8f61c2a813881fee1d8f8331565ce98a'
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('get').replace_with(get)
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('sha512sum').replace_with(sha512sum)

        for i in range(0,5):
            # download file from resources
            if not iceprod.core.functions.download('http://x2100.icecube.wisc.edu/downloads/globus.tar.gz',self.test_dir,cache=True,options=download_options):
                raise Exception, 'simple http: download failed'
            if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
                raise Exception, 'simple http: downloaded file does not exist'
            data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
            if data2 != data:
                raise Exception('simple http: data not equal.  expected %r and got %r'%(data,data2))

            # download file from svn
            if not iceprod.core.functions.download('http://code.icecube.wisc.edu/svn/projects/simprod-scripts/trunk/simulation/generators.py',self.test_dir,cache=True,options=download_options):
                raise Exception, 'svn http: download failed'
            if not os.path.isfile(os.path.join(self.test_dir,'generators.py')):
                raise Exception, 'svn http: downloaded file does not exist'
            data2 = open(os.path.join(self.test_dir,'generators.py')).read()
            if data2 != data:
                raise Exception('svn http: data not equal.  expected %r and got %r'%(data,data2))

            # download file from local file system
            if not iceprod.core.functions.download(os.path.join(self.test_dir,'generators.py'),os.path.join(self.test_dir,'generators2.py'),cache=True,options=download_options):
                raise Exception, 'local cp: download failed'
            if not os.path.isfile(os.path.join(self.test_dir,'generators2.py')):
                raise Exception, 'local cp: copied file does not exist'
            data2 = open(os.path.join(self.test_dir,'generators2.py')).read()
            if data2 != data:
                raise Exception('local cp: data not equal.  expected %r and got %r'%(data,data2))

            # download file from gsiftp
            if not iceprod.core.functions.download('gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz',self.test_dir,cache=True,options=download_options):
                raise Exception('gsiftp: wget failed')
            if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
                raise Exception('gsiftp: downloaded file does not exist')
            data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
            if get.url != 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz':
                raise Exception('gsiftp: url is incorrect: %r'%get.url)
            if data2 != data:
                raise Exception('gsiftp: data not equal.  expected %r and got %r'%(data,data2))

    @unittest_reporter
    def test_400_wput(self):
        """Test the wput function"""
        upload_options = {'http_username':'icecube',
                          'http_password':'skua',
                          'key':'key'}

        data = os.urandom(1024) # 1KB upload
        def response():
            return ''
        self.fetch_error = Exception('GET invalid')
        self.post_response = response
        self.post_headers = ['http/1.1 200']

        # upload file using http
        self.put_called = False
        up_file = os.path.join(self.test_dir,str(random.randint(100,1000)))
        with open(up_file,'w') as f:
            f.write(str(random.randint(1000,1000000)))
        upload_addr = 'http://test/upload'
        ret = iceprod.core.functions.wput(up_file,
                                          upload_addr,
                                          options=upload_options)
        if ret:
            raise Exception('simple http: returned error %r'%ret)
        if not self.put_called:
            raise Exception('simple http: put not called')
        if self.put_args[0][0] != upload_addr and (
           'url' not in self.put_args[1] or
           self.put_args[1]['url'] != upload_addr):
           raise Exception('simple http: put address incorrect')
        if self.put_args[0][1] != up_file and (
           'filename' not in self.put_args[1] or
           self.put_args[1]['filename'] != up_file):
           raise Exception('simple http: put filename incorrect')

        # test upload error
        self.put_called = False
        self.put_error = Exception('upload error')
        try:
            ret= iceprod.core.functions.wput(up_file,
                                             upload_addr,
                                             options=upload_options)
        except:
            pass
        else:
            raise Exception('simple http: succeeded when supposed to fail')

        # upload file to local file system
        ret = iceprod.core.functions.wput(up_file,up_file+'.bak',
                                           options=upload_options)
        if ret:
            raise Exception('local cp: wput failed %r'%ret)
        if not os.path.isfile(up_file+'.bak'):
            raise Exception('local cp: copied file does not exist')
        if not filecmp.cmp(up_file,up_file+'.bak',shallow=False):
            raise Exception('local cp: data not equal')

        # upload file to gsiftp
        def put(url,filename=None):
            put.url = url
            return True
        put.url = None
        def put_chksum(url):
            put_chksum.url = url
            return iceprod.core.functions.sha512sum(up_file)
        put_chksum.url = None
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('put').replace_with(put)
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('sha512sum').replace_with(put_chksum)

        upload_addr = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/testing'
        ret = iceprod.core.functions.wput(up_file,
                                          upload_addr,
                                          options=upload_options)
        if ret:
            raise Exception('gsiftp: wput failed %r'%ret)
        if put.url != upload_addr:
            raise Exception('gsiftp: url is incorrect: %r'%put.url)

    @unittest_reporter(name='wput(proxy=True)')
    def test_401_wput(self):
        """Test the wput proxy function"""
        upload_options = {'http_username':'icecube',
                          'http_password':'skua',
                          'proxy_addr':'localhost',
                          'key':'key'}

        data = os.urandom(1024) # 1KB upload
        def response():
            try:
                url = self.post_args[0][0]
            except:
                raise Exception('error with url formatting')

            if url.endswith('/upload'):
                try:
                    body = json_decode(self.post_args[1]['postbody'])
                except:
                    raise Exception('error with body formatting')
                if body['type'] == 'upload':
                    response.url = body['url']
                    return json_encode({'type':'upload',
                                        'url':body['url'],
                                        'upload':'http://localhost/upload/testing'
                                       })
                elif body['type'] == 'check':
                    return json_encode({'type':'check',
                                        'url':body['url'],
                                        'result':response.result
                                       })
                else:
                    raise Exception('bad type')
            else:
                logger.error('url is %s',url)
                raise Exception('got something other than upload address')

        self.fetch_error = Exception('GET invalid')
        self.post_response = response
        self.post_headers = ['http/1.1 200']

        # upload file using http
        self.put_called = False
        response.url = None
        response.result = True
        up_file = os.path.join(self.test_dir,str(random.randint(100,1000)))
        with open(up_file,'w') as f:
            f.write(str(random.randint(1000,1000000)))
        upload_addr = 'http://test/upload'
        ret = iceprod.core.functions.wput(up_file,
                                          upload_addr,
                                          proxy=True,
                                          options=upload_options)
        if ret:
            raise Exception('simple http: returned error %r'%ret)
        if not self.put_called:
            raise Exception('simple http: put not called')
        if response.url != upload_addr:
           raise Exception('simple http: put address incorrect')
        if self.put_args[0][1] != up_file and (
           'filename' not in self.put_args[1] or
           self.put_args[1]['filename'] != up_file):
           raise Exception('simple http: put filename incorrect')

        # test upload error
        self.put_called = False
        response.url = None
        response.result = True
        self.put_error = Exception('upload error')
        try:
            ret= iceprod.core.functions.wput(up_file,
                                             upload_addr,
                                             proxy=True,
                                             options=upload_options)
        except:
            pass
        else:
            if not ret:
                raise Exception('simple http: succeeded when supposed to fail')


        # upload file to gsiftp
        upload_addr = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/testing'
        response.url = None
        response.result = True
        self.put_called = False
        self.put_error = None
        ret = iceprod.core.functions.wput(up_file,
                                          upload_addr,
                                          proxy=True,
                                          options=upload_options)
        if ret:
            raise Exception('gsiftp: wput failed %r'%ret)
        if not self.put_called:
            raise Exception('gsiftp: put not called')
        if response.url != upload_addr:
            raise Exception('gsiftp: url is incorrect: %r'%response.url)

    @unittest_reporter
    def test_402_upload(self):
        """Test the upload function"""
        upload_options = {'http_username':'icecube',
                          'http_password':'skua',
                          'key':'key'}

        data = os.urandom(1024) # 1KB upload
        def response():
            return ''
        self.fetch_error = Exception('GET invalid')
        self.post_response = response
        self.post_headers = ['http/1.1 200']

        # upload file using http
        self.put_called = False
        up_file = os.path.join(self.test_dir,str(random.randint(100,1000)))
        with open(up_file,'w') as f:
            f.write(str(random.randint(1000,1000000)))
        upload_addr = 'http://test/upload'
        ret = iceprod.core.functions.upload(up_file,
                                          upload_addr,
                                          options=upload_options)
        if not ret:
            raise Exception('simple http: returned error %r'%ret)
        if not self.put_called:
            raise Exception('simple http: put not called')
        if (len(self.put_args[0]) < 1 or
            self.put_args[0][0] != upload_addr) and (
           'url' not in self.put_args[1] or
           self.put_args[1]['url'] != upload_addr):
           raise Exception('simple http: put address incorrect')
        if (len(self.put_args[0]) < 2 or
            self.put_args[0][1] != up_file) and (
           'filename' not in self.put_args[1] or
           self.put_args[1]['filename'] != up_file):
           raise Exception('simple http: put filename incorrect')

        # test upload error
        self.put_called = False
        self.put_error = Exception('upload error')
        ret= iceprod.core.functions.upload(up_file,
                                         upload_addr,
                                         options=upload_options)
        if ret:
            raise Exception('simple http: succeeded when supposed to fail')

        # upload file to local file system
        ret = iceprod.core.functions.upload(up_file,up_file+'.bak',
                                           options=upload_options)
        if not ret:
            raise Exception('local cp: upload failed %r'%ret)
        if not os.path.isfile(up_file+'.bak'):
            raise Exception('local cp: copied file does not exist')
        if not filecmp.cmp(up_file,up_file+'.bak',shallow=False):
            raise Exception('local cp: data not equal')

        # upload file to gsiftp
        def put(url,filename=None):
            put.url = url
            return True
        put.url = None
        def put_chksum(url):
            put_chksum.url = url
            return iceprod.core.functions.sha512sum(up_file)
        put_chksum.url = None
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('put').replace_with(put)
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('sha512sum').replace_with(put_chksum)

        upload_addr = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/testing'
        ret = iceprod.core.functions.upload(up_file,
                                          upload_addr,
                                          options=upload_options)
        if not ret:
            raise Exception('gsiftp: upload failed %r'%ret)
        if put.url != upload_addr:
            raise Exception('gsiftp: url is incorrect: %r'%put.url)

    @unittest_reporter(name='upload(proxy=True)')
    def test_403_upload(self):
        """Test the upload proxy function"""
        upload_options = {'http_username':'icecube',
                          'http_password':'skua',
                          'proxy_addr':'localhost',
                          'key':'key'}

        data = os.urandom(1024) # 1KB upload
        def response():
            try:
                url = self.post_args[0][0]
            except:
                raise Exception('error with url formatting')

            if url.endswith('/upload'):
                try:
                    body = json_decode(self.post_args[1]['postbody'])
                except:
                    raise Exception('error with body formatting')
                if body['type'] == 'upload':
                    response.url = body['url']
                    return json_encode({'type':'upload',
                                        'url':body['url'],
                                        'upload':'http://localhost/upload/testing'
                                       })
                elif body['type'] == 'check':
                    return json_encode({'type':'check',
                                        'url':body['url'],
                                        'result':response.result
                                       })
                else:
                    raise Exception('bad type')
            else:
                logger.error('url is %s',url)
                raise Exception('got something other than upload address')

        self.fetch_error = Exception('GET invalid')
        self.post_response = response
        self.post_headers = ['http/1.1 200']

        # upload file using http
        self.put_called = False
        response.url = None
        response.result = True
        up_file = os.path.join(self.test_dir,str(random.randint(100,1000)))
        with open(up_file,'w') as f:
            f.write(str(random.randint(1000,1000000)))
        upload_addr = 'http://test/upload'
        ret = iceprod.core.functions.upload(up_file,
                                          upload_addr,
                                          proxy=True,
                                          options=upload_options)
        if not ret:
            raise Exception('simple http: returned error %r'%ret)
        if not self.put_called:
            raise Exception('simple http: put not called')
        if response.url != upload_addr:
           raise Exception('simple http: put address incorrect')
        if self.put_args[0][1] != up_file and (
           'filename' not in self.put_args[1] or
           self.put_args[1]['filename'] != up_file):
           raise Exception('simple http: put filename incorrect')

        # test upload error
        self.put_called = False
        response.url = None
        response.result = True
        self.put_error = Exception('upload error')
        ret= iceprod.core.functions.upload(up_file,
                                         upload_addr,
                                         proxy=True,
                                         options=upload_options)
        if ret:
            raise Exception('simple http: succeeded when supposed to fail')


        # upload file to gsiftp
        upload_addr = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/testing'
        response.url = None
        response.result = True
        self.put_called = False
        self.put_error = None
        ret = iceprod.core.functions.upload(up_file,
                                          upload_addr,
                                          proxy=True,
                                          options=upload_options)
        if not ret:
            raise Exception('gsiftp: upload failed %r'%ret)
        if not self.put_called:
            raise Exception('gsiftp: put not called')
        if response.url != upload_addr:
            raise Exception('gsiftp: url is incorrect: %r'%response.url)

    @unittest_reporter(name='upload(proxy=prefix)')
    def test_404_upload(self):
        """Test the upload proxy selection"""
        upload_options = {'http_username':'icecube',
                          'http_password':'skua',
                          'proxy_addr':'localhost',
                          'key':'key'}

        data = os.urandom(1024) # 1KB upload
        def response():
            try:
                url = self.post_args[0][0]
            except:
                raise Exception('error with url formatting')

            if url.endswith('/upload'):
                try:
                    body = json_decode(self.post_args[1]['postbody'])
                except:
                    raise Exception('error with body formatting')
                if body['type'] == 'upload':
                    response.url = body['url']
                    return json_encode({'type':'upload',
                                        'url':body['url'],
                                        'upload':'http://localhost/upload/testing'
                                       })
                elif body['type'] == 'check':
                    return json_encode({'type':'check',
                                        'url':body['url'],
                                        'result':response.result
                                       })
                else:
                    raise Exception('bad type')
            else:
                logger.error('url is %s',url)
                raise Exception('got something other than upload address')

        self.fetch_error = Exception('GET invalid')
        self.post_response = response
        self.post_headers = ['http/1.1 200']
        proxy = ('http','gsiftp')

        # upload file using http
        self.put_called = False
        response.url = None
        response.result = True
        up_file = os.path.join(self.test_dir,str(random.randint(100,1000)))
        with open(up_file,'w') as f:
            f.write(str(random.randint(1000,1000000)))
        upload_addr = 'http://test/upload'
        ret = iceprod.core.functions.upload(up_file,
                                          upload_addr,
                                          proxy=proxy,
                                          options=upload_options)
        if not ret:
            raise Exception('simple http: returned error %r'%ret)
        if not self.put_called:
            raise Exception('simple http: put not called')
        if response.url != upload_addr:
           raise Exception('simple http: put address incorrect')
        if self.put_args[0][1] != up_file and (
           'filename' not in self.put_args[1] or
           self.put_args[1]['filename'] != up_file):
           raise Exception('simple http: put filename incorrect')

        # upload file to gsiftp
        def put(url,filename=None):
            put.url = url
            return True
        put.url = None
        def put_chksum(url):
            put_chksum.url = url
            return iceprod.core.functions.sha512sum(up_file)
        put_chksum.url = None
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('put').replace_with(put)
        flexmock(iceprod.core.gridftp.GridFTP).should_receive('sha512sum').replace_with(put_chksum)


        upload_addr = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/testing'
        response.url = None
        response.result = True
        self.put_called = False
        self.put_error = None
        ret = iceprod.core.functions.upload(up_file,
                                          upload_addr,
                                          proxy=proxy,
                                          options=upload_options)
        if not ret:
            raise Exception('gsiftp: upload failed %r'%ret)
        if not self.put_called:
            raise Exception('gsiftp: put not called')
        if response.url != upload_addr:
            raise Exception('gsiftp: url is incorrect: %r'%response.url)

        # upload file to gsiftp without proxy
        upload_addr = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/testing'
        response.url = None
        response.result = True
        self.put_called = False
        self.put_error = None
        proxy = 'http'
        ret = iceprod.core.functions.upload(up_file,
                                          upload_addr,
                                          proxy=proxy,
                                          options=upload_options)
        if not ret:
            raise Exception('gsiftp: upload failed %r'%ret)
        if self.put_called:
            raise Exception('gsiftp: proxy was used')
        if response.url:
            raise Exception('gsiftp: proxy was used for url')
        if put.url != upload_addr:
            raise Exception('gsiftp: url is incorrect: %r'%put.url)

    @unittest_reporter
    def test_500_getuser(self):
        """Test the getuser function"""
        user = iceprod.core.functions.getuser()
        ext_user = user
        try:
            ext_user = os.environ['USER']
        except:
            try:
                ext_user = os.environ['LOGNAME']
            except:
                try:
                    ext_user = subprocess.check_output(['/usr/bin/whoami'])
                    ext_user = ext_user.strip()
                except:
                    logger.warn('cannot get the username manually')
                    pass
        if user != ext_user:
            raise Exception('Username is incorrect. %s != %s',user,ext_user)

    @unittest_reporter
    def test_501_platform(self):
        """Test the platform function"""
        platform = iceprod.core.functions.platform()
        ext_platform = platform
        try:
            arch = subprocess.check_output("uname -m | "
                "sed -e 's/Power Macintosh/ppc/ ; s/i686/i386/'",
                shell=true).strip()
            ostype = subprocess.check_output("uname",shell=True).strip()
            if ostype == 'Linux':
                ver = subprocess.check_output("ldd --version|"
                    "awk 'NR>1{exit};{print $(NF)}'",
                    shell=True).strip()
            else:
                ver = subprocess.check_output("uname -r",
                    shell=True).strip()
            ext_platform = '%s.%s.%s'%(arch,ostype,ver)
        except:
            pass
        if platform != ext_platform:
            raise Exception('Platform is incorrect. %s != %s',
                platform,ext_platform)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(functions_test))
    suite.addTests(loader.loadTestsFromNames(alltests,functions_test))
    return suite
