"""
Test script for common functions
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('functions')

import os
import shutil
import random
import string
import subprocess
import tempfile

try:
    pass
except:
    pass

try:
    import psutil
except ImportError:
    psutil = None

import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import requests_mock

from tornado.testing import AsyncTestCase

import iceprod.core.dataclasses
import iceprod.core.util
import iceprod.core.gridftp
import iceprod.core.functions


class functions_test(AsyncTestCase):
    def setUp(self):
        super(functions_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.symlink(os.path.join(curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    @unittest_reporter
    def test_001_uncompress(self):
        """Test uncompressing a file"""
        for ext in ('gz','bz2','xz','lzma'):
            for i in range(10):
                # create test file
                filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
                while os.path.exists(filename):
                    filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
                with open(filename,'w') as f:
                    file_contents = ''
                    for x in range(1000):
                        file_contents += str(random.choice(string.ascii_letters))
                    f.write(file_contents)

                # compress
                outfile = iceprod.core.functions.compress(filename, ext)
                logger.info('compressed file is %s'%outfile)
                if outfile != filename+'.'+ext:
                    raise Exception('did not create correct filename')
                if not os.path.isfile(outfile):
                    raise Exception('did not create compressed file')

                # remove original file
                if os.path.exists(filename):
                    os.remove(filename)

                # uncompress
                files = iceprod.core.functions.uncompress(outfile)
                logger.info('uncompress returned %r',files)
                if not isinstance(files,str):
                    raise Exception('uncompress did not return a string of the new file name')
                if not os.path.isfile(files):
                    raise Exception('uncompress returned an invalid file name')

                # check file
                if not os.path.samefile(filename,files):
                    logger.warning('file names are different')
                with open(files, 'r') as f:
                    results = f.read(len(file_contents)*10)
                    if file_contents != results:
                        raise Exception('contents not the same')

    @unittest_reporter(name='uncompress() with tar files')
    def test_002_uncompress_tar(self):
        """Test uncompressing a file with tar"""
        for ext in ('tar.gz','tgz','tar.bz2','tbz','tar.xz','tar.lzma'):
            for i in range(10):
                filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
                while os.path.exists(filename):
                    filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
                os.mkdir(filename)
                infiles = {}
                for _ in range(10):
                    # create test file
                    fname = os.path.join(filename,str(random.randint(0,100000)))
                    with open(fname,'w') as f:
                        file_contents = ''
                        for x in range(1000):
                            file_contents += str(random.choice(string.ascii_letters))
                        f.write(file_contents)
                    infiles[fname] = file_contents

                # compress
                outfile = iceprod.core.functions.compress(filename, ext)
                logger.info('compressed file is %s'%outfile)
                if outfile != filename+'.'+ext:
                    raise Exception('did not create correct filename')
                if not os.path.isfile(outfile):
                    raise Exception('did not create compressed file')

                # remove original file
                if os.path.exists(filename):
                    shutil.rmtree(filename)

                # uncompress
                files = iceprod.core.functions.uncompress(outfile)
                logger.info('uncompress returned %r',files)
                if set(files) != set(x.replace(self.test_dir+'/','') for x in infiles):
                    raise Exception('not the same files')
                
                # check files
                for fname in infiles:
                    if not os.path.exists(fname):
                        logger.warning('file names are different')
                    with open(fname, 'r') as f:
                        results = f.read(len(file_contents)*10)
                        if infiles[fname] != results:
                            raise Exception('contents not the same')

    @unittest_reporter(name='uncompress() tar - other dir')
    def test_003_uncompress_tar(self):
        """Test uncompressing a file with tar"""
        for ext in ('tar.gz','tgz','tar.bz2','tbz','tar.xz','tar.lzma'):
            filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            while os.path.exists(filename):
                filename = os.path.join(self.test_dir,'test_uncompress'+str(random.randint(0,100000)))
            os.mkdir(filename)
            infiles = {}
            for _ in range(10):
                # create test file
                fname = os.path.join(filename,str(random.randint(0,100000)))
                with open(fname,'w') as f:
                    file_contents = ''
                    for x in range(1000):
                        file_contents += str(random.choice(string.ascii_letters))
                    f.write(file_contents)
                infiles[fname] = file_contents

            # compress
            outfile = iceprod.core.functions.compress(filename, ext)
            logger.info('compressed file is %s'%outfile)
            if outfile != filename+'.'+ext:
                raise Exception('did not create correct filename')
            if not os.path.isfile(outfile):
                raise Exception('did not create compressed file')

            # remove original file
            if os.path.exists(filename):
                shutil.rmtree(filename)

            local_dir = os.path.join(self.test_dir, 'local')
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)

            # uncompress
            files = iceprod.core.functions.uncompress(outfile, out_dir=local_dir)
            logger.info('uncompress returned %r',files)
            if set(files) != set(x.replace(self.test_dir+'/','') for x in infiles):
                raise Exception('not the same files')

            # check that files are extracted in local dir
            logger.info('local dir %r: %r', local_dir, os.listdir(local_dir))
            if files[0].split(os.path.sep)[0] not in os.listdir(local_dir):
                raise Exception('did not extract in local dir')

            # check files
            for fname in infiles:
                common = os.path.commonprefix([local_dir, fname])
                fname2 = os.path.join(local_dir, fname[len(common):])
                if not os.path.exists(fname2):
                    raise Exception('file %r does not exist'%fname2)
                with open(fname2, 'r') as f:
                    results = f.read(len(file_contents)*10)
                    if infiles[fname] != results:
                        raise Exception('contents not the same')


    @unittest_reporter
    def test_020_iscompressed(self):
        """Test the iscompressed function with various extensions"""
        for i in range(0,10):
            if not iceprod.core.functions.iscompressed('test.gz'):
                raise Exception('failed on .gz')
            if not iceprod.core.functions.iscompressed('test.tar.gz'):
                raise Exception('failed on .tar.gz')
            if not iceprod.core.functions.iscompressed('test.tgz'):
                raise Exception('failed on .tgz')
            if not iceprod.core.functions.iscompressed('test.tar.bz2'):
                raise Exception('failed on .tar.bz2')
            if not iceprod.core.functions.iscompressed('test.lzma'):
                raise Exception('failed on .lzma')
            if not iceprod.core.functions.iscompressed('test.xz'):
                raise Exception('failed on .xz')
            if not iceprod.core.functions.iscompressed('test.tar.lzma'):
                raise Exception('failed on .tar.lzma')
            if iceprod.core.functions.iscompressed('test'):
                raise Exception('failed on (no ext)')
            if iceprod.core.functions.iscompressed('test.doc'):
                raise Exception('failed on .doc')
            if iceprod.core.functions.iscompressed('test.xml'):
                raise Exception('failed on .xml')
            if iceprod.core.functions.iscompressed('test.gzhelp'):
                raise Exception('failed on .gzhelp')

    @unittest_reporter
    def test_021_istarred(self):
        """Test the istarred function with various extensions"""
        for i in range(0,10):
            if not iceprod.core.functions.istarred('test.tar.gz'):
                raise Exception('failed on .tar.gz')
            if not iceprod.core.functions.istarred('test.tgz'):
                raise Exception('failed on .tgz')
            if not iceprod.core.functions.istarred('test.tar.bz2'):
                raise Exception('failed on .tar.bz2')
            if not iceprod.core.functions.istarred('test.tar.lzma'):
                raise Exception('failed on .tar.lzma')
            if iceprod.core.functions.istarred('test'):
                raise Exception('failed on (no ext)')
            if iceprod.core.functions.istarred('test.doc'):
                raise Exception('failed on .doc')
            if iceprod.core.functions.istarred('test.xml'):
                raise Exception('failed on .xml')
            if iceprod.core.functions.istarred('test.gzhelp'):
                raise Exception('failed on .gzhelp')

    @unittest_reporter
    def test_100_md5sum(self):
        """Test the creation of md5sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_md5sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                external, file = out.decode('utf-8').split()
            except Exception:
                raise Exception('failed to get external md5sum')

            if internal != external:
                raise Exception('failed md5sum check')

            os.remove(filename)

    @unittest_reporter
    def test_101_check_md5sum(self):
        """Test the checking of md5sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_md5sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                raise Exception('md5sum as str failed')

            os.remove(filename)

        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_md5sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
                filename = os.path.join(self.test_dir,'test_md5sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get md5sum from functions
            if subprocess.call('md5sum %s > %s.md5sum'%(filename,filename),shell=True):
                raise Exception('failed to generate md5sum')

            # check md5sum
            if not iceprod.core.functions.check_md5sum(filename,filename+'.md5sum'):
                raise Exception('md5sum as list failed')

            os.remove(filename)
            os.remove(filename+'.md5sum')

    @unittest_reporter
    def test_102_sha1sum(self):
        """Test the creation of sha1sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha1sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                external, file = out.decode('utf-8').split()
            except Exception:
                raise Exception('failed to get external sha1sum')

            if internal != external:
                raise Exception('failed sha1sum check')

            os.remove(filename)

    @unittest_reporter
    def test_103_check_sha1sum(self):
        """Test the checking of sha1sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha1sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                raise Exception('sha1sum as str failed')

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
                raise Exception('failed to generate sha1sum')

            # check sha1sum
            if not iceprod.core.functions.check_sha1sum(filename,filename+'.sha1sum'):
                raise Exception('sha1sum as list failed')

            os.remove(filename)
            os.remove(filename+'.sha1sum')

    @unittest_reporter
    def test_104_sha256sum(self):
        """Test the creation of sha256sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha256sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                external, file = out.decode('utf-8').split()
            except Exception:
                raise Exception('failed to get external sha256sum')

            if internal != external:
                raise Exception('failed sha256sum check')

            os.remove(filename)

    @unittest_reporter
    def test_105_check_sha256sum(self):
        """Test the checking of sha256sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha256sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                raise Exception('sha256sum as str failed')

            os.remove(filename)

        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha256sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
                filename = os.path.join(self.test_dir,'test_sha256sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha256sum from functions
            if subprocess.call('sha256sum %s > %s.sha256sum'%(filename,filename),shell=True):
                raise Exception('failed to generate sha256sum')

            # check sha256sum
            if not iceprod.core.functions.check_sha256sum(filename,filename+'.sha256sum'):
                raise Exception('sha256sum as list failed')

            os.remove(filename)
            os.remove(filename+'.sha256sum')

    @unittest_reporter
    def test_106_sha512sum(self):
        """Test the creation of sha512sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
                filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += random.choice(string.ascii_letters)
                f.write(file_contents)

            # get sha512sum from functions
            internal = iceprod.core.functions.sha512sum(filename)
            out = subprocess.Popen('sha512sum %s'%filename,shell=True,stdout=subprocess.PIPE).communicate()[0]
            try:
                external, file = out.decode('utf-8').split()
            except Exception:
                raise Exception('failed to get external sha512sum')

            self.assertEqual(internal, external)

            os.remove(filename)

    @unittest_reporter
    def test_107_check_sha512sum(self):
        """Test the checking of sha512sums of files"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                raise Exception('sha512sum as str failed')

            os.remove(filename)

        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            while os.path.exists(filename):
                filename = os.path.join(self.test_dir,'test_sha512sum'+str(random.randint(0,100000)))
            with open(filename,'w') as f:
                file_contents = ''
                for x in range(0,1000):
                    file_contents += str(random.choice(string.ascii_letters))
                f.write(file_contents)

            # get sha512sum from functions
            if subprocess.call('sha512sum %s > %s.sha512sum'%(filename,filename),shell=True):
                raise Exception('failed to generate sha512sum')

            # check sha512sum
            if not iceprod.core.functions.check_sha512sum(filename,filename+'.sha512sum'):
                raise Exception('sha512sum as list failed')

            os.remove(filename)
            os.remove(filename+'.sha512sum')

    @unittest_reporter
    def test_200_removedirs(self):
        """Test removing files and directories"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_removedirs'+str(random.randint(0,100000)))
            while os.path.exists(filename):
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
                raise Exception('removedirs failed to remove %s')%filename

        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            while os.path.exists(dir):
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
                raise Exception('removedirs failed to remove %s')%dir

    @unittest_reporter
    def test_201_copy(self):
        """Test copying files and directories"""
        for i in range(0,10):
            # create test file
            filename = os.path.join(self.test_dir,'test_copy'+str(random.randint(0,100000)))
            while os.path.exists(filename):
                filename = os.path.join(self.test_dir,'test_copy'+str(random.randint(0,100000)))
            filename2 = os.path.join(self.test_dir,'test_copy'+str(random.randint(0,100000)))
            while os.path.exists(filename2) or filename == filename2:
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
                raise Exception('copy failed to copy %s to %s')%(filename,filename2)
            with open(filename2,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception('contents not the same')

        for i in range(0,10):
            # create test directory
            dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            while os.path.exists(dir):
                dir = os.path.join(self.test_dir,str(random.randint(0,100000)))
            dir2 = os.path.join(self.test_dir,str(random.randint(0,100000)))
            while os.path.exists(dir2) or dir == dir2:
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
                raise Exception('copy failed to copy %s to %s')%(dir,dir2)
            # check file
            if not os.path.isfile(filename2):
                raise Exception('copy failed to copy %s to %s')%(filename,filename2)
            with open(filename2,'r') as f:
                results = f.read(len(file_contents)*10)
                if file_contents != results:
                    raise Exception('contents not the same')

    @unittest_reporter(skip=not psutil)
    def test_300_getInterfaces(self):
        """Test the getInterfaces function"""
        # get interfaces
        ifaces = iceprod.core.functions.getInterfaces()
        if not ifaces:
            raise Exception('getInterfaces() returned None')

        loop = None
        eth = []
        for name in ifaces:
            if name == 'lo':
                loop = ifaces[name]
            else:
                eth.append(ifaces[name])

        # check that we can see the loopback interface
        if not loop:
            raise Exception('No loop interface')

        # check that we can see an ethernet interface
        if len(eth) < 1:
            raise Exception('No ethernet interfaces')
        for e in eth:
            logging.info('%s',e)

    @patch('socket.getfqdn')
    @patch('socket.gethostname')
    @unittest_reporter
    def test_301_gethostname(self, fqdn, hostname):
        fqdn.return_value = 'myhost'
        hostname.return_value = 'myhost'
        host = iceprod.core.functions.gethostname()
        self.assertEqual(host, 'myhost')

        hostname.return_value = 'myhost.test.com'
        host = iceprod.core.functions.gethostname()
        self.assertEqual(host, 'myhost.test.com')

        fqdn.return_value = 'myhost.foo.bar'
        host = iceprod.core.functions.gethostname()
        self.assertEqual(host, 'myhost.test.com')

        fqdn.return_value = 'myhost.foo.bar.baz'
        host = iceprod.core.functions.gethostname()
        self.assertEqual(host, 'myhost.foo.bar.baz')

    @unittest_reporter
    def test_302_isurl(self):
        """Test the isurl function"""
        good_urls = ['http://www.google.com',
                     'https://skua.icecube.wisc.edu:9080',
                     'http://test.com?blah=1#60',
                     'gsiftp://gridftp.icecube.wisc.edu',
                     'ftp://x2100.icecube.wisc.edu',
                     'file:/data/exp']
        bad_urls = ['slkdjf:/sldfjlksd',
                    'rpc://test']
        for i in range(0,10):
            for url in good_urls:
                if not iceprod.core.functions.isurl(url):
                    raise Exception('isurl thought %s was not a valid url'%url)
            for url in bad_urls:
                if iceprod.core.functions.isurl(url):
                    raise Exception('isurl thought %s was a valid url'%url)

    @requests_mock.mock()
    @unittest_reporter(name='download() http')
    async def test_303_download(self, http_mock):
        """Test the download function"""
        download_options = {'username': 'user',
                            'password': 'pass',}

        data = b'the data'
        md5sum = b'3d5f3303ed6ce28c2d5ac1192118f0e2'

        # download file from resources
        http_mock.get('/globus.tar.gz', content=data)
        http_mock.get('/globus.tar.gz.md5sum', content=md5sum+b' globus.tar.gz')
        await iceprod.core.functions.download('http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                self.test_dir,options=download_options)
        if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
            raise Exception('downloaded file does not exist')
        self.assertTrue(http_mock.called)
        data2 = open(os.path.join(self.test_dir,'globus.tar.gz'),'rb').read()
        self.assertEqual(data2, data, msg='data not equal')

    @unittest_reporter(name='download() file')
    async def test_304_download(self):
        """Test the download function"""
        data = 'the data'
        md5sum = '3d5f3303ed6ce28c2d5ac1192118f0e2'

        # download file from local file system
        filename = os.path.join(self.test_dir,'generators.py')
        with open(filename, 'w') as f:
            f.write(data)
        await iceprod.core.functions.download(filename,
                os.path.join(self.test_dir,'generators2.py'))
        if not os.path.isfile(os.path.join(self.test_dir,'generators2.py')):
            raise Exception('local cp: copied file does not exist')
        data2 = open(os.path.join(self.test_dir,'generators2.py')).read()
        self.assertEqual(data2, data, msg='data not equal')

    @patch('iceprod.core.functions.GridFTP')
    @unittest_reporter(name='download() gridftp')
    async def test_305_download(self, gridftp):
        """Test the download function"""
        # download file from gsiftp
        data = 'the data'
        def get(url,filename=None):
            logger.info('fake get: url=%r, filename=%r', url, filename)
            if url.endswith('globus.tar.gz'):
                with open(filename,'w') as f:
                    f.write(data)
                get.url = url
            else:
                raise Exception()
        get.url = None
        gridftp.get = get

        await iceprod.core.functions.download('gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz',
                self.test_dir)
        if not os.path.isfile(os.path.join(self.test_dir,'globus.tar.gz')):
            raise Exception('gsiftp: downloaded file does not exist')
        data2 = open(os.path.join(self.test_dir,'globus.tar.gz')).read()
        self.assertEqual(get.url, 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz')
        self.assertEqual(data2, data, msg='data not equal')

    @requests_mock.mock()
    @unittest_reporter(name='download() http - query params')
    async def test_306_download(self, http_mock):
        """Test the download function"""
        data = b'the data'
        md5sum = b'3d5f3303ed6ce28c2d5ac1192118f0e2'

        # download file from resources
        url = 'http://prod-exe.icecube.wisc.edu/globus.tar.gz?a=1'
        http_mock.get('/globus.tar.gz?a=1', content=data)
        http_mock.get('/globus.tar.gz.md5sum', content=md5sum+b' globus.tar.gz')
        out_file = await iceprod.core.functions.download(url, self.test_dir)
        self.assertEqual(out_file, os.path.join(self.test_dir,'globus.tar.gz'))
        if not os.path.isfile(out_file):
            raise Exception('downloaded file does not exist')
        self.assertTrue(http_mock.called)
        data2 = open(out_file,'rb').read()
        self.assertEqual(data2, data, msg='data not equal')

    @unittest_reporter(name='download() errors')
    async def test_320_download(self):
        """Test the download function"""
        data = b'the data'
        md5sum = b'3d5f3303ed6ce28c2d5ac1192118f0e2'
        download_options = {}

        filename = os.path.join(self.test_dir, 'generators.py')
        out_dir = os.path.join(self.test_dir, 'output')
        os.makedirs(out_dir)
        output_file = os.path.join(out_dir, 'generators.py')
        with open(filename, 'wb') as f:
            f.write(data)
        with open(filename+'.md5sum', 'wb') as f:
            f.write(md5sum+b' generators.py')

        # bad url
        with self.assertRaises(Exception):
            await iceprod.core.functions.download(filename+'blah', out_dir)

    @requests_mock.mock()
    @unittest_reporter(name='upload() http')
    async def test_402_upload(self, http_mock):
        """Test the upload function"""
        download_options = {'username': 'user',
                            'password': 'pass',}
        data = b'the data'

        # upload file to http
        http_mock.put('/globus.tar.gz', content=b'')
        http_mock.get('/globus.tar.gz', content=data)
        filename = os.path.join(self.test_dir, 'globus.tar.gz')
        with open(filename, 'wb') as f:
            f.write(data)
        await iceprod.core.functions.upload(filename,
                'http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                options=download_options)
        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'PUT', msg='not a PUT request')
        self.assertEqual(os.path.basename(req.url), 'globus.tar.gz', msg='bad upload url')

        # test bad upload
        http_mock.get('/globus.tar.gz', content=b'blah')
        with self.assertRaises(Exception):
            await iceprod.core.functions.upload(filename,
                    'http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                    options=download_options)

    @requests_mock.mock()
    @unittest_reporter(name='upload() http POST')
    async def test_403_upload(self, http_mock):
        """Test the upload function"""
        download_options = {'username': 'user',
                            'password': 'pass',}
        data = b'the data'

        # upload file to http
        http_mock.put('/globus.tar.gz', content=b'', status_code=405)
        http_mock.post('/globus.tar.gz', content=b'')
        http_mock.get('/globus.tar.gz', content=data)
        filename = os.path.join(self.test_dir, 'globus.tar.gz')
        with open(filename, 'wb') as f:
            f.write(data)
        await iceprod.core.functions.upload(filename,
                'http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                options=download_options)
        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'PUT', msg='not a PUT request first')
        self.assertEqual(os.path.basename(req.url), 'globus.tar.gz', msg='bad upload url')
        req = http_mock.request_history[1]
        self.assertEqual(req.method, 'POST', msg='not a POST request second')
        self.assertEqual(os.path.basename(req.url), 'globus.tar.gz', msg='bad upload url')

        # test bad upload
        http_mock.get('/globus.tar.gz', content=b'blah')
        with self.assertRaises(Exception):
            await iceprod.core.functions.upload(filename,
                    'http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                    options=download_options)

    @requests_mock.mock()
    @unittest_reporter(name='upload() http s3 ETAG')
    async def test_403_upload(self, http_mock):
        """Test the upload function with ETAG"""
        download_options = {}
        data = b'the data'
        filename = os.path.join(self.test_dir, 'globus.tar.gz')
        with open(filename, 'wb') as f:
            f.write(data)

        # upload file to http
        http_mock.put('/globus.tar.gz', content=b'', headers={'ETAG': iceprod.core.functions.md5sum(filename)})
        http_mock.get('/globus.tar.gz', content=b'', status_code=403)
        await iceprod.core.functions.upload(filename,
                'http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                options=download_options)
        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'PUT', msg='not a PUT request first')
        self.assertEqual(os.path.basename(req.url), 'globus.tar.gz', msg='bad upload url')
        self.assertEqual(len(http_mock.request_history), 1, msg='more than one http request')

        # test bad upload
        http_mock.put('/globus.tar.gz', content=b'', headers={'ETAG': 'blah'})
        with self.assertRaises(Exception):
            await iceprod.core.functions.upload(filename,
                    'http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                    options=download_options)

    @unittest_reporter(name='upload() file')
    async def test_404_upload(self):
        """Test the upload function"""
        data = 'the data'

        # upload file to local file system
        filename = os.path.join(self.test_dir, 'generators.py')
        out_dir = os.path.join(self.test_dir, 'output')
        os.makedirs(out_dir)
        output_file = os.path.join(out_dir, 'generators.py')
        with open(filename, 'w') as f:
            f.write(data)
        await iceprod.core.functions.upload('file:'+filename, 'file:'+output_file)
        if not os.path.isfile(output_file):
            raise Exception('copied file does not exist')
        data2 = open(output_file).read()
        self.assertEqual(data2, data, msg='data not equal')

        # test overwriting dest
        with open(output_file, 'w') as f:
            f.write('other data')
        await iceprod.core.functions.upload('file:'+filename, 'file:'+output_file)
        if not os.path.isfile(output_file):
            raise Exception('copied file does not exist')
        data2 = open(output_file).read()
        self.assertEqual(data2, data, msg='data not equal')

    @patch('iceprod.core.functions.GridFTP')
    @unittest_reporter(name='upload() gridftp')
    async def test_405_upload(self, gridftp):
        """Test the upload function"""
        data = 'the data'
        sha512sum = '8580e83fc859a2786430406fd41c7c6a0d3ac77b7eff07bc94c880f5b6e86b87320ea25cb3f3c5a3881236cf8bda92cb8f61c2a813881fee1d8f8331565ce98a'
        
        def put(url,filename=None):
            logger.info('fake get: url=%r, filename=%r', url, filename)
            if url.endswith('globus.tar.gz'):
                put.url = url
                put.filename = filename
            elif url.endswith('globus2.tar.gz'):
                raise Exception('expected failure')
            else:
                raise Exception()
        put.url = None
        put.filename = None
        gridftp.put = put
        gridftp.sha512sum.return_value = sha512sum

        filename = os.path.join(self.test_dir, 'globus.tar.gz')
        with open(filename, 'w') as f:
            f.write(data)
        await iceprod.core.functions.upload(filename,
                'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz')
        self.assertEqual(put.url, 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz')
        self.assertEqual(put.filename, filename)

        # test gridftp error
        with self.assertRaises(Exception):
            await iceprod.core.functions.upload(filename,
                    'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus2.tar.gz')

        # test checksum error
        gridftp.sha512sum.return_value = 'blah'
        with self.assertRaises(Exception):
            await iceprod.core.functions.upload(filename,
                    'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz')

    @unittest_reporter(name='upload() dir')
    async def test_410_upload(self):
        """Test the upload function"""
        data = 'the data'

        # upload file to local file system
        in_dir = os.path.join(self.test_dir, 'input')
        filename = os.path.join(in_dir, 'generators.py')
        out_dir = os.path.join(self.test_dir, 'output')
        os.makedirs(in_dir)
        os.makedirs(out_dir)
        output_file = os.path.join(out_dir, 'generators')
        with open(filename, 'w') as f:
            f.write(data)
        await iceprod.core.functions.upload(in_dir, 'file:'+output_file)
        if not os.path.isfile(output_file):
            raise Exception('tar file does not exist')
        os.chdir(out_dir)
        subprocess.check_call(['tar','-axf',output_file])
        final_out = os.path.join('input','generators.py')
        if not os.path.isfile(final_out):
            raise Exception('copied file does not exist')
        data2 = open(final_out).read()
        self.assertEqual(data2, data, msg='data not equal')

    @unittest_reporter(name='upload() errors')
    async def test_420_upload(self):
        # bad request type
        filename = os.path.join(self.test_dir, 'globus.tar.gz')
        with self.assertRaises(Exception):
            await iceprod.core.functions.upload(filename,
                    'foobar://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus2.tar.gz')

        # src doesn't exist
        filename = os.path.join(self.test_dir, 'globus.tar.gz')
        with self.assertRaises(Exception):
            await iceprod.core.functions.upload(filename,
                    'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus2.tar.gz')

    @requests_mock.mock()
    @unittest_reporter(name='delete() http')
    def test_503_delete(self, http_mock):
        """Test the delete function"""
        download_options = {'username': 'user',
                            'password': 'pass',}
        data = 'the data'

        # delete file from http
        http_mock.delete('/globus.tar.gz', content=b'')
        iceprod.core.functions.delete('http://prod-exe.icecube.wisc.edu/globus.tar.gz',
                options=download_options)
        self.assertTrue(http_mock.called)
        req = http_mock.request_history[0]
        self.assertEqual(req.method, 'DELETE', msg='not a DELETE request')
        self.assertEqual(os.path.basename(req.url), 'globus.tar.gz', msg='bad delete url')

        # test http error
        try:
            iceprod.core.functions.delete('http://prod-exe.icecube.wisc.edu/globus2.tar.gz')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter(name='delete() file')
    def test_504_delete(self):
        """Test the delete function"""
        data = 'the data'

        # delete file from local file system
        filename = os.path.join(self.test_dir, 'generators.py')
        with open(filename, 'w') as f:
            f.write(data)
        iceprod.core.functions.delete(filename)
        if os.path.isfile(filename):
            raise Exception('delete file exists')

        # test with file: prefix
        with open(filename, 'w') as f:
            f.write(data)
        iceprod.core.functions.delete('file:'+filename)
        if os.path.isfile(filename):
            raise Exception('delete file exists')

        # test with non-existent file
        iceprod.core.functions.delete('file:'+filename)
        if os.path.isfile(filename):
            raise Exception('delete file exists')

    @patch('iceprod.core.functions.GridFTP')
    @unittest_reporter(name='delete() gridftp')
    def test_505_delete(self, gridftp):
        """Test the delete function"""
        data = 'the data'
        sha512sum = '8580e83fc859a2786430406fd41c7c6a0d3ac77b7eff07bc94c880f5b6e86b87320ea25cb3f3c5a3881236cf8bda92cb8f61c2a813881fee1d8f8331565ce98a'
        
        def delete(url):
            logger.info('fake delete: url=%r', url)
            if url.endswith('globus.tar.gz'):
                return
            elif url.endswith('globus2.tar.gz'):
                raise Exception('expected failure')
            else:
                raise Exception()
        gridftp.rmtree = delete

        url = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus.tar.gz'
        iceprod.core.functions.delete(url)

        # test gridftp error
        try:
            url = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/globus2.tar.gz'
            iceprod.core.functions.delete(url)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # test gridftp error
        try:
            url = 'gsiftp://data.icecube.wisc.edu/data/sim/sim-new/downloads/blah.tar.gz'
            iceprod.core.functions.delete(url)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter(name='delete() errors')
    def test_510_delete(self):
        try:
            url = 'blah://test.test'
            iceprod.core.functions.delete(url)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(functions_test))
    suite.addTests(loader.loadTestsFromNames(alltests,functions_test))
    return suite
