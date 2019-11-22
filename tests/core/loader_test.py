"""
Test script for loader.sh
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('loader_test')

import os, sys, time
import shutil
import tempfile
import random
import string
import subprocess
import threading
import unittest

from flexmock import flexmock

from iceprod.core import to_log


class loader_test(unittest.TestCase):
    def setUp(self):
        super(loader_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        self.curdir = os.getcwd()
        os.symlink(os.path.join(self.curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(self.curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        # clean up environment
        base_env = dict(os.environ)
        def reset_env():
            for k in set(os.environ).difference(base_env):
                del os.environ[k]
            for k in base_env:
                os.environ[k] = base_env[k]
        self.addCleanup(reset_env)

        self.real_loader = os.path.join(self.curdir,'bin','loader.sh')

    @unittest_reporter(name=' ')
    def test_01(self):
        """Test basic loader functionality"""
        # replace the exec call in the loader, so it doesn't do anything
        loader_lines = open(self.real_loader).readlines()
        test_loader = os.path.join(self.test_dir,'loader.sh')
        with open(test_loader,'w') as f:
            for line in loader_lines:
                line = line.strip('\n')
                if (line.startswith('$cmd') or
                    line.startswith('rm -Rf')):
                    continue
                f.write(line+'\n')

        # call help
        cmd = '/bin/sh %s -h'%test_loader
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0].decode('utf-8')
        if 'Iceprod core framework starter script.' not in out:
            logger.info(out)
            raise Exception('help: did not display usage info')

        # call main
        cmd = '/bin/sh %s'%test_loader
        proc = subprocess.Popen(cmd,shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        out = proc.communicate()[0].decode('utf-8')
        if proc.returncode:
            logger.info(out)
            raise Exception('main: error raised')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('main: did not echo cmd')

        # test cvmfs
        os.remove('iceprod')
        try:
            cvmfs_dir = os.path.join(self.test_dir,'cvmfs')
            os.mkdir(cvmfs_dir)
            with open(os.path.join(cvmfs_dir,'setup.sh'),'w') as f:
                f.write('#!/bin/sh\necho "export PYTHOPATH=$PWD/lib/python2.7/site-pacakges"\n')

            cmd = '/bin/sh %s -s %s'%(test_loader,cvmfs_dir)
            proc = subprocess.Popen(cmd,shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
            out = proc.communicate()[0].decode('utf-8')
            if proc.returncode:
                logger.info(out)
                raise Exception('main: error raised')
            if 'cvmfs' not in out:
                logger.info('%s', out)
                raise Exception('main: did not echo CVMFS')
            if 'i3exec' not in out:
                logger.info(out)
                raise Exception('env cache: did not echo cmd')
        finally:
            os.symlink(os.path.join(self.curdir, 'iceprod'),
                       os.path.join(self.test_dir, 'iceprod'))

        # explicitly set env dir
        env_name = self.test_dir
        cmd = '/bin/sh %s -e %s'
        cmd = cmd%(test_loader,env_name)
        proc = subprocess.Popen(cmd,shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        out = proc.communicate()[0].decode('utf-8')
        if proc.returncode:
            logger.info(out)
            raise Exception('env filename: error raised')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('env filename: did not echo cmd')

        # test username,password
        username = 'user'
        password = 'password'
        cmd = '/bin/sh %s -u %s -p %s'
        cmd = cmd%(test_loader,username,password)
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0].decode('utf-8')
        if proc.returncode:
            logger.info(out)
            raise Exception('username,password: error raised')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('username,password: did not echo cmd')

        # test x509
        x509 = 'x509_file'
        cmd = '/bin/sh %s -x %s'
        cmd = cmd%(test_loader,x509)
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0].decode('utf-8')
        if proc.returncode:
            logger.info(out)
            raise Exception('x509: error raised')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('x509: did not echo cmd')

        # test bad arg
        cmd = '/bin/sh %s -x'%test_loader
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0].decode('utf-8')
        if not proc.returncode:
            logger.info(out)
            raise Exception('bad arg: did not raise error')

        # test argument passthrough
        arg = '--passkey kn23sdf9 --arg 3'
        cmd = '/bin/sh %s %s'
        cmd = cmd%(test_loader,arg)
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0].decode('utf-8')
        if proc.returncode:
            logger.info(out)
            raise Exception('argument passthrough: error raised')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('argument passthrough: did not echo cmd')
        if arg not in out:
            logger.info(out)
            raise Exception('argument passthrough: did not echo arg')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(loader_test))
    suite.addTests(loader.loadTestsFromNames(alltests,loader_test))
    return suite
