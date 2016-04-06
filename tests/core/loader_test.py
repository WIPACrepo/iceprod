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

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

from iceprod.core import to_log


class loader_test(unittest.TestCase):
    def setUp(self):
        super(loader_test,self).setUp()

        self.orig_dir = os.path.abspath(os.path.expandvars(os.getcwd()))

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

        self.real_loader = os.path.join(self.orig_dir,'bin','loader.sh')

        self.chdir = os.path.join(self.test_dir,'ch')
        if not os.path.exists(self.chdir):
            os.mkdir(self.chdir)
        os.chdir(self.chdir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)
        super(loader_test,self).tearDown()

    @unittest_reporter(name=' ')
    def test_01(self):
        """Test basic loader functionality"""
        # replace the exec call in the loader, so it doesn't do anything
        loader_lines = open(self.real_loader).readlines()
        test_loader = os.path.join(self.chdir,'loader.sh')
        with open(test_loader,'w') as f:
            for line in loader_lines:
                line = line.strip('\n')
                if (line.startswith('exec $cmd') or
                    line.startswith('rm -Rf')):
                    continue
                f.write(line+'\n')

        # give us a fake parrot
        parrot_path = os.path.join(self.chdir,'parrot_run')
        with open(parrot_path,'w') as f:
            f.write('#!/bin/sh\necho $@\n')
        subprocess.call('chmod +x %s'%parrot_path,shell=True)
        if 'PATH' in os.environ:
            os.environ['PATH'] = self.chdir+':'+os.environ['PATH']
        else:
            os.environ['PATH'] = self.chdir
        if self.chdir not in subprocess.check_output('type parrot_run',shell=True):
            raise Exception('failed to set PATH')

        # call help
        cmd = '/bin/sh %s -h'%test_loader
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0]
        if 'Iceprod core framework starter script.' not in out:
            logger.info(out)
            raise Exception('help: did not display usage info')

        # call main
        cmd = '/bin/sh %s'%test_loader
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0]
        if proc.returncode:
            logger.info(out)
            raise Exception('main: error raised')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('main: did not echo cmd')

        # test cvmfs
        cvmfs_dir = os.path.join(self.test_dir,'cvmfs')
        os.mkdir(cvmfs_dir)
        with open(os.path.join(cvmfs_dir,'setup.sh'),'w') as f:
            f.write('#!/bin/sh\necho "export PYTHOPATH=$PWD/lib/python2.7/site-pacakges"\n')

        cmd = '/bin/sh %s -s %s'%(test_loader,cvmfs_dir)
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0]
        if proc.returncode:
            logger.info(out)
            raise Exception('main: error raised')
        if 'cvmfs' not in out:
            logger.info(out)
            raise Exception('main: did not echo CVMFS')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('env cache: did not echo cmd')

        # explicitly set platform
        platform = 'test'
        cmd = '/bin/sh %s -m %s'
        cmd = cmd%(test_loader,platform)
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0]
        if proc.returncode:
            logger.info(out)
            raise Exception('platform: error raised')
        if 'i3exec' not in out:
            logger.info(out)
            raise Exception('platform: did not echo cmd')

        # explicitly set env dir
        env_name = self.chdir
        cmd = '/bin/sh %s -e %s'
        cmd = cmd%(test_loader,env_name)
        proc = subprocess.Popen(cmd,shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        out = proc.communicate()[0]
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
        out = proc.communicate()[0]
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
        out = proc.communicate()[0]
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
        out = proc.communicate()[0]
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
        out = proc.communicate()[0]
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
