"""
Test script for main iceprod server
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('iceprod_server_test')

import os
import time
import tempfile
import shutil
import subprocess

import unittest

import psutil

def get_children():
    ret = {}
    for p in psutil.process_iter():
        try:
            attrs = p.as_dict(['name','cmdline'])
            if 'iceprod' in attrs['name'] or 'iceprod' in str(attrs['cmdline']):
                ret[p.pid()] = p
                for pp in p.children(recursive=True):
                    ret[pp.pid()] = pp
        except:
            pass
    if os.getpid() in ret:
        del ret[os.getpid()]
    return ret.values()

class iceprod_server_test(unittest.TestCase):
    def setUp(self):
        super(iceprod_server_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    @unittest_reporter(skip=True)
    def test_01_run(self):
        # make sure we don't try running the grid
        with open('iceprod_config.json','w') as f:
            f.write('{"modules":{"queue":false},"logging":{"level":"debug"}}')
        subprocess.check_call(['../bin/iceprod_server.py','--pidfile','pid','start'])
        try:
            time.sleep(4)
            self.assertTrue(os.path.exists('pid'))
            alive_children = get_children()
            logger.info('children: %r', alive_children)

            subprocess.check_call(['../bin/iceprod_server.py','--pidfile','pid','stop'])
            self.assertFalse(os.path.exists('pid'))
            dead_children = get_children()
            if dead_children:
                raise Exception('did not kill all children')
        finally:
            if os.path.exists('var/log'):
                for f in os.listdir('var/log'):
                    p = os.path.join('var/log',f)
                    if os.path.isfile(p):
                        logger.info('log for %s:\n\n%s\n\n', f, open(p).read())
            if os.path.exists('pid'):
                subprocess.check_call(['../bin/iceprod_server.py','--pidfile','pid','kill'])

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(iceprod_server_test))
    suite.addTests(loader.loadTestsFromNames(alltests,iceprod_server_test))
    return suite
