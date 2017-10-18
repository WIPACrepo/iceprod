"""
Test script for dbmethods.misc
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('dbmethods_test')

import os, sys, time
import shutil
import tempfile
import random
import stat
try:
    import StringIO
except ImportError:
    from io import StringIO
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable
import unittest

import tornado.escape

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base


class dbmethods_misc_test(dbmethods_base):
    @unittest_reporter
    def test_nothing(self):
        """Test nothing"""
        pass #placeholder for tests to be written

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_misc_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_misc_test))
    return suite
