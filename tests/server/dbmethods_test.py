"""
Test script for dbmethods
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('dbmethods_test')

import os, sys, time
import shutil
import tempfile
import random
import stat
import StringIO
from itertools import izip
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable

try:
    import unittest2 as unittest
except ImportError:
    import unittest
    
import tornado.escape

from flexmock import flexmock

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods
from iceprod.server.modules.db import DBAPI

class DB():
    cfg = {}
    tables = DBAPI.tables
    def sql_read_task(*args):
        raise Exception('read task called')
    def sql_write_task(*args):
        raise Exception('write task called')
    def blocking_task(*args):
        raise Exception('blocking task called')
    def non_blocking_task(*args):
        raise Exception('non-blocking task called')
    def increment_id(*args):
        raise Exception('increment_id called')
    def _increment_id_helper(*args):
        raise Exception('_increment_id_helper called')
    def _dbsetup(*args):
        return (None,None)
    def _db_read(*args):
        raise Exception('_db_read called')
    def _db_write(*args):
        raise Exception('_db_write called')

class dbmethods_base(unittest.TestCase):
    def setUp(self):
        super(dbmethods_base,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        
        # get hostname
        hostname = functions.gethostname()
        if hostname is None:
            hostname = 'localhost'
        elif isinstance(hostname,set):
            hostname = hostname.pop()
        self.hostname = hostname
        
        # mock DB
        self._db = dbmethods.DBMethods(DB())
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(dbmethods_base,self).tearDown()

class dbmethods_test(dbmethods_base):
    def test_001_filtered_input(self):
        """Test filtered_input"""
        try:
            # try some strings
            strings = {'select * from test':'select * from test',
                       'insert into test; drop table test':'insert into test drop table test',
                       'delete from test "\'"':'delete from test ',
                      }
            
            for s in strings:
                correct = strings[s]
                ret = dbmethods.filtered_input(s)
                if ret != correct:
                    raise Exception('got %r but should be %r'%(ret,correct))
            
        except Exception as e:
            logger.error('Error running dbmethods filtered_input test - %s',str(e))
            printer('Test dbmethods filtered_input',False)
            raise
        else:
            printer('Test dbmethods filtered_input')

    def test_002_datetime2str(self):
        """Test datetime2str"""
        try:
            tests = {datetime(2012,11,4,12,20,24):'2012-11-04T12:20:24',
                     datetime(1283,10,29,18,39,20):'1283-10-29T18:39:20',
                     datetime(3920,8,12,1,1,2):'3920-08-12T01:01:02',
                     datetime(12,3,14,23,12,59):'0012-03-14T23:12:59',
                    }
            
            for t in tests:
                correct = tests[t]
                ret = dbmethods.datetime2str(t)
                if ret != correct:
                    raise Exception('got %r but should be %r'%(ret,correct))
            
        except Exception as e:
            logger.error('Error running dbmethods datetime2str test - %s',str(e))
            printer('Test dbmethods datetime2str',False)
            raise
        else:
            printer('Test dbmethods datetime2str')

    def test_003_str2datetime(self):
        """Test str2datetime"""
        try:
            tests = {'2012-11-04T12:20:24':datetime(2012,11,4,12,20,24),
                     '1283-10-29T18:39:20':datetime(1283,10,29,18,39,20),
                     '3920-08-12T01:01:02':datetime(3920,8,12,1,1,2),
                     '0012-03-14T23:12:59':datetime(12,3,14,23,12,59),
                     '1234-05-12T13:02:19.19283':datetime(1234,5,12,13,2,19,192830),
                    }
            
            for t in tests:
                correct = tests[t]
                ret = dbmethods.str2datetime(t)
                if ret != correct:
                    raise Exception('got %r but should be %r'%(ret,correct))
            
        except Exception as e:
            logger.error('Error running dbmethods str2datetime test - %s',str(e))
            printer('Test dbmethods str2datetime',False)
            raise
        else:
            printer('Test dbmethods str2datetime')

    def test_004_list_to_dict(self):
        """Test list_to_dict"""
        # special note: use DB.subclasses[0] because this is an _method
        try:
            alltables = {t:OrderedDict([(x,i) for i,x in enumerate(DB.tables[t])]) for t in DB.tables}
            
            # test all tables individually
            for t in alltables:
                ret = self._db.subclasses[0]._list_to_dict(t,alltables[t].values())
                if ret != alltables[t]:
                    raise Exception('got %r but should be %r'%(ret,alltables[t]))
            
            # test some multiples
            groupkeys = []
            groupvalues = []
            groupans = {}
            nleft = 0
            for t in alltables:
                if not nleft:
                    if groupkeys:
                        ret = self._db.subclasses[0]._list_to_dict(groupkeys,groupvalues)
                        if ret != groupans:
                            raise Exception('got %r but should be %r'%(ret,groupans))
                    nleft = random.randint(1,10)
                groupkeys.append(t)
                groupvalues.extend(alltables[t].values())
                groupans.update(alltables[t])
            if groupkeys:
                ret = self._db.subclasses[0]._list_to_dict(groupkeys,groupvalues)
                if ret != groupans:
                    raise Exception('got %r but should be %r'%(ret,groupans))
            
        except Exception as e:
            logger.error('Error running dbmethods list_to_dict test - %s',str(e))
            printer('Test dbmethods list_to_dict',False)
            raise
        else:
            printer('Test dbmethods list_to_dict')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_test))
    return suite
