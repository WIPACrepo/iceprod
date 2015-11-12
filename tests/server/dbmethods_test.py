"""
Test script for dbmethods
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, messaging_mock

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
from tornado.testing import AsyncTestCase

from flexmock import flexmock

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods
from iceprod.server.modules.db import DBAPI, SQLite

class FakeThreadPool:
    """
    Fakes a threadpool interface and runs immediately.
    """
    def __init__(self,init=None,named=False):
        self.init = init
        self.named = named
    def start(self,*args,**kwargs):
        pass
    def finish(self,*args,**kwargs):
        pass
    def disable_output_queue(self,*args,**kwargs):
        pass
    def add_task(self,func,*args,**kwargs):
        cb = None
        if self.named:
            func = args[0]
        if 'callback' in kwargs:
            cb = kwargs.pop('callback')
        if self.init:
            kwargs['init'] = self.init()
        try:
            ret = func(*args,**kwargs)
        except Exception as e:
            ret = e
        if cb:
            cb(ret)

class DB(SQLite):
    """
    A fake :class:`iceprod.server.db.SQLite` object.
    """
    def __init__(self,cfg=None,messaging=None,**kwargs):
        if not cfg:
            cfg = {}
        self.tmpdir = tempfile.mkdtemp()
        self.queries = []
        self.failures = None
        cfg['db'] = {}
        cfg['db']['name'] = os.path.join(self.tmpdir,'db')
        super(DB,self).__init__(cfg,messaging,**kwargs)

    def setup(self, tables={}):
        for t in tables:
            conn,archive_conn = self._dbsetup()
            super(DB,self)._db_write(conn,'delete from %s'%t,tuple(),None,None,None)
            for row in tables[t]:
                sql = ('insert into %s ('%t)+','.join('"'+k+'"' for k in row.keys())
                sql += ') values ('+','.join('?' for _ in row)+')'
                bindings = row.values()
                super(DB,self)._db_write(conn,sql,bindings,None,None,None)
        self.queries = []
        self.failures = None

    def get(self, tables=None):
        output = {}
        class A:
            def __init__(s2):
                s2.db = self
        base = dbmethods._Methods_Base(A())
        if tables:
            conn,archive_conn = self._dbsetup()
            for t in tables:
                output[t] = []
                sql = 'select * from %s'%t
                ret = super(DB,self)._db_read(conn,sql,tuple(),None,None,None)
                for row in ret:
                    output[t].append(base._list_to_dict(t,row))
        return output

    def get_trace(self):
        return queries

    # overrides below

    def start(self):
        # start fake thread pools
        self.write_pool = FakeThreadPool(init=self._dbsetup)
        self.read_pool = FakeThreadPool(init=self._dbsetup)
        self.blocking_pool = FakeThreadPool(named=True)
        self.non_blocking_pool = FakeThreadPool()
        logger.debug('started threadpools')

    def stop(self):
        super(DB,self).stop()
        shutil.rmtree(self.tmpdir)

    def _db_read(self,conn,sql,bindings,archive_conn,archive_sql,archive_bindings):
        self.queries.append((sql,bindings))
        if (self.failures is True or len(self.queries) == self.failures
            or (isinstance(self.failures,dict) and len(self.queries) in self.failures)):
            logger.warn('injected SQLError')
            raise Exception('SQLError')
        return super(DB,self)._db_read(conn,sql,bindings,archive_conn,archive_sql,archive_bindings)

    def _db_write(self,conn,sql,bindings,archive_conn,archive_sql,archive_bindings):
        self.queries.append((sql,bindings))
        if (self.failures is True or len(self.queries) == self.failures
            or (isinstance(self.failures,dict) and len(self.queries) in self.failures)):
            logger.warn('injected SQLError')
            raise Exception('SQLError')
        return super(DB,self)._db_write(conn,sql,bindings,archive_conn,archive_sql,archive_bindings)


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
        self.mock = DB()
        self.mock.start()
        self._db = dbmethods.DBMethods(self.mock)

    def tearDown(self):
        self.mock.stop()
        shutil.rmtree(self.test_dir)
        super(dbmethods_base,self).tearDown()

class decorator_test(AsyncTestCase):
    @unittest_reporter(name=' decorator')
    def test_000_decorator(self):
        """Test decorator"""
        def cb(*args,**kwargs):
            cb.called = True
            cb.args = args
            cb.kwargs = kwargs
            self.stop()

        # test callback and default timeout
        @dbmethods.dbmethod
        def test(callback=None):
            callback()
        cb.called = False
        test(callback=cb)
        self.wait(timeout=0.1)
        if not cb.called:
            raise Exception('callback not called')

        # test kwarg in decorator timeout
        @dbmethods.dbmethod(timeout=0.1)
        def test2(callback=None):
            pass
        cb.called = False
        test2(callback=cb)
        self.wait(timeout=0.2)
        if not cb.called:
            raise Exception('callback not called')

        # test kwarg in function timeout
        @dbmethods.dbmethod
        def test3(timeout=None,callback=None):
            pass
        cb.called = False
        test3(timeout=0.1,callback=cb)
        self.wait(timeout=0.2)
        if not cb.called:
            raise Exception('callback not called')

        # test callback firing after timeout
        @dbmethods.dbmethod(timeout=0.1)
        def test4(callback=None):
            time.sleep(0.2)
            logger.info('calling delayed callback')
            callback()
        cb.called = False
        test4(callback=cb)
        self.wait(timeout=0.15)
        if not cb.called:
            raise Exception('callback not called')
        cb.called = False
        try:
            self.wait(timeout=0.2)
        except Exception:
            pass
        if cb.called:
            raise Exception('callback called twice')

        # test timeout firing after callback
        @dbmethods.dbmethod(timeout=0.1)
        def test5(callback=None):
            callback()
        cb.called = False
        test5(callback=cb)
        self.wait(timeout=0.05)
        if not cb.called:
            raise Exception('callback not called')
        cb.called = False
        try:
            self.wait(timeout=0.15)
        except Exception:
            pass
        if cb.called:
            raise Exception('callback called twice')

        # test non-timeout branch
        @dbmethods.dbmethod(timeout=0.05)
        def test6(callback=None):
            pass
        cb.called = False
        test6()
        try:
            self.wait(timeout=0.1)
        except Exception:
            pass
        if cb.called:
            raise Exception('callback called unexpectedly')

    @unittest_reporter(name=' decorator with class method')
    def test_001_decorator(self):
        """Test decorator"""
        def cb(*args,**kwargs):
            cb.called = True
            cb.args = args
            cb.kwargs = kwargs
            self.stop()

        # test callback and default timeout
        class test_class:
            @dbmethods.dbmethod
            def test(self,callback=None):
                callback()
        cb.called = False
        a = test_class()
        a.test(callback=cb)
        self.wait(timeout=0.1)
        if not cb.called:
            raise Exception('callback not called')

class dbmethods_test(dbmethods_base):
    @unittest_reporter
    def test_001_filtered_input(self):
        """Test filtered_input"""
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

    @unittest_reporter
    def test_002_datetime2str(self):
        """Test datetime2str"""
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

    @unittest_reporter
    def test_003_str2datetime(self):
        """Test str2datetime"""
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

    @unittest_reporter
    def test_004_list_to_dict(self):
        """Test list_to_dict"""
        # special note: use DB.subclasses[0] because this is an _method
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

    @unittest_reporter
    def test_010_send_to_master(self):
        """Test send_to_master"""
        m = messaging_mock()
        self._db.db.messaging = m

        def cb(ret=None):
            cb.ret = ret
        cb.ret = None

        # special note: use DB.subclasses[0] to get the first subclass,
        #               which will have access to _Methods_Base methods
        arg = {'table':['sql1','sql2']}
        m.ret = {'master_updater':{'add':True}}
        self._db.subclasses[0]._send_to_master(arg,callback=cb)
        if cb.ret is not True:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return True')
        logger.info('m args: %r',m.called)
        if not m.called:
            raise Exception('did not call messaging to master')
        if m.called[0][0] != 'master_updater':
            raise Exception('did not call master_updater')
        if m.called[0][1] != 'add':
            raise Exception('did not call master_updater.add')
        if m.called[0][2][0] != arg:
            raise Exception('did not call with arg')

        m.ret = {'master_updater':{'add':False}}
        self._db.subclasses[0]._send_to_master(arg,callback=cb)
        if cb.ret is not False:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return False')

        m.ret = {'master_updater':{'add':Exception('test')}}
        self._db.subclasses[0]._send_to_master(arg,callback=cb)
        if cb.ret != m.ret['master_updater']['add']:
            logger.info('ret: %r',cb.ret)
            raise Exception('did not return Exception')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(decorator_test))
    suite.addTests(loader.loadTestsFromNames(alltests,decorator_test))
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_test))
    return suite
