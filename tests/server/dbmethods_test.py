"""
Test script for dbmethods
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, services_mock

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
import unittest
try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock

from tornado.concurrent import Future
import tornado.escape
import tornado.gen
from tornado.ioloop import IOLoop
from tornado.testing import AsyncTestCase, gen_test


from iceprod.functools_future import partialmethod

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods
from iceprod.server.modules.db import db, SQLite


class Base:
    """Fake dbmethods"""
    def __init__(self):
        self.calls = {}
        class Parent:
            def __init__(self, calls):
                self.calls = calls
            @tornado.gen.coroutine
            def db_call(self, name, *args, **kwargs):
                logger.debug('db call for %r',name)
                if name in self.calls:
                    raise tornado.gen.Return(self.calls[name](*args,**kwargs))
                else:
                    raise Exception('method not found')
        self.parent = Parent(self.calls)

class decorators_test(AsyncTestCase):
    def setUp(self):
        super(decorators_test,self).setUp()
        self.maxDiff = 10000

    @unittest_reporter(name='memcache - standalone')
    def test_001_memcache(self):
        @dbmethods.memcache()
        def foo(x):
            return x

        ret = yield foo(1)
        self.assertEqual(ret,1)
        self.assertEqual(foo.cache_info().hits, 0)
        self.assertEqual(foo.cache_info().misses, 1)
        self.assertEqual(foo.cache_info().currsize, 1)

        ret = yield foo(1)
        self.assertEqual(ret,1)
        self.assertEqual(foo.cache_info().hits, 1)
        self.assertEqual(foo.cache_info().misses, 1)
        self.assertEqual(foo.cache_info().currsize, 1)

        ret = yield foo(2)
        self.assertEqual(ret,2)
        self.assertEqual(foo.cache_info().hits, 1)
        self.assertEqual(foo.cache_info().misses, 2)
        self.assertEqual(foo.cache_info().currsize, 2)

    @unittest_reporter(name='memcache - member func')
    def test_002_memcache(self):
        class Bar:
            @dbmethods.memcache()
            def foo(self, x):
                return x

        b = Bar()

        ret = yield b.foo(1)
        self.assertEqual(ret,1)
        self.assertEqual(b.foo.cache_info().hits, 0)
        self.assertEqual(b.foo.cache_info().misses, 1)
        self.assertEqual(b.foo.cache_info().currsize, 1)

        ret = yield b.foo(1)
        self.assertEqual(ret,1)
        self.assertEqual(b.foo.cache_info().hits, 1)
        self.assertEqual(b.foo.cache_info().misses, 1)
        self.assertEqual(b.foo.cache_info().currsize, 1)

        ret = yield b.foo(2)
        self.assertEqual(ret,2)
        self.assertEqual(b.foo.cache_info().hits, 1)
        self.assertEqual(b.foo.cache_info().misses, 2)
        self.assertEqual(b.foo.cache_info().currsize, 2)

    @unittest_reporter(name='memcache - maxsize')
    def test_003_memcache(self):
        @dbmethods.memcache(size=2)
        def foo(x):
            return x

        ret = yield foo(1)
        self.assertEqual(ret,1)

        ret = yield foo(2)
        self.assertEqual(ret,2)

        ret = yield foo(3)
        self.assertEqual(ret,3)

        hits = foo.cache_info().hits
        ret = yield foo(1)
        self.assertEqual(ret,1)
        self.assertEqual(foo.cache_info().hits, hits)

    @unittest_reporter(name='memcache - clear')
    def test_003_memcache(self):
        @dbmethods.memcache()
        def foo(x):
            return x

        ret = yield foo(1)
        self.assertEqual(ret,1)

        foo.cache_clear()
        ret = yield foo(1)
        self.assertEqual(ret,1)
        self.assertEqual(foo.cache_info().hits, 0)

    @unittest_reporter(name='memcache - generator function')
    def test_004_memcache(self):
        @dbmethods.memcache()
        @tornado.gen.coroutine
        def foo(x):
            raise tornado.gen.Return(x)

        ret = yield foo(1)
        self.assertEqual(ret,1)

        ret = yield foo(1)
        self.assertEqual(ret,1)
        self.assertEqual(foo.cache_info().hits, 1)

    @unittest_reporter(name='authorization - no auth')
    def test_100_authorization(self):
        class Bar(Base):
            @dbmethods.authorization()
            def foo(self, x):
                return x
            @dbmethods.authorization(user=True)
            def baz(self, x):
                return x

        b = Bar()
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1)

        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.baz(1)

    @unittest_reporter(name='authorization - any user')
    def test_101_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(user=True)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'foo','roles':['bar']})

        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)
        
        b.calls['auth_get_passkey_membership'] = Mock(side_effect=Exception())
        with self.assertRaises(Exception):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - specific user')
    def test_102_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(user='foo')
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'foo','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(side_effect=Exception())
        with self.assertRaises(Exception):
            yield b.foo(1, passkey='blah')
        
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - user list')
    def test_103_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(user=['foo','bar'])
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)
        
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - user func')
    def test_104_authorization(self):
        class Bar(Base):
            def helper(self, user_id, x):
                logger.debug('helper %s %s', user_id, x)
                return user_id == 'bar'
            @dbmethods.authorization(user=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - user func yield')
    def test_105_authorization(self):
        class Bar(Base):
            @tornado.gen.coroutine
            def helper(self, user_id, x):
                logger.debug('helper %s %s', user_id, x)
                raise tornado.gen.Return(user_id == 'bar')
            @dbmethods.authorization(user=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - user func nonmember')
    def test_106_authorization(self):
        def helper(user_id, x):
            logger.debug('helper %s %s', user_id, x)
            return user_id == 'bar'
        class Bar(Base):
            @dbmethods.authorization(user=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - user func error')
    def test_107_authorization(self):
        def helper(user_id, x):
            logger.debug('helper %s %s', user_id, x)
            raise Exception('func error')
        class Bar(Base):
            @dbmethods.authorization(user=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - specific role')
    def test_112_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(role='bar')
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'foo','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(side_effect=Exception())
        with self.assertRaises(Exception):
            yield b.foo(1, passkey='blah')

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'foo','roles':['baz']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - role list')
    def test_113_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(role=['foo','bar'])
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)
        
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['baz']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - role func')
    def test_114_authorization(self):
        class Bar(Base):
            def helper(self, roles, x):
                logger.debug('helper %r %s', roles, x)
                return 'bar' in roles
            @dbmethods.authorization(role=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['baz']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - role func yield')
    def test_115_authorization(self):
        class Bar(Base):
            @tornado.gen.coroutine
            def helper(self, roles, x):
                logger.debug('helper %r %s', roles, x)
                raise tornado.gen.Return('bar' in roles)
            @dbmethods.authorization(role=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['baz']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - role func nonmember')
    def test_116_authorization(self):
        def helper(roles, x):
            logger.debug('helper %r %s', roles, x)
            return 'bar' in roles
        class Bar(Base):
            @dbmethods.authorization(role=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['baz']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - role func error')
    def test_117_authorization(self):
        def helper(roles, x):
            logger.debug('helper %r %s', roles, x)
            raise Exception('func error')
        class Bar(Base):
            @dbmethods.authorization(role=helper)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - cookie user')
    def test_120_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(user='foo')
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_user_membership'] = Mock(return_value={'username':'foo','roles':['bar']})
        ret = yield b.foo(1, cookie_id='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_user_membership'] = Mock(side_effect=Exception())
        with self.assertRaises(Exception):
            yield b.foo(1, cookie_id='blah')

        b.calls['auth_get_user_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, cookie_id='blah')

    @unittest_reporter(name='authorization - site')
    def test_130_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(site=True)
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_authorize_site'] = Mock(return_value=True)
        ret = yield b.foo(1, passkey='blah', site_id='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_authorize_site'] = Mock(side_effect=Exception())
        with self.assertRaises(Exception):
            yield b.foo(1, passkey='blah', site_id='blah')

        b.calls['auth_authorize_site'] = Mock(return_value=False)
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah', site_id='blah')

    @unittest_reporter(name='authorization - expression')
    def test_140_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(user='bar',role='bar',expression='user or role')
            def foo(self, x):
                return x

        b = Bar()
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'foo','roles':['bar']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)
        
        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'bar','roles':['foo']})
        ret = yield b.foo(1, passkey='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_passkey_membership'] = Mock(side_effect=Exception())
        with self.assertRaises(Exception):
            yield b.foo(1, passkey='blah')

        b.calls['auth_get_passkey_membership'] = Mock(return_value={'username':'foo','roles':['baz']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, passkey='blah')

    @unittest_reporter(name='authorization - generator function')
    def test_150_authorization(self):
        class Bar(Base):
            @dbmethods.authorization(user='foo')
            @tornado.gen.coroutine
            def foo(self, x):
                raise tornado.gen.Return(x)

        b = Bar()
        b.calls['auth_get_user_membership'] = Mock(return_value={'username':'foo','roles':['bar']})
        ret = yield b.foo(1, cookie_id='blah')
        self.assertEqual(ret, 1)

        b.calls['auth_get_user_membership'] = Mock(side_effect=Exception())
        with self.assertRaises(Exception):
            yield b.foo(1, cookie_id='blah')

        b.calls['auth_get_user_membership'] = Mock(return_value={'username':'baz','roles':['bar']})
        with self.assertRaisesRegexp(Exception, 'authorization error'):
            yield b.foo(1, cookie_id='blah')

class TestExecutor(object):
    def __init__(self, *args, **kwargs):
        pass
    def submit(self, fn, *args, **kwargs):
        f = Future()
        f.set_result(fn(*args, **kwargs))
        return f
    def map(self, fn, *iterables, **kwargs):
        for i in iterables:
            yield self.submit(fn, i)
    def shutdown(self, wait=True):
        pass
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        pass

class TestDB(SQLite):
    def __init__(self, *args, **kwargs):
        super(TestDB, self).__init__(*args, **kwargs)
        self.failures = None
        self.calls = []
    @tornado.gen.coroutine
    def query(self, sql, bindings=None):
        n_call = len(self.calls)
        self.calls.append((sql,bindings))
        if self.failures:
            if isinstance(self.failures, Exception):
                raise self.failures
            elif isinstance(self.failures, Iterable):
                logger.info('ncall: %d', n_call)
                try:
                    f = self.failures[n_call]
                except IndexError:
                    pass
                else:
                    if isinstance(f, Exception):
                        raise f
                    elif f:
                        raise Exception('QueryError')
            else:
                raise Exception('QueryError')
        ret = yield super(TestDB, self).query(sql, bindings)
        raise tornado.gen.Return(ret)

class dbmethods_base(AsyncTestCase):
    def setUp(self):
        super(dbmethods_base,self).setUp()
        self.maxDiff = 10000

        try:
            orig_dir = os.getcwd()
            self.test_dir = tempfile.mkdtemp(dir=orig_dir)
            os.chdir(self.test_dir)
            def clean_dir():
                os.chdir(orig_dir)
                shutil.rmtree(self.test_dir)
            self.addCleanup(clean_dir)

            # get hostname
            hostname = functions.gethostname()
            if hostname is None:
                hostname = 'localhost'
            self.hostname = hostname

            # set config
            self.cfg = {'db':{'type':'sqlite',
                              'name':'test',
                              'name_setting':'test_setting',
                              'nthreads':1},
                        'site_id':'abcd',
                       }

            # mock module communication
            self.services = services_mock()
            self.services.ret['daemon']['stop'] = True
            self.services.ret['master_updater']['add'] = None

            # mock DB
            self.mock = db(self.cfg, self.io_loop, TestExecutor(), self.services)
            self.mock.db = TestDB(self.mock)
            self.addCleanup(self.mock.stop)
            self.db = self.mock.service
        except:
            logger.warn('error setting up dbmethods', exc_info=True)
            raise

    @tornado.gen.coroutine
    def set_tables(self, tables):
        """
        Set all table entries.

        Args:
            tables (dict): table entries
        """
        for t in tables:
            if t == 'setting':
                continue
            yield self.mock.db.query('delete from %s'%t)
            for row in tables[t]:
                sql = ('insert into %s ('%t)+','.join('"'+k+'"' for k in row.keys())
                sql += ') values ('+','.join('?' for _ in row)+')'
                bindings = row.values()
                yield self.mock.db.query(sql, bindings)

    def set_failures(self, failures=None):
        """
        Set expected DB query failures.

        If given a list, it will match each subsequent call with a list entry.

        Args:
            failures (list): Boolean, Exception, or list of those.
        """
        self.mock.db.failures = failures
        self.mock.db.calls = []

    @tornado.gen.coroutine
    def get_tables(self, tables):
        """
        Get all entries in tables.

        Args:
            tables (list): list of table names

        Returns:
            dict: table entries
        """
        output = {}
        cl = dbmethods._Methods_Base(self.mock)
        for t in tables:
            output[t] = []
            ret = yield self.mock.db.query('select * from %s'%t)
            for row in ret:
                output[t].append(cl._list_to_dict(t,row))
        raise tornado.gen.Return(output)

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
            self.assertEqual(ret, correct)

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
            self.assertEqual(ret, correct)

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
            self.assertEqual(ret, correct)

    @unittest_reporter
    def test_004_list_to_dict(self):
        """Test list_to_dict"""
        alltables = {t:OrderedDict([(x,i) for i,x in enumerate(self.mock.db.tables[t])]) for t in self.mock.db.tables}

        cl = dbmethods._Methods_Base(self.mock)

        # test all tables individually
        for t in alltables:
            ret = cl._list_to_dict(t,alltables[t].values())
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
                    ret = cl._list_to_dict(groupkeys,groupvalues)
                    if ret != groupans:
                        raise Exception('got %r but should be %r'%(ret,groupans))
                nleft = random.randint(1,10)
            groupkeys.append(t)
            groupvalues.extend(alltables[t].values())
            groupans.update(alltables[t])
        if groupkeys:
            ret = cl._list_to_dict(groupkeys,groupvalues)
            if ret != groupans:
                raise Exception('got %r but should be %r'%(ret,groupans))

    @unittest_reporter
    def test_010_send_to_master(self):
        """Test send_to_master"""

        cl = dbmethods._Methods_Base(self.mock)
        
        arg = {'table':['sql1','sql2']}
        self.services.ret['master_updater']['add'] = None
        yield cl._send_to_master(arg)
        logger.info('m args: %r', self.services.called)
        if not self.services.called:
            raise Exception('did not call messaging to master')
        if self.services.called[0][0] != 'master_updater':
            raise Exception('did not call master_updater')
        if self.services.called[0][1] != 'add':
            raise Exception('did not call master_updater.add')
        if ((not self.services.called[0][2]) or
            self.services.called[0][2][0] != arg):
            raise Exception('did not call with arg')

        self.services.ret['master_updater']['add'] = Exception()
        yield cl._send_to_master(arg)
        # it should not raise an exception

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(decorators_test))
    suite.addTests(loader.loadTestsFromNames(alltests,decorators_test))
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_test))
    return suite
