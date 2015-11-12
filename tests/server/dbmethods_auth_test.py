"""
Test script for dbmethods.auth
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

from .dbmethods_test import dbmethods_base,DB

class dbmethods_auth_test(dbmethods_base):
    @unittest_reporter
    def test_010_auth_get_site_auth(self):
        """Test auth_get_site_auth"""
        raise Exception('fixme')
        data = {'site_id':1,'auth_key':'key'}

        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # normal site test
        sql_read_task.ret = [[data['site_id'],data['auth_key']]]

        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('normal site: callback not called')
        if cb.ret != data:
            raise Exception('normal site: callback ret != data')

        # site not in db
        sql_read_task.ret = []
        cb.called = False

        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback did not receive exception')

        # site in db twice
        sql_read_task.ret = [[1,2],[3,4]]
        cb.called = False

        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('in db twice: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('in db twice: callback did not receive exception')

        # bad db info
        sql_read_task.ret = [[data['site_id']]]
        cb.called = False

        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('bad db info: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('bad db info: callback did not receive exception')

        # sql error
        sql_read_task.ret = Exception('sql error')
        cb.called = False

        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_011_auth_authorize_site(self):
        """Test auth_authorize_site"""
        raise Exception('fixme')
        data = {'site_id':1,'auth_key':'key'}

        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # normal site
        sql_read_task.ret = [[data['site_id'],data['auth_key']]]

        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('normal site: callback not called')
        if cb.ret is not True:
            raise Exception('normal site: callback ret != True')

        # site not in db
        sql_read_task.ret = []
        cb.called = False

        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback did not receive exception')

        # site in db twice
        sql_read_task.ret = [[1,2],[3,4]]
        cb.called = False

        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('in db twice: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('in db twice: callback did not receive exception')

        # bad db info
        sql_read_task.ret = [[data['site_id']]]
        cb.called = False

        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('bad db info: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('bad db info: callback did not receive exception')

        # sql error
        sql_read_task.ret = Exception('sql error')
        cb.called = False

        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_012_auth_authorize_task(self):
        """Test auth_authorize_task"""
        raise Exception('fixme')
        data = {'passkey':1,'expire':'2100-01-01T01:01:01'}

        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            callback(sql_read_task.ret)
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # normal task
        sql_read_task.ret = [[data['passkey'],data['expire']]]

        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        if cb.ret is not True:
            raise Exception('normal task: callback ret != True')

        # site not in db
        sql_read_task.ret = []
        cb.called = False

        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback did not receive exception')

        # site in db twice
        sql_read_task.ret = [[1,2],[3,4]]
        cb.called = False

        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('in db twice: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('in db twice: callback did not receive exception')

        # bad db info
        sql_read_task.ret = [[data['passkey']]]
        cb.called = False

        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('bad db info: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('bad db info: callback did not receive exception')

        # sql error
        sql_read_task.ret = Exception('sql error')
        cb.called = False

        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_400_auth_new_passkey(self):
        """Test auth_new_passkey"""
        raise Exception('fixme')
        def sql_write_task(sql,bindings,callback):
            sql_write_task.sql = sql
            sql_write_task.bindings = bindings
            if bindings[0] in sql_write_task.task_ret:
                callback(sql_write_task.task_ret[bindings[0]])
            else:
                callback(Exception('sql error'))
        def increment_id(table,conn=None):
            increment_id.table = table
            if table in increment_id.ret:
                return increment_id.ret[table]
            else:
                raise Exception('sql error')
        flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)
        flexmock(DB).should_receive('increment_id').replace_with(increment_id)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # everything working
        newid = 'theid'
        cb.called = False
        increment_id.ret = {'passkey':newid}
        sql_write_task.task_ret = {newid:{}}

        self._db.auth_new_passkey(callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if newid not in sql_write_task.bindings:
            raise Exception('expiration: newid not in sql')

        # expiration
        newid = 'theid'
        exp = datetime.utcnow()+timedelta(seconds=10)
        cb.called = False
        increment_id.ret = {'passkey':newid}
        sql_write_task.task_ret = {newid:{}}

        self._db.auth_new_passkey(exp,callback=cb)

        if cb.called is False:
            raise Exception('expiration: callback not called')
        if newid not in sql_write_task.bindings:
            raise Exception('expiration: newid not in sql')
        if dbmethods.datetime2str(exp) not in sql_write_task.bindings:
            raise Exception('expiration: expiration not in sql')

        # expiration2
        newid = 'theid'
        exp = 10
        cb.called = False
        increment_id.ret = {'passkey':newid}
        sql_write_task.task_ret = {newid:{}}

        self._db.auth_new_passkey(exp,callback=cb)

        if cb.called is False:
            raise Exception('expiration2: callback not called')
        if newid not in sql_write_task.bindings:
            raise Exception('expiration2: newid not in sql')

        # bad expiration
        newid = 'theid'
        exp = 'theexp'
        cb.called = False
        increment_id.ret = {'passkey':newid}
        sql_write_task.task_ret = {newid:{}}

        try:
            self._db.auth_new_passkey(exp,callback=cb)
        except:
            pass
        else:
            raise Exception('bad expiration: did not raise Exception')

        # sql_write_task error
        newid = 'theid'
        exp = 10
        cb.called = False
        increment_id.ret = {'passkey':newid}
        sql_write_task.task_ret = {}

        self._db.auth_new_passkey(exp,callback=cb)

        if cb.called is False:
            raise Exception('sql_write_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_write_task error: callback not called')

        # increment_id error
        newid = 'theid'
        exp = 10
        cb.called = False
        increment_id.ret = {}
        sql_write_task.task_ret = {}

        try:
            self._db.auth_new_passkey(exp,callback=cb)
        except:
            pass
        else:
            raise Exception('increment_id error: did not raise Exception')

    @unittest_reporter
    def test_401_auth_get_passkey(self):
        """Test auth_get_passkey"""
        raise Exception('fixme')
        def sql_read_task(sql,bindings,callback):
            sql_read_task.sql = sql
            sql_read_task.bindings = bindings
            if bindings[0] in sql_read_task.task_ret:
                callback(sql_read_task.task_ret[bindings[0]])
            else:
                callback(Exception('sql error'))
        flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # everything working
        key = 'thekey'
        exp = datetime.utcnow()
        cb.called = False
        sql_read_task.task_ret = {key:[['theid',key,dbmethods.datetime2str(exp)]]}

        self._db.auth_get_passkey(key,callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')
        if cb.ret != exp:
            logger.error('cb.ret = %r',exp)
            raise Exception('everything working: callback ret != expiration')

        # passkey error
        key = None
        exp = 'expiration'
        cb.called = False
        sql_read_task.task_ret = {key:[['theid',key,exp]]}

        try:
            self._db.auth_get_passkey(key,callback=cb)
        except:
            pass
        else:
            raise Exception('passkey error: did not raise Exception')

        # sql_read_task error
        key = 'thekey'
        exp = 'expiration'
        cb.called = False
        sql_read_task.task_ret = {}

        self._db.auth_get_passkey(key,callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error: callback ret != Exception')

        # sql_read_task error2
        key = 'thekey'
        exp = 'expiration'
        cb.called = False
        sql_read_task.task_ret = {key:[]}

        self._db.auth_get_passkey(key,callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error2: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error2: callback ret != Exception')

        # sql_read_task error3
        key = 'thekey'
        exp = 'expiration'
        cb.called = False
        sql_read_task.task_ret = {key:[['id','key']]}

        self._db.auth_get_passkey(key,callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error3: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error3: callback ret != Exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_auth_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_auth_test))
    return suite
