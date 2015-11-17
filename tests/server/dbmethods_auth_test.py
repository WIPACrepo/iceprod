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
        data = {'site_id':1,'auth_key':'key'}


        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
                {'site_id':2,'auth_key':'key'},
                {'site_id':3,'auth_key':'key'},
            ],
            'setting':[
                {'site_id':1},
            ],
        }

        # normal site test
        cb.called = False
        self.mock.setup(tables)
        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('normal site: callback not called')
        if cb.ret != data:
            raise Exception('normal site: callback ret != data')

        # site not in db
        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
                {'site_id':2,'auth_key':'key'},
                {'site_id':3,'auth_key':'key'},
            ],
            'setting':[
                {'site_id':4},
            ],
        }

        cb.called = False
        self.mock.setup(tables)
        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback did not receive exception')


        # site in db twice
        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
                {'site_id':2,'auth_key':'key'},
                {'site_id':3,'auth_key':'key'},
            ],
            'setting':[
                {'site_id':1},
                {'site_id':2},
            ],
        }
        self.mock.setup(tables)
        cb.called = False
        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('in db twice: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('in db twice: callback did not receive exception')

        # bad db info
        tables['site'] = [{'site_id':1}]
        self.mock.setup(tables)

        cb.called = False
        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('bad db info: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('bad db info: callback did not receive exception')

        # sql error
        cb.called = False
        self.mock.failures = True
        self._db.auth_get_site_auth(callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_011_auth_authorize_site(self):
        """Test auth_authorize_site"""
        data = {'site_id':1,'auth_key':'key'}
        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
            ],
            'setting':[
                {'site_id':1},
            ],
            'passkey':[
                {'passkey_id':1,'expire':'2100-01-01T01:01:01'}
            ],
        }

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # normal site
        self.mock.setup(tables)
        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('normal site: callback not called')
        if cb.ret is not True:
            raise Exception('normal site: callback ret != True')

        # site not in db
        cb.called = False
        self._db.auth_authorize_site(2,'key',callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback did not receive exception')

        # bad db info
        tables['site'] = [{'site_id':1}]
        self.mock.setup(tables)
        cb.called = False

        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('bad db info: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('bad db info: callback did not receive exception')

        # sql error
        self.mock.failures = True
        cb.called = False

        self._db.auth_authorize_site(1,'key',callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_012_auth_authorize_task(self):
        """Test auth_authorize_task"""
        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
            ],
            'setting':[
                {'site_id':1},
            ],
            'passkey':[
                {'key':1,'expire':'2100-01-01T01:01:01'}
            ],
        }

        self.mock.setup(tables)

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # normal task
        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('normal task: callback not called')
        if cb.ret is not True:
            raise Exception('normal task: callback ret != True')

        # site not in db
        cb.called = False

        self._db.auth_authorize_task(2,callback=cb)

        if cb.called is False:
            raise Exception('not in db: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('not in db: callback did not receive exception')


        # bad db info
        tables['passkey'] = [{'key':1}]
        self.mock.setup(tables)
        cb.called = False

        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('bad db info: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('bad db info: callback did not receive exception')

        # sql error
        self.mock.failures = True
        cb.called = False

        self._db.auth_authorize_task(1,callback=cb)

        if cb.called is False:
            raise Exception('sql error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql error: callback ret != Exception')

    @unittest_reporter
    def test_400_auth_new_passkey(self):
        """Test auth_new_passkey"""
        #raise Exception('fixme')

        exp = datetime.utcnow()
        key = 'thekey'
        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
            ],
            'setting':[
                {'site_id':1},
            ],
            'passkey':[
                {'key':key,'expire':dbmethods.datetime2str(exp)}
            ],
        }

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False

        # everything working
        cb.called = False

        self._db.auth_new_passkey(callback=cb)

        if cb.called is False:
            raise Exception('everything working: callback not called')

        # Now check if the entry is there
        cb.called = False
        self._db.auth_authorize_task(cb.ret,callback=cb)
        if cb.called is False:
            raise Exception('everything working/check id: callback not called')
        if isinstance(cb.ret,Exception):
            raise Exception('everything working/check id: newid not in database')

        # expiration
        exp = datetime.utcnow()+timedelta(seconds=10)
        cb.called = False

        self._db.auth_new_passkey(exp,callback=cb)

        if cb.called is False:
            raise Exception('expiration: callback not called')

        cb.called = False
        self._db.auth_get_passkey(cb.ret,callback=cb)
        if cb.called is False:
            raise Exception('expiration check: callback not called')
        if exp != cb.ret:
            raise Exception('expiration check: expiration does not match')

        # expiration2
        exp = 10
        cb.called = False

        self._db.auth_new_passkey(exp,callback=cb)

        if cb.called is False:
            raise Exception('expiration2: callback not called')


        # bad expiration
        exp = 'theexp'
        cb.called = False

        try:
            self._db.auth_new_passkey(exp,callback=cb)
        except:
            pass
        else:
            raise Exception('bad expiration: did not raise Exception')

        # sql_write_task error
        self.mock.failures = True
        exp = 10
        cb.called = False

        self._db.auth_new_passkey(exp,callback=cb)

        if cb.called is False:
            raise Exception('sql_write_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_write_task error: did not raise exception')

        # increment_id error
        exp = 10
        cb.called = False

        self.mock.failures = False
        self._db.auth_new_passkey(exp,callback=cb)
        cb.called = False
        self._db.auth_authorize_task(cb.ret,callback=cb)

        if cb.ret is False:
            raise Exception('increment_id error: callback not called')
        else:
            raise Exception('increment_id error: did not raise Exception')

    @unittest_reporter
    def test_401_auth_get_passkey(self):
        """Test auth_get_passkey"""

        exp = datetime.utcnow()
        key = 'thekey'
        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
            ],
            'setting':[
                {'site_id':1},
            ],
            'passkey':[
                {'key':key,'expire':dbmethods.datetime2str(exp)}
            ],
        }

        def cb(ret):
            cb.called = True
            cb.ret = ret
        cb.called = False
        self.mock.setup(tables)

        # everything working
        cb.called = False

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

        try:
            self._db.auth_get_passkey(key,callback=cb)
        except:
            pass
        else:
            raise Exception('passkey error: did not raise Exception')

        # sql_read_task error
        key = 'thekey'
        self.mock.failures = True
        cb.called = False

        self._db.auth_get_passkey(key,callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error: callback ret != Exception')

        # sql_read_task error2
        key = 'thekey'
        exp = 'expiration'
        tables['passkey'] = [{'key':key,'expire':exp}]
        self.mock.failures = 0
        self.mock.setup(tables)
        cb.called = False

        self._db.auth_get_passkey(key,callback=cb)

        if cb.called is False:
            raise Exception('sql_read_task error2: callback not called')
        if not isinstance(cb.ret,Exception):
            raise Exception('sql_read_task error2: callback ret != Exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_auth_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_auth_test))
    return suite
