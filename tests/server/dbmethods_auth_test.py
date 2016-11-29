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
import unittest

import tornado.escape

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods
import iceprod.server.modules.db

from .dbmethods_test import dbmethods_base

class dbmethods_auth_test(dbmethods_base):
    @unittest_reporter
    def test_010_auth_get_site_auth(self):
        """Test auth_get_site_auth"""

        tables = {
            'site':[
                {'site_id':1,'auth_key':'key1'},
            ],
        }

        # normal site test
        yield self.set_tables(tables)
        ret = yield self.db['auth_get_site_auth'](1)
        self.assertEqual(ret, 'key1')

        # site not in db
        try:
            ret = yield self.db['auth_get_site_auth'](2)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # bad db info
        tables2 = {
            'site':[
                {'site_id':1},
            ],
        }
        yield self.set_tables(tables2)

        try:
            ret = yield self.db['auth_get_site_auth'](1)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # sql error
        yield self.set_tables(tables)
        self.set_failures(True)
        try:
            ret = yield self.db['auth_get_site_auth'](1)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_011_auth_authorize_site(self):
        """Test auth_authorize_site"""
        tables = {
            'site':[
                {'site_id':1,'auth_key':'key'},
            ],
        }

        # normal site
        yield self.set_tables(tables)
        yield self.db['auth_authorize_site'](1, 'key')

        # site not in db
        try:
            yield self.db['auth_authorize_site'](2, 'key')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # key error
        try:
            yield self.db['auth_authorize_site'](1, 'bad')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # bad db info
        tables2 = {
            'site':[
                {'site_id':1},
            ],
        }
        yield self.set_tables(tables2)
        try:
            yield self.db['auth_authorize_site'](1, 'key')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # sql error
        yield self.set_tables(tables)
        self.set_failures(True)
        try:
            yield self.db['auth_authorize_site'](1, 'key')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_012_auth_authorize_task(self):
        """Test auth_authorize_task"""
        tables = {
            'passkey':[
                {'auth_key':1,'expire':'2100-01-01T01:01:01'},
                {'auth_key':2,'expire':'2000-01-01T01:01:01'},
            ],
        }
        yield self.set_tables(tables)


        # normal task
        yield self.db['auth_authorize_task'](1)

        # site not in db
        try:
            yield self.db['auth_authorize_task'](3)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # expired key
        try:
            yield self.db['auth_authorize_task'](2)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # bad db info
        tables2 = {
            'passkey':[
                {'auth_key':1,}
            ],
        }
        yield self.set_tables(tables2)
        try:
            yield self.db['auth_authorize_task'](1)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # sql error
        yield self.set_tables(tables)
        self.set_failures(True)
        try:
            yield self.db['auth_authorize_task'](1)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_020_auth_new_passkey(self):
        """Test auth_new_passkey"""
        tables = {
            'setting':[
                {'passkey_last':'0'},
            ],
        }
        yield self.set_tables(tables)

        # everything working
        ret = yield self.db['auth_new_passkey']()

        # Now check if the entry is there
        yield self.db['auth_authorize_task'](ret)

        # expiration
        exp = datetime.utcnow()+timedelta(seconds=10)

        ret = yield self.db['auth_new_passkey'](exp)
        exp2 = yield self.db['auth_get_passkey'](ret)
        self.assertEqual(exp, exp2)

        # expiration2
        exp = 10
        ret = yield self.db['auth_new_passkey'](exp)

        # bad expiration
        exp = 'theexp'
        try:
            ret = yield self.db['auth_new_passkey'](exp)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # with user id
        exp = 10
        ret = yield self.db['auth_new_passkey'](exp, user_id='123')

        # sql error
        self.set_failures(True)
        try:
            ret = yield self.db['auth_new_passkey'](exp)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_021_auth_get_passkey(self):
        """Test auth_get_passkey"""

        exp = datetime.utcnow()
        key = 'thekey'
        tables = {
            'passkey':[
                {'auth_key':key,'expire':dbmethods.datetime2str(exp)}
            ],
        }
        yield self.set_tables(tables)

        # everything working
        ret = yield self.db['auth_get_passkey'](key)
        self.assertEqual(exp, ret)

        # passkey error
        try:
            ret = yield self.db['auth_get_passkey']('key2')
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # failed to specify key
        try:
            ret = yield self.db['auth_get_passkey'](None)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        # sql error
        self.set_failures(True)
        try:
            ret = yield self.db['auth_get_passkey'](key)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

    @unittest_reporter
    def test_100_add_site_to_master(self):
        """Test add_site_to_master"""

        tables = {'site':[]}
        site_id = 'thesite'
        yield self.set_tables(tables)

        # everything working
        ret = yield self.db['add_site_to_master'](site_id)

        endtables = (yield self.get_tables(['site']))['site']
        if not endtables or endtables[0]['site_id'] != site_id:
            raise Exception('site not added')
        if ret != endtables[0]['auth_key']:
            raise Exception('passkey invalid')

        # sql error
        yield self.set_tables(tables)
        self.set_failures(True)
        try:
            ret = yield self.db['add_site_to_master'](site_id)
        except:
            pass
        else:
            raise Exception('did not raise Exception')

        self.set_failures(False)
        endtables = (yield self.get_tables(['site']))['site']
        if endtables:
            raise Exception('tables modified')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_auth_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_auth_test))
    return suite
