"""
  Test script for database server module

  copyright (c) 2012 the icecube collaboration  
"""

from __future__ import print_function
import logging
try:
    from server_tester import printer, glob_tests
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    logging.basicConfig()
logger = logging.getLogger('db_test')

import os
import sys
import time
import shutil
import signal

from server_tester import server_module

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

from iceprod.core import functions
from iceprod.server import module
from iceprod.server.modules import db


""" List of things that must be mocked:
    def _start_db(self):
    def _start_rpc(self):
    def _stop_db(self,force=False):
    def _stop_rpc(self):
    def _setup_tables(self):
    def _backup_worker(self):
    def _dbsetup(self):
    def _db_read(self,sql,bindings,archive_sql,archive_bindings):
    def _db_write(self,sql,bindings,archive_sql,archive_bindings):
    def _increment_id_helper(self,table):
"""

class dbapi_test(unittest.TestCase):
    def setUp(self):
        super(dbapi_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
        # get hostname
        hostname = functions.gethostname()
        if hostname is None:
            hostname = 'localhost'
        elif isinstance(hostname,set):
            hostname = hostname.pop()
        self.hostname = hostname
        
        # set db class
        self._dbclass = db.DBAPI
        
        flexmock(module).should_receive('logger').and_return(logging)
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(dbapi_test,self).tearDown()
    
    def test_01_init(self):
        """Test init"""
        try:
            # mock db
            def start():
                start.called = True
            flexmock(self._dbclass).should_receive('start').replace_with(start)
            def tables():
                tables.called = True
            flexmock(self._dbclass).should_receive('_setup_tables').replace_with(tables)
            def init():
                init.called = True
            flexmock(self._dbclass).should_receive('init').replace_with(init)
            
            start.called = False
            tables.called = False
            init.called = False
            
            cfg = {'test':1}
            newdb = self._dbclass(cfg)
            if not newdb:
                raise Exception('init did not return db object')
            elif start.called != True:
                raise Exception('init did not call start')
            elif tables.called != True:
                raise Exception('init did not call _setup_tables')
            elif init.called != True:
                raise Exception('init did not call init')
            elif not newdb.cfg or 'test' not in newdb.cfg or newdb.cfg['test'] != 1:
                raise Exception('init did not copy cfg properly')
            
        except Exception, e:
            logger.error('Error running db init test - %s',str(e))
            printer('Test db:%s init'%self._dbclass.__name__,False)
            raise
        else:
            printer('Test db:%s init'%self._dbclass.__name__)
    
    def test_02_start_stop(self):
        """Test start_stop"""
        try:
            # mock db
            def _start_db():
                _start_db.called = True
            flexmock(self._dbclass).should_receive('_start_db').replace_with(_start_db)
            def _start_rpc():
                _start_rpc.called = True
            flexmock(self._dbclass).should_receive('_start_rpc').replace_with(_start_rpc)
            def _stop_db(force=False):
                _stop_db.called = True
            flexmock(self._dbclass).should_receive('_stop_db').replace_with(_stop_db)
            def _stop_rpc():
                _stop_rpc.called = True
            flexmock(self._dbclass).should_receive('_stop_rpc').replace_with(_stop_rpc)
            def tables():
                tables.called = True
            flexmock(self._dbclass).should_receive('_setup_tables').replace_with(tables)
            def init():
                init.called = True
            flexmock(self._dbclass).should_receive('init').replace_with(init)
            
            _start_db.called = False
            _start_rpc.called = False
            _stop_db.called = False
            _stop_rpc.called = False
            tables.called = False
            init.called = False
            
            # test start
            cfg = {'db':{'name':'name','numthreads':1,'sqlite_cachesize':1000}}
            newdb = self._dbclass(cfg)
            if not newdb:
                raise Exception('start_stop did not return db object')
            elif _start_db.called != True:
                raise Exception('start_stop did not call _start_db')
            elif _start_rpc.called != True:
                raise Exception('start_stop did not call _start_rpc')
            elif tables.called != True:
                raise Exception('start_stop did not call _setup_tables')
            elif init.called != True:
                raise Exception('start_stop did not call init')
            
            # test stop
            newdb.stop()
            if _stop_db.called != True:
                raise Exception('start_stop did not call _stop_db')
            elif _stop_rpc.called != True:
                raise Exception('start_stop did not call _stop_rpc')
            
        except Exception, e:
            logger.error('Error running db start_stop test - %s',str(e))
            printer('Test db:%s start_stop'%self._dbclass.__name__,False)
            raise
        else:
            printer('Test db:%s start_stop'%self._dbclass.__name__)
    
    
class sqlite_test(dbapi_test):
    def setUp(self):
        super(sqlite_test,self).setUp()
        
        # set db class
        self._dbclass = db.SQLite
    
    def tearDown(self):
        super(sqlite_test,self).tearDown()
    

try:
    import MySQLdb
except:
    logger.error('Cannot import MySQLdb. MySQL db not tested')
    print('Cannot import MySQLdb. MySQL db not tested')
    mysql_test = None
else:
    class mysql_test(dbapi_test):
        def setUp(self):
            super(mysql_test,self).setUp()
            
            # set db class
            self._dbclass = db.MySQL
        
        def tearDown(self):
            super(mysql_test,self).tearDown()
        
    
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbapi_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbapi_test))
    alltests = glob_tests(loader.getTestCaseNames(sqlite_test))
    suite.addTests(loader.loadTestsFromNames(alltests,sqlite_test))
    if mysql_test:
        alltests = glob_tests(loader.getTestCaseNames(mysql_test))
        suite.addTests(loader.loadTestsFromNames(alltests,mysql_test))
    return suite
