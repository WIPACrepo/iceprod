"""
  Test script for dbmethods

  copyright (c) 2013 the icecube collaboration
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
logger = logging.getLogger('dbmethods_test')

import os, sys, time
import shutil
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
    
class dbmethods_test(unittest.TestCase):
    def setUp(self):
        super(dbmethods_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
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
        super(dbmethods_test,self).tearDown()
    
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
        try:
            alltables = {t:OrderedDict([(x,i) for i,x in enumerate(DB.tables[t])]) for t in DB.tables}
            
            # test all tables individually
            for t in alltables:
                ret = self._db._list_to_dict(t,alltables[t].values())
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
                        ret = self._db._list_to_dict(groupkeys,groupvalues)
                        if ret != groupans:
                            raise Exception('got %r but should be %r'%(ret,groupans))
                    nleft = random.randint(1,10)
                groupkeys.append(t)
                groupvalues.extend(alltables[t].values())
                groupans.update(alltables[t])
            if groupkeys:
                ret = self._db._list_to_dict(groupkeys,groupvalues)
                if ret != groupans:
                    raise Exception('got %r but should be %r'%(ret,groupans))
            
        except Exception as e:
            logger.error('Error running dbmethods list_to_dict test - %s',str(e))
            printer('Test dbmethods list_to_dict',False)
            raise
        else:
            printer('Test dbmethods list_to_dict')

    def test_010_get_site_auth(self):
        """Test get_site_auth"""
        try:
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
            
            self._db.get_site_auth(callback=cb)
            
            if cb.called is False:
                raise Exception('normal site: callback not called')
            if cb.ret != data:
                raise Exception('normal site: callback ret != data')
            
            # site not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.get_site_auth(callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('not in db: callback did not receive exception')
                
            # site in db twice
            sql_read_task.ret = [[1,2],[3,4]]
            cb.called = False
            
            self._db.get_site_auth(callback=cb)
            
            if cb.called is False:
                raise Exception('in db twice: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('in db twice: callback did not receive exception')
            
            # bad db info
            sql_read_task.ret = [[data['site_id']]]
            cb.called = False
            
            self._db.get_site_auth(callback=cb)
            
            if cb.called is False:
                raise Exception('bad db info: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('bad db info: callback did not receive exception')
            
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.get_site_auth(callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_site_auth test - %s',str(e))
            printer('Test dbmethods get_site_auth',False)
            raise
        else:
            printer('Test dbmethods get_site_auth')

    def test_011_authorize_site(self):
        """Test authorize_site"""
        try:
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
            
            self._db.authorize_site(1,'key',callback=cb)
            
            if cb.called is False:
                raise Exception('normal site: callback not called')
            if cb.ret is not True:
                raise Exception('normal site: callback ret != True')
            
            # site not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.authorize_site(1,'key',callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('not in db: callback did not receive exception')
                
            # site in db twice
            sql_read_task.ret = [[1,2],[3,4]]
            cb.called = False
            
            self._db.authorize_site(1,'key',callback=cb)
            
            if cb.called is False:
                raise Exception('in db twice: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('in db twice: callback did not receive exception')
            
            # bad db info
            sql_read_task.ret = [[data['site_id']]]
            cb.called = False
            
            self._db.authorize_site(1,'key',callback=cb)
            
            if cb.called is False:
                raise Exception('bad db info: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('bad db info: callback did not receive exception')
            
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.authorize_site(1,'key',callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods authorize_site test - %s',str(e))
            printer('Test dbmethods authorize_site',False)
            raise
        else:
            printer('Test dbmethods authorize_site')
    
    def test_012_authorize_task(self):
        """Test authorize_task"""
        try:
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
            
            self._db.authorize_task(1,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            if cb.ret is not True:
                raise Exception('normal task: callback ret != True')
            
            # site not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.authorize_task(1,callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('not in db: callback did not receive exception')
                
            # site in db twice
            sql_read_task.ret = [[1,2],[3,4]]
            cb.called = False
            
            self._db.authorize_task(1,callback=cb)
            
            if cb.called is False:
                raise Exception('in db twice: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('in db twice: callback did not receive exception')
            
            # bad db info
            sql_read_task.ret = [[data['passkey']]]
            cb.called = False
            
            self._db.authorize_task(1,callback=cb)
            
            if cb.called is False:
                raise Exception('bad db info: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('bad db info: callback did not receive exception')
            
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.authorize_task(1,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods authorize_task test - %s',str(e))
            printer('Test dbmethods authorize_task',False)
            raise
        else:
            printer('Test dbmethods authorize_task')
    
    def test_020_in_cache(self):
        """Test in_cache"""
        try:
            uid = 'asdsdf'
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            
            def cb(ret,ret2):
                cb.called = True
                cb.ret = (ret,ret2)
            cb.called = False
            
            # normal task
            sql_read_task.ret = [[uid]]
            
            self._db.in_cache('http://test.ing/test',callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            if cb.ret != (True,uid):
                raise Exception('normal task: callback ret != (True,uid)')
            
            # not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.in_cache(1,callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if cb.ret != (False,None):
                raise Exception('not in db: callback ret != (False,None)')
                
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.in_cache(1,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret[0],Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods in_cache test - %s',str(e))
            printer('Test dbmethods in_cache',False)
            raise
        else:
            printer('Test dbmethods in_cache')

    def test_021_remove_from_cache(self):
        """Test remove_from_cache"""
        try:
            uid = 'asdsdf'
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_write_task').replace_with(sql_read_task)
            
            def cb(ret,ret2):
                cb.called = True
                cb.ret = (ret,ret2)
            cb.called = False
            
            # normal task
            sql_read_task.ret = None
            
            self._db.remove_from_cache('http://test.ing/test',callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            if cb.ret != (False,None):
                raise Exception('normal task: callback ret != (False,None)')
            
            # not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.remove_from_cache(1,callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if cb.ret != (False,None):
                raise Exception('not in db: callback ret != (False,None)')
                
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.remove_from_cache(1,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret[0],Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods remove_from_cache test - %s',str(e))
            printer('Test dbmethods remove_from_cache',False)
            raise
        else:
            printer('Test dbmethods remove_from_cache')

    def test_022_get_cache_checksum(self):
        """Test get_cache_checksum"""
        try:
            url = 'asdsdf'
            checksum = 'lksdfn'
            checksum_type = 'sha512'
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            
            def cb(*args):
                cb.called = True
                cb.ret = args
            cb.called = False
            
            # normal task
            sql_read_task.ret = [[checksum,checksum_type]]
            
            self._db.get_cache_checksum(url,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            if cb.ret != (True,checksum,checksum_type):
                raise Exception('normal task: callback ret != checksum')
            
            # not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.get_cache_checksum(url,callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if cb.ret != (False,None):
                raise Exception('not in db: callback ret != False')
                
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.get_cache_checksum(url,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret[0],Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_cache_checksum test - %s',str(e))
            printer('Test dbmethods get_cache_checksum',False)
            raise
        else:
            printer('Test dbmethods get_cache_checksum')

    def test_023_get_cache_size(self):
        """Test get_cache_size"""
        try:
            url = 'asdsdf'
            size = 403
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            
            def cb(ret,ret2):
                cb.called = True
                cb.ret = (ret,ret2)
            cb.called = False
            
            # normal task
            sql_read_task.ret = [[size]]
            
            self._db.get_cache_size(url,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            if cb.ret != (True,size):
                raise Exception('normal task: callback ret != size')
            
            # not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.get_cache_size(url,callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if cb.ret != (False,None):
                raise Exception('not in db: callback ret != False')
                
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.get_cache_size(url,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret[0],Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_cache_size test - %s',str(e))
            printer('Test dbmethods get_cache_size',False)
            raise
        else:
            printer('Test dbmethods get_cache_size')

    def test_025_check_cache_space(self):
        """Test check_cache_space"""
        try:
            download_dir = self.test_dir
            priority = 5
            size = 403
            url = 'http://test.ing/test'
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                if sql == 'select sum(size) as s from download':
                    callback(sql_read_task.download_size)
                elif sql == 'select delete_priority, sum(size) as s from cache group by delete_priority':
                    callback(sql_read_task.cache_priority)
                elif sql.startswith('select uid from cache where delete_priority = '):
                    callback(sql_read_task.uid_in_cache)
                elif sql.startswith('select sum(size) as s from cache where delete_priority'):
                    callback(sql_read_task.priority_size)
                else:
                    callback(Exception('unknown sql'))
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            def sql_write_task(sql,bindings,callback):
                sql_write_task.sql = sql
                sql_write_task.bindings = bindings
                callback(sql_write_task.ret)
            flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)
            def blocking_task(func,*args,**kwargs):
                cb = None
                if 'callback' in kwargs:
                    cb = kwargs.pop('callback')
                ret = func(*args,**kwargs)
                if cb:
                    cb(ret)
            flexmock(DB).should_receive('non_blocking_task').replace_with(blocking_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # plenty of space
            sql_read_task.download_size = [[100]]
            sql_read_task.cache_priority = [[1,1478],
                                            [4,124534],
                                            [6,12312],
                                            [8,123123123]
                                           ]
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = []
            priority = 5
            size = 403
            # set free space to 3GB
            flexmock(functions).should_receive('freespace').and_return(3*1024**3)
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            if cb.ret != True:
                raise Exception('normal task: callback ret != True')
                
            # less than 1GB limit
            sql_read_task.download_size = [[100]]
            sql_read_task.cache_priority = [[1,1478],
                                            [4,124534],
                                            [6,12312],
                                            [8,123123123]
                                           ]
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = []
            priority = 5
            size = 403
            # set free space to 1MB
            flexmock(functions).should_receive('freespace').and_return(1024**2)
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('free space <1GB: callback not called')
            if cb.ret != False:
                raise Exception('free space <1GB: callback ret != False')
                
            # sql exception on download size
            sql_read_task.download_size = Exception('error')
            sql_read_task.cache_priority = [[1,1478],
                                            [4,124534],
                                            [6,12312],
                                            [8,123123123]
                                           ]
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = []
            priority = 5
            size = 403
            # set free space to 2GB
            flexmock(functions).should_receive('freespace').and_return(2*1024**3)
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('exception on download size: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('exception on download size: callback ret != Exception')
                
            # no download size returned
            sql_read_task.download_size = []
            sql_read_task.cache_priority = [[1,1478],
                                            [4,124534],
                                            [6,12312],
                                            [8,123123123]
                                           ]
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = []
            priority = 5
            size = 403
            # set free space to 2GB
            flexmock(functions).should_receive('freespace').and_return(2*1024**3)
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('no download size: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('no download size: callback ret != Exception')
                
            # sql exception on cache size
            sql_read_task.download_size = [[100]]
            sql_read_task.cache_priority = Exception('error')
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = []
            priority = 5
            size = 403
            # set free space to 2GB
            flexmock(functions).should_receive('freespace').and_return(2*1024**3)
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('exception on cache size: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('exception on cache size: callback ret != Exception')
                
            # no cache size returned
            sql_read_task.download_size = [[100]]
            sql_read_task.cache_priority = []
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = []
            priority = 5
            size = 403
            # set free space to 2GB
            flexmock(functions).should_receive('freespace').and_return(2*1024**3)
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('no cache size: callback not called')
            if cb.ret != True:
                raise Exception('no cache size: callback ret != True')
                
            # disk already full of higher priority items
            sql_read_task.download_size = [[100]]
            sql_read_task.cache_priority = [[1,1470000000],
                                            [4,124534],
                                            [6,12312],
                                            [8,123123123]
                                           ]
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = []
            priority = 5
            size = 903000000
            # set free space to 2GB - things in cache
            flexmock(functions).should_receive('freespace').and_return(2*1024**3-1593259969)
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('disk full of high priority: callback not called')
            if cb.ret != False:
                raise Exception('disk full of high priority: callback ret != False')
                
            # delete lower priority item to create space
            sql_read_task.download_size = [[100]]
            sql_read_task.cache_priority = [[1,14700000],
                                            [4,124534],
                                            [6,12312],
                                            [8,123123123]
                                           ]
            sql_read_task.uid_in_cache = [['sdf']]
            sql_read_task.priority_size = [[3400]]
            sql_write_task.ret = [[12]]
            priority = 5
            size = 903000000
            # set free space to 2GB - things in cache
            flexmock(functions).should_receive('freespace').and_return(2*1024**3-137959969)
            with open(os.path.join(download_dir,'sdf'),'w') as f:
                f.write('sdf')
            
            self._db.check_cache_space(download_dir,size,priority,url,callback=cb)
            
            if cb.called is False:
                raise Exception('delete lower priority: callback not called')
            if cb.ret != True:
                raise Exception('delete lower priority: callback ret != True')
            
        except Exception as e:
            logger.error('Error running dbmethods check_cache_space test - %s',str(e))
            printer('Test dbmethods check_cache_space',False)
            raise
        else:
            printer('Test dbmethods check_cache_space')

    def test_029_add_to_cache(self):
        """Test add_to_cache"""
        try:
            priority = 5
            size = 403
            url = 'http://test.ing/test'
            uid = 'sdf'
            checksum = 'sdfasdf'
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_write_task').replace_with(sql_read_task)
            flexmock(DB).should_receive('increment_id').and_return(1)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # normal task
            sql_read_task.ret = [[]]
            
            self._db.add_to_cache(url,uid,size,checksum,'sha512',priority,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            if isinstance(cb.ret,Exception):
                raise Exception('normal task: callback ret is Exception')
            if sql_read_task.sql != 'insert into cache (cache_id,permanent_url,uid,size,checksum,checksum_type,delete_priority) values (?,?,?,?,?,?,?)':
                raise Exception('normal task: sql incorrect')
            
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.add_to_cache(url,uid,size,checksum,'sha512',priority,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods add_to_cache test - %s',str(e))
            printer('Test dbmethods add_to_cache',False)
            raise
        else:
            printer('Test dbmethods add_to_cache')

    def test_100_get_site_id(self):
        """Test get_site_id"""
        try:
            site_id = 'asdfasdfsdf'
            
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
            sql_read_task.ret = [[site_id]]
            
            self._db.get_site_id(callback=cb)
            
            if cb.called is False:
                raise Exception('normal site: callback not called')
            if cb.ret != site_id:
                raise Exception('normal site: callback ret != site_id')
            
            # site not in db
            sql_read_task.ret = []
            cb.called = False
            
            self._db.get_site_id(callback=cb)
            
            if cb.called is False:
                raise Exception('not in db: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('not in db: callback ret != Exception')
                
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.get_site_id(callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_site_id test - %s',str(e))
            printer('Test dbmethods get_site_id',False)
            raise
        else:
            printer('Test dbmethods get_site_id')

    def test_110_get_active_tasks(self):
        """Test get_active_tasks"""
        try:
            task = OrderedDict([('task_id','asdf'),
                    ('status','queued'),
                    ('prev_status','waiting'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',0),
                    ('depends',None),
                   ])
            task2 = OrderedDict([('task_id','gdf'),
                    ('status','queued'),
                    ('prev_status','waiting'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',0),
                    ('depends',None),
                   ])
            task3 = OrderedDict([('task_id','ertert'),
                    ('status','processing'),
                    ('prev_status','queued'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',1),
                    ('depends',None),
                   ])
            gridspec = 'klsjdfl.grid1'
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # single task
            cb.called = False
            sql_read_task.ret = [task.values()]
            
            self._db.get_active_tasks(gridspec,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            ret_should_be = {task['status']:{task['task_id']:task}}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('normal task: callback ret != task')
            if not sql_read_task.sql.startswith('select task.* from search join task on search.task_id = task.task_id '):
                raise Exception('normal task: sql incorrect')
                
            # no tasks
            cb.called = False
            sql_read_task.ret = []
            
            self._db.get_active_tasks(gridspec,callback=cb)
            
            if cb.called is False:
                raise Exception('no task: callback not called')
            if cb.ret != {}:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('no task: callback ret != {}')
            if not sql_read_task.sql.startswith('select task.* from search join task on search.task_id = task.task_id '):
                raise Exception('no task: sql incorrect')
            
            # several tasks
            cb.called = False
            sql_read_task.ret = [task.values(),task2.values(),task3.values()]
            
            self._db.get_active_tasks(gridspec,callback=cb)
            
            if cb.called is False:
                raise Exception('several tasks: callback not called')
            ret_should_be = {task['status']:{task['task_id']:task,
                                             task2['task_id']:task2},
                             task3['status']:{task3['task_id']:task3}}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('several tasks: callback ret != task task2 task3')
            if not sql_read_task.sql.startswith('select task.* from search join task on search.task_id = task.task_id '):
                raise Exception('several tasks: sql incorrect')
            
            # sql error
            sql_read_task.ret = Exception('sql error')
            cb.called = False
            
            self._db.get_active_tasks(gridspec,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_active_tasks test - %s',str(e))
            printer('Test dbmethods get_active_tasks',False)
            raise
        else:
            printer('Test dbmethods get_active_tasks')

    def test_111_set_task_status(self):
        """Test set_task_status"""
        try:
            def _db_write(conn,sql,bindings,*args):
                def w(s,b):
                    _db_write.sql.append(s)
                    _db_write.bindings.append(b)
                    if b[0] in _db_write.task_ret:
                        return True
                    else:
                        raise Exception('sql error')
                if isinstance(sql,basestring):
                    return w(sql,bindings)
                elif isinstance(sql,Iterable):
                    ret = None
                    for s,b in izip(sql,bindings):
                        ret = w(s,b)
                    return ret
            flexmock(DB).should_receive('_db_write').replace_with(_db_write)
            def non_blocking_task(cb):
                non_blocking_task.called = True
                cb()
            flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # single task
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('waiting')
            cb.called = False
            task = 'asfsd'
            status = 'waiting'
            
            self._db.set_task_status(task,status,callback=cb)
            
            if cb.called is False:
                raise Exception('single task: callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('single task: callback ret == Exception')
            if (len(_db_write.bindings) != 2 or 
                _db_write.bindings[0] != (status,task) or
                _db_write.bindings[1][0] != status or 
                _db_write.bindings[1][-1] != task):
                logger.info('sql bindings: %r',_db_write.bindings)
                raise Exception('single task: sql bindings != (status,task_id)')
                
            # no task
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('waiting')
            cb.called = False
            task = None
            status = 'waiting'
            
            try:
                self._db.set_task_status(task,status,callback=cb)
            except:
                pass
            else:
                raise Exception('no task: exception not raised')
            
            if cb.called is not False:
                raise Exception('no task: callback called')
            
            # multiple tasks (dict)
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('waiting')
            cb.called = False
            task = OrderedDict([('asfsd',{}),('gsdf',{})])
            status = 'waiting'
            
            self._db.set_task_status(task,status,callback=cb)
            
            if cb.called is False:
                raise Exception('multiple tasks (dict): callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('multiple tasks (dict): callback ret == Exception')
            expected = [(status,'asfsd','gsdf'),(status,'asfsd','gsdf')]
            if (len(_db_write.bindings) != 2 or 
                _db_write.bindings[0] != expected[0] or
                _db_write.bindings[1][0] != expected[1][0] or 
                _db_write.bindings[1][2:] != expected[1][1:]):
                logger.info('sql bindings: %r',_db_write.bindings)
                logger.info('expected bindings: %r',expected)
                raise Exception('multiple tasks (dict): sql bindings incorrect')
            
            # multiple tasks (list)
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('waiting')
            cb.called = False
            task = ['asfsd','gsdf']
            status = 'waiting'
            
            self._db.set_task_status(task,status,callback=cb)
            
            if cb.called is False:
                raise Exception('multiple tasks (list): callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('multiple tasks (list): callback ret == Exception')
            expected = [(status,'asfsd','gsdf'),(status,'asfsd','gsdf')]
            if (len(_db_write.bindings) != 2 or 
                _db_write.bindings[0] != expected[0] or
                _db_write.bindings[1][0] != expected[1][0] or 
                _db_write.bindings[1][2:] != expected[1][1:]):
                logger.info('sql bindings: %r',_db_write.bindings)
                logger.info('expected bindings: %r',expected)
                raise Exception('multiple tasks (list): sql bindings incorrect')
                
            # sql error
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = {}
            cb.called = False
            task = 'asfsd'
            status = 'waiting'
            
            self._db.set_task_status(task,status,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods set_task_status test - %s',str(e))
            printer('Test dbmethods set_task_status',False)
            raise
        else:
            printer('Test dbmethods set_task_status')

    def test_112_reset_tasks(self):
        """Test reset_tasks"""
        try:
            def _db_write(conn,sql,bindings,*args):
                def w(s,b):
                    _db_write.sql.append(s)
                    _db_write.bindings.append(b)
                    if b[0] in _db_write.task_ret:
                        return True
                    else:
                        raise Exception('sql error')
                if isinstance(sql,basestring):
                    return w(sql,bindings)
                elif isinstance(sql,Iterable):
                    ret = None
                    for s,b in izip(sql,bindings):
                        ret = w(s,b)
                    return ret
            flexmock(DB).should_receive('_db_write').replace_with(_db_write)
            def non_blocking_task(cb):
                non_blocking_task.called = True
                cb()
            flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
            
            def cb(ret=None):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # single task
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset','failed')
            cb.called = False
            reset = 'asfsd'
            
            self._db.reset_tasks(reset,callback=cb)
            
            if cb.called is False:
                raise Exception('single task: callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('single task: callback ret == Exception')
            expected = [('reset',reset),('reset',reset)]
            if (len(_db_write.bindings) != 2 or 
                _db_write.bindings[0] != expected[0] or
                _db_write.bindings[1][0] != expected[1][0] or 
                _db_write.bindings[1][2:] != expected[1][1:]):
                logger.info('sql bindings: %r',_db_write.bindings)
                raise Exception('single task: sql bindings != (reset,task_id)')
            
            # single task with fail
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset','failed')
            cb.called = False
            reset = 'asfsd'
            fail = 'sdfsdf'
            
            self._db.reset_tasks(reset,fail,callback=cb)
            
            if cb.called is False:
                raise Exception('single task w/fail: callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('single task w/fail: callback ret == Exception')
            expected = [('reset',reset),('reset',reset),
                        ('failed',fail),('failed',fail)]
            if (len(_db_write.bindings) != 4 or 
                _db_write.bindings[0] != expected[0] or
                _db_write.bindings[1][0] != expected[1][0] or 
                _db_write.bindings[1][2:] != expected[1][1:] or 
                _db_write.bindings[2] != expected[2] or
                _db_write.bindings[3][0] != expected[3][0] or 
                _db_write.bindings[3][2:] != expected[3][1:]):
                logger.info('sql bindings: %r',_db_write.bindings)
                logger.info('expected bindings: %r',expected)
                raise Exception('single task w/fail: sql bindings incorrect')
            
            # single fail task
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset','failed')
            cb.called = False
            fail = 'sdfsdf'
            
            self._db.reset_tasks(fail=fail,callback=cb)
            
            if cb.called is False:
                raise Exception('single fail task: callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('single fail task: callback ret == Exception')
            expected = [('failed',fail),('failed',fail)]
            if (len(_db_write.bindings) != 2 or 
                _db_write.bindings[0] != expected[0] or
                _db_write.bindings[1][0] != expected[1][0] or 
                _db_write.bindings[1][2:] != expected[1][1:]):
                logger.info('sql bindings: %r',_db_write.bindings)
                logger.info('expected bindings: %r',expected)
                raise Exception('single task w/fail: sql bindings incorrect')
            
            # multiple tasks (dict)
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset','failed')
            cb.called = False
            reset = OrderedDict([('asfsd',{}),('gsdf',{})])
            fail = OrderedDict([('asfsd',{}),('gsdf',{})])
            
            self._db.reset_tasks(reset,fail,callback=cb)
            
            if cb.called is False:
                raise Exception('multiple tasks (dict): callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('multiple tasks (dict): callback ret == Exception')
            expected = [('reset','asfsd','gsdf'),('reset','asfsd','gsdf'),
                        ('failed','asfsd','gsdf'),('failed','asfsd','gsdf')]
            if (len(_db_write.bindings) != 4 or 
                _db_write.bindings[0] != expected[0] or
                _db_write.bindings[1][0] != expected[1][0] or 
                _db_write.bindings[1][2:] != expected[1][1:] or 
                _db_write.bindings[2] != expected[2] or
                _db_write.bindings[3][0] != expected[3][0] or 
                _db_write.bindings[3][2:] != expected[3][1:]):
                logger.info('sql bindings: %r',_db_write.bindings)
                logger.info('expected bindings: %r',expected)
                raise Exception('multiple tasks (dict): sql bindings incorrect')
            
            # multiple tasks (list)
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset','failed')
            cb.called = False
            reset = ['asfsd','gsdf']
            fail = ['asfsd','gsdf']
            
            self._db.reset_tasks(reset,fail,callback=cb)
            
            if cb.called is False:
                raise Exception('multiple tasks (list): callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('%r',cb.ret)
                raise Exception('multiple tasks (list): callback ret == Exception')
            expected = [('reset','asfsd','gsdf'),('reset','asfsd','gsdf'),
                        ('failed','asfsd','gsdf'),('failed','asfsd','gsdf')]
            if (len(_db_write.bindings) != 4 or 
                _db_write.bindings[0] != expected[0] or
                _db_write.bindings[1][0] != expected[1][0] or 
                _db_write.bindings[1][2:] != expected[1][1:] or 
                _db_write.bindings[2] != expected[2] or
                _db_write.bindings[3][0] != expected[3][0] or 
                _db_write.bindings[3][2:] != expected[3][1:]):
                logger.info('sql bindings: %r',_db_write.bindings)
                logger.info('expected bindings: %r',expected)
                raise Exception('multiple tasks (list): sql bindings incorrect')
                
            # sql error in reset
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('failed',)
            cb.called = False
            reset = 'asfsd'
            
            self._db.reset_tasks(reset,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error in reset: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error in reset: callback ret != Exception')
                
            # sql error in fail
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset',)
            cb.called = False
            reset = 'asfsd'
            fail = 'kljsdf'
            
            self._db.reset_tasks(reset,fail,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error in fail: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error in fail: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods reset_tasks test - %s',str(e))
            printer('Test dbmethods reset_tasks',False)
            raise
        else:
            printer('Test dbmethods reset_tasks')

    def test_113_get_task(self):
        """Test get_task"""
        try:
            task = OrderedDict([('task_id','asdf'),
                    ('status','queued'),
                    ('prev_status','waiting'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',0),
                    ('depends',None),
                   ])
            task2 = OrderedDict([('task_id','gdf'),
                    ('status','queued'),
                    ('prev_status','waiting'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',0),
                    ('depends',None),
                   ])
            task3 = OrderedDict([('task_id','ertert'),
                    ('status','processing'),
                    ('prev_status','queued'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',1),
                    ('depends',None),
                   ])
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # single task
            cb.called = False
            sql_read_task.ret = [task.values()]
            task_id = task['task_id']
            
            self._db.get_task(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            ret_should_be = task
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('normal task: callback ret != task')
            if not sql_read_task.sql.startswith('select * from task where task_id ='):
                raise Exception('normal task: sql incorrect')
                
            # no tasks
            cb.called = False
            sql_read_task.ret = []
            task_id = task['task_id']
            
            self._db.get_task(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('no task: callback not called')
            if cb.ret != None:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('no task: callback ret != None')
            if not sql_read_task.sql.startswith('select * from task where task_id ='):
                raise Exception('no task: sql incorrect')
                
            # no tasks sql issue
            cb.called = False
            sql_read_task.ret = None
            task_id = task['task_id']
            
            self._db.get_task(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('no task: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('no task: callback ret != Exception')
            if not sql_read_task.sql.startswith('select * from task where task_id ='):
                raise Exception('no task: sql incorrect')
                
            # no task_id
            cb.called = False
            sql_read_task.ret = []
            task_id = None
            
            try:
                self._db.get_task(task_id,callback=cb)
            except:
                pass
            else:
                raise Exception('no task_id: exception not raised')
            
            if cb.called is not False:
                raise Exception('no task_id: callback called, but not supposed to be')
            
            # several tasks
            cb.called = False
            sql_read_task.ret = [task.values(),task2.values(),task3.values()]
            task_id = [task['task_id'],task2['task_id'],task3['task_id']]
            
            self._db.get_task(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('several tasks: callback not called')
            ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('several tasks: callback ret != task task2 task3')
            if not sql_read_task.sql.startswith('select * from task where task_id in'):
                raise Exception('several tasks: sql incorrect')
            
            # sql error
            cb.called = False
            sql_read_task.ret = Exception('sql error')
            task_id = task['task_id']
            
            self._db.get_task(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_task test - %s',str(e))
            printer('Test dbmethods get_task',False)
            raise
        else:
            printer('Test dbmethods get_task')

    def test_114_get_task_by_grid_queue_id(self):
        """Test get_task_by_grid_queue_id"""
        try:
            task = OrderedDict([('task_id','asdf'),
                    ('status','queued'),
                    ('prev_status','waiting'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',0),
                    ('depends',None),
                   ])
            task2 = OrderedDict([('task_id','gdf'),
                    ('status','queued'),
                    ('prev_status','waiting'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',0),
                    ('depends',None),
                   ])
            task3 = OrderedDict([('task_id','ertert'),
                    ('status','processing'),
                    ('prev_status','queued'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',self.test_dir),
                    ('grid_queue_id','lkn'),
                    ('failures',0),
                    ('evictions',1),
                    ('depends',None),
                   ])
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # single task
            cb.called = False
            sql_read_task.ret = [task.values()]
            task_id = task['grid_queue_id']
            
            self._db.get_task_by_grid_queue_id(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('normal task: callback not called')
            ret_should_be = task
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('normal task: callback ret != task')
            if not sql_read_task.sql.startswith('select * from task where grid_queue_id ='):
                raise Exception('normal task: sql incorrect')
                
            # no tasks
            cb.called = False
            sql_read_task.ret = []
            task_id = task['grid_queue_id']
            
            self._db.get_task_by_grid_queue_id(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('no task: callback not called')
            if cb.ret != None:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('no task: callback ret != None')
            if not sql_read_task.sql.startswith('select * from task where grid_queue_id ='):
                raise Exception('no task: sql incorrect')
                
            # no tasks sql issue
            cb.called = False
            sql_read_task.ret = None
            task_id = task['grid_queue_id']
            
            self._db.get_task_by_grid_queue_id(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('no task: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('no task: callback ret != Exception')
            if not sql_read_task.sql.startswith('select * from task where grid_queue_id ='):
                raise Exception('no task: sql incorrect')
                
            # no task_id
            cb.called = False
            sql_read_task.ret = []
            task_id = None
            
            try:
                self._db.get_task_by_grid_queue_id(task_id,callback=cb)
            except:
                pass
            else:
                raise Exception('no task_id: exception not raised')
            
            if cb.called is not False:
                raise Exception('no task_id: callback called, but not supposed to be')
            
            # several tasks
            cb.called = False
            sql_read_task.ret = [task.values(),task2.values(),task3.values()]
            task_id = [task['grid_queue_id'],task2['grid_queue_id'],task3['grid_queue_id']]
            
            self._db.get_task_by_grid_queue_id(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('several tasks: callback not called')
            ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('several tasks: callback ret != task task2 task3')
            if not sql_read_task.sql.startswith('select * from task where grid_queue_id in'):
                raise Exception('several tasks: sql incorrect')
            
            # sql error
            cb.called = False
            sql_read_task.ret = Exception('sql error')
            task_id = task['grid_queue_id']
            
            self._db.get_task_by_grid_queue_id(task_id,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_task_by_grid_queue_id test - %s',str(e))
            printer('Test dbmethods get_task_by_grid_queue_id',False)
            raise
        else:
            printer('Test dbmethods get_task_by_grid_queue_id')

    def test_115_set_submit_dir(self):
        """Test set_submit_dir"""
        try:
            def sql_write_task(sql,bindings,callback):
                sql_write_task.sql = sql
                sql_write_task.bindings = bindings
                callback(sql_write_task.ret)
            flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # single task
            sql_write_task.ret = None
            cb.called = False
            task = 'asfsd'
            submit_dir = 'waiting'
            
            self._db.set_submit_dir(task,submit_dir,callback=cb)
            
            if cb.called is False:
                raise Exception('single task: callback not called')
            if cb.ret is not None:
                raise Exception('single task: callback ret != None')
            if sql_write_task.bindings != (submit_dir,task):
                logger.info('sql bindings: %r',sql_write_task.bindings)
                raise Exception('single task: sql bindings != (status,status,task_id)')
                
            # no task
            sql_write_task.ret = None
            cb.called = False
            task = None
            submit_dir = 'waiting1'
            
            try:
                self._db.set_submit_dir(task,submit_dir,callback=cb)
            except:
                pass
            else:
                raise Exception('no task: exception not raised')
            
            if cb.called is not False:
                raise Exception('no task: callback called')
            
            # sql error
            sql_write_task.ret = Exception('sql error')
            cb.called = False
            task = 'asfsd'
            submit_dir = 'waiting2'
            
            self._db.set_submit_dir(task,submit_dir,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods set_submit_dir test - %s',str(e))
            printer('Test dbmethods set_submit_dir',False)
            raise
        else:
            printer('Test dbmethods set_submit_dir')

    def test_119_buffer_jobs_tasks(self):
        """Test buffer_jobs_tasks"""
        try:
            def non_blocking_task(cb):
                non_blocking_task.called = True
                cb()
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                if bindings[0] in sql_read_task.task_ret:
                    callback(sql_read_task.task_ret[bindings[0]])
                else:
                    callback(Exception('sql error'))
            def _db_read(conn,sql,bindings,*args):
                _db_read.sql = sql
                _db_read.bindings = bindings
                if bindings[0] in _db_read.task_ret:
                    return _db_read.task_ret[bindings[0]]
                else:
                    raise Exception('sql error')
            def _db_write(conn,sql,bindings,*args):
                _db_write.sql.append(sql)
                _db_write.bindings.append(bindings)
                if _db_write.task_ret:
                    return True
                else:
                    raise Exception('sql error')
            def increment_id(table,conn=None):
                increment_id.table = table
                if table in increment_id.ret:
                    return increment_id.ret[table]
                else:
                    raise Exception('sql error')
            flexmock(DB).should_receive('_increment_id_helper').replace_with(increment_id)
            flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            flexmock(DB).should_receive('_db_read').replace_with(_db_read)
            flexmock(DB).should_receive('_db_write').replace_with(_db_write)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            gridspec = 'msdfiner'
            now = datetime.utcnow()
            dataset = OrderedDict([('dataset_id','lknser834'),
                                   ('name','test dataset'),
                                   ('description','a simple test'),
                                   ('gridspec',gridspec),
                                   ('status','processing'),
                                   ('username','user'),
                                   ('institution','inst'),
                                   ('submit_host','localhost'),
                                   ('priority',0),
                                   ('jobs_submitted',2),
                                   ('trays',1),
                                   ('tasks_submitted',2),
                                   ('start_date',dbmethods.datetime2str(now)),
                                   ('end_date',''),
                                   ('temporary_storage',''),
                                   ('global_storage',''),
                                   ('parent_id','sdf'),
                                   ('stat_keys','[]'),
                                   ('categoryvalue_ids',''),
                                   ('debug',True),
                                  ])
            search = OrderedDict([('task_id','gdf'),
                    ('job_id','3ns8'),
                    ('dataset_id','lknser834'),
                    ('gridspec','nsd89n3'),
                    ('name','0'),
                    ('task_status','queued'),
                   ])
            config_xml = """<?xml version='1.0' encoding='UTF-8'?>
<!DOCTYPE configuration PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "http://x2100.icecube.wisc.edu/dtd/iceprod.v3.dtd">
<configuration xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='config.xsd' version='3.0' iceprod_version='2.0' parentid='0'>
  <task name="task1">
    <tray name="Corsika">
      <module name="generate_corsika" class="generators.CorsikaIC" />
    </tray>
  </task>
</configuration>
"""
            
            # return values for first two callbacks
            sql_read_task.task_ret = {
                gridspec:
                    [[dataset['dataset_id'],
                      dataset['status'],
                      dataset['gridspec'],
                      dataset['jobs_submitted'],
                      dataset['tasks_submitted']
                    ]],
                dataset['dataset_id']:
                    [[search['dataset_id'],
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ]]
            }
            # return values for blocking
            _db_read.task_ret = {
                dataset['dataset_id']:[[dataset['dataset_id'],config_xml]]
            }
            increment_id.ret = {'job':'newjob',
                                'task':'newtask',
                               }
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = True
            cb.called = False
            
            num = 10
            self._db.buffer_jobs_tasks(gridspec,num,callback=cb)
            if cb.called is False:
                raise Exception('buffer 1d,1j,1t: callback not called')
            if isinstance(cb.ret,Exception):
                raise Exception('buffer 1d,1j,1t: exception returned %s'%cb.ret)
            
            # now try for multiple datasets
            # return values for first two callbacks
            sql_read_task.task_ret = {
                gridspec:
                    [[dataset['dataset_id'],
                      dataset['status'],
                      dataset['gridspec'],
                      dataset['jobs_submitted'],
                      dataset['tasks_submitted']
                    ],
                     [dataset['dataset_id']+'l',
                      dataset['status'],
                      dataset['gridspec'],
                      dataset['jobs_submitted'],
                      dataset['tasks_submitted']
                    ]],
                dataset['dataset_id']:
                    [[search['dataset_id'],
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ],
                     [search['dataset_id']+'l',
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ]],
                dataset['dataset_id']+'l':
                    [[search['dataset_id'],
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ],
                     [search['dataset_id']+'l',
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ]]
            }
            # return values for blocking
            _db_read.task_ret = {
                dataset['dataset_id']:
                    [[dataset['dataset_id'],config_xml],
                     [dataset['dataset_id']+'l',config_xml]],
                dataset['dataset_id']+'l':
                    [[dataset['dataset_id'],config_xml],
                     [dataset['dataset_id']+'l',config_xml]]
            }
            increment_id.ret = {'job':'newjob',
                                'task':'newtask',
                               }
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = True
            cb.called = False
            
            num = 10
            self._db.buffer_jobs_tasks(gridspec,num,callback=cb)
            if cb.called is False:
                raise Exception('buffer 2d,1j,1t: callback not called')
            if isinstance(cb.ret,Exception):
                raise Exception('buffer 2d,1j,1t: exception returned %s'%cb.ret)
            
            
            # now try for multiple gridspecs and datasets
            # return values for first two callbacks
            sql_read_task.task_ret = {
                '%'+gridspec+'%':
                    [[dataset['dataset_id'],
                      dataset['status'],
                      dataset['gridspec'],
                      dataset['jobs_submitted'],
                      dataset['tasks_submitted']
                    ],
                     [dataset['dataset_id']+'l',
                      dataset['status'],
                      dataset['gridspec']+'a',
                      dataset['jobs_submitted'],
                      dataset['tasks_submitted']
                    ]],
                dataset['dataset_id']:
                    [[search['dataset_id'],
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ]],
                dataset['dataset_id']+'l':
                    [[search['dataset_id'],
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ]]
            }
            # return values for blocking
            _db_read.task_ret = {
                dataset['dataset_id']:
                    [[dataset['dataset_id'],config_xml],
                     [dataset['dataset_id']+'l',config_xml]],
                dataset['dataset_id']+'l':
                    [[dataset['dataset_id'],config_xml],
                     [dataset['dataset_id']+'l',config_xml]]
            }
            increment_id.ret = {'job':'newjob',
                                'task':'newtask',
                               }
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = True
            cb.called = False
            
            num = 10
            self._db.buffer_jobs_tasks([gridspec,gridspec+'a'],num,callback=cb)
            if cb.called is False:
                raise Exception('buffer 2d,1j,1t 2gs: callback not called')
            if isinstance(cb.ret,Exception):
                raise Exception('buffer 2d,1j,1t 2gs: exception returned %s'%cb.ret)
            if len(_db_write.sql) != 6:
                for s,b in zip(_db_write.sql,_db_write.bindings):
                    logger.info('%s',s)
                    logger.info('%r',b)
                raise Exception('buffer 2d,1j,1t 2gs: not enough jobs queued')
            
            # now try with task names
            gridspec = 'msdfiner'
            now = datetime.utcnow()
            dataset = OrderedDict([('dataset_id','lknser834'),
                                   ('name','test dataset'),
                                   ('description','a simple test'),
                                   ('gridspec','{"task1":"'+gridspec+'"}'),
                                   ('status','processing'),
                                   ('username','user'),
                                   ('institution','inst'),
                                   ('submit_host','localhost'),
                                   ('priority',0),
                                   ('jobs_submitted',2),
                                   ('trays',1),
                                   ('tasks_submitted',2),
                                   ('start_date',dbmethods.datetime2str(now)),
                                   ('end_date',''),
                                   ('temporary_storage',''),
                                   ('global_storage',''),
                                   ('parent_id','sdf'),
                                   ('stat_keys','[]'),
                                   ('categoryvalue_ids',''),
                                   ('debug',True),
                                  ])
            search = OrderedDict([('task_id','gdf'),
                    ('job_id','3ns8'),
                    ('dataset_id','lknser834'),
                    ('gridspec','nsd89n3'),
                    ('name','0'),
                    ('task_status','queued'),
                   ])
            config_xml = """<?xml version='1.0' encoding='UTF-8'?>
<!DOCTYPE configuration PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "http://x2100.icecube.wisc.edu/dtd/iceprod.v3.dtd">
<configuration xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='config.xsd' version='3.0' iceprod_version='2.0' parentid='0'>
  <task name="task1">
    <tray name="Corsika">
      <module name="generate_corsika" class="generators.CorsikaIC" />
    </tray>
  </task>
</configuration>
"""
            
            # return values for first two callbacks
            sql_read_task.task_ret = {
                gridspec:
                    [[dataset['dataset_id'],
                      dataset['status'],
                      dataset['gridspec'],
                      dataset['jobs_submitted'],
                      dataset['tasks_submitted']
                    ]],
                dataset['dataset_id']:
                    [[search['dataset_id'],
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ]]
            }
            # return values for blocking
            _db_read.task_ret = {
                dataset['dataset_id']:[[dataset['dataset_id'],config_xml]]
            }
            increment_id.ret = {'job':'newjob',
                                'task':'newtask',
                               }
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = True
            cb.called = False
            
            num = 10
            self._db.buffer_jobs_tasks(gridspec,num,callback=cb)
            if cb.called is False:
                raise Exception('buffer 1d,1j,1t taskname: callback not called')
            if isinstance(cb.ret,Exception):
                raise Exception('buffer 1d,1j,1t taskname: exception returned %s'%cb.ret)
            
            
            # now try with task already buffered
            gridspec = 'msdfiner'
            now = datetime.utcnow()
            dataset = OrderedDict([('dataset_id','lknser834'),
                                   ('name','test dataset'),
                                   ('description','a simple test'),
                                   ('gridspec','{"task1":"'+gridspec+'"}'),
                                   ('status','processing'),
                                   ('username','user'),
                                   ('institution','inst'),
                                   ('submit_host','localhost'),
                                   ('priority',0),
                                   ('jobs_submitted',2),
                                   ('trays',1),
                                   ('tasks_submitted',2),
                                   ('start_date',dbmethods.datetime2str(now)),
                                   ('end_date',''),
                                   ('temporary_storage',''),
                                   ('global_storage',''),
                                   ('parent_id','sdf'),
                                   ('stat_keys','[]'),
                                   ('categoryvalue_ids',''),
                                   ('debug',True),
                                  ])
            search = OrderedDict([('task_id','gdf'),
                    ('job_id','3ns8'),
                    ('dataset_id','lknser834'),
                    ('gridspec',gridspec),
                    ('name','0'),
                    ('task_status','waiting'),
                   ])
            config_xml = """<?xml version='1.0' encoding='UTF-8'?>
<!DOCTYPE configuration PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "http://x2100.icecube.wisc.edu/dtd/iceprod.v3.dtd">
<configuration xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='config.xsd' version='3.0' iceprod_version='2.0' parentid='0'>
  <task name="task1">
    <tray name="Corsika">
      <module name="generate_corsika" class="generators.CorsikaIC" />
    </tray>
  </task>
</configuration>
"""
            
            # return values for first two callbacks
            sql_read_task.task_ret = {
                gridspec:
                    [[dataset['dataset_id'],
                      dataset['status'],
                      dataset['gridspec'],
                      dataset['jobs_submitted'],
                      dataset['tasks_submitted']
                    ]],
                dataset['dataset_id']:
                    [[search['dataset_id'],
                      search['job_id'],
                      search['task_id'],
                      search['gridspec'],
                      search['task_status'],
                    ]]
            }
            # return values for blocking
            _db_read.task_ret = {
                dataset['dataset_id']:[[dataset['dataset_id'],config_xml]]
            }
            increment_id.ret = {'job':'newjob',
                                'task':'newtask',
                               }
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = True
            cb.called = False
            
            num = 10
            self._db.buffer_jobs_tasks(gridspec,num,callback=cb)
            if cb.called is False:
                raise Exception('buffer 1d,1j,1t buffered: callback not called')
            if isinstance(cb.ret,Exception):
                raise Exception('buffer 1d,1j,1t buffered: exception returned %s'%cb.ret)
            
            
            # now try with buffer full
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = True
            cb.called = False
            
            num = 1
            self._db.buffer_jobs_tasks(gridspec,num,callback=cb)
            if cb.called is False:
                raise Exception('buffer 1d,1j,1t buffer full: callback not called')
            if isinstance(cb.ret,Exception):
                raise Exception('buffer 1d,1j,1t buffer full: exception returned %s'%cb.ret)
            
        except Exception as e:
            logger.error('Error running dbmethods buffer_jobs_tasks test - %s',str(e))
            printer('Test dbmethods buffer_jobs_tasks',False)
            raise
        else:
            printer('Test dbmethods buffer_jobs_tasks')

    def test_120_get_queueing_datasets(self):
        """Test get_queueing_datasets"""
        try:
            dataset_id = 'asdfasdf'
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                callback(sql_read_task.ret)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            gridspec = 'lksdf.grid1'
            now = datetime.utcnow()
            dataset = OrderedDict([('dataset_id','lknser834'),
                                   ('name','test dataset'),
                                   ('description','a simple test'),
                                   ('gridspec',gridspec),
                                   ('status','processing'),
                                   ('username','user'),
                                   ('institution','inst'),
                                   ('submit_host','localhost'),
                                   ('priority',0),
                                   ('jobs_submitted',2),
                                   ('trays',1),
                                   ('tasks_submitted',2),
                                   ('start_date',dbmethods.datetime2str(now)),
                                   ('end_date',''),
                                   ('temporary_storage',''),
                                   ('global_storage',''),
                                   ('parent_id','sdf'),
                                   ('stat_keys','[]'),
                                   ('categoryvalue_ids',''),
                                   ('debug',True),
                                  ])
            
            # single dataset
            cb.called = False
            sql_read_task.ret = [dataset.values()]
            
            self._db.get_queueing_datasets(gridspec,callback=cb)
            
            if cb.called is False:
                raise Exception('single dataset: callback not called')
            expected = {dataset['dataset_id']:dataset}
            if cb.ret != expected:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',expected)
                raise Exception('single dataset: callback ret != task')
            if not sql_read_task.sql.startswith('select dataset.* from dataset '):
                raise Exception('single dataset: sql incorrect')
                
            # no dataset
            cb.called = False
            sql_read_task.ret = []
            gridspec = 'lksdf.grid1'
            
            self._db.get_queueing_datasets(gridspec,callback=cb)
            
            if cb.called is False:
                raise Exception('no task: callback not called')
            if cb.ret != {}:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('no dataset: callback ret != {}')
            if not sql_read_task.sql.startswith('select dataset.* from dataset '):
                raise Exception('no dataset: sql incorrect')
            
            # sql error
            cb.called = False
            sql_read_task.ret = Exception('sql error')
            gridspec = 'lksdf.grid1'
            
            self._db.get_queueing_datasets(gridspec,callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_queueing_datasets test - %s',str(e))
            printer('Test dbmethods get_queueing_datasets',False)
            raise
        else:
            printer('Test dbmethods get_queueing_datasets')

    def test_121_get_queueing_tasks(self):
        """Test get_queueing_tasks"""
        try:
            task = OrderedDict([
                    ('task_id','asdf'),
                    ('job_id','nsdf'),
                    ('dataset_id','adnj'),
                    ('gridspec','ggg.g1'),
                    ('name','1'),
                    ('task_status','waiting'),
                    ('debug',True),
                   ])
            task2 = OrderedDict([
                    ('task_id','bgdf'),
                    ('job_id','nsdf'),
                    ('dataset_id','adnj'),
                    ('gridspec','ggg.g1'),
                    ('name','1'),
                    ('task_status','waiting'),
                    ('debug',False),
                   ])
            task3 = OrderedDict([
                    ('task_id','erte'),
                    ('job_id','nsdf'),
                    ('dataset_id','adnj'),
                    ('gridspec','ggg.g1'),
                    ('name','1'),
                    ('task_status','waiting'),
                    ('debug',False),
                   ])
            task4 = OrderedDict([
                    ('task_id','sdtr'),
                    ('job_id','nsdf'),
                    ('dataset_id','adnj'),
                    ('gridspec','ggg.g1'),
                    ('name','1'),
                    ('task_status','waiting'),
                    ('debug',True),
                   ])
            
            def _db_read(conn,sql,bindings,*args):
                _db_read.sql.append(sql)
                _db_read.bindings.append(bindings)
                if bindings[0] in _db_read.task_ret:
                    return _db_read.task_ret[bindings[0]]
                else:
                    raise Exception('sql error')
            def non_blocking_task(cb):
                non_blocking_task.called = True
                cb()
            flexmock(DB).should_receive('_db_read').replace_with(_db_read)
            flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # single dataset
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            _db_read.task_ret = {'adnj':[['adnj',task['task_id'],'',task['task_status']]],
                                 task['task_id']:[task.values()]}
            dataset_prios = {'adnj':1}
            
            self._db.get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)
            
            if cb.called is False:
                raise Exception('single dataset: callback not called')
            ret_should_be = {task['task_id']:task}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('single dataset: callback ret != task')
                
            # no tasks
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            _db_read.task_ret = {'adnj':[]}
            dataset_prios = {'adnj':1}
            
            self._db.get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)
            
            if cb.called is False:
                raise Exception('no task: callback not called')
            ret_should_be = {}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('no task: callback ret != {}')
                
            # no tasks sql error
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            _db_read.task_ret = {}
            dataset_prios = {'adnj':1}
            
            self._db.get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_read error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_read error: callback ret != Exception')
            
            # no dataset_prios
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            dataset_prios = None
            
            try:
                self._db.get_queueing_tasks(dataset_prios,'ggg.g1',1,callback=cb)
            except:
                pass
            else:
                raise Exception('no dataset_prios: exception not raised')
            
            if cb.called is not False:
                raise Exception('no dataset_prios: callback called, but not supposed to be')
                
            # no callback
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            _db_read.task_ret = {'adnj':[['adnj',task['task_id'],'',task['task_status']]],
                                 task['task_id']:[task.values()]}
            dataset_prios = {'adnj':1}
            
            try:
                self._db.get_queueing_tasks(dataset_prios,'ggg.g1',1)
            except:
                pass
            else:
                raise Exception('no callback: exception not raised')
            
            if cb.called is not False:
                raise Exception('no callback: callback called, but not supposed to be')
            
            # several tasks in same dataset
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            _db_read.task_ret = {'adnj':[['adnj',task['task_id'],'',task['task_status']],
                                         ['adnj',task2['task_id'],'',task2['task_status']],
                                         ['adnj',task3['task_id'],'',task3['task_status']],
                                        ],
                                 task['task_id']:[task.values(),task2.values(),task3.values()]}
            dataset_prios = {'adnj':1}
            
            self._db.get_queueing_tasks(dataset_prios,'ggg.g1',3,callback=cb)
            
            if cb.called is False:
                raise Exception('several tasks in same dataset: callback not called')
            ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('several tasks in same dataset: callback ret != task task2 task3')
            
            # several tasks in diff dataset
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            sql1 = [['adnj',task['task_id'],'',task['task_status']],
                    ['nksd',task2['task_id'],'',task2['task_status']],
                    ['nksd',task3['task_id'],'',task3['task_status']],
                   ]
            _db_read.task_ret = {'adnj':sql1,
                                 'nksd':sql1,
                                 task2['task_id']:[task.values(),task2.values(),task3.values()]}
            dataset_prios = {'adnj':.3,'nksd':.7}
            
            self._db.get_queueing_tasks(dataset_prios,'ggg.g1',3,callback=cb)
            
            if cb.called is False:
                raise Exception('several tasks in diff dataset: callback not called')
            ret_should_be = {task['task_id']:task,task2['task_id']:task2,task3['task_id']:task3}
            if cb.ret != ret_should_be:
                logger.info('sql = %r, bindings = %r',_db_read.sql,_db_read.bindings)
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('several tasks in diff dataset: callback ret != task task2 task3')
            
            # priority weighted towards one dataset
            cb.called = False
            _db_read.sql = []
            _db_read.bindings = []
            sql1 = [['adnj',task['task_id'],'',task['task_status']],
                    ['nksd',task2['task_id'],'',task2['task_status']],
                    ['nksd',task3['task_id'],'',task3['task_status']],
                    ['nksd',task4['task_id'],'',task4['task_status']],
                   ]
            _db_read.task_ret = {'adnj':sql1,
                                 'nksd':sql1,
                                 task2['task_id']:[task2.values(),task3.values(),task4.values()]}
            dataset_prios = {'adnj':.2,'nksd':.8}
            
            self._db.get_queueing_tasks(dataset_prios,'ggg.g1',3,callback=cb)
            
            if cb.called is False:
                raise Exception('priority weighting dataset: callback not called')
            ret_should_be = {task2['task_id']:task2,task3['task_id']:task3,task4['task_id']:task4}
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('priority weighting dataset: callback ret != task2 task3 task4')
            
        except Exception as e:
            logger.error('Error running dbmethods get_queueing_tasks test - %s',str(e))
            printer('Test dbmethods get_queueing_tasks',False)
            raise
        else:
            printer('Test dbmethods get_queueing_tasks')


    def test_200_rpc_new_task(self):
        """Test rpc_new_task"""
        try:
            search = OrderedDict([('task_id','gdf'),
                    ('job_id','3ns8'),
                    ('dataset_id','sdj43'),
                    ('gridspec','nsd89n3'),
                    ('name','the_name'),
                    ('job_status','processing'),
                    ('task_status','queued'),
                   ])
            task = OrderedDict([('task_id','gdf'),
                    ('status','queued'),
                    ('prev_status','waiting'),
                    ('error_message',None),
                    ('status_changed',datetime.now()),
                    ('submit_dir',None),
                    ('grid_queue_id',None),
                    ('failures',0),
                    ('evictions',0),
                    ('depends',None),
                   ])
            def blocking_task(cb):
                blocking_task.called = True
                cb()
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                if bindings[0] in sql_read_task.task_ret:
                    callback(sql_read_task.task_ret[bindings[0]])
                else:
                    callback(Exception('sql error'))
            def _db_read(conn,sql,bindings,*args):
                _db_read.sql = sql
                _db_read.bindings = bindings
                if bindings[0] in _db_read.task_ret:
                    return _db_read.task_ret[bindings[0]]
                else:
                    raise Exception('sql error')
            def _db_write(conn,sql,bindings,*args):
                def w(s,b):
                    _db_write.sql.append(s)
                    _db_write.bindings.append(b)
                    if b[0] in _db_write.task_ret:
                        return True
                    else:
                        raise Exception('sql error')
                if isinstance(sql,basestring):
                    return w(sql,bindings)
                elif isinstance(sql,Iterable):
                    ret = None
                    for s,b in izip(sql,bindings):
                        ret = w(s,b)
                    return ret
            flexmock(DB).should_receive('blocking_task').replace_with(blocking_task)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            flexmock(DB).should_receive('_db_read').replace_with(_db_read)
            flexmock(DB).should_receive('_db_write').replace_with(_db_write)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # everything working
            cb.called = False
            _db_read.task_ret = {search['gridspec']:[search.values()]}
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = {'processing':[]}
            sql_read_task.task_ret = {search['dataset_id']:[['configid','somexml']]}
            
            self._db.rpc_new_task(gridspec=search['gridspec'],
                                  platform='platform',
                                  hostname=self.hostname, 
                                  ifaces=None,
                                  callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            ret_should_be = 'somexml'
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('everything working: callback ret != task')
            
            # no queued jobs
            cb.called = False
            _db_read.task_ret = {search['gridspec']:[]}
            
            self._db.rpc_new_task(gridspec=search['gridspec'],
                                  platform='platform',
                                  hostname=self.hostname, 
                                  ifaces=None,
                                  callback=cb)
            
            if cb.called is False:
                raise Exception('no queued jobs: callback not called')
            ret_should_be = None
            if cb.ret != ret_should_be:
                logger.error('cb.ret = %r',cb.ret)
                logger.error('ret should be = %r',ret_should_be)
                raise Exception('no queued jobs: callback ret != task')
            
            # _db_read error
            cb.called = False
            _db_read.task_ret = {}
            
            self._db.rpc_new_task(gridspec=search['gridspec'],
                                  platform='platform',
                                  hostname=self.hostname, 
                                  ifaces=None,
                                  callback=cb)
            
            if cb.called is False:
                raise Exception('_db_read error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_read error: callback ret != Exception')
            
            # _db_read error2
            cb.called = False
            _db_read.task_ret = {search['gridspec']:None}
            
            self._db.rpc_new_task(gridspec=search['gridspec'],
                                  platform='platform',
                                  hostname=self.hostname, 
                                  ifaces=None,
                                  callback=cb)
            
            if cb.called is False:
                raise Exception('_db_read error2: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_read error2: callback ret != Exception')
            
            # _db_write error
            cb.called = False
            _db_read.task_ret = {search['gridspec']:[search.values()]}
            _db_write.task_ret = {}
            
            self._db.rpc_new_task(gridspec=search['gridspec'],
                                  platform='platform',
                                  hostname=self.hostname, 
                                  ifaces=None,
                                  callback=cb)
            
            if cb.called is False:
                raise Exception('_db_write error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_write error: callback ret != Exception')
            
            # sql_read_task error
            cb.called = False
            _db_read.task_ret = {search['gridspec']:[search.values()]}
            _db_write.task_ret = {'processing':[]}
            sql_read_task.task_ret = {}
            
            self._db.rpc_new_task(gridspec=search['gridspec'],
                                  platform='platform',
                                  hostname=self.hostname, 
                                  ifaces=None,
                                  callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error: callback ret != Exception')
            
            # sql_read_task error2
            cb.called = False
            _db_read.task_ret = {search['gridspec']:[search.values()]}
            _db_write.task_ret = {'processing':[]}
            sql_read_task.task_ret = {search['dataset_id']:[]}
            
            self._db.rpc_new_task(gridspec=search['gridspec'],
                                  platform='platform',
                                  hostname=self.hostname, 
                                  ifaces=None,
                                  callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error2: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error2: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods rpc_new_task test - %s',str(e))
            printer('Test dbmethods rpc_new_task',False)
            raise
        else:
            printer('Test dbmethods rpc_new_task')

    def test_201_rpc_finish_task(self):
        """Test rpc_finish_task"""
        try:
            def blocking_task(cb):
                blocking_task.called = True
                cb()
            def _db_read(conn,sql,bindings,*args):
                _db_read.sql = sql
                _db_read.bindings = bindings
                selects = sql[:sql.find('from')].replace('select','')
                selects = ','.join(selects.replace(' ','').split(','))
                if selects in _db_read.task_ret:
                    return _db_read.task_ret[selects]
                else:
                    raise Exception('sql error')
            def _db_write(conn,sql,bindings,*args):
                def w(s,b):
                    _db_write.sql.append(s)
                    _db_write.bindings.append(b)
                    if b[0] in _db_write.task_ret:
                        return True
                    else:
                        raise Exception('sql error')
                if isinstance(sql,basestring):
                    return w(sql,bindings)
                elif isinstance(sql,Iterable):
                    ret = None
                    for s,b in izip(sql,bindings):
                        ret = w(s,b)
                    return ret
            def increment_id(table,conn=None):
                increment_id.table = table
                if table in increment_id.ret:
                    return increment_id.ret[table]
                else:
                    raise Exception('sql error')
            flexmock(DB).should_receive('blocking_task').replace_with(blocking_task)
            flexmock(DB).should_receive('_db_read').replace_with(_db_read)
            flexmock(DB).should_receive('_db_write').replace_with(_db_write)
            flexmock(DB).should_receive('_increment_id_helper').replace_with(increment_id)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # everything working
            cb.called = False
            _db_read.task_ret = {'task_stat_id,task_id':[],
                                 'dataset_id,job_id,jobs_submitted,tasks_submitted':[['d','j',2,2]],
                                 'task_id,task_status':[['task','complete']]}
            increment_id.ret = {'task_stat':'new_task_stat'}
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete','new_task_stat')
            
            stats = {'name1':123123,'name2':968343}
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('read_sql %r %r',_db_read.sql,_db_read.bindings)
                logger.info('write_sql %r %r',_db_write.sql,_db_write.bindings)
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret is Exception')
            
            # distributed job
            cb.called = False
            _db_read.task_ret = {'task_stat_id,task_id':[],
                                 'dataset_id,job_id,jobs_submitted,tasks_submitted':[['d','j',1,2]],
                                 'task_id,task_status':[['task','complete']]}
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete','new_task_stat')
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('distributed job: callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('read_sql %r %r',_db_read.sql,_db_read.bindings)
                logger.info('write_sql %r %r',_db_write.sql,_db_write.bindings)
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('distributed job: callback ret is Exception')
            if _db_write.sql[-1].startswith('update job set status'):
                raise Exception('distributed job: wrongly updated job status')
            
            # set_status error
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('new_task_stat')
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('set_status error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('set_status error: callback ret != Exception')
            
            # _db_read error
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete')
            _db_read.task_ret = {}
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_read error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_read error: callback ret != Exception')
            
            # _db_read error2
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete')
            _db_read.task_ret = {'task_stat_id,task_id':[],
                                 'task_id,task_status':[['task','complete']]}
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_read error2: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_read error2: callback ret != Exception')
            
            # _db_read error3
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete')
            _db_read.task_ret = {'task_stat_id,task_id':[],
                                 'dataset_id,job_id,jobs_submitted,tasks_submitted':[['d','j',2,2]]}
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_read error3: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_read error3: callback ret != Exception')
            
            # _db_write error
            cb.called = False
            _db_read.task_ret = {'task_stat_id,task_id':[],
                                 'dataset_id,job_id,jobs_submitted,tasks_submitted':[['d','j',2,2]],
                                 'task_id,task_status':[['task','complete']]}
            increment_id.ret = {'task_stat':'new_task_stat'}
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete')
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_write error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_write error: callback ret != Exception')
            
            # update stats
            cb.called = False
            _db_read.task_ret = {'task_stat_id,task_id':[['new_task_stat','task']],
                                 'dataset_id,job_id,jobs_submitted,tasks_submitted':[['d','j',2,2]],
                                 'task_id,task_status':[['task','complete']]}
            increment_id.ret = {}
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete',json_encode(stats))
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('update stats: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('update stats: callback ret is Exception')
            
            # _db_write update error
            cb.called = False
            _db_read.task_ret = {'task_stat_id,task_id':[['new_task_stat','task']],
                                 'dataset_id,job_id,jobs_submitted,tasks_submitted':[['d','j',2,2]],
                                 'task_id,task_status':[['task','complete']]}
            increment_id.ret = {}
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('complete',)
            
            self._db.rpc_finish_task('task',stats,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_write update error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_write update error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods rpc_finish_task test - %s',str(e))
            printer('Test dbmethods rpc_finish_task',False)
            raise
        else:
            printer('Test dbmethods rpc_finish_task')

    def test_202_rpc_task_error(self):
        """Test rpc_task_error"""
        try:
            def non_blocking_task(cb):
                non_blocking_task.called = True
                cb()
            def _db_read(conn,sql,bindings,*args):
                _db_read.sql = sql
                _db_read.bindings = bindings
                if bindings[0] in _db_read.task_ret:
                    return _db_read.task_ret[bindings[0]]
                else:
                    raise Exception('sql error')
            def _db_write(conn,sql,bindings,*args):
                def w(s,b):
                    _db_write.sql.append(s)
                    _db_write.bindings.append(b)
                    if b[0] in _db_write.task_ret:
                        return True
                    else:
                        raise Exception('sql error')
                if isinstance(sql,basestring):
                    return w(sql,bindings)
                elif isinstance(sql,Iterable):
                    ret = None
                    for s,b in izip(sql,bindings):
                        ret = w(s,b)
                    return ret
            flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
            flexmock(DB).should_receive('_db_read').replace_with(_db_read)
            flexmock(DB).should_receive('_db_write').replace_with(_db_write)
            flexmock(DB).should_receive('cfg').and_return({'queue':{'max_resets':10}})
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # everything working
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset',)
            _db_read.task_ret  = {'task':[['task',0]]}
            
            self._db.rpc_task_error('task',callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret is Exception')
            
            # failure
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('failed',)
            _db_read.task_ret  = {'task':[['task',9]]}
            
            self._db.rpc_task_error('task',callback=cb)
            
            if cb.called is False:
                raise Exception('failure: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('failure: callback ret is Exception')
            
            # sql_read_task error
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset',)
            _db_read.task_ret  = {}
            
            self._db.rpc_task_error('task',callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error: callback ret != Exception')
            
            # sql_read_task error2
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = ('reset',)
            _db_read.task_ret  = {'task':[]}
            
            self._db.rpc_task_error('task',callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error2: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error2: callback ret != Exception')
            
            # sql_write_task error
            cb.called = False
            _db_write.sql = []
            _db_write.bindings = []
            _db_write.task_ret = {}
            _db_read.task_ret  = {'task':[['task',0]]}
            
            self._db.rpc_task_error('task',callback=cb)
            
            if cb.called is False:
                raise Exception('sql_write_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_write_task error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods rpc_task_error test - %s',str(e))
            printer('Test dbmethods rpc_task_error',False)
            raise
        else:
            printer('Test dbmethods rpc_task_error')

    def test_203_rpc_upload_logfile(self):
        """Test rpc_upload_logfile"""
        try:
            def blocking_task(cb):
                blocking_task.called = True
                cb()
            def non_blocking_task(cb):
                non_blocking_task.called = True
                cb()
            def _db_read(conn,sql,bindings,*args):
                _db_read.sql = sql
                _db_read.bindings = bindings
                if bindings[0] in _db_read.task_ret:
                    return _db_read.task_ret[bindings[0]]
                else:
                    raise Exception('sql error')
            def _db_write(conn,sql,bindings,*args):
                _db_write.sql = sql
                _db_write.bindings = bindings
                if _db_write.task_ret is not None:
                    return _db_write.task_ret
                else:
                    raise Exception('sql error')
            def increment_id(table,conn=None):
                increment_id.table = table
                if table in increment_id.ret:
                    return increment_id.ret[table]
                else:
                    raise Exception('sql error')
            flexmock(DB).should_receive('blocking_task').replace_with(blocking_task)
            flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
            flexmock(DB).should_receive('_db_read').replace_with(_db_read)
            flexmock(DB).should_receive('_db_write').replace_with(_db_write)
            flexmock(DB).should_receive('_increment_id_helper').replace_with(increment_id)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # everything working
            cb.called = False
            _db_read.task_ret = {'task':[]}
            increment_id.ret = {'task_log':'new_task_log'}
            _db_write.task_ret = []
            
            name = 'logfile'
            data = 'thelogfiledata'
            self._db.rpc_upload_logfile('task',name,data,callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret is Exception')
            
            # _db_read error
            cb.called = False
            _db_read.task_ret = None
            
            self._db.rpc_upload_logfile('task',name,data,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_read error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_read error: callback ret != Exception')
            
            # _db_write error
            cb.called = False
            _db_read.task_ret = {'task':[]}
            increment_id.ret = {'task_log':'new_task_log'}
            _db_write.task_ret = None
            
            self._db.rpc_upload_logfile('task',name,data,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_write error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_write error: callback ret != Exception')
            
            # update stats
            cb.called = False
            _db_read.task_ret = {'task':[['new_task_log','task']]}
            increment_id.ret = {}
            _db_write.task_ret = []
            
            self._db.rpc_upload_logfile('task',name,data,callback=cb)
            
            if cb.called is False:
                raise Exception('update stats: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('update stats: callback ret is Exception')
            
            # _db_write update error
            cb.called = False
            _db_read.task_ret = {'task':[['new_task_log','task']]}
            increment_id.ret = {}
            _db_write.task_ret = None
            
            self._db.rpc_upload_logfile('task',name,data,callback=cb)
            
            if cb.called is False:
                raise Exception('_db_write update error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('_db_write update error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods rpc_upload_logfile test - %s',str(e))
            printer('Test dbmethods rpc_upload_logfile',False)
            raise
        else:
            printer('Test dbmethods rpc_upload_logfile')

    def test_204_rpc_stillrunning(self):
        """Test rpc_stillrunning"""
        try:
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
            
            # processing
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','processing']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('processing: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('processing: callback ret is Exception')
            if cb.ret != True:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('processing: callback ret != True')
            
            # queued
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','queued']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('queued: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('queued: callback ret is Exception')
            if cb.ret != True:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('queued: callback ret != True')
            
            # reset
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','reset']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('reset: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('reset: callback ret is Exception')
            if cb.ret != False:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('reset: callback ret != False')
            
            # resume
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','resume']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('resume: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('resume: callback ret is Exception')
            if cb.ret != False:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('resume: callback ret != False')
            
            # suspended
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','suspended']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('suspended: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('suspended: callback ret is Exception')
            if cb.ret != False:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('suspended: callback ret != False')
            
            # failed
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','failed']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('failed: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('failed: callback ret is Exception')
            if cb.ret != False:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('failed: callback ret != False')
            
            # waiting
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','waiting']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('waiting: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('waiting: callback ret is Exception')
            if cb.ret != False:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('waiting: callback ret != False')
            
            # complete
            cb.called = False
            sql_read_task.task_ret = {'task':[['task','complete']]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('complete: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('complete: callback ret is Exception')
            if cb.ret != False:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('complete: callback ret != False')
            
            # sql error
            cb.called = False
            sql_read_task.task_ret = {}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('sql error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql error: callback ret != Exception')
            
            # sql error2
            cb.called = False
            sql_read_task.task_ret = {'task':[]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('sql error2: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql error2: callback ret != Exception')
            
            # sql error3
            cb.called = False
            sql_read_task.task_ret = {'task':[[]]}
            
            self._db.rpc_stillrunning('task',callback=cb)
            
            if cb.called is False:
                raise Exception('sql error3: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql error3: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods rpc_stillrunning test - %s',str(e))
            printer('Test dbmethods rpc_stillrunning',False)
            raise
        else:
            printer('Test dbmethods rpc_stillrunning')

    def test_300_new_upload(self):
        """Test new_upload"""
        try:
            def blocking_task(cb):
                blocking_task.called = True
                cb()
            def increment_id(table,conn=None):
                increment_id.table = table
                if table in increment_id.ret:
                    return increment_id.ret[table]
                else:
                    raise Exception('sql error')
            def sql_write_task(sql,bindings,callback):
                sql_write_task.sql = sql
                sql_write_task.bindings = bindings
                if bindings[0] in sql_write_task.task_ret:
                    callback(sql_write_task.task_ret[bindings[0]])
                else:
                    callback(Exception('sql error'))
            flexmock(DB).should_receive('blocking_task').replace_with(blocking_task)
            flexmock(DB).should_receive('increment_id').replace_with(increment_id)
            flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # everything working
            cb.called = False
            increment_id.ret = {'upload':24}
            sql_write_task.task_ret = {24:[]}
            
            args = ('url',10293,'cksum','type')
            self._db.new_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret is Exception')
            if not cb.ret:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret is empty')
            
            # sql_write_task error
            cb.called = False
            increment_id.ret = {'upload':24}
            sql_write_task.task_ret = {}
            
            args = ('url',10293,'cksum','type')
            self._db.new_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_write_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_write_task error: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods new_upload test - %s',str(e))
            printer('Test dbmethods new_upload',False)
            raise
        else:
            printer('Test dbmethods new_upload')

    def test_301_is_upload_addr(self):
        """Test is_upload_addr"""
        try:
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
            uid = '9af37cb1'
            cb.called = False
            sql_read_task.task_ret = {uid:[['id','url',uid,10293,
                                            'cksum','type','uploading',1]]}
            
            self._db.is_upload_addr(uid,callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if cb.ret is not True:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret != True')
            
            # sql_read_task error
            cb.called = False
            sql_read_task.task_ret = {}
            
            self._db.is_upload_addr(uid,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error: callback ret != Exception')
            
            # sql_read_task error2
            cb.called = False
            sql_read_task.task_ret = {uid:[]}
            
            self._db.is_upload_addr(uid,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error2: callback not called')
            if cb.ret is not False:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error2: callback ret != False')
            
        except Exception as e:
            logger.error('Error running dbmethods is_upload_addr test - %s',str(e))
            printer('Test dbmethods is_upload_addr',False)
            raise
        else:
            printer('Test dbmethods is_upload_addr')

    def test_302_handle_upload(self):
        """Test handle_upload"""
        try:
            cfg = {'webserver':{'tmp_upload_dir':os.path.join(self.test_dir,'tmpupload'),
                                'upload_dir':os.path.join(self.test_dir,'upload'),
                                'static_dir':os.path.join(self.test_dir,'static'),
                                'proxycache_dir':os.path.join(self.test_dir,'proxy'),
                               },
                  }
            # make sure directories are set up properly
            for d in cfg['webserver']:
                if '_dir' in d:
                    path = cfg['webserver'][d]
                    path = os.path.expanduser(os.path.expandvars(path))
                    try:
                        os.makedirs(path)
                    except:
                        pass
            def non_blocking_task(cb):
                non_blocking_task.called = True
                cb()
            def sql_write_task(sql,bindings,callback):
                sql_write_task.sql = sql
                sql_write_task.bindings = bindings
                if bindings[0] in sql_write_task.task_ret:
                    callback(sql_write_task.task_ret[bindings[0]])
                else:
                    callback(Exception('sql error'))
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                if bindings[0] in sql_read_task.task_ret:
                    callback(sql_read_task.task_ret[bindings[0]])
                else:
                    callback(Exception('sql error'))
            def increment_id(table,conn=None):
                increment_id.table = table
                if table in increment_id.ret:
                    return increment_id.ret[table]
                else:
                    raise Exception('sql error')
            flexmock(DB).should_receive('non_blocking_task').replace_with(non_blocking_task)
            flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            flexmock(DB).should_receive('increment_id').replace_with(increment_id)
            flexmock(DB).should_receive('cfg').and_return(cfg)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            # make test data file
            filename = str(random.randint(0,10000))
            filecontents = os.urandom(10**3)
            dest_path = os.path.join(self.test_dir,filename)
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            # everything working
            uid = '9af37cb1'
            cb.called = False
            sql_read_task.task_ret = {uid:[['id',self.hostname+'/url',uid,10293,
                                            'cksum','type','uploading',1]]}
            sql_write_task.task_ret = {'uploaded':[[]],'cacheid':[[]]}
            increment_id.ret = {'cache':'cacheid'}
            outfile = os.path.join(cfg['webserver']['proxycache_dir'],filename)
            
            args = (uid,'filename','content_type',dest_path,self.hostname)
            self._db.handle_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if cb.ret is not True:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret != True')
            if os.path.exists(dest_path):
                raise Exception('everything working: file not moved')
            if not os.path.exists(outfile):
                raise Exception('everything working: file not at destination')
            try:
                os.remove(outfile)
            except:
                pass
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            # sql_read_task error
            cb.called = False
            sql_read_task.task_ret = {}
            sql_write_task.task_ret = {'uploaded':[[]],'cacheid':[[]]}
            increment_id.ret = {'cache':'cacheid'}
            outfile = os.path.join(cfg['webserver']['proxycache_dir'],filename)
            
            args = (uid,'filename','content_type',dest_path,self.hostname)
            self._db.handle_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error: callback ret != Exception')
            try:
                os.remove(outfile)
            except:
                pass
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            # sql_read_task error2
            cb.called = False
            sql_read_task.task_ret = {uid:[]}
            sql_write_task.task_ret = {'uploaded':[[]],'cacheid':[[]]}
            increment_id.ret = {'cache':'cacheid'}
            outfile = os.path.join(cfg['webserver']['proxycache_dir'],filename)
            
            args = (uid,'filename','content_type',dest_path,self.hostname)
            self._db.handle_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error2: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error2: callback ret != Exception')
            try:
                os.remove(outfile)
            except:
                pass
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            # sql_write_task error
            cb.called = False
            sql_read_task.task_ret = {uid:[['id',self.hostname+'/url',uid,10293,
                                            'cksum','type','uploading',1]]}
            sql_write_task.task_ret = {}
            increment_id.ret = {'cache':'cacheid'}
            outfile = os.path.join(cfg['webserver']['proxycache_dir'],filename)
            
            args = (uid,'filename','content_type',dest_path,self.hostname)
            self._db.handle_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_write_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_write_task error: callback ret != Exception')
            try:
                os.remove(outfile)
            except:
                pass
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            # sql_write_task error3
            cb.called = False
            sql_read_task.task_ret = {uid:[['id',self.hostname+'/url',uid,10293,
                                            'cksum','type','uploading',1]]}
            sql_write_task.task_ret = {'uploaded':[[]]}
            increment_id.ret = {'cache':'cacheid'}
            outfile = os.path.join(cfg['webserver']['proxycache_dir'],filename)
            
            args = (uid,'filename','content_type',dest_path,self.hostname)
            self._db.handle_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_write_task error3: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_write_task error3: callback ret != Exception')
            try:
                os.remove(outfile)
            except:
                pass
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            # reupload
            cb.called = False
            sql_read_task.task_ret = {uid:[['id','otherhost/url',uid,10293,
                                            'cksum','type','uploading',1]]}
            sql_write_task.task_ret = {'still uploading':[[]],'uploaded':[[]]}
            outfile = os.path.join(cfg['webserver']['upload_dir'],filename)
            
            args = (uid,'filename','content_type',dest_path,self.hostname)
            self._db.handle_upload(*args,callback=cb)
            
            for _ in xrange(100):
                if sql_write_task.bindings[0] != 'uploaded':
                    time.sleep(0.1)
                else:
                    break
            if sql_write_task.bindings[0] != 'uploaded':
                raise Exception('reupload: did not finish site to site')
            if cb.called is False:
                raise Exception('reupload: callback not called')
            if cb.ret is not True:
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('reupload: callback ret != True')
            if os.path.exists(dest_path):
                raise Exception('reupload: file not moved')
            if not os.path.exists(outfile):
                raise Exception('reupload: file not at destination')
            try:
                os.remove(outfile)
            except:
                pass
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
            # reupload write error
            cb.called = False
            sql_read_task.task_ret = {uid:[['id','otherhost/url',uid,10293,
                                            'cksum','type','uploading',1]]}
            sql_write_task.task_ret = {}
            outfile = os.path.join(cfg['webserver']['upload_dir'],filename)
            
            args = (uid,'filename','content_type',dest_path,self.hostname)
            self._db.handle_upload(*args,callback=cb)
            
            if cb.called is False:
                raise Exception('reupload write error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('reupload write error: callback ret != Exception')
            try:
                os.remove(outfile)
            except:
                pass
            with open(dest_path,'w') as f:
                f.write(filecontents)
            
        except Exception as e:
            logger.error('Error running dbmethods handle_upload test - %s',str(e))
            printer('Test dbmethods handle_upload',False)
            raise
        else:
            printer('Test dbmethods handle_upload')

    def test_303_check_upload(self):
        """Test check_upload"""
        try:
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
            url = '/theurl'
            cb.called = False
            sql_read_task.task_ret = {url:[['ac92e7','uploading']]}
            
            self._db.check_upload(url,callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if cb.ret != 'uploading':
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret != uploading')
            
            # sql_read_task error
            cb.called = False
            sql_read_task.task_ret = {}
            
            self._db.check_upload(url,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error: callback ret != Exception')
            
            # sql_read_task error2
            cb.called = False
            sql_read_task.task_ret = {url:[]}
            
            self._db.check_upload(url,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error2: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error2: callback ret != Exception')
            
            # sql_read_task error3
            cb.called = False
            sql_read_task.task_ret = {url:[['ac92e7']]}
            
            self._db.check_upload(url,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error3: callback not called')
            if not isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('sql_read_task error3: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods check_upload test - %s',str(e))
            printer('Test dbmethods check_upload',False)
            raise
        else:
            printer('Test dbmethods check_upload')
    
    def test_400_new_passkey(self):
        """Test new_passkey"""
        try:
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
            
            self._db.new_passkey(callback=cb)
            
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
            
            self._db.new_passkey(exp,callback=cb)
            
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
            
            self._db.new_passkey(exp,callback=cb)
            
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
                self._db.new_passkey(exp,callback=cb)
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
            
            self._db.new_passkey(exp,callback=cb)
            
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
                self._db.new_passkey(exp,callback=cb)
            except:
                pass
            else:
                raise Exception('increment_id error: did not raise Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods new_passkey test - %s',str(e))
            printer('Test dbmethods new_passkey',False)
            raise
        else:
            printer('Test dbmethods new_passkey')
    
    def test_401_get_passkey(self):
        """Test get_passkey"""
        try:
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
            
            self._db.get_passkey(key,callback=cb)
            
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
                self._db.get_passkey(key,callback=cb)
            except:
                pass
            else:
                raise Exception('passkey error: did not raise Exception')
            
            # sql_read_task error
            key = 'thekey'
            exp = 'expiration'
            cb.called = False
            sql_read_task.task_ret = {}
            
            self._db.get_passkey(key,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql_read_task error: callback ret != Exception')
            
            # sql_read_task error2
            key = 'thekey'
            exp = 'expiration'
            cb.called = False
            sql_read_task.task_ret = {key:[]}
            
            self._db.get_passkey(key,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error2: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql_read_task error2: callback ret != Exception')
            
            # sql_read_task error3
            key = 'thekey'
            exp = 'expiration'
            cb.called = False
            sql_read_task.task_ret = {key:[['id','key']]}
            
            self._db.get_passkey(key,callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error3: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql_read_task error3: callback ret != Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_passkey test - %s',str(e))
            printer('Test dbmethods get_passkey',False)
            raise
        else:
            printer('Test dbmethods get_passkey')


    def test_600_cron_dataset_completion(self):
        """Test cron_dataset_completion"""
        try:
            def sql_read_task(sql,bindings,callback):
                sql_read_task.sql = sql
                sql_read_task.bindings = bindings
                if bindings[0] in sql_read_task.task_ret:
                    callback(sql_read_task.task_ret[bindings[0]])
                else:
                    callback(Exception('sql error'))
            def sql_write_task(sql,bindings,callback):
                sql_write_task.sql = sql
                sql_write_task.bindings = bindings
                if isinstance(sql,Iterable):
                    bindings = bindings[0]
                if bindings[0] in sql_write_task.task_ret:
                    callback(sql_write_task.task_ret[bindings[0]])
                else:
                    callback(Exception('sql error'))
            flexmock(DB).should_receive('sql_read_task').replace_with(sql_read_task)
            flexmock(DB).should_receive('sql_write_task').replace_with(sql_write_task)
            
            def cb(ret):
                cb.called = True
                cb.ret = ret
            cb.called = False
            
            datasets = [['d1',1,1],
                        ['d2',2,4],
                        ['d3',3,9],
                        ['d4',1,1]]
            status = [[datasets[0][0],'complete'],
                      [datasets[1][0],'complete'],
                      [datasets[1][0],'complete'],
                      [datasets[1][0],'complete'],
                      [datasets[1][0],'complete'],
                      [datasets[2][0],'complete'],
                      [datasets[2][0],'complete'],
                      [datasets[2][0],'complete'],
                      [datasets[2][0],'failed'],
                      [datasets[2][0],'failed'],
                      [datasets[2][0],'failed'],
                      [datasets[2][0],'complete'],
                      [datasets[2][0],'complete'],
                      [datasets[2][0],'complete'],
                      [datasets[3][0],'suspended']
                     ]
            
            # everything working
            cb.called = False
            sql_read_task.task_ret = {'processing':datasets[0:1],
                                      datasets[0][0]:status[0:1]}
            sql_write_task.task_ret = {'complete':{}}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('everything working: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('everything working: callback ret is Exception')
            
            # no processing datasets
            cb.called = False
            sql_read_task.task_ret = {'processing':[]}
            sql_write_task.task_ret = {}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('no processing datasets: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('no processing datasets: callback ret is Exception')
            
            # tasks not completed
            cb.called = False
            sql_read_task.task_ret = {'processing':datasets[0:1],
                                      datasets[0][0]:[[datasets[0][0],'processing']]}
            sql_write_task.task_ret = {}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('tasks not completed: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('tasks not completed: callback ret is Exception')
            
            # sql_read_task error
            cb.called = False
            sql_read_task.task_ret = {}
            sql_write_task.task_ret = {'complete':{}}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql_read_task error: callback ret != Exception')
            
            # sql_read_task error2
            cb.called = False
            sql_read_task.task_ret = {'processing':datasets[0:1]}
            sql_write_task.task_ret = {'complete':{}}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('sql_read_task error2: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql_read_task error2: callback ret != Exception')
            
            # sql_write_task error
            cb.called = False
            sql_read_task.task_ret = {'processing':datasets[0:1],
                                      datasets[0][0]:status[0:1]}
            sql_write_task.task_ret = {}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('sql_write_task error: callback not called')
            if not isinstance(cb.ret,Exception):
                raise Exception('sql_write_task error: callback ret != Exception')
            
            # multiple datasets of same status
            cb.called = False
            sql_read_task.task_ret = {'processing':datasets[0:2],
                                      datasets[0][0]:status[0:5]}
            sql_write_task.task_ret = {'complete':{}}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('multiple datasets of same status: callback not called')
            if isinstance(cb.ret,Exception):
                logger.info('read_sql %r %r',sql_read_task.sql,sql_read_task.bindings)
                logger.info('write_sql %r %r',sql_write_task.sql,sql_write_task.bindings)
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('multiple datasets of same status: callback ret is Exception')
            
            # multiple datasets of different status
            cb.called = False
            sql_read_task.task_ret = {'processing':datasets,
                                      datasets[0][0]:status}
            sql_write_task.task_ret = {'complete':{},
                                       'errors':{},
                                       'suspended':{}}
            
            self._db.cron_dataset_completion(callback=cb)
            
            if cb.called is False:
                raise Exception('multiple datasets of different status: callback not called')
            if isinstance(cb.ret,Exception):
                logger.error('cb.ret = %r',cb.ret)
                raise Exception('multiple datasets of different status: callback ret is Exception')
            
        except Exception as e:
            logger.error('Error running dbmethods get_passkey test - %s',str(e))
            printer('Test dbmethods get_passkey',False)
            raise
        else:
            printer('Test dbmethods get_passkey')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_test))
    return suite
