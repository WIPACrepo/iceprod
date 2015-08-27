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


class dbmethods_misc_test(dbmethods_base):
    @unittest_reporter
    def test_020_in_cache(self):
        """Test in_cache"""
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

    @unittest_reporter
    def test_021_remove_from_cache(self):
        """Test remove_from_cache"""
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

    @unittest_reporter
    def test_022_get_cache_checksum(self):
        """Test get_cache_checksum"""
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

    @unittest_reporter
    def test_023_get_cache_size(self):
        """Test get_cache_size"""
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

    @unittest_reporter
    def test_025_check_cache_space(self):
        """Test check_cache_space"""
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

    @unittest_reporter
    def test_029_add_to_cache(self):
        """Test add_to_cache"""
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

    @unittest_reporter
    def test_300_new_upload(self):
        """Test new_upload"""
        def blocking_task(name,cb):
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

    @unittest_reporter
    def test_301_is_upload_addr(self):
        """Test is_upload_addr"""
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

    @unittest_reporter
    def test_302_handle_upload(self):
        """Test handle_upload"""
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

    @unittest_reporter
    def test_303_check_upload(self):
        """Test check_upload"""
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


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_misc_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_misc_test))
    return suite
