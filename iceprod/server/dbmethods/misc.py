"""
Misc database methods
"""

import os
import logging
from datetime import datetime,timedelta
from functools import partial,reduce
import operator
from collections import OrderedDict, Iterable
import math
import uuid
import shutil
from io import BytesIO

import iceprod.core.functions
from iceprod.core.util import Node_Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import dbmethod,_Methods_Base,datetime2str,str2datetime

logger = logging.getLogger('dbmethods.misc')

class misc(_Methods_Base):
    """
    misc DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    ## Proxying ##

    @dbmethod
    def in_cache(self,url,callback=None):
        """Get whether or not the url is in the cache.
        Returns (incache,uid) tuple"""
        sql = 'select uid from cache where permanent_url = ?'
        bindings = (url,)
        cb = partial(self._in_cache_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _in_cache_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret,None)
            elif ret is None or len(ret) < 1:
                callback(False,None)
            else:
                callback(True,ret[0][0])

    @dbmethod
    def remove_from_cache(self,url,callback=None):
        """Delete the url from the cache.
        Returns (False,None) tuple"""
        sql = 'delete from cache where permanent_url = ?'
        bindings = (url,)
        cb = partial(self._remove_from_cache_callback,callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _remove_from_cache_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret,None)
            else:
                callback(False,None)

    @dbmethod
    def get_cache_checksum(self,url,callback=None):
        """Get checksum if url is in the cache.
        Returns (incache,checksum) tuple"""
        sql = 'select checksum,checksum_type from cache where permanent_url = ?'
        bindings = (url,)
        cb = partial(self._get_cache_checksum_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_cache_checksum_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret,None)
            elif ret is None or len(ret) < 1:
                callback(False,None)
            else:
                callback(True,ret[0][0],ret[0][1])

    @dbmethod
    def get_cache_size(self,url,callback=None):
        """Get size if url is in the cache.
        Returns (incache,size) tuple"""
        sql = 'select size from cache where permanent_url = ?'
        bindings = (url,)
        cb = partial(self._get_cache_size_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_cache_size_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret,None)
            elif ret is None or len(ret) < 1:
                callback(False,None)
            else:
                callback(True,ret[0][0])

    @dbmethod
    def check_cache_space(self,downloaddir,size,priority=5,url=None,callback=None):
        """Check space on disk and potentially in use from downloading.
        Allocate space for the current download if possible.
        Returns True/False"""
        if not isinstance(priority,(int,long)):
            raise Exception('priority is not a number')
        cb = partial(self._check_cache_space_callback,downloaddir,size,priority,url,callback=callback)
        self.db.non_blocking_task(iceprod.core.functions.freespace,downloaddir,callback=cb)
    def _check_cache_space_callback(self,downloaddir,size,priority,url,ret,callback=None):
        # leave 1GB free space on the disk
        sizedisk = ret - 1024*1024*1024
        if sizedisk < 0:
            # disk already full
            callback(False)
        # get the currently downloading file sizes
        sql = 'select sum(size) as s from download'
        bindings = tuple()
        cb = partial(self._check_cache_space_callback2,downloaddir,size,sizedisk,priority,url,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _check_cache_space_callback2(self,downloaddir,sizerequest,sizedisk,priority,url,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if len(ret) < 1:
                callback(Exception('Failed to get downloading table size'))
            else:
                sizedownloading = ret[0][0]
                # get the size of the files in the cache, grouped by priority
                sql = 'select delete_priority, sum(size) as s from cache group by delete_priority'
                bindings = tuple()
                cb = partial(self._check_cache_space_callback3,downloaddir,sizerequest,sizedisk,sizedownloading,priority,url,callback=callback)
                self.db.sql_read_task(sql,bindings,callback=cb)
    def _check_cache_space_callback3(self,downloaddir,sizerequest,sizedisk,sizedownloading,priority,url,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if len(ret) < 1:
                sizecache = {}
                sizecacheworsepriority = 0
            else:
                sizecache = {row[0]:row[1] for row in ret}
                sizecacheworsepriority = sum([sizecache[p] for p in sizecache if p >= priority])
            if (sizerequest < sizedisk-sizedownloading-sizecacheworsepriority):
                # request can fit on disk
                callback(True)
            elif (sizerequest >= sizedisk-sizedownloading):
                # disk already full from downloading items, so request denied
                callback(False)
            else:
                # must remove worse priority files from cache first
                ndelete = 10
                priority_delete_list = sorted([p for p in sizecache.keys() if p >= priority],reverse=True)
                def delete_files(p2,ret):
                    # ret is size of priority grouping
                    if ret is None or len(ret) < 1 or len(ret[0]) < 1:
                        callback(Exception('Failed to delete from cache table. Returned None'))
                    size = ret[0][0]
                    if size <= 0.0:
                        del sizecache[p2]
                        del priority_delete_list[p2]
                    else:
                        sizecache[p2] = size
                    sizecacheworsepriority = sum([sizecache[p] for p in sizecache if p >= priority])
                    if (sizerequest < sizedisk-sizedownloading-sizecacheworsepriority):
                        # request can fit on disk
                        callback(True)
                    else:
                        # continue deleting files
                        self._check_cache_space_delete(downloaddir,priority_delete_list[0],ndelete,callback=delete_files)
                self._check_cache_space_delete(downloaddir,priority_delete_list[0],ndelete,callback=delete_files)
    def _check_cache_space_delete(self,downloaddir,priority,number,callback=None):
        """delete n items from priority, returning the size of the priority group remaining"""
        def cb2(ret):
            # return remaining size of priority group
            sql = 'select sum(size) as s from cache where delete_priority = ?'
            bindings = (priority,)
            cb3 = partial(callback,priority)
            self.db.sql_read_task(sql,bindings,callback=cb3)
        def cb(ret):
            if len(ret) < 1:
                callback(priority,None)
            uids = {row[0] for row in ret}
            # delete files from disk
            try:
                for u in uids:
                    os.remove(os.path.join(downloaddir,u))
            except Exception as e:
                logger.warning('cannot remove file while creating cache space: %s',str(e))
                callback(priority,None)
            else:
                # delete from database
                sql = 'delete from cache where delete_priority = ? and uid in (?)'
                bindings = (priority,','.join(uids))
                self.db.sql_write_task(sql,bindings,callback=cb2)
        # get n items to delete
        sql = 'select uid from cache where delete_priority = ? limit ?'
        bindings = (priority,)
        cb3 = partial(callback,priority)
        self.db.sql_read_task(sql,bindings,callback=cb)

    @dbmethod
    def add_to_cache(self,url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
        """Add file to cache after it is uploaded or downloaded"""
        sql = 'insert into cache (cache_id,permanent_url,uid,size,checksum,'
        sql += 'checksum_type,delete_priority) values (?,?,?,?,?,?,?)'
        cache_id = self.db.increment_id('cache')
        bindings = (cache_id,url,uid,size,checksum,priority)
        self.db.sql_write_task(sql,bindings,callback=callback)


    ### upload functions ###

    @dbmethod
    def new_upload(self,url,size,checksum,checksum_type,callback=None):
        """Allocate a new upload if possible"""
        cb = partial(self._new_upload_blocking,url,size,checksum,
                     checksum_type,callback=callback)
        self.db.blocking_task('new_upload',cb)
    def _new_upload_blocking(self,url,size,checksum,checksum_type,callback=None):
        uid = uuid.uuid4().hex
        id = self.db.increment_id('upload')
        sql = 'insert into upload (upload_id,permanent_url,uid,size,checksum,'
        sql += 'checksum_type,status,delete_priority) values (?,?,?,?,?,?,?,?)'
        bindings = (id,url,uid,size,checksum,checksum_type,'uploading',1)
        cb = partial(self._new_upload_callback,uid,callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _new_upload_callback(self,uid,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(uid)

    @dbmethod
    def is_upload_addr(self,uid,callback=None):
        """Test to see if this address is a valid upload address"""
        sql = 'select * from upload where uid = ? and status = "uploading"'
        bindings = (uid,)
        cb = partial(self._is_upload_addr_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _is_upload_addr_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif ret and ret[0]:
            callback(True)
        else:
            callback(False)

    @dbmethod
    def handle_upload(self,uid,name,content_type,path,host,callback=None):
        """Handle an uploaded file, moving it from tmp space to
           a more permanent location.
        """
        sql = 'select * from upload where uid = ?'
        bindings = (uid,)
        cb = partial(self._handle_upload_callback,uid,path,host,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _handle_upload_callback(self,uid,path,host,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret:
            callback(Exception('sql error in handle_upload'))
        else:
            try:
                upload = self._list_to_dict('upload',ret[0])
            except:
                logger.warn('error coverting sql ret to dict')
                pass

            # move file out of the tmp upload directory
            if host in upload['permanent_url']:
                # save to cache permanently
                dest = self.db.cfg['webserver']['proxycache_dir']
                status = 'uploaded'
                cb = partial(self._handle_upload_cache_callback,upload,
                             callback=callback)
            else:
                # upload somewhere else
                dest = self.db.cfg['webserver']['upload_dir']
                status = 'still uploading'
                cb = partial(self._handle_upload_reupload_callback,upload,
                             os.path.join(dest,uid), callback=callback)
            shutil.move(path,os.path.expanduser(os.path.expandvars(dest)))
            sql = 'update upload set status = ? '
            sql += ' where uid = ?'
            bindings = (status,uid)
            self.db.sql_write_task(sql,bindings,callback=cb)
    def _handle_upload_cache_callback(self,upload,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            # move to cache table
            id = self.db.increment_id('cache')
            sql = 'insert into cache (cache_id,permanent_url,uid,size,'
            sql += 'checksum,checksum_type,delete_priority) '
            sql += ' values (?,?,?,?,?,?,?)'
            bindings = (id,upload['permanent_url'],upload['uid'],
                        upload['size'],upload['checksum'],
                        upload['checksum_type'],upload['delete_priority'])
            cb = partial(self._handle_upload_cache_callback2,
                         callback=callback)
            self.db.sql_write_task(sql,bindings,callback=cb)
    def _handle_upload_cache_callback2(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(True)
    def _handle_upload_reupload_callback(self,upload,path,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            # send callback early, then continue working
            callback(True)

            # upload to another site
            cb2 = partial(self._handle_upload_reupload_callback2,upload['uid'])
            cb = partial(self.misc_site_to_site_upload, path,
                         upload['permanent_url'], callback=cb2)
            self.db.non_blocking_task(cb)
    def _handle_upload_reupload_callback2(self,uid,ret=None):
        if not isinstance(ret,Exception):
            sql = 'update upload set status = ? '
            sql += ' where uid = ?'
            bindings = ('uploaded',uid)
            def cb(ret2):
                pass
            self.db.sql_write_task(sql,bindings,callback=cb)

    @dbmethod
    def check_upload(self,url,callback=None):
        """Check an upload's status"""
        sql = 'select uid,status from upload where url = ?'
        bindings = (url,)
        cb = partial(self._check_upload_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _check_upload_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or len(ret[0]) < 2:
            callback(Exception('sql error in check_upload'))
        else:
            callback(ret[0][1])


    ### site to site ###

    @dbmethod
    def misc_site_to_site_upload(self,src,dest,callback=None):
        if callback:
            callback()
        # TODO: actually write this method

    @dbmethod
    def misc_get_tables_for_task(self,task_ids,callback=None):
        """
        Get all tables necessary to run task(s).

        :param task_ids: Either a single, or an iterable of task_ids.
        :returns: (via callback) dict of table entries.
        """
        if isinstance(task_ids,str):
            task_ids = [task_ids]
        elif not isinstance(task_ids,Iterable):
            callback(Exception('task_ids not Iterable'))
        else:
            cb = partial(self._misc_get_tables_for_task_blocking,task_ids,
                         callback=callback)
            self.db.non_blocking_task(cb)
    def _misc_get_tables_for_task_blocking(self,task_ids,callback=None):
        conn,archive_conn = self.db._dbsetup()
        tables = {}

        sql = 'select * from search where task_id in ('
        sql += ','.join('?' for _ in task_ids)
        sql += ')'
        bindings = tuple(task_ids)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if not ret:
            callback({})
            return

        search_table = {}
        keys = []
        for row in ret:
            search_table[row[0]] = self._list_to_dict('search',row)
            if not keys:
                keys = search_table[row[0]].keys()
        tables['search'] = {'keys':keys,'values':ret}

        sql = 'select * from task where task_id in ('
        sql += ','.join('?' for _ in task_ids)
        sql += ')'
        bindings = tuple(task_ids)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        task_rel_ids = set()
        if ret:
            keys = []
            for row in ret:
                row2 = self._list_to_dict('task',row)
                task_rel_ids.add(row2['task_rel_id'])
                if not keys:
                    keys = row2.keys()
            tables['task'] = {'keys':keys,'values':ret}

        sql = 'select * from task_rel where task_rel_id in ('
        sql += ','.join('?' for _ in task_rel_ids)
        sql += ')'
        bindings = tuple(task_rel_ids)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if ret:
            keys = []
            for row in ret:
                if not keys:
                    keys = self._list_to_dict('task',row).keys()
                    break
            tables['task'] = {'keys':keys,'values':ret}

        job_ids = set(search_table[id]['job_id'] for id in task_ids)
        sql = 'select * from job where job_id in ('
        sql += ','.join('?' for _ in job_ids)
        sql += ')'
        bindings = tuple(job_ids)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if ret:
            keys = []
            for row in ret:
                if not keys:
                    keys = self._list_to_dict('job',row).keys()
                    break
            tables['job'] = {'keys':keys,'values':ret}

        dataset_ids = set(search_table[id]['dataset_id'] for id in task_ids)
        sql = 'select * from dataset where dataset_id in ('
        sql += ','.join('?' for _ in dataset_ids)
        sql += ')'
        bindings = tuple(dataset_ids)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        categoryvalue_ids = set()
        if ret:
            keys = []
            for row in ret:
                row2 = self._list_to_dict('dataset',row)
                if row2['categoryvalue_id']:
                    categoryvalue_ids.add(row2['categoryvalue_id'])
                if not keys:
                    keys = row2.keys()
            tables['dataset'] = {'keys':keys,'values':ret}

        sql = 'select * from config where dataset_id in ('
        sql += ','.join('?' for _ in dataset_ids)
        sql += ')'
        bindings = tuple(dataset_ids)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if ret:
            keys = []
            for row in ret:
                if not keys:
                    keys = self._list_to_dict('config',row).keys()
                    break
            tables['config'] = {'keys':keys,'values':ret}

        categorydef_ids = set()
        if categoryvalue_ids:
            sql = 'select * from categoryvalue where categoryvalue_id in ('
            sql += ','.join('?' for _ in categoryvalue_ids)
            sql += ')'
            bindings = tuple(categoryvalue_ids)
            try:
                ret = self.db._db_read(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            if ret:
                keys = []
                for row in ret:
                    row2 = self._list_to_dict('categoryvalue',row)
                    categorydef_ids.add(row2['categorydef_id'])
                    if not keys:
                        keys = row2.keys()
                tables['categoryvalue'] = {'keys':keys,'values':ret}

        if categorydef_ids:
            sql = 'select * from categorydef where categorydef_id in ('
            sql += ','.join('?' for _ in categorydef_ids)
            sql += ')'
            bindings = tuple(categorydef_ids)
            try:
                ret = self.db._db_read(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            if ret:
                keys = []
                for row in ret:
                    if not keys:
                        keys = self._list_to_dict('categorydef',row).keys()
                        break
                tables['categorydef'] = {'keys':keys,'values':ret}

        callback(tables)

    @dbmethod
    def misc_update_tables(self,tables,callback=None):
        """
        Update the DB tables with the incoming information.

        :param tables: A dict of {table_name:{keys:[],values:[[]]}}
        :returns: (via callback) success or failure
        """
        if not tables or not isinstance(tables,dict):
            callback(Exception('tables not a dict'))
        else:
            cb = partial(self._misc_update_tables_blocking,tables,
                         callback=callback)
            self.db.non_blocking_task(cb)
    def _misc_misc_update_tables_blocking(tables,callback=None):
        conn,archive_conn = self.db._dbsetup()
        for name in tables:
            sql = 'replace into ? ('
            sql += ','.join('?' for _ in tables[name]['keys'])
            sql += ') values ('
            sql += ','.join('?' for _ in tables[name]['keys'])
            sql += ')'
            bindings = [name]+tables[name]['keys']
            for values in tables[name]['values']:
                bindings2 = tuple(bindings+values)
                try:
                    ret = self.db._db_write(conn,sql,bindings2,None,None,None)
                except Exception as e:
                    ret = e
                if isinstance(ret,Exception):
                    callback(ret)
                    return
        callback()
