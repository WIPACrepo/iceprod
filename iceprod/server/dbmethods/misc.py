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
from iceprod.core.util import Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime

logger = logging.getLogger('dbmethods.misc')

class misc(_Methods_Base):
    """
    misc DB methods.
    
    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """
    
    ## Proxying ##
    
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
    
    def add_to_cache(self,url,uid,size,checksum,checksum_type='sha512',priority=5,callback=None):
        """Add file to cache after it is uploaded or downloaded"""
        sql = 'insert into cache (cache_id,permanent_url,uid,size,checksum,'
        sql += 'checksum_type,delete_priority) values (?,?,?,?,?,?,?)'
        cache_id = self.db.increment_id('cache')
        bindings = (cache_id,url,uid,size,checksum,priority)
        self.db.sql_write_task(sql,bindings,callback=callback)
    
    
    ### upload functions ###
    
    def new_upload(self,url,size,checksum,checksum_type,callback=None):
        """Allocate a new upload if possible"""
        cb = partial(self._new_upload_blocking,url,size,checksum,
                     checksum_type,callback=callback)
        self.db.blocking_task(cb)
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
            cb = partial(self.site_to_site_upload, path,
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
    
    def site_to_site_upload(self,src,dest,callback=None):
        if callback:
            callback()
        # TODO: actually write this method
    
    