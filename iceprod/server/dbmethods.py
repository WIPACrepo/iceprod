"""
Database methods
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
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

logger = logging.getLogger('dbmethods')

def filtered_input(input):
    """Filter input to sql in cases where we can't use bindings.
       Just remove all " ' ; : ? characters, since
       those won't be needed in proper names"""
    def filter(s):
        if isinstance(s, str):
            return s.replace("'","").replace('"',"").replace(';','').replace(':','').replace('?','')
        elif isinstance(s, (int,long,real,complex)):
            return s
        else: # if it's not a basic type, discard it
            return ''
        
    if isinstance(input, list):
        return map(filter,input)
    elif isinstance(input,dict):
        ret = {}
        for x in input:
            ret[filter(x)] = filter(input[x])
        return ret
    elif isinstance(input,OrderedDict):
        ret = OrderedDict()
        for x in input:
            ret[filter(x)] = filter(input[x])
        return ret
    else:
        return filter(input)

def datetime2str(dt):
    """Convert a datetime object to ISO 8601 string"""
    return dt.isoformat()
def str2datetime(st):
    """Convert a ISO 8601 string to datetime object"""
    if '.' in st:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S")



class DBMethods():
    """The actual methods to be called on the database.
    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument."""
    def __init__(self,db):
        self.db = db
    
    def _list_to_dict(self,table,input):
        """Convert an input that is a list of values from a table 
           into a dict of values from that table."""
        if isinstance(table,basestring):
            if table not in self.db.tables:
                raise Exception('bad table')
            keys = self.db.tables[table]
        elif isinstance(table,Iterable):
            if not set(table) <= set(self.db.tables):
                raise Exception('bad table')
            keys = reduce(lambda a,b:a+self.db.tables[b].keys(), table, [])
        else:
            raise Exception('bad table type')
        
        ret = OrderedDict()
        try:
            for i,k in enumerate(keys):
                ret[k] = input[i]
        except:
            logger.warn('error making table %s dict from return values %r',
                         table,input)
            raise
        return ret
    
    
    ## Authorization ##
    
    def get_site_auth(self,callback=None):
        """Get current site's id and key for authentication and authorization with other sites.
        Returns (site_id,key) tuple"""
        sql = 'select site.site_id,site.auth_key from site join setting on site.site_id = setting.site_id'
        bindings = tuple()
        cb = partial(self._get_site_auth_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_site_auth_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                if len(ret) < 1:
                    callback(Exception('No site match for current site name'))
                elif len(ret) > 1:
                    callback(Exception('More than one site match for current site name'))
                elif len(ret[0]) < 2:
                    callback(Exception('Row does not have both site and key'))
                else:
                    r = {'site_id':ret[0][0],
                         'auth_key':ret[0][1]}
                    callback(r)
    
    def authorize_site(self,site,key,callback=None):
        """Validate site and key for authorization.
        Returns True/Exception"""
        sql = 'select site_id,auth_key from site where site_id = ?'
        bindings = (site,)
        cb = partial(self._authorize_site_callback,key,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _authorize_site_callback(self,key,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                if len(ret) < 1:
                    callback(Exception('No site match for current site id'))
                elif len(ret) > 1:
                    callback(Exception('More than one site match for current site id'))
                elif len(ret[0]) < 2:
                    callback(Exception('Row does not have both site and key'))
                else:
                    callback(key == ret[0][1])
    
    def authorize_task(self,key,callback=None):
        """Validate key for authorization.
        Returns True/Exception"""
        sql = 'select key,expire from passkey where key = ?'
        bindings = (key,)
        cb = partial(self._authorize_task_callback,key,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _authorize_task_callback(self,key,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                if len(ret) < 1:
                    callback(Exception('No match for passkey'))
                elif len(ret) > 1:
                    callback(Exception('More than one match for passkey'))
                elif len(ret[0]) < 2:
                    callback(Exception('Row does not have both key and expiration time'))
                else:
                    k = ret[0][0]
                    d = str2datetime(ret[0][1])
                    if k != key:
                        callback(Exception('Passkey returned from db does not match key'))
                    elif d < datetime.now():
                        callback(Exception('Passkey is expired'))
                    else:
                        callback(True)
    
    
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
    
    
    ## Queueing ##
    
    def _get_task_from_ret(self,ret):
        tasks = OrderedDict()
        try:
            for row in ret:
                tasks[row[0]] = self._list_to_dict('task',row)
        except:
            return {}
        else:
            return tasks
    
    def get_site_id(self,callback=None):
        """Get the current site_id"""
        sql = 'select site_id from setting'
        bindings = tuple()
        cb = partial(self._get_site_id_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_site_id_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            elif ret is None or len(ret) < 1 or len(ret[0]) < 1:
                callback(Exception('no site id'))
            else:
                callback(ret[0][0])
    
    def set_site_queues(self,site_id,queues,callback=None):
        """Set the site queues"""
        cb = partial(self._set_site_queues_blocking,site_id,queues,
                     callback=callback)
        self.db.blocking_task(cb)
    def _set_site_queues_blocking(self,site_id,queues,callback=None):
        try:
            queues = json_encode(queues)
        except Exception as e:
            logger.warn('set_site_queues(): cannot encode queues to json')
            callback(e)
            return
        
        conn,archive_conn = self.db._dbsetup()
        sql = 'select * from site where site_id = ?'
        bindings = (site_id,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif len(ret) > 0 and len(ret[0]) > 0:
            # already a site entry, so just update
            sql = 'update site set queues = ? where site_id = ?'
            bindings = (queues,site_id)
        else:
            # add a new site entry
            sql = 'insert into site (site_id,queues) values (?,?)'
            bindings = (site_id,queues)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(True)
    
    def get_active_tasks(self,gridspec,callback=None):
        """Get a dict of active tasks (queued,processing,reset,resume) on this site and plugin, 
           returning {status:{tasks}} where each task = join of search and task tables"""
        sql = 'select task.* from search join task on search.task_id = task.task_id '
        sql += 'where search.gridspec like ? '
        sql += ' and search.task_status in ("queued","processing","reset","resume")'
        bindings = ('%'+gridspec+'%',)
        cb = partial(self._get_active_tasks_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_active_tasks_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                task_groups = {}
                if ret is not None:
                    tasks = self._get_task_from_ret(ret)
                    for task_id in tasks:
                        status = tasks[task_id]['status']
                        if status not in task_groups:
                            task_groups[status] = {}
                        task_groups[status][task_id] = tasks[task_id]
                callback(task_groups)
    
    def set_task_status(self,task,status,callback=None):
        """Set the status of a task"""
        if not isinstance(task,Iterable):
            raise Exception('task is not a str or iterable')
        cb = partial(self._set_task_status_blocking,task,status,
                     callback=callback)
        self.db.non_blocking_task(cb)
    def _set_task_status_blocking(self,task,status,callback=None):
        conn,archive_conn = self.db._dbsetup()
        now = datetime.utcnow()
        if isinstance(task,basestring):
            # single task
            sql = 'update search set task_status = ? '
            sql += ' where task_id = ?'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id = ?'
            bindings = (status,task)
            bindings2 = (status,datetime2str(now),task)
        elif isinstance(task,Iterable):
            b = ','.join(['?' for _ in xrange(len(task))])
            sql = 'update search set task_status = ? '
            sql += ' where task_id in ('+b+')'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id in ('+b+')'
            bindings = (status,)+tuple(task)
            bindings2 = (status,datetime2str(now))+tuple(task)
        try:
            ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(True)
    
    def reset_tasks(self,reset=[],fail=[],callback=None):
        """Reset and fail specified tasks"""
        def cb(ret=None):
            if isinstance(ret,Exception):
                callback(ret)
                return
            if fail:
                self.set_task_status(fail,'failed',callback=callback)
            else:
                callback()
        if reset:
            self.set_task_status(reset,'reset',callback=cb)
        else:
            cb(True)
    
    def get_task(self,task_id=None,callback=None):
        """Get tasks specified by task_id (can be id or list of ids).
           Returns either a single task, or a dict of many tasks."""
        def cb(ret):
            if isinstance(ret,Exception):
                callback(ret)
            elif ret is None:
                callback(Exception('error getting task: ret is None'))
            else:
                tasks = self._get_task_from_ret(ret)
                if isinstance(task_id,str):
                    try:
                        callback(tasks.values()[0])
                    except:
                        callback(None)
                else:
                    callback(tasks)
        
        if isinstance(task_id,str):
            # single task
            sql = 'select * from task where task_id = ?'
            bindings = (task_id,)
        elif isinstance(task_id,Iterable):
            # multiple tasks
            b = ','.join(['?' for _ in xrange(len(task_id))])
            sql = 'select * from task where task_id in ('+b+')'
            bindings = tuple(task_id)
        else:
            raise Exception('task_id is not a str or iterable')
        
        self.db.sql_read_task(sql,bindings,callback=cb)
    
    def get_task_by_grid_queue_id(self,grid_queue_id,callback=None):
        """Get tasks specified by grid_queue_id (can be id or list of ids)"""
        def cb(ret):
            if isinstance(ret,Exception):
                callback(ret)
            elif ret is None:
                callback(Exception('error getting task: ret is None'))
            else:
                tasks = self._get_task_from_ret(ret)
                if isinstance(grid_queue_id,str):
                    try:
                        callback(tasks.values()[0])
                    except:
                        callback(None)
                else:
                    callback(tasks)
        
        if isinstance(grid_queue_id,str):
            # single task
            sql = 'select * from task where grid_queue_id = ?'
            bindings = (grid_queue_id,)
        elif isinstance(grid_queue_id,Iterable):
            # multiple tasks
            b = ','.join(['?' for _ in xrange(len(grid_queue_id))])
            sql = 'select * from task where grid_queue_id in ('+b+')'
            bindings = tuple(grid_queue_id)
        else:
            raise Exception('grid_queue_id is not a str or iterable')
        
        self.db.sql_read_task(sql,bindings,callback=cb)
    
    def set_submit_dir(self,task,submit_dir,callback=None):
        """Set the submit_dir of a task"""
        if not task:
            raise Exception('No task')
        sql = 'update task set submit_dir = ? '
        sql += ' where task_id = ?'
        bindings = (submit_dir,task)
        self.db.sql_write_task(sql,bindings,callback=callback)
    
    def buffer_jobs_tasks(self,gridspec,num_tasks,callback=None):
        """Create a buffer of jobs and tasks ahead of queueing"""
        sql = 'select dataset_id,status,gridspec,jobs_submitted,'
        sql += 'tasks_submitted from dataset where '
        if isinstance(gridspec,basestring):
            sql += 'gridspec like "%?%"'
            bindings = (gridspec,)
            gridspec = [gridspec]
        elif isinstance(gridspec,Iterable):
            if len(gridspec) < 1:
                logger.info('in buffer_jobs_tasks, no gridspec %r',gridspec)
                raise Exception('no gridspec defined')
            sql += '('+(' or '.join(['gridspec like ?' for _ in gridspec]))+')'
            bindings = tuple(['%'+g+'%' for g in gridspec])
        else:
            logger.info('in buffer_jobs_tasks, unknown gridspec %r',gridspec)
            raise Exception('unknown gridspec type')
        sql += ' and status = ?'
        bindings += ('processing',)
        logger.debug('in buffer_jobs_tasks, buffering on %r',gridspec)
        cb = partial(self._buffer_jobs_tasks_callback,gridspec,
                     num_tasks,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _buffer_jobs_tasks_callback(self,gridspec,num_tasks,ret,callback=None):
        logger.debug('in _buffer_jobs_tasks_callback, ret = %r',ret)
        possible_datasets = {}
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for d,s,gs,js,ts in ret:
                logger.debug('cb:gs=%r,gridspec=%r',gs,gridspec)
                try:
                    gs = json_decode(gs)
                except:
                    logger.debug('gs not a json object, must be str')
                    if gs not in gridspec:
                        continue # not a local dataset
                else:
                    logger.debug('gs is json object %r',gs)
                    for g in gs.values():
                        if g not in gridspec:
                            continue # not a local dataset
                possible_datasets[d] = {'gridspec':gs,'jobs':js,'tasks':ts}
        if len(possible_datasets) < 1:
            # nothing to buffer
            logger.info('nothing to buffer (cb)')
            callback(True)
            return
        sql = 'select dataset_id,job_id,task_id,gridspec,task_status from search '
        sql += ' where dataset_id in ('
        sql += ','.join(['?' for _ in possible_datasets])
        sql += ')'
        bindings = tuple(possible_datasets)
        cb = partial(self._buffer_jobs_tasks_callback2,gridspec,
                     num_tasks,possible_datasets,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _buffer_jobs_tasks_callback2(self,gridspec,num_tasks,possible_datasets,
                                     ret,callback=None):
        logger.debug('in _buffer_jobs_tasks_callback2, ret = %r',ret)
        already_buffered = {}
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            job_ids = set()
            for d,j,t,gs,st in ret:
                logger.debug('cb2:gs=%r,gridspec=%r',gs,gridspec)
                if gs not in gridspec:
                    continue
                if d not in already_buffered:
                    already_buffered[d] = {'jobs_submitted':0,
                                           'tasks_submitted':0,
                                           'tasks_buffered':0,
                                          }
                already_buffered[d]['tasks_submitted'] += 1
                if j not in job_ids:
                    already_buffered[d]['jobs_submitted'] += 1
                    job_ids.add(j)
                if st == 'waiting':
                    already_buffered[d]['tasks_buffered'] += 1
        need_to_buffer = {}
        for d in possible_datasets:
            total_jobs = possible_datasets[d]['jobs']
            total_tasks = possible_datasets[d]['tasks']
            tasks_per_job = float(total_tasks/total_jobs)
            if d in already_buffered:
                total_tasks -= already_buffered[d]['tasks_submitted']
                total_jobs -= already_buffered[d]['jobs_submitted']
            n = num_tasks
            poss = total_tasks
            if poss < n:
                n = poss
            if n > 0:
                n = int(math.ceil(num_tasks/tasks_per_job))
                poss = total_jobs
                if poss < n:
                    n = poss
                if n > 0:
                    need_to_buffer[d] = n
        if not need_to_buffer:
            # nothing to buffer
            logger.info('nothing to buffer (cb2)')
            callback(True)
        else:
            # create jobs and tasks
            cb = partial(self._buffer_jobs_tasks_blocking,possible_datasets,
                         need_to_buffer,callback=callback)
            self.db.non_blocking_task(cb)
    def _buffer_jobs_tasks_blocking(self,possible_datasets,need_to_buffer,
                                    callback=None):
        logger.debug('in _buffer_jobs_tasks_blocking')
        conn,archive_conn = self.db._dbsetup()
        now = datetime2str(datetime.utcnow())
        
        # get dataset config
        sql = 'select dataset_id,config_data from config where dataset_id in '
        sql += '('+(','.join(['?' for _ in need_to_buffer]))+')'
        bindings = tuple(need_to_buffer)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        for d,c in ret:
            try:
                possible_datasets[d]['config'] = serialization.serialize_json.loads(c)
            except:
                logger.info('config for dataset %s not loaded',d,exc_info=True)
        
        for dataset in need_to_buffer:
            logger.debug('buffering dataset %s',dataset)
            total_jobs = possible_datasets[dataset]['jobs']
            total_tasks = possible_datasets[dataset]['tasks']
            tasks_per_job = int(total_tasks/total_jobs)
            
            # make jobs
            jobs = []
            sql_bindings = []
            for _ in xrange(need_to_buffer[dataset]):
                jobs.append((self.db._increment_id_helper('job',conn),
                             'processing',now))
                sql_bindings.append('(?,?,?)')
            sql = 'insert into job (job_id,status,status_changed) values '
            sql += ','.join(sql_bindings)
            bindings = reduce(lambda a,b:a+b,jobs)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            
            # make tasks
            task_names = []
            task_depends = []
            if 'config' in possible_datasets[dataset]:
                tt = possible_datasets[dataset]['config']['tasks']
                if len(tt) != tasks_per_job:
                    logger.warn('config tasks len != tasks_per_job for '
                                 'dataset %s. ignoring...',dataset)
                    for t in xrange(tasks_per_job):
                        task_names.append(str(t))
                        task_depends.append([])
                else:
                    task_names = [t['name'] for t in tt]
                    try:
                        for t in tt:
                            task_depends.append([task_names.index(d) 
                                                 for d in t['depends']])
                    except ValueError:
                        logger.error('task dependency not in tasks for '
                                     'dataset %s. skipping dataset',dataset)
                        continue
            else:
                logger.info('config for dataset %s does not exist or '
                             'does not have proper task names. ignoring...',
                             dataset)
                for t in xrange(tasks_per_job):
                    task_names.append(str(t))
                    task_depends.append([])
            tasks = []
            search = []
            task_bindings = []
            search_bindings = []
            q_10 = '('+(','.join(['?' for _ in xrange(10)]))+')'
            q_6 = '('+(','.join(['?' for _ in xrange(6)]))+')'
            for job_id,job_status,n in jobs:
                task_ids = [self.db._increment_id_helper('task',conn) 
                            for t in xrange(tasks_per_job)]
                for t in xrange(tasks_per_job):
                    task_id = task_ids[t]
                    depends = ','.join([task_ids[n] for n in task_depends[t]])
                    tasks.append((task_id, 'waiting', 'waiting', '', now,
                                 '', '', 0, 0, depends))
                    task_bindings.append(q_10)
                    name = task_names[t]
                    if isinstance(possible_datasets[dataset]['gridspec'],basestring):
                        gs = possible_datasets[dataset]['gridspec']
                    else:
                        try:
                            gs = possible_datasets[dataset]['gridspec'][name]
                        except:
                            logger.error('cannot find task name in dataset '
                                        'gridspec def: %r %r',dataset,name)
                            continue
                    search.append((task_id, job_id, dataset, gs,
                                   name, 'waiting'))
                    search_bindings.append(q_6)
            sql = 'insert into task (task_id,status,prev_status,'
            sql += 'error_message,status_changed,submit_dir,grid_queue_id,'
            sql += 'failures,evictions,depends) values '
            sql += ','.join(task_bindings)
            bindings = reduce(lambda a,b:a+b,tasks)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            sql = 'insert into search (task_id,job_id,dataset_id,gridspec,'
            sql += 'name,task_status) values '
            sql += ','.join(search_bindings)
            bindings = reduce(lambda a,b:a+b,search)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
        
        # done buffering
        callback(True)
    
    def get_queueing_datasets(self,gridspec,callback=None):
        """Get datasets that are currently in processing status on gridspec.
           Returns a list of dataset_ids"""
        def cb(ret):
            if isinstance(ret,Exception):
                callback(ret)
            else:
                datasets = {}
                if ret is not None:
                    for row in ret:
                        d = self._list_to_dict('dataset',row)
                        datasets[d['dataset_id']] = d
                callback(datasets)
        sql = 'select dataset.* from dataset join search on '
        sql += 'search.dataset_id = dataset.dataset_id where '
        sql += 'dataset.status = "processing" and '
        sql += 'search.gridspec = ? and '
        sql += 'search.task_status = "waiting" '
        bindings = (gridspec,)
        self.db.sql_read_task(sql,bindings,callback=cb)
    
    def get_queueing_tasks(self,dataset_prios,gridspec,num=20,callback=None):
        """Get tasks to queue based on dataset priorities.
        
        :param dataset_prios: a dict of {dataset_id:priority} where sum(priorities)=1
        :param gridspec: the grid to queue on
        :param num: number of tasks to queue
        :returns: {task_id:task}
        """
        if callback is None:
            raise Exception('need a callback')
        if dataset_prios is None or not isinstance(dataset_prios,dict):
            raise Exception('dataset_prios not a dict')
        if not gridspec:
            callback({})
            return
        cb = partial(self._get_queueing_tasks_blocking,dataset_prios,
                     gridspec,num,callback=callback)
        self.db.non_blocking_task(cb)
    def _get_queueing_tasks_blocking(self,dataset_prios,gridspec,num,
                                     callback=None):
        conn,archive_conn = self.db._dbsetup()
        # get all tasks for processing datasets so we can do dependency check
        sql = 'select search.dataset_id,task.task_id,task.depends,task.status '
        sql += ' from search join task on search.task_id = task.task_id '
        sql += ' where dataset_id in ('+','.join(['?' for _ in dataset_prios])
        sql += ') and gridspec = ?'
        bindings = tuple(dataset_prios)+(gridspec,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        tasks = {}
        datasets = {k:OrderedDict() for k in dataset_prios}
        if ret:
            for dataset,id,depends,status in ret:
                if status == 'waiting':
                    datasets[dataset][id] = depends
                tasks[id] = status
        # get actual tasks
        task_prio = []
        for dataset in dataset_prios:
            limit = int(math.ceil(dataset_prios[dataset]*num))
            for task_id in datasets[dataset]:
                depends = datasets[dataset][task_id]
                satisfied = True
                if depends:
                    for dep in depends.split(','):
                        if dep not in tasks or tasks[dep] != 'complete':
                            satisfied = False
                            break
                if satisfied:
                    # task can be queued now
                    task_prio.append((dataset_prios[dataset],task_id))
                    limit -= 1
                    if limit <= 0:
                        break
        if not task_prio:
            callback({})
            return
        # sort by prio, low to high (so when we pop we get higher first)
        task_prio.sort(key=operator.itemgetter(0),reverse=True)
        # return first num tasks
        task_prio = [t for p,t in task_prio[:num]]
        tasks = {}
        sql = 'select search.*,dataset.debug from search '
        sql += ' join dataset on search.dataset_id = dataset.dataset_id '
        sql += ' where search.task_id in ('
        sql += ','.join(['?' for _ in task_prio])+')'
        bindings = tuple(task_prio)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if ret:
            tasks = {}
            for row in ret:
                tmp = self._list_to_dict('search',row[:-1])
                tmp['debug'] = row[-1]
                tasks[tmp['task_id']] = tmp
        callback(tasks)

    def get_cfg_for_task(self,task_id,callback=None):
        """Get a cfg for a task"""
        if not task_id:
            raise Exception('bad task_id')
        sql = 'select task_id,dataset_id from search where task_id = ?'
        bindings = (task_id,)
        cb = partial(self._get_cfg_for_task_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_cfg_for_task_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or len(ret) < 1 or len(ret[0]) < 2:
            callback(Exception('get_cfg_for_task did not return a dataset_id'))
        else:
            dataset_id = ret[0][1]
            self.get_cfg_for_dataset(dataset_id,callback=callback)
    
    def get_cfg_for_dataset(self,dataset_id,callback=None):
        """Get a cfg for a dataset"""
        if not dataset_id:
            raise Exception('bad datset_id')
        sql = 'select config_id,config_data from config where dataset_id = ?'
        bindings = (dataset_id,)
        cb = partial(self._get_cfg_for_dataset_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_cfg_for_dataset_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or len(ret) < 1 or len(ret[0]) < 2:
            callback(Exception('get_cfg_for_dataset did not return a config'))
        else:
            logger.debug('config for dataset: %r',ret)
            values = {}
            for config_id,config_data in ret:
                values = {'config_id':config_id,
                          'config_data':config_data,
                         }
            if 'config_data' in values:
                callback(values['config_data'])
            else:
                callback(None)
    
    
    ### Task RPC functions ###
    
    def rpc_echo(self,value,callback=None):
        """Echo a single value. Just a test to see if rpc is working"""
        return value
    
    def rpc_new_task(self, gridspec=None, platform=None, hostname=None, 
                 ifaces=None, python_unicode=None, callback=None):
        """Get a new task from the queue specified by the gridspec,
           based on the platform, hostname, network interfaces, python unicode.
           Save plaform,hostname,network in nodes table.
           Returns a runnable config file with site content.
        """
        if not gridspec:
            raise Exception('gridspec is not given')
        args = {'gridspec':gridspec,
                'platform':platform,
                'hostname':hostname,
                'ifaces':ifaces,
                'python_unicode':python_unicode,
               }
        cb2 = partial(self._rpc_new_task_callback,args,callback=callback)
        cb = partial(self._rpc_new_task_blocking,args,callback=cb2)
        self.db.blocking_task(cb)
    def _rpc_new_task_blocking(self,args,callback=None):
        """This executes in a single thread regardless of the number of
           parallel requests for a new task.
        """
        conn,archive_conn = self.db._dbsetup()
        sql = 'select * from search '
        sql += ' where search.gridspec = ? and search.task_status = queued'
        sql += ' limit 1'
        bindings = (args['gridspec'],)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        elif ret is None:
            callback(Exception('sql error in _new_task_blocking'))
        elif not ret:
            callback([])
        else:
            newtask = {}
            logger.debug('new task: %r',ret)
            try:
                newtask = self._list_to_dict('search',ret[0])
            except:
                logger.warn('error converting search results',exc_info=True)
                pass
            if not newtask:
                callback(newtask)
            now = datetime.utcnow()
            sql = 'update search set task_status = ? '
            sql += ' where task_id = ?'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id = ?'
            bindings = ('processing',newtask['task_id'])
            bindings2 = ('processing',datetime2str(now),newtask['task_id'])
            try:
                ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
            else:
                callback(newtask)
    def _rpc_new_task_callback(self,args,task,callback=None):
        if isinstance(task,Exception):
            callback(task)
        elif task is None:
            callback(Exception('_new_task_blocking did not return a task'))
        elif not task:
            callback(None)
        else:
            self.get_cfg_for_dataset(task['dataset_id'],callback=callback)

    def rpc_set_processing(self,task,callback=None):
        """Set a task to the processing status"""
        return self.set_task_status(task,'processing',callback=callback)

    def rpc_finish_task(self,task,stats={},callback=None):
        """Do task completion operations.
        Takes a task_id and a stats dict as input.
        """
        stats = json_encode(stats)
        cb = partial(self._rpc_finish_task_blocking,task,stats,callback=callback)
        self.db.blocking_task(cb)
    def _rpc_finish_task_blocking(self,task,stats,callback=None):
        conn,archive_conn = self.db._dbsetup()
        
        # update task status
        now = datetime.utcnow()
        sql = 'update search set task_status = ? '
        sql += ' where task_id = ?'
        sql2 = 'update task set prev_status = status, '
        sql2 += ' status = ?, status_changed = ? where task_id = ?'
        bindings = ('complete',task)
        bindings2 = ('complete',datetime2str(now),task)
        try:
            ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        
        # update task statistics
        sql = 'select task_stat_id,task_id from task_stat where task_id = ?'
        bindings = (task,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        task_stat_id = None
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for ts,t in ret:
                task_stat_id = ts
        if task_stat_id:
            logger.debug('replace previous task_stat')
            sql = 'update task_stat set stat = ? where task_stat_id = ?'
            bindings = (stats,task_stat_id)
        else:
            logger.debug('insert new task_stat')
            task_stat_id = self.db._increment_id_helper('task_stat',conn)
            sql = 'insert into task_stat (task_stat_id,task_id,stat) values '
            sql += ' (?, ?, ?)'
            bindings = (task_stat_id,task,stats)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        
        # check if whole job is finished
        sql = 'select dataset_id,job_id,jobs_submitted,tasks_submitted '
        sql += ' from search '
        sql += ' join dataset on search.dataset_id = dataset.dataset_id '
        sql += ' where task_id = ?'
        bindings = (task,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        dataset_id = None
        job_id = None
        total_jobs = None
        total_tasks = None
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for d_id,j_id,njobs,ntasks in ret:
                dataset_id = d_id
                job_id = j_id
                total_jobs = njobs
                total_tasks = ntasks
        if not dataset_id or not job_id or not total_jobs or not total_tasks:
            callback(Exception('cannot find dataset or job id'))
            return
        tasks_per_job = int(total_tasks/total_jobs)
        sql = 'select task_id,task_status from search where job_id = ?'
        bindings = (job_id,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        task_statuses = set()
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret and len(ret) == tasks_per_job:
            logger.debug('tasks_per_job = %d, len(ret) = %d',tasks_per_job,len(ret))
            # require that all tasks for this job are in our db
            # means that distributed jobs can only complete at the master
            for t,s in ret:
                task_statuses.add(s)
        job_status = None
        if task_statuses and not task_statuses&{'waiting','queued','processing','resume','reset'}:
            if not task_statuses-{'complete'}:
                job_status = 'complete'
            elif not task_statuses-{'complete','failed'}:
                job_status = 'errors'
            elif not task_statuses-{'complete','failed','suspended'}:
                job_status = 'suspended'
        if job_status:
            # update job status
            logger.info('job %s marked as %s',job_id,job_status)
            sql = 'update job set status = ?, status_changed = ? '
            sql += ' where job_id = ?'
            bindings = (job_status,datetime2str(now),job_id)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            
            if job_status == 'complete':
                # TODO: collate task stats
                pass
                
        callback(True)

    def rpc_task_error(self,task,callback=None):
        """Mark task as ERROR"""
        if not task:
            raise Exception('no task specified')
        cb = partial(self._rpc_task_error_blocking,task,callback=callback)
        self.db.non_blocking_task(cb)
    def _rpc_task_error_blocking(self,task,callback=None):
        conn,archive_conn = self.db._dbsetup()
        sql = 'select task_id,failures from task where task_id = ?'
        bindings = (task,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret:
            callback(Exception('sql error in task_error'))
        else:
            task = None
            failures=0
            for t,f in ret:
                task = t
                failures = f
                break
            failures += 1
            if failures >= self.db.cfg['queue']['max_resets']:
                status = 'failed'
            else:
                status = 'reset'
            
            now = datetime.utcnow()
            sql = 'update search set task_status = ? '
            sql += ' where task_id = ?'
            bindings = (status,task)
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, failures = ?, status_changed = ? where task_id = ?'
            bindings2 = (status,failures,datetime2str(now),task)
            try:
                ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
            else:
                callback(True)
    
    def rpc_upload_logfile(self,task,name,data,callback=None):
        """Uploading of a logfile from a task"""
        cb2 = partial(self._rpc_upload_logfile_callback,callback=callback)
        cb = partial(self._rpc_upload_logfile_blocking,task,name,data,callback=cb2)
        self.db.blocking_task(cb)
    def _rpc_upload_logfile_blocking(self,task,name,data,callback=None):
        conn,archive_conn = self.db._dbsetup()
        sql = 'select task_log_id,task_id from task_log where '
        sql += ' task_id = ? and name = ?'
        bindings = (task,name)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        task_log_id = None
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for ts,t in ret:
                task_log_id = ts
        if task_log_id:
            logger.debug('replace previous task_log')
            sql = 'update task_log set data = ? where task_log_id = ?'
            bindings = (data,task_log_id)
        else:
            logger.debug('insert new task_log')
            task_log_id = self.db._increment_id_helper('task_log',conn)
            sql = 'insert into task_log (task_log_id,task_id,name,data) '
            sql += ' values (?,?,?,?)'
            bindings = (task_log_id,task,name,data)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(True)
    def _rpc_upload_logfile_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif ret is None:
            callback(Exception('sql error in upload_logfile'))
        else:
            callback(True)

    def rpc_stillrunning(self,task,callback=None):
        """Check that the task is still in a running state"""
        sql = 'select task_id,status from task where task_id = ?'
        bindings = (task,)
        cb = partial(self._rpc_stillrunning_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _rpc_stillrunning_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or not ret[0]:
            callback(Exception('sql error in stillrunning'))
        else:
            if ret[0][1] in ('queued','processing'):
                callback(True)
            else:
                callback(False)
    
    def rpc_submit_dataset(self,data,difplus='',description='',gridspec='',
                           njobs=1,stat_keys=[],debug=False,
                           callback=None):
        """Submit a dataset"""
        cb = partial(self._rpc_submit_dataset_blocking,data,difplus,description,
                     gridspec,njobs,stat_keys,debug,
                     callback=callback)
        self.db.blocking_task(cb)
    def _rpc_submit_dataset_blocking(self,config_data,difplus,description,gridspec,
                                     njobs,stat_keys,debug,
                                     callback=None):
        conn,archive_conn = self.db._dbsetup()
        dataset_id = self.db._increment_id_helper('dataset',conn)
        config_id = self.db._increment_id_helper('config',conn)
        if isinstance(config_data,dict):
            config = config_data
            try:
                config_data = serialization.serialize_json.dumps(config)
            except:
                logger.info('error serializing config: %r', config,
                            exc_info=True)
                callback(e)
                return
        else:
            try:
                config = serialization.serialize_json.loads(config_data)
            except Exception as e:
                logger.info('error deserializing config: %r', config_data,
                            exc_info=True)
                callback(e)
                return
        try:
            njobs = int(njobs)
            ntasks = len(config['tasks'])*njobs
            ntrays = sum(len(x['trays']) for x in config['tasks'])
        except Exception as e:
            logger.info('error reading ntasks and ntrays from submitting config',
                        exc_info=True)
            callback(e)
            return
        sql = 'insert into config (config_id,dataset_id,config_data,difplus_data)'
        sql += ' values (?,?,?,?)'
        bindings = (config_id,dataset_id,config_data,difplus)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if isinstance(gridspec,dict):
            gridspec = json_encode(gridspec)
        stat_keys = json_encode(stat_keys)
        categories = '' # TODO: make configurable
        bindings = (dataset_id,'name',description,gridspec,'processing',
                    'user','institution','localhost',0,njobs,ntrays,ntasks,
                    datetime2str(datetime.utcnow()),'','','','',stat_keys,
                    categories,debug)
        sql = 'insert into dataset (dataset_id,name,description,gridspec,'
        sql += 'status,username,institution,submit_host,priority,'
        sql += 'jobs_submitted,trays,tasks_submitted,start_date,end_date,'
        sql += 'temporary_storage,global_storage,parent_id,stat_keys,'
        sql += 'categoryvalue_ids,debug)'
        sql += ' values ('+','.join(['?' for _ in bindings])+')'
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(True)


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
    
    
    ### Passkey functions ###
    
    def new_passkey(self,expiration=3600,callback=None):
        """Make a new passkey.  Default expiration in 1 hour."""
        if isinstance(expiration,(int,float)):
            expiration = datetime.utcnow()+timedelta(seconds=expiration)
        elif not isinstance(expiration,datetime):
            raise Exception('bad expiration')
        
        passkey_id = self.db.increment_id('passkey')
        passkey = uuid.uuid4().hex
        sql = 'insert into passkey (passkey_id,key,expire) '
        sql += ' values (?,?,?)'
        bindings = (passkey_id,passkey,datetime2str(expiration))
        cb = partial(self._new_passkey_callback,passkey,callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _new_passkey_callback(self,passkey,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(passkey)
    
    def get_passkey(self,passkey,callback=None):
        """Get the expiration datetime of a passkey"""
        if not passkey:
            raise Exception('bad expiration')
        
        sql = 'select * from passkey where key = ?'
        bindings = (passkey,)
        cb = partial(self._get_passkey_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_passkey_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or len(ret) < 1 or len(ret[0]) < 3:
            callback(Exception('get_passkey did not return a passkey'))
        else:
            try:
                expiration = str2datetime(ret[0][2])
            except Exception as e:
                callback(e)
            else:
                callback(expiration)
    
    ### website ###
    
    def get_tasks_by_status(self,gridspec=None,dataset_id=None,callback=None):
        """Get the number of tasks in each state on this site and plugin, 
           returning {status:num}
        """
        sql = 'select search.task_status, count(*) as num from search '
        bindings = tuple()
        if dataset_id:
            sql += ' where search.dataset_id = ? '
            bindings += (dataset_id,)
        if gridspec:
            if 'where' not in sql:
                sql += ' where '
            else:
                sql += ' and '
            sql += ' search.gridspec like ? '
            bindings += ('%'+gridspec+'%',)
        sql += ' group by search.task_status '
        cb = partial(self._get_tasks_by_status_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_tasks_by_status_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                task_groups = {}
                if ret and ret[0]:
                    for status,num in ret:
                        task_groups[status] = num
                callback(task_groups)
    
    def get_datasets_by_status(self,gridspec=None,callback=None):
        """Get the number of datasets in each state on this site and plugin, 
           returning {status:num}
        """
        sql = 'select dataset.status, count(*) as num from search '
        sql += ' join dataset on search.dataset_id = dataset.dataset_id '
        if gridspec:
            sql += 'where search.gridspec like "%?%" '
            bindings = (gridspec,)
        else:
            bindings = None
        sql += ' group by dataset.status '
        cb = partial(self._get_datasets_by_status_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_datasets_by_status_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                dataset_groups = {}
                if ret and ret[0]:
                    for status,num in ret:
                        dataset_groups[status] = num
                callback(dataset_groups)
    
    def get_datasets_details(self,dataset_id=None,status=None,gridspec=None,
                          callback=None):
        """Get the number of datasets in each state on this site and plugin, 
           returning {status:num}
        """
        sql = 'select dataset.* from search '
        sql += ' join dataset on search.dataset_id = dataset.dataset_id '
        bindings = tuple()
        if dataset_id:
            sql += ' where search.dataset_id = ? '
            bindings += (dataset_id,)
        if status:
            if 'where' not in sql:
                sql += ' where '
            else:
                sql += ' and '
            sql += ' dataset.status = ? '
            bindings += (status,)
        if gridspec:
            if 'where' not in sql:
                sql += ' where '
            else:
                sql += ' and '
            sql += ' search.gridspec like ? '
            bindings += ('%'+gridspec+'%',)
        cb = partial(self._get_datasets_details_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_datasets_details_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                datasets = {}
                if ret:
                    for row in ret:
                        tmp = self._list_to_dict('dataset',row)
                        datasets[tmp['dataset_id']] = tmp
                callback(datasets)
    
    def get_tasks_details(self,task_id=None,status=None,gridspec=None,
                          dataset_id=None,callback=None):
        """Get the number of tasks in each state on this site and plugin, 
           returning {status:num}
        """
        sql = 'select search.*,task.* from search '
        sql += ' join task on search.task_id = task.task_id '
        bindings = tuple()
        if task_id:
            sql += ' where search.task_id = ? '
            bindings += (task_id,)
        if status:
            if 'where' not in sql:
                sql += ' where '
            sql += ' search.task_status = ? '
            bindings += (status,)
        if dataset_id:
            if 'where' not in sql:
                sql += ' where '
            else:
                sql += ' and '
            sql += ' search.dataset_id = ? '
            bindings += (dataset_id,)
        if gridspec:
            if 'where' not in sql:
                sql += ' where '
            else:
                sql += ' and '
            sql += ' search.gridspec like ? '
            bindings += ('%'+gridspec+'%',)
        cb = partial(self._get_tasks_details_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_tasks_details_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                tasks = {}
                if ret:
                    for row in ret:
                        tmp = self._list_to_dict(['search','task'],row)
                        tasks[tmp['task_id']] = tmp
                callback(tasks)
    
    def get_logs(self,task_id,lines=None,callback=None):
        """Get the logs for a task, returns {log_name:text}"""
        sql = 'select * from task_log where task_id = ?'
        bindings = (task_id,)
        cb = partial(self._get_logs_callback,lines,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_logs_callback(self,lines,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            logs = {}
            for row in ret:
                tmp = self._list_to_dict('task_log',row)
                if tmp['name'] and tmp['data']:
                    data = json_compressor.uncompress(tmp['data'])
                    if lines and isinstance(lines,int):
                        data = '\n'.join(data.rsplit('\n',lines+1)[-1*lines:])
                    logs[tmp['name']] = data
            callback(logs)
    
    def get_gridspec(self,callback=None):
        """Get the possible gridspecs that we know about"""
        sql = 'select site_id,queues from site'
        bindings = tuple()
        cb = partial(self._get_gridspec_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _get_gridspec_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            gridspecs = {}
            for site_id,queues in ret:
                try:
                    gridspecs.update(json_decode(queues))
                except:
                    pass
            callback(gridspecs)
    
    ### site to site ###
    
    def site_to_site_upload(self,src,dest,callback=None):
        if callback:
            callback()
        # TODO: actually write this method
    
    
    ### scheduled tasks ###
    
    def cron_dataset_completion(self,callback=None):
        """Check for newly completed datasets and mark them as such"""
        sql = 'select dataset_id,jobs_submitted,tasks_submitted '
        sql += ' from dataset where status = ? '
        bindings = ('processing',)
        cb = partial(self._cron_dataset_completion_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _cron_dataset_completion_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            datasets = OrderedDict()
            for dataset_id,njobs,ntasks in ret:
                datasets[dataset_id] = {'jobs_submitted':njobs,
                                        'tasks_submitted':ntasks,
                                        'task_status':set(),
                                        'ntasks':0}
            if not datasets:
                callback(True)
                return
            sql = 'select dataset_id,task_status from search '
            sql += ' where dataset_id in ('
            sql += ','.join(['?' for _ in datasets])
            sql += ')'
            bindings = tuple(datasets.keys())
            cb = partial(self._cron_dataset_completion_callback2,datasets,
                         callback=callback)
            self.db.sql_read_task(sql,bindings,callback=cb)
    def _cron_dataset_completion_callback2(self,datasets,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            for dataset_id,task_status in ret:
                datasets[dataset_id]['ntasks'] += 1
                datasets[dataset_id]['task_status'].add(task_status)
            
            dataset_status = {}
            for dataset_id in datasets:
                total_tasks = datasets[dataset_id]['tasks_submitted']
                #tasks_per_job = int(total_tasks/total_jobs)
                ntasks = datasets[dataset_id]['ntasks']
                if ntasks < total_tasks:
                    continue # not all tasks accounted for
                task_statuses = datasets[dataset_id]['task_status']
                if not task_statuses&{'waiting','queued','processing','resume','reset'}:
                    logger.info('dataset %s task statues %r',dataset_id,task_statuses)
                    if not task_statuses-{'complete'}:
                        dataset_status[dataset_id] = 'complete'
                    elif not task_statuses-{'complete','failed'}:
                        dataset_status[dataset_id] = 'errors'
                    elif not task_statuses-{'complete','failed','suspended'}:
                        dataset_status[dataset_id] = 'suspended'
            if dataset_status:
                # update dataset statuses
                now = datetime2str(datetime.utcnow())
                statuses = {}
                for dataset_id in dataset_status:
                    status = dataset_status[dataset_id]
                    logger.info('dataset %s marked as %s',dataset_id,status)
                    if status not in statuses:
                        statuses[status] = set()
                    statuses[status].add(dataset_id)
                multi_sql = []
                multi_bindings = []
                for s in statuses:
                    bindings = (s,)
                    sql = 'update dataset set status = ?'
                    if s == 'complete':
                        sql += ', end_date = ? '
                        bindings += (now,)
                    sql += ' where dataset_id in ('
                    sql += ','.join(['?' for _ in statuses[s]])
                    sql += ')'
                    bindings += tuple([d for d in statuses[s]])
                    multi_sql.append(sql)
                    multi_bindings.append(bindings)
                cb = partial(self._cron_dataset_completion_callback3,
                             callback=callback)
                self.db.sql_write_task(multi_sql,multi_bindings,callback=cb)
            else:
                callback(True)
    def _cron_dataset_completion_callback3(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            # TODO: consolidate dataset statistics
            callback(True)
    
    