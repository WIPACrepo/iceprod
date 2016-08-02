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

# load mysql Error
try:
    import MySQLdb
except ImportError:
    try:
        import pymysql as MySQLdb
    except:
        class MySQLdb:
            class Error(Exception):
                pass

import iceprod.core.functions
from iceprod.core.util import Node_Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import dbmethod,_Methods_Base,filtered_input,datetime2str,str2datetime

logger = logging.getLogger('dbmethods.misc')

class misc(_Methods_Base):
    """
    misc DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

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

        sql = 'select depends from task where task_id in ('
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
        task_ids = set(task_ids)
        for row in ret:
            for d in row[0].split(','):
                d = d.strip()
                if d:
                    task_ids.add(d)

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
        group_ids = set()
        if ret:
            keys = []
            for row in ret:
                row2 = self._list_to_dict('dataset',row)
                if row2['categoryvalue_ids']:
                    for cv_id in row2['categoryvalue_ids'].split(','):
                        categoryvalue_ids.add(cv_id)
                if row2['group_id']:
                    group_ids.add(row2['group_id'])
                if not keys:
                    keys = row2.keys()
            tables['dataset'] = {'keys':keys,'values':ret}

        sql = 'select * from groups where group_ids in ('
        sql += ','.join('?' for _ in group_ids) + ')'
        bindings = tuple(group_ids)
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
                    keys = self._list_to_dict('groups',row).keys()
                    break
            tables['groups'] = {'keys':keys,'values':ret}

        sql = 'select * from task_rel where dataset_id in ('
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
                    keys = self._list_to_dict('task_rel',row).keys()
                    break
            tables['task_rel'] = {'keys':keys,'values':ret}

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
    def misc_update_tables(self, tables, callback=None):
        """
        Update the DB tables with the incoming information.

        :param tables: A dict of {table_name:{keys:[],values:[[]]}}
        :returns: (via callback) success or failure
        """
        if not tables:
            callback()
            return
        if not isinstance(tables,dict):
            callback(Exception('tables not a dict'))
        else:
            cb = partial(self._misc_update_tables_blocking,tables,
                         callback=callback)
            self.db.non_blocking_task(cb)
    def _misc_update_tables_blocking(self, tables, callback=None):
        conn,archive_conn = self.db._dbsetup()
        try:
            for name in tables:
                sql = 'replace into %s ('%filtered_input(name)
                sql += ','.join(str(filtered_input(k)) for k in tables[name]['keys'])
                sql += ') values ('
                sql += ','.join('?' for _ in tables[name]['keys'])
                sql += ')'
                for values in tables[name]['values']:
                    bindings = tuple(values)
                    try:
                        ret = self.db._db_write(conn,sql,bindings,None,None,None)
                    except Exception as e:
                        ret = e
                    if isinstance(ret,Exception):
                        callback(ret)
                        return
        except Exception as e:
            logger.warn('error updating tables', exc_info=True)
            callback(e)
        callback()

    @dbmethod
    def misc_update_master_db(self, table, index, timestamp, sql,
                       bindings, callback=None):
        """
        Update the DB with incoming information (query provided).

        :param table: The table affected.
        :param index: That table's index id.
        :param timestamp: An ISO 8601 UTC timestamp.
        :param sql: An sql statement.
        :param bindings: Bindings for the sql statement.
        :returns: (via callback) success or failure
        """
        cb = partial(self._misc_update_db_blocking, table, index,
                     timestamp, sql, bindings, callback=callback)
        self.db.blocking_task('update',cb)
    def _misc_update_db_blocking(self, table, index, timestamp, sql,
                                      bindings, callback=None):
        conn,archive_conn = self.db._dbsetup()
        try:
            sql2 = 'select timestamp from master_update_history '
            sql2 += 'where table_name = ? and update_index = ?'
            bindings2 = (table,index)
            ret = self.db._db_read(conn,sql2,bindings2,None,None,None)
            prev_timestamp = None
            for row in ret:
                prev_timestamp = row[0]
            if prev_timestamp and prev_timestamp >= timestamp:
                logger.info('newer data already present for %s %s %s',
                            table, index, timestamp)
                callback(None)
                return
            ret = self.db._db_write(conn,sql,tuple(bindings),None,None,None)
        except MySQLdb.Error:
            logger.warn('dropping history for %r', sql, exc_info=True)
        except Exception as e:
            logger.warn('error updating master', exc_info=True)
            ret = e
        else:
            sql2 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
            bindings2 = (table,index,timestamp)
            try:
                ret = self.db._db_write(conn,sql2,bindings2,None,None,None)
            except MySQLdb.Error:
                logger.warn('mysql error updating update_history',
                            exc_info=True)
            except Exception as e:
                logger.warn('error updating update_history', exc_info=True)
                ret = e
        if callback:
            callback(ret)
