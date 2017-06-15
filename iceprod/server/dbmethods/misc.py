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

import tornado.gen

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
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import _Methods_Base,filtered_input,datetime2str,str2datetime

logger = logging.getLogger('dbmethods.misc')

class misc(_Methods_Base):
    """
    misc DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    ### site to site ###

    def misc_site_to_site_upload(self,src,dest):
        # TODO: actually write this method
        raise NotImplementedError()

    @tornado.gen.coroutine
    def misc_get_tables_for_task(self,task_ids):
        """
        Get all tables necessary to run task(s).

        Args:
            task_ids (iterable): An iterable of task_ids

        Returns:
            dict: table entries
        """
        if isinstance(task_ids,str):
            task_ids = [task_ids]
        elif not isinstance(task_ids,Iterable):
            raise Exception('task_ids not Iterable')
        task_ids = set(task_ids)

        tables = {}

        sql = 'select depends from task where task_id in (%s)'
        keys = []
        values = []
        for f in self._bulk_select(sql, task_ids):
            ret = yield f
            for row in ret:
                for d in row[0].split(','):
                    d = d.strip()
                    if d:
                        task_ids.add(d)

        job_ids = set()
        dataset_ids = set()
        sql = 'select * from search where task_id in (%s)'
        keys = []
        values = []
        for f in self._bulk_select(sql, task_ids):
            ret = yield f
            for row in ret:
                tmp = self._list_to_dict('search',row)
                if not keys:
                    keys = list(tmp.keys())
                job_ids.add(tmp['job_id'])
                dataset_ids.add(tmp['dataset_id'])
            values.extend(ret)
        if keys and values:
            tables['search'] = {'keys':keys,'values':values}
        else:
            raise tornado.gen.Return({})

        sql = 'select * from task where task_id in (%s)'
        keys = []
        values = []
        for f in self._bulk_select(sql, task_ids):
            ret = yield f
            for row in ret:
                if not keys:
                    keys = list(self._list_to_dict('task',row).keys())
                    break
            values.extend(ret)
        if keys and values:
            tables['task'] = {'keys':keys,'values':values}

        sql = 'select * from job where job_id in (%s)'
        keys = []
        values = []
        for f in self._bulk_select(sql, job_ids):
            ret = yield f
            for row in ret:
                if not keys:
                    keys = list(self._list_to_dict('job',row).keys())
                    break
            values.extend(ret)
        if keys and values:
            tables['job'] = {'keys':keys,'values':values}

        categoryvalue_ids = set()
        group_ids = set()
        sql = 'select * from dataset where dataset_id in (%s)'
        keys = []
        values = []
        for f in self._bulk_select(sql, dataset_ids):
            ret = yield f
            for row in ret:
                row2 = self._list_to_dict('dataset',row)
                if row2['categoryvalue_ids']:
                    for cv_id in row2['categoryvalue_ids'].split(','):
                        categoryvalue_ids.add(cv_id)
                if 'groupd_id' in row2 and row2['group_id']:
                    group_ids.add(row2['group_id'])
                if not keys:
                    keys = list(row2.keys())
            values.extend(ret)
        if keys and values:
            tables['dataset'] = {'keys':keys,'values':values}

        if group_ids:
            sql = 'select * from groups where group_ids in (%s)'
            keys = []
            values = []
            for f in self._bulk_select(sql, group_ids):
                ret = yield f
                for row in ret:
                    if not keys:
                        keys = list(self._list_to_dict('groups',row).keys())
                        break
                values.extend(ret)
            if keys and values:
                tables['groups'] = {'keys':keys,'values':values}

        sql = 'select * from task_rel where dataset_id in (%s)'
        keys = []
        values = []
        for f in self._bulk_select(sql, dataset_ids):
            ret = yield f
            for row in ret:
                if not keys:
                    keys = self._list_to_dict('task_rel',row).keys()
                    break
            values.extend(ret)
        if keys and values:
            tables['task_rel'] = {'keys':keys,'values':values}

        sql = 'select * from config where dataset_id in (%s)'
        keys = []
        values = []
        for f in self._bulk_select(sql, dataset_ids):
            ret = yield f
            for row in ret:
                if not keys:
                    keys = list(self._list_to_dict('config',row).keys())
                    break
            values.extend(ret)
        if keys and values:
            tables['config'] = {'keys':keys,'values':values}

        categorydef_ids = set()
        if categoryvalue_ids:
            sql = 'select * from categoryvalue where categoryvalue_id in (%s)'
            keys = []
            values = []
            for f in self._bulk_select(sql, categoryvalue_ids):
                ret = yield f
                for row in ret:
                    row2 = self._list_to_dict('categoryvalue',row)
                    categorydef_ids.add(row2['categorydef_id'])
                    if not keys:
                        keys = list(row2.keys())
                values.extend(ret)
            if keys and values:
                tables['categoryvalue'] = {'keys':keys,'values':values}

        if categorydef_ids:
            sql = 'select * from categorydef where categorydef_id in (%s)'
            keys = []
            values = []
            for f in self._bulk_select(sql, categorydef_ids):
                ret = yield f
                for row in ret:
                    if not keys:
                        keys = list(self._list_to_dict('categorydef',row).keys())
                        break
                values.extend(ret)
            if keys and values:
                tables['categorydef'] = {'keys':keys,'values':values}

        raise tornado.gen.Return(tables)

    @tornado.gen.coroutine
    def misc_update_tables(self, tables):
        """
        Update the DB tables with the incoming information.

        Args:
            tables (dict): {table_name:{keys:[],values:[[]]}}

        Returns:
            bool: success or failure
        """
        if not tables:
            return
        if not isinstance(tables,dict):
            raise Exception('tables not a dict')
        try:
            for name in tables:
                sql = 'replace into %s ('%filtered_input(name)
                sql += ','.join(str(filtered_input(k)) for k in tables[name]['keys'])
                sql += ') values ('
                sql += ','.join('?' for _ in tables[name]['keys'])
                sql += ')'
                for values in tables[name]['values']:
                    bindings = tuple(values)
                    yield self.parent.db.query(sql, bindings)
        except:
            logger.warn('error updating tables', exc_info=True)
            raise

    @tornado.gen.coroutine
    def misc_update_master_db(self, table, index, timestamp, sql, bindings):
        """
        Update the DB with incoming information (query provided).

        Args:
            table (str): The table affected
            index (str): That table's index id
            timestamp (str): An ISO 8601 UTC timestamp
            sql (str): An sql statement
            bindings (tuple): Bindings for the sql statement
        """
        with (yield self.parent.db.acquire_lock('update_master')):
            try:
                sql2 = 'select timestamp from master_update_history '
                sql2 += 'where table_name = ? and update_index = ?'
                bindings2 = (table,index)
                ret = yield self.parent.db.query(sql2, bindings2)
                prev_timestamp = None
                for row in ret:
                    prev_timestamp = row[0]
                if prev_timestamp and prev_timestamp >= timestamp:
                    logger.info('newer data already present for %s %s %s',
                                table, index, timestamp)
                    return
                yield self.parent.db.query(sql, bindings)
            except MySQLdb.Error:
                logger.warn('dropping history for %r', sql, exc_info=True)
            except:
                logger.warn('error updating master', exc_info=True)
                raise
            else:
                master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                sql2 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                bindings2 = (master_update_history_id,table,index,timestamp)
                try:
                    yield self.parent.db.query(sql2, bindings2)
                except MySQLdb.Error:
                    logger.warn('mysql error updating update_history',
                                exc_info=True)
                except:
                    logger.warn('error updating update_history', exc_info=True)
                    raise
