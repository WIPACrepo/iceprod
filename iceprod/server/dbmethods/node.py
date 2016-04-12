"""
Node database methods
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

from iceprod.server.dbmethods import dbmethod,_Methods_Base,datetime2str,str2datetime,nowstr

logger = logging.getLogger('dbmethods.node')

class node(_Methods_Base):
    """
    The node DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    @dbmethod
    def node_update(self,hostname=None,domain=None,callback=None,**kwargs):
        """
        Update node data.

        Non-blocking, with no return value. Call and forget.

        :param hostname: hostname of node
        :param domain: domain of node
        :param \*\*kwargs: gridspec and other statistics
        """
        if not (hostname and domain):
            logger.debug('node_update(): missing hostname or domain')
            return
        elif 'gridspec' not in kwargs:
            logger.debug('node_update(): missing gridspec')
        cb = partial(self._node_update_blocking,hostname,domain,**kwargs)
        self.db.blocking_task('node_stats',cb)
    def _node_update_blocking(self,hostname,domain,**kwargs):
        conn,archive_conn = self.db._dbsetup()
        now = nowstr()
        sql = 'select * from node where hostname = ? and domain = ?'
        bindings = (hostname,domain)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            logger.info('exception from _node_update_blocking(): %r',ret)
            return
        elif not ret or len(ret) < 1 or len(ret[0]) < 1:
            # insert new row
            node_id = self.db._increment_id_helper('node',conn)
            sql = 'insert into node (node_id,hostname,domain,last_update,stats)'
            sql += ' values (?,?,?,?,?)'
            bindings = (node_id,hostname,domain,now,json_encode(kwargs))
        else:
            # update row
            try:
                row = self._list_to_dict('node',ret[0])
                node_id = row['node_id']
                old_stats = json_decode(row['stats'])
                stats = kwargs.copy()
                for k in set(stats) & set(old_stats):
                    if (isinstance(old_stats[k],dict) and
                        isinstance(stats[k],dict)):
                        for kk in set(old_stats[k]) - set(stats[k]):
                            stats[k][kk] = old_stats[k][kk]
                for k in set(old_stats) - set(stats):
                    stats[k] = old_stats[k]
                sql = 'update node set last_update=?, stats=? where node_id = ?'
                bindings = (now,json_encode(stats),node_id)
            except Exception:
                logger.warn('error in _node_update_blocking()',
                            exc_info=True)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            logger.info('exception2 from _node_update_blocking(): %r',ret)
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('node',node_id,now)
                try:
                    self.db._db_write(conn,sql3,bindings3,None,None,None)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                self._send_to_master(('node',node_id,now,sql,bindings))

    @dbmethod
    def node_collate_resources(self,site_id=None,node_include_age=30, callback=None):
        """
        Collate node resources into site resources.

        Non-blocking, with no return value. Call and forget.

        :param site_id: The site to assign resources to
        :param node_include_age: The number of days a node can age before
                                 not being included.

        (Note: callback is a dummy parameter)
        """
        if not site_id:
            return
        sql = 'select * from node where last_update > ?'
        old_date = datetime.utcnow()-timedelta(days=node_include_age)
        bindings = (datetime2str(old_date),)
        cb = partial(self._node_collate_resources_cb,site_id=site_id)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _node_collate_resources_cb(self,ret,site_id=None):
        if isinstance(ret,Exception):
            logger.info('exception in node_collate_resources: %r',ret)
        elif not ret:
            logger.debug('no results returned for node_collate_resources')
        else:
            try:
                grid_resources = {}
                for row in ret:
                    row = self._list_to_dict('node',row)
                    stats = json_decode(row['stats'])
                    gridspec = stats.pop('gridspec')
                    if gridspec not in grid_resources:
                        grid_resources[gridspec] = {}
                    for resource in set(Node_Resources)&set(stats):
                        if not stats[resource]:
                            continue # resource is 0 or False
                        if (resource in grid_resources[gridspec]
                            and isinstance(grid_resources[gridspec][resource],Number)
                            and isinstance(stats[resource],Number)):
                            grid_resources[gridspec][resource] += stats[resource]
                        else:
                            grid_resources[gridspec][resource] = stats[resource]
                if grid_resources:
                    cb = partial(self._node_collate_resources_blocking,site_id,grid_resources)
                    self.db.blocking_task('node_stats',cb)
            except Exception:
                logger.info('error in _node_collate_resources_cb',
                                 exc_info=True)
    def _node_collate_resources_blocking(self,site_id=None,grid_resources=None):
        conn,archive_conn = self.db._dbsetup()
        sql = 'select queues from site where site_id = ?'
        bindings = (site_id,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            logger.info('failed to get site queues for site %r',site_id,
                             exc_info=True)
            return
        if isinstance(ret,Exception):
            logger.debug('exception in _node_collate_resources_blocking for site %r: %r',
                              site_id,ret)
        elif not ret or not ret[0]:
            logger.debug('no site queues for site %r',site_id)
        else:
            try:
                queues = json_decode(ret[0][0])
                for gridspec in queues:
                    if gridspec in grid_resources:
                        gg = grid_resources[gridspec]
                        if 'resources' not in queues[gridspec]:
                            queues[gridspec]['resources'] = dict.from_keys(gg.keys(),[0,0])
                        qq = queues[gridspec]['resources']
                        for r in gg:
                            if r not in qq:
                                qq[r] = [0,0]
                            if isinstance(gg[r],Number):
                                qq[r][1] += gg[r]-qq[r][0]
                                if qq[r][1] < 0:
                                    qq[r][1] = 0
                                qq[r][0] = gg[r]
                            else:
                                qq[r][0] = gg[r]
                        for r in set(qq)-set(gg):
                            qq[r] = [0,0]
                sql = 'update site set queues = ? where site_id = ?'
                bindings = (json_encode(queues),site_id)
                try:
                    self.db._db_write(conn,sql,bindings,None,None,None)
                except Exception:
                    logger.info('failed to update resources for site %r',
                                site_id,exc_info=True)
                else:
                    if self._is_master():
                        sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                        bindings3 = ('site',site_id,now)
                        try:
                            self.db._db_write(conn,sql3,bindings3,None,None,None)
                        except Exception as e:
                            logger.info('error updating master_update_history',
                                        exc_info=True)
                    else:
                        self._send_to_master(('site',site_id,now,sql,bindings))
            except Exception:
                logger.info('error in _node_collate_resources_blocking',
                            exc_info=True)

    @dbmethod
    def node_get_site_resources(self,site_id=None,empty_only=True,callback=None):
        """
        Get all resources for a site.

        :param site_id: The site to examine
        :param empty_only: Get only the empty resources, defaults to True
        :returns: (via callback) dict of resources
        """
        if not site_id:
            callback(Exception('no site_id defined'))
            return
        sql = 'select queues from site where site_id = ?'
        bindings = (site_id,)
        cb = partial(self._node_get_site_resources_cb,site_id,empty_only=empty_only,
                     callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _node_get_site_resources_cb(self,site_id,ret,empty_only=True,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or not ret[0]:
            callback(Exception('no queues found'))
        else:
            index = 1 if empty_only else 0
            try:
                queues = json_decode(ret[0][0])
                resources = {}
                for grids in queues.values():
                    if 'resources' not in grids:
                        continue
                    for k in grids['resources']:
                        if isinstance(grids['resources'][k][0],Number):
                            if k in resources:
                                resources[k] += grids['resources'][k][index]
                            else:
                                resources[k] = grids['resources'][k][index]
                        else:
                            resources[k] = grids['resources'][k][0]
                callback(resources)
            except Exception as e:
                logger.warn('error in get_site_resources',exc_info=True)
                callback(e)
