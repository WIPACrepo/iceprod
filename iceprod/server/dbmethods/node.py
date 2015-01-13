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
from iceprod.core.util import Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime

logger = logging.getLogger('dbmethods.node')

class node(_Methods_Base):
    """
    The node DB methods.
    
    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """
    
    def node_update(self,hostname=None,domain=None,**kwargs):
        """
        Update node data.
        
        Non-blocking, with no return value. Call and forget.
        
        :param hostname: hostname of node
        :param domain: domain of node
        :param **kwargs: gridspec and other statistics
        """
        if not (hostname and domain):
            logger.debug('node_update(): missing hostname or domain')
        elif 'gridspec' not in kwargs:
            logger.debug('node_update(): missing gridspec')
        else:
            cb = partial(self._node_update_blocking,hostname,domain,**kwargs)
            self.db.blocking_task('node_stats',cb)
    def _node_update_blocking(self,hostname,domain,ret,**kwargs):
        conn,archive_conn = self.db._dbsetup()
        now = datetime2str(datetime.utcnow())
        sql = 'select * from node where hostname = ? and domain = ?'
        bindings = (hostname,domain)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            self.logger.info('exception from _node_update_blocking(): %r',ret)
            return
        elif not ret or len(ret) < 1 or len(ret[0]) < 1:
            # insert new row
            sql = 'insert into node (node_id,hostname,domain,last_update,stats)'
            sql += ' values (?,?,?,?,?)'
            bindings = (self.db._increment_id_helper('node',conn),
                        hostname,domain,now,json_encode(kwargs))
        else:
            # update row
            row = self._list_to_dict('node',ret[0])
            old_stats = json_decode(row['stats'])
            stats = kwargs.copy()
            for k in set(stats) & set(old_stats):
                for kk in set(old_stats[k]) - set(stats):
                    stats[k][kk] = old_stats[k][kk]
            sql = 'update node set last_update=?, stats=? where node_id = ?'
            bindings = (now,json_encode(stats),row['node_id'])
        
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            self.logger.info('exception2 from _node_update_blocking(): %r',ret)
    
    def node_collate_resources(self,site_id=None,node_include_age=30):
        """
        Collate node resources into site resources.
        
        Non-blocking, with no return value. Call and forget.
        
        :param site_id: The site to assign resources to
        :param node_include_age: The number of days a node can age before
                                 not being included.
        """
        if not site_id:
            return
        sql = 'select * from node where last_update > ?'
        old_date = datetime.utcnow()-timedelta(days=node_include_age)
        bindings = (datetime2str(old_date),)
        cb = partial(self._node_collate_resources_cb,site_id=site_id)
        self.db.sql_read_task(sql,tuple(),callback=cb)
    def _node_collate_resources_cb(self,ret,site_id=None):
        if isinstance(ret,Exception):
            self.logger.debug('exception in node_collate_resources: %r',ret)
        elif not ret:
            self.logger.debug('no results returned for node_collate_resources')
        else:
            try:
                grid_resources = {}
                for row in ret:
                    row = self._list_to_dict('node',row)
                    stats = json_decode(row['stats'])
                    gridspec = stats.pop('gridspec')
                    if gridspec not in grid_resources:
                        grid_resources[gridspec] = {}
                    for resource in set(Resources)&set(stats):
                        if resource in grid_resources[gridspec]:
                            if (isinstance(grid_resources[gridspec][resource],Number)
                                and isinstance(stats[resource],Number)):
                                grid_resources[gridspec][resource] += stats[resource]
                        else:
                            grid_resources[gridspec][resource] = stats[resource]
                if grid_resources:
                    cb = partial(self._node_collate_resources_blocking,site_id,grid_resources)
                    self.db.blocking_task('node_stats',cb)
            except Exception:
                self.logger.info('error in _node_collate_resources_cb',
                                 exc_info=True)
    def _node_collate_resources_blocking(self,site_id=None,grid_resources=None):
        conn,archive_conn = self.db._dbsetup()
        sql = 'select queues from site where site_id = ?'
        bindings = (site_id,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            self.logger.info('failed to get site queues for site %r',site_id,
                             exc_info=True)
            return
        if isinstance(ret,Exception):
            self.logger.debug('exception in _node_collate_resources_blocking for site %r: %r',
                              site_id,ret)
        elif not ret or not ret[0]:
            self.logger.debug('no site queues for site %r',site_id)
        else:
            try:
                queues = json_decode(ret[0][0])
                for gridspec in queues:
                    if gridspec in grid_resources:
                        queues[gridspec]['resources'] = grid_resources[gridspec]
                sql = 'update site set queues = ? where site_id = ?'
                bindings = (json_encode(queues),suite_id)
                try:
                    self.db._db_write(conn,sql,bindings,None,None,None)
                except Exception:
                    self.logger.info('failed to update resources for site %r',
                                     site_id,exc_info=True)
            except Exception:
                self.logger.info('error in _node_collate_resources_blocking',
                                 exc_info=True)
    
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
        sql = 'select queues from sites where site_id = ?'
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
                        if k in resources and isinstance(resources[k],Number):
                            resources[k] += grids['resources'][k][index]
                        else:
                            resources[k] = grids['resources'][k][index]
                return resources
            except Exception as e:
                callback(e)
    