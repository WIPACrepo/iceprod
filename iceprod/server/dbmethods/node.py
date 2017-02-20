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

import tornado.gen

import iceprod.core.functions
from iceprod.core.util import Node_Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime,nowstr

logger = logging.getLogger('dbmethods.node')

class node(_Methods_Base):
    """
    The node DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    @tornado.gen.coroutine
    def node_update(self,hostname=None,domain=None,**kwargs):
        """
        Update node data.

        Args:
            hostname (str): hostname of node
            domain (str): domain of node
            \*\*kwargs: gridspec and other statistics
        """
        if not (hostname and domain):
            logger.debug('node_update(): missing hostname or domain')
            return
        elif 'gridspec' not in kwargs:
            logger.debug('node_update(): missing gridspec')
        with (yield self.parent.db.acquire_lock('node')):
            now = nowstr()
            sql = 'select * from node where hostname = ? and domain = ?'
            bindings = (hostname,domain)
            ret = yield self.parent.db.query(sql, bindings)
            if (not ret) or len(ret) < 1 or len(ret[0]) < 1:
                # insert new row
                node_id = yield self.parent.db.increment_id('node')
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
                    logger.warn('error in node_update()', exc_info=True)
            yield self.parent.db.query(sql, bindings)
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('node',node_id,now)
                try:
                    ret = yield self.parent.db.query(sql3, bindings3)
                except:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                yield self._send_to_master(('node',node_id,now,sql,bindings))

    @tornado.gen.coroutine
    def node_collate_resources(self, site_id=None, node_include_age=30):
        """
        Collate node resources into site resources.

        Args:
            site_id (str): The site to assign resources to
            node_include_age (int): The number of days a node can age before
                                    not being included
        """
        if not site_id:
            return
        try:
            sql = 'select * from node where last_update > ?'
            old_date = datetime.utcnow()-timedelta(days=node_include_age)
            bindings = (datetime2str(old_date),)
            ret = yield self.parent.db.query(sql, bindings)
            if not ret:
                logger.debug('no results returned for node_collate_resources')
                return
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
            if not grid_resources:
                return

            with (yield self.parent.db.acquire_lock('node')):
                sql = 'select queues from site where site_id = ?'
                bindings = (site_id,)
                ret = yield self.parent.db.query(sql, bindings)
                if (not ret) or not ret[0]:
                    logger.debug('no site queues for site %r',site_id)
                    return

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
                yield self.parent.db.query(sql, bindings)
                if self._is_master():
                    sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                    bindings3 = ('site',site_id,nowstr())
                    try:
                        yield self.parent.db.query(sql3, bindings3)
                    except:
                        logger.info('error updating master_update_history',
                                    exc_info=True)
                else:
                    yield self._send_to_master(('site',site_id,nowstr(),sql,bindings))
        except:
            logger.info('error in node_collate_resources', exc_info=True)

    @tornado.gen.coroutine
    def node_get_site_resources(self, site_id=None, empty_only=True):
        """
        Get all resources for a site.

        Args:
            site_id (str): The site to examine
            empty_only (bool): Get only the empty resources, defaults to True

        Returns:
            dict: resources
        """
        if not site_id:
            raise Exception('no site_id defined')
        sql = 'select queues from site where site_id = ?'
        bindings = (site_id,)
        ret = yield self.parent.db.query(sql, bindings)
        if (not ret) or not ret[0]:
            raise Exception('no queues found')
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
        except:
            logger.warn('error in get_site_resources', exc_info=True)
            raise
        else:
            raise tornado.gen.Return(resources)
