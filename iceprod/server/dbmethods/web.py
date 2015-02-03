"""
Website database methods
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

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime, filtered_input

logger = logging.getLogger('dbmethods.web')

class web(_Methods_Base):
    """
    The website DB methods.
    
    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """
    
    def web_get_tasks_by_status(self,gridspec=None,dataset_id=None,callback=None):
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
        cb = partial(self._web_get_tasks_by_status_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _web_get_tasks_by_status_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                task_groups = {}
                if ret and ret[0]:
                    for status,num in ret:
                        task_groups[status] = num
                callback(task_groups)
    
    def web_get_datasets(self,gridspec=None,groups=None,callback=None,**filters):
        """Get the number of datasets in each state on this site and plugin, 
           returning {status:num}
        """
        sql = 'select '
        if groups:
            groups = filtered_input(groups)
            sql += ','.join(groups) + ', count(*) as num '
        else:
            sql += ' * '
        sql += ' from dataset '
        bindings = []
        if gridspec or any(filters.values()):
            sql += ' where '
        if gridspec:
            sql += ' gridspec like "%?%" '
            bindings.append(gridspec)
        for f in filters:
            if filters[f]:
                sql += ' '+filtered_input(f)+' in ('
                sql += ','.join('?' for _ in range(len(filters[f])))
                sql += ') '
                bindings.extend(filters[f])
        if groups:
            sql += ' group by ' + ','.join(groups)
        cb = partial(self._web_get_datasets_callback,groups,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
            
    def _web_get_datasets_grouper(self, data,groups,val):
        if len(groups) == 1:
            data[groups[0]] = val
        else:
            self._web_get_datasets_grouper(data[groups[0]],groups[1:],val)
    def _web_get_datasets_callback(self,groups,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            elif groups:
                dataset_groups = {}
                if ret and ret[0]:
                    for row in ret:
                        self._web_get_datasets_grouper(dataset_groups,groups,row[-1])
                callback(dataset_groups)
            else:
                callback([self._list_to_dict('dataset',x) for x in ret])
    
    def web_get_datasets_details(self,dataset_id=None,status=None,gridspec=None,
                          callback=None):
        """Get the number of datasets in each state on this site and plugin, 
           returning {status:num}
        """
        sql = 'select dataset.* from dataset '
        bindings = tuple()
        if dataset_id:
            sql += ' where dataset.dataset_id = ? '
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
            sql += ' dataset.gridspec like ? '
            bindings += ('%'+gridspec+'%',)
        cb = partial(self._web_get_datasets_details_callback,callback=callback)
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
    
    def web_get_tasks_details(self,task_id=None,status=None,gridspec=None,
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
        cb = partial(self._web_get_tasks_details_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _web_get_tasks_details_callback(self,ret,callback=None):
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
    
    def web_get_logs(self,task_id,lines=None,callback=None):
        """Get the logs for a task, returns {log_name:text}"""
        sql = 'select * from task_log where task_id = ?'
        bindings = (task_id,)
        cb = partial(self._web_get_logs_callback,lines,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _web_get_logs_callback(self,lines,ret,callback=None):
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
    
    def web_get_gridspec(self,callback=None):
        """Get the possible gridspecs that we know about"""
        sql = 'select site_id,queues from site'
        bindings = tuple()
        cb = partial(self._web_get_gridspec_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _web_get_gridspec_callback(self,ret,callback=None):
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
    
    def web_get_sites(self,callback=None,**kwargs):
        """Get sites matching kwargs"""
        # TODO: finish this
        raise NotImplementedException()
