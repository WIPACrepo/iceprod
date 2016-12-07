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

import tornado.gen

import iceprod.core.functions
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime, filtered_input

logger = logging.getLogger('dbmethods.web')

class web(_Methods_Base):
    """
    The website DB methods.
    """

    @tornado.gen.coroutine
    def web_get_tasks_by_status(self, gridspec=None, dataset_id=None):
        """
        Get the number of tasks in each state on this site and plugin.

        Args:
            gridspec (str): grid and plugin id
            dataset_id (str): dataset id

        Returns:
           dict: {status:num}
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
        ret = yield self.parent.db.query(sql, bindings)
        task_groups = {}
        for status,num in ret:
            task_groups[status] = num
        raise tornado.gen.Return(task_groups)

    @tornado.gen.coroutine
    def web_get_datasets(self, gridspec=None, groups=None, **filters):
        """
        Get the number of datasets in each state on this site and plugin.

        Filters are specified as key=['match1','match2']

        Args:
            gridspec (str): grid and plugin id
            groups (iterable): Fields to group by
            **filters (dict): (optional) filters for the query

        Returns:
            list: [{dataset}]
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
            sql += ' gridspec like ? '
            bindings.append('%'+gridspec+'%')
        for f in filters:
            if filters[f]:
                sql += ' '+filtered_input(f)+' in ('
                sql += ','.join('?' for _ in range(len(filters[f])))
                sql += ') '
                bindings.extend(filters[f])
        if groups:
            sql += ' group by ' + ','.join(groups)
        ret = yield self.parent.db.query(sql, bindings)

        def grouper(data, groups, val):
            if len(groups) == 1:
                data[groups[0]] = val
            else:
                if groups[0] not in data:
                    data[groups[0]] = {}
                grouper(data[groups[0]], groups[1:], val)

        if groups:
            dataset_groups = {}
            for row in ret:
                grouper(dataset_groups,row[:-1],row[-1])
            ret = dataset_groups
        else:
            ret = [self._list_to_dict('dataset',x) for x in ret]
        raise tornado.gen.Return(ret)

    @tornado.gen.coroutine
    def web_get_datasets_details(self, dataset_id=None, status=None,
                                 gridspec=None):
        """
        Get the number of datasets in each state on this site and plugin.

        Args:
            dataset_id (str): dataset id
            status (str): dataset status
            gridspec (str): grid and plugin id

        Returns:
            dict: {status:num}
        """
        sql = 'select * from dataset '
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
        ret = yield self.parent.db.query(sql, bindings)
        datasets = {}
        for row in ret:
            tmp = self._list_to_dict('dataset',row)
            datasets[tmp['dataset_id']] = tmp
        raise tornado.gen.Return(datasets)

    @tornado.gen.coroutine
    def web_get_tasks_details(self, task_id=None, status=None, gridspec=None,
                              dataset_id=None):
        """
        Get the number of tasks in each state on this site and plugin.

        Args:
            task_id (str): task id
            status (str): task status
            gridspec (str): grid and plugin id
            dataset_id (str): dataset id

        Returns:
            dict: {status:num}
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
        ret = yield self.parent.db.query(sql, bindings)
        tasks = {}
        for row in ret:
            tmp = self._list_to_dict(['search','task'],row)
            tasks[tmp['task_id']] = tmp
        raise tornado.gen.Return(tasks)

    @tornado.gen.coroutine
    def web_get_logs(self, task_id, lines=None):
        """
        Get the logs for a task.

        Args:
            task_id (str): task id
            lines (int): tail this number of lines (default: all lines)

        Returns:
            dict: {log_name:text}
        """
        sql = 'select * from task_log where task_id = ?'
        bindings = (task_id,)
        ret = yield self.parent.db.query(sql, bindings)
        logs = {}
        for row in ret:
            tmp = self._list_to_dict('task_log',row)
            if tmp['name'] and tmp['data']:
                data = json_compressor.uncompress(tmp['data'])
                if lines and isinstance(lines,int):
                    data = '\n'.join(data.rsplit('\n',lines+1)[-1*lines:])
                logs[tmp['name']] = data
        def sort_key(k):
            log_order = ['stdout','stderr','stdlog']
            if k in log_order:
                return '_'+str(log_order.index(k))
            else:
                return k
        ret = OrderedDict((k,logs[k]) for k in sorted(logs,key=sort_key))
        raise tornado.gen.Return(ret)

    @tornado.gen.coroutine
    def web_get_gridspec(self):
        """
        Get the possible gridspecs that we know about.

        Returns:
            dict: {gridspecs}
        """
        sql = 'select site_id,queues from site'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        gridspecs = {}
        for site_id,queues in ret:
            try:
                gridspecs.update(json_decode(queues))
            except:
                pass
        raise tornado.gen.Return(gridspecs)

    def web_get_sites(self, **kwargs):
        """Get sites matching kwargs"""
        # TODO: finish this
        raise NotImplementedException()

    @tornado.gen.coroutine
    def web_get_dataset_by_name(self, name, callback=None):
        """
        Get a dataset by its name.

        Args:
            name (str): dataset name

        Returns:
            str: dataset id
        """
        sql = 'select dataset_id from dataset where name = ?'
        bindings = (name,)
        ret = yield self.parent.db.query(sql, bindings)
        if len(ret) == 1:
            raise tornado.gen.Return(ret[0][0])
        else:
            raise Exception('name not found')

    @tornado.gen.coroutine
    def web_get_task_completion_stats(self, dataset_id):
        """
        Get the task completion stats for a dataset.

        Columns:
            task_name
            task_type
            num_queued
            num_running
            num_completions
            avg_runtime
            max_runtime
            min_runtime
            error_count
            efficiency

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {task_name: {column: num} }
        """
        # get task name/type
        logger.info('get task name/type')
        task_rel = {}
        task_rel_index = {}
        sql = 'select task_rel_id, task_index, name, requirements '
        sql += ' from task_rel where dataset_id = ?'
        bindings = (dataset_id,)
        ret = yield self.parent.db.query(sql, bindings)
        for trid, index, name, req in ret:
            task_rel[trid] = ('GPU' if 'gpu' in req.lower() else 'CPU', name)
            task_rel_index[index] = trid
        # get sorted order for task_rel_ids
        task_rel_ids = [task_rel_index[x] for x in sorted(task_rel_index)]

        # get status numbers
        logger.info('get status numbers')
        sql = 'select count(*), status, sum(walltime), sum(walltime_err), '
        sql += 'sum(walltime_err_n), max(walltime), min(walltime), task_rel_id '
        sql += 'from task where task_rel_id in (%s) group by task_rel_id,status'
        task_groups = {trid:[0,0,0,0.0,0.0,0,0,0] for trid in task_rel}
        for f in self._bulk_select(sql,task_rel_ids):
            ret = yield f
            for n,status,wall,wall_e,wall_en,wall_max,wall_min,trid in ret:
                if status == 'queued':
                    task_groups[trid][0] += n
                elif status == 'processing':
                    task_groups[trid][1] += n
                elif status == 'complete':
                    task_groups[trid][2] += n
                    task_groups[trid][3] += wall
                    task_groups[trid][4] += wall+wall_e
                    task_groups[trid][5] += wall_en
                    if ((not task_groups[trid][6]) or
                        task_groups[trid][6] < wall_max):
                        task_groups[trid][6] = wall_max
                    if ((not task_groups[trid][7]) or
                        task_groups[trid][7] > wall_min):
                        task_groups[trid][7] = wall_min

        logger.info('make stats')
        stats = OrderedDict()
        for trid in task_rel_ids:
            if task_groups[trid][4]:
                avg = round(task_groups[trid][3]/task_groups[trid][2],2)
                eff = int(task_groups[trid][3]*100/task_groups[trid][4])
            else:
                avg = 0
                eff = 0
            stats[trid] = {
                'task_name': task_rel[trid][1],
                'task_type': task_rel[trid][0],
                'num_queued': task_groups[trid][0],
                'num_running': task_groups[trid][1],
                'num_completions': task_groups[trid][2],
                'avg_runtime': avg,
                'max_runtime': task_groups[trid][6],
                'min_runtime': task_groups[trid][7],
                'error_count': task_groups[trid][5],
                'efficiency': eff,
            }

        raise tornado.gen.Return(stats)
