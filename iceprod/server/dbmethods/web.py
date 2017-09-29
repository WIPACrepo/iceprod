"""
Website database methods
"""

import os
import logging
from datetime import datetime,timedelta
from functools import partial,reduce
import operator
from collections import OrderedDict, Iterable, defaultdict
import math
import uuid
import shutil
from io import BytesIO

import tornado.gen

import iceprod.core.functions
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor
from iceprod.server import GlobalID
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
        task_groups = {status:num for status,num in ret}
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
            ret.sort(key=lambda x:x['start_date'], reverse=True)
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
        sql = 'select * from search '
        bindings = tuple()
        if task_id:
            sql += ' where task_id = ? '
            bindings += (task_id,)
        if status:
            if 'where' not in sql:
                sql += ' where '
            sql += ' task_status = ? '
            bindings += (status,)
        if dataset_id:
            if 'where' not in sql:
                sql += ' where '
            else:
                sql += ' and '
            sql += ' dataset_id = ? '
            bindings += (dataset_id,)
        if gridspec:
            if 'where' not in sql:
                sql += ' where '
            else:
                sql += ' and '
            sql += ' gridspec like ? '
            bindings += ('%'+gridspec+'%',)
        ret = yield self.parent.db.query(sql, bindings)
        tasks = {}
        job_ids = defaultdict(list)
        for row in ret:
            tmp = self._list_to_dict('search',row)
            tasks[tmp['task_id']] = tmp
            job_ids[tmp['job_id']].append(tmp['task_id'])
        sql = 'select * from task where task_id in (%s)'
        for f in self._bulk_select(sql, tasks):
            for row in (yield f):
                tmp = self._list_to_dict('task',row)
                tasks[tmp['task_id']].update(tmp)
        sql = 'select job_id, job_index from job where job_id in (%s)'
        for f in self._bulk_select(sql, job_ids):
            for job_id,job_index in (yield f):
                for task_id in job_ids[job_id]:
                    tasks[task_id]['job_index'] = job_index
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
            except Exception:
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
        task_groups = {trid:[0,0,0,0,0,0.0,0.0,0,0,0] for trid in task_rel}
        for f in self._bulk_select(sql,task_rel_ids):
            ret = yield f
            for n,status,wall,wall_e,wall_en,wall_max,wall_min,trid in ret:
                if status == 'waiting':
                    task_groups[trid][0] += n
                elif status == 'queued':
                    task_groups[trid][1] += n
                elif status == 'processing':
                    task_groups[trid][2] += n
                elif status in ('resume','reset','failed'):
                    task_groups[trid][3] += n
                elif status == 'complete':
                    task_groups[trid][4] += n
                    task_groups[trid][5] += wall
                    task_groups[trid][6] += wall+wall_e
                    task_groups[trid][7] += wall_en
                    if ((not task_groups[trid][8]) or
                        task_groups[trid][8] < wall_max):
                        task_groups[trid][8] = wall_max
                    if ((not task_groups[trid][9]) or
                        task_groups[trid][9] > wall_min):
                        task_groups[trid][9] = wall_min

        logger.info('make stats')
        stats = OrderedDict()
        for trid in task_rel_ids:
            if task_groups[trid][6]:
                avg = task_groups[trid][5]/task_groups[trid][4]
                eff = task_groups[trid][5]/task_groups[trid][6]
            else:
                avg = 0
                eff = 0
            stats[trid] = {
                'task_name': task_rel[trid][1],
                'task_type': task_rel[trid][0],
                'num_waiting': task_groups[trid][0],
                'num_queued': task_groups[trid][1],
                'num_running': task_groups[trid][2],
                'num_error': task_groups[trid][3],
                'num_completions': task_groups[trid][4],
                'avg_runtime': avg,
                'max_runtime': task_groups[trid][8],
                'min_runtime': task_groups[trid][9],
                'error_count': task_groups[trid][7],
                'efficiency': eff,
            }

        raise tornado.gen.Return(stats)

    @tornado.gen.coroutine
    def web_get_job_counts_by_status(self, status=None, dataset_id=None):
        """
        Get count of jobs by status.

        Args:
            status (str): status to restrict by
            dataset_id (str): dataset id

        Returns:
           dict: {status: count}
        """
        where_query = {}
        if dataset_id:
            where_query['dataset_id'] = dataset_id
        if status:
            where_query['status'] = status

        sql = 'select status,count(*) from job'
        if where_query:
            sql += ' where '
            sql += ' and '.join(w+' = ?' for w in where_query)
        sql += ' group by status'
        bindings = tuple(where_query.values())
        ret = yield self.parent.db.query(sql, bindings)
        jobs = {status:num for status,num in ret}
        raise tornado.gen.Return(jobs)

    @tornado.gen.coroutine
    def web_get_jobs_by_status(self, status=None, dataset_id=None):
        """
        Get basic job info.

        Args:
            status (str): status to restrict by
            dataset_id (str): dataset id

        Returns:
           dict: [job_info]
        """
        where_query = {}
        if dataset_id:
            where_query['dataset_id'] = dataset_id
        if status:
            where_query['status'] = status

        sql = 'select * from job'
        if where_query:
            sql += ' where '
            sql += ' and '.join(w+' = ?' for w in where_query)
        bindings = tuple(where_query.values())
        ret = yield self.parent.db.query(sql, bindings)
        jobs = [self._list_to_dict(['job'],row) for row in ret]
        jobs.sort(key=lambda j:j['job_index'])
        raise tornado.gen.Return(jobs)

    @tornado.gen.coroutine
    def web_get_jobs_details(self, job_id):
        """
        Get job details for a job_id.

        Args:
            job_id (str): job_id

        Returns:
            dict: {job_id:details}
        """
        sql = 'select * from job where job_id = ?'
        bindings = (job_id,)
        ret = yield self.parent.db.query(sql, bindings)
        job = {}
        for row in ret:
            job.update(self._list_to_dict(['job'],row))

        sql = 'select search.*,task.* from search '
        sql += ' join task on search.task_id = task.task_id '
        sql += ' where job_id = ?'
        bindings = (job_id,)
        ret = yield self.parent.db.query(sql, bindings)
        tasks = []
        for row in ret:
            tmp = self._list_to_dict(['search','task'],row)
            job['dataset_id'] = tmp.pop('dataset_id')
            if tmp['requirements']:
                tmp['requirements'] = json_decode(tmp['requirements'])
            tasks.append(tmp)

        if 'dataset_id' in job:
            sql = 'select task_rel_id,task_index,requirements '
            sql += ' from task_rel where dataset_id = ?'
            bindings = (job['dataset_id'],)
            ret = yield self.parent.db.query(sql, bindings)
            task_rels = {}
            for task_rel_id,task_index,requirements in ret:
                reqs = {}
                if requirements:
                    try:
                        reqs = json_decode(requirements)
                    except Exception:
                        pass
                task_rels[task_rel_id] = {
                    'index': task_index,
                    'reqs': reqs,
                }
            for t in tasks:
                tmp = task_rels[t['task_rel_id']]['reqs'].copy()
                if t['requirements']:
                    tmp.update(t['requirements'])
                t['requirements'] = tmp
                t['index'] = task_rels[t['task_rel_id']]['index']
            tasks.sort(key=lambda t:t['index'])
        job['tasks'] = tasks
        raise tornado.gen.Return(job)