"""
Queue database methods
"""

import time
import logging
from datetime import datetime
from functools import partial,reduce
import operator
from collections import OrderedDict, defaultdict, Counter, Iterable
import math
import random

import tornado.gen

from iceprod.core.dataclasses import Number,String
from iceprod.core.exe import Config
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server import GlobalID
from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime,nowstr
from iceprod.server import task_queue

logger = logging.getLogger('dbmethods.queue')

class queue(_Methods_Base):
    """
    The Queue DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    def _queue_get_task_from_ret(self, ret):
        tasks = OrderedDict()
        try:
            for row in ret:
                dict_row = self._list_to_dict('task',row)
                dict_row['status_changed'] = str2datetime(dict_row['status_changed'])
                tasks[row[0]] = dict_row
        except Exception:
            return {}
        else:
            return tasks

    @tornado.gen.coroutine
    def queue_set_site_queues(self, site_id, queues):
        """
        Set the site queues

        Args:
            site_id (str): The site id
            queues (dict): The new site queues
        """
        with (yield self.parent.db.acquire_lock('site')):
            sql = 'select * from site where site_id = ?'
            bindings = (site_id,)
            ret = yield self.parent.db.query(sql, bindings)
            if len(ret) > 0 and len(ret[0]) > 0:
                # already a site entry, so just update
                try:
                    old_site = self._list_to_dict('site',ret[0])
                    old_queues = json_decode(old_site['queues'])
                    for k in set(queues) & set(old_queues):
                        try:
                            for kk in set(old_queues[k]['resources']) - set(queues[k]['resources']):
                                queues[k]['resources'][kk] = old_queues[k]['resources'][kk]
                        except Exception:
                            try:
                                queues[k]['resources'] = old_queues[k]['resources']
                            except Exception:
                                queues[k]['resources'] = {}
                    queues = json_encode(queues)
                except Exception:
                    logger.warning('set_site_queues(): cannot encode queues to json')
                    raise
                sql = 'update site set queues = ? where site_id = ?'
                bindings = (queues,site_id)
            else:
                # add a new site entry
                try:
                    queues = json_encode(queues)
                except Exception:
                    logger.warning('set_site_queues(): cannot encode queues to json')
                    raise
                sql = 'insert into site (site_id,queues) values (?,?)'
                bindings = (site_id,queues)
            yield self.parent.db.query(sql, bindings)
            if self._is_master():
                master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                bindings3 = (master_update_history_id,'site',site_id,nowstr())
                try:
                    yield self.parent.db.query(sql3, bindings3)
                except Exception:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                yield self._send_to_master(('site',site_id,nowstr(),sql,bindings))

    @tornado.gen.coroutine
    def queue_get_active_tasks(self, gridspec=None):
        """
        Get a dict of active tasks (waiting,queued,processing,reset,resume).

        Args:
            gridspec (str): The gridspec (None for master)

        Returns:
            dict: {status:{task_id:task}}
        """
        try:
            sql = 'select task_id from search where '
            sql += 'task_status in ("waiting","queued","processing","reset","resume")'
            if gridspec:
                sql += ' and gridspec like ?'
                bindings = ('%'+gridspec+'%',)
            else:
                bindings = tuple()
            ret = yield self.parent.db.query(sql, bindings)
            tasks = set(row[0] for row in ret)

            sql = 'select * from task where task_id in (%s)'
            task_groups = {}
            for f in self._bulk_select(sql,tasks):
                ret = yield f
                tasks = self._queue_get_task_from_ret(ret)
                for task_id in tasks:
                    status = tasks[task_id]['status']
                    if status not in ("waiting","queued","processing","reset","resume"):
                        continue
                    if status not in task_groups:
                        task_groups[status] = {}
                    task_groups[status][task_id] = tasks[task_id]
        except Exception:
            logger.info('error getting active tasks', exc_info=True)
            raise
        else:
            raise tornado.gen.Return(task_groups)

    @tornado.gen.coroutine
    def queue_get_grid_tasks(self, gridspec):
        """
        Get a list of tasks (queued, processing) on this
        site and plugin.

        Args:
            gridspec (str): The gridspec (None for master)

        Returns:
            list: [(task_id, grid_queue_id, submit_time, and submit_dir)]
        """
        try:
            sql = 'select task_id from search '
            sql += 'where gridspec like ? '
            sql += ' and task_status in ("queued","processing")'
            bindings = ('%'+gridspec+'%',)
            ret = yield self.parent.db.query(sql, bindings)
            tasks = set(row[0] for row in ret)

            sql = 'select * from task where task_id in (%s)'
            task_ret = []
            for f in self._bulk_select(sql,tasks):
                ret = self._queue_get_task_from_ret((yield f))
                for task_id in ret:
                    if ret[task_id]['status'] not in ("queued","processing"):
                        continue
                    task_ret.append({
                        'task_id': task_id,
                        'grid_queue_id': ret[task_id]['grid_queue_id'],
                        'submit_time': ret[task_id]['status_changed'],
                        'submit_dir': ret[task_id]['submit_dir'],
                    })
            logger.info("***********queued tasks: %r", task_ret)
        except Exception:
            logger.info('error getting grid tasks', exc_info=True)
            raise
        else:
            raise tornado.gen.Return(task_ret)

    @tornado.gen.coroutine
    def queue_set_task_status(self, task, status):
        """
        Set the status of a task, except if it's complete.

        Args:
            task (str or iterable): task_id or iterable of task_ids
            status (str): status to set
        """
        if isinstance(task,String):
            task = [task]
        elif not isinstance(task,Iterable):
            raise Exception('unknown type for task')
        sql = 'select task_id from search where '
        sql += 'task_id in (%s) and task_status != "complete"'
        tids = []
        for f in self._bulk_select(sql,task):
            ret = yield f
            tids.extend(row[0] for row in ret)
        logger.debug("task_ids: %r",tids)
        
        now = nowstr()
        sql = 'update search set task_status = ? '
        sql += ' where task_id in (%s)'
        bindings = (status,)
        sql2 = 'update task set prev_status = status, '
        sql2 += ' status = ?, status_changed = ? where task_id in (%s)'
        bindings2 = (status,now)

        self._bulk_select(sql, tids, extra_bindings=bindings)
        self._bulk_select(sql2, tids, extra_bindings=bindings2)

        for tt in tids:
            if self._is_master():
                master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                bindings3 = (master_update_history_id,'search',tt,now)
                master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                bindings4 = (master_update_history_id,'task',tt,now)
                yield self.parent.db.query([sql3,sql3],[bindings3,bindings4])
            else:
                bindings = (status,tt)
                bindings2 = (status,now,tt)
                yield self._send_to_master(('search',tt,now,sql%'?',bindings))
                yield self._send_to_master(('task',tt,now,sql2%'?',bindings2))

    @tornado.gen.coroutine
    def queue_reset_tasks(self, reset=[], fail=[]):
        """Reset and fail specified tasks"""
        if reset:
            yield self.queue_set_task_status(reset,'reset')
        if fail:
            yield self.queue_set_task_status(fail,'failed')

    @tornado.gen.coroutine
    def queue_get_task(self, task_id):
        """
        Get tasks specified by task_id.

        Args:
            task (str or iterable): task_id or iterable of task_ids

        Returns:
            dict: tasks
        """
        if isinstance(task_id,str):
            # single task
            sql = 'select * from task where task_id = ?'
            bindings = (task_id,)
        elif isinstance(task_id,Iterable):
            # multiple tasks
            b = ','.join(['?' for _ in range(len(task_id))])
            sql = 'select * from task where task_id in ('+b+')'
            bindings = tuple(task_id)
        else:
            raise Exception('task_id is not a str or iterable')

        ret = yield self.parent.db.query(sql,bindings)
        raise tornado.gen.Return(self._queue_get_task_from_ret(ret))

    @tornado.gen.coroutine
    def queue_get_task_by_grid_queue_id(self, grid_queue_id):
        """
        Get tasks specified by grid_queue_id.

        Args:
            grid_queue_id (str): Id or list of ids

        Returns:
            dict: tasks
        """
        if isinstance(grid_queue_id,str):
            # single task
            sql = 'select * from task where grid_queue_id = ?'
            bindings = (grid_queue_id,)
        elif isinstance(grid_queue_id,Iterable):
            # multiple tasks
            b = ','.join(['?' for _ in range(len(grid_queue_id))])
            sql = 'select * from task where grid_queue_id in ('+b+')'
            bindings = tuple(grid_queue_id)
        else:
            raise Exception('grid_queue_id is not a str or iterable')

        ret = yield self.parent.db.query(sql, bindings)
        raise tornado.gen.Return(self._queue_get_task_from_ret(ret))

    @tornado.gen.coroutine
    def queue_set_submit_dir(self, task, submit_dir):
        """
        Set the submit_dir of a task.

        Args:
            task (str): task_id
            submit_dir (str): Submit directory
        """
        if not task:
            raise Exception('No task')
        sql = 'update task set submit_dir = ? '
        sql += ' where task_id = ?'
        bindings = (submit_dir,task)
        yield self.parent.db.query(sql, bindings)
        if self._is_master():
            master_update_history_id = yield self.parent.db.increment_id('master_update_history')
            sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
            bindings3 = (master_update_history_id,'task',task,nowstr())
            try:
                yield self.parent.db.query(sql3, bindings3)
            except Exception:
                logger.info('error updating master_update_history',
                            exc_info=True)
        else:
            yield self._send_to_master(('task',task,nowstr(),sql,bindings))

    @tornado.gen.coroutine
    def queue_set_grid_queue_id(self, task, grid_queue_id):
        """
        Set the grid_queue_id of a task.

        Args:
            task (str): task_id
            grid_queue_id (str): Grid queue id
        """
        if not task:
            raise Exception('No task')
        sql = 'update task set grid_queue_id = ? '
        sql += ' where task_id = ?'
        bindings = (grid_queue_id,task)
        yield self.parent.db.query(sql, bindings)
        if self._is_master():
            master_update_history_id = yield self.parent.db.increment_id('master_update_history')
            sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
            bindings3 = (master_update_history_id,'task',task,nowstr())
            try:
                yield self.parent.db.query(sql3, bindings3)
            except Exception:
                logger.info('error updating master_update_history',
                            exc_info=True)
        else:
            yield self._send_to_master(('task',task,nowstr(),sql,bindings))

    @tornado.gen.coroutine
    def queue_buffer_jobs_tasks(self, gridspec=None, num_jobs=1000):
        """
        Create a buffer of jobs and tasks ahead of queueing.

        Args:
            gridspec (str or iterable): Single or multiple gridspecs to match.
                                        `None` for global queueing.
            num_jobs (int): Number of jobs to buffer.
        """
        now = nowstr()
        # get possible datasets to buffer from
        sql = 'select dataset_id, status, gridspec, jobs_submitted '
        sql += 'from dataset where status = ? '
        bindings = ('processing',)
        if isinstance(gridspec, String):
            sql += 'and gridspec like ?'
            bindings += ('%'+gridspec+'%',)
            gridspec = [gridspec]
        elif isinstance(gridspec, Iterable):
            if len(gridspec) < 1:
                logger.info('in buffer_jobs_tasks, no gridspec %r', gridspec)
                raise Exception('no gridspec defined')
            sql += 'and ('+(' or '.join(['gridspec like ?' for _ in gridspec]))+')'
            bindings += tuple(['%'+g+'%' for g in gridspec])
        elif gridspec:
            logger.info('in buffer_jobs_tasks, unknown gridspec %r', gridspec)
            raise Exception('unknown gridspec type')
        ret = yield self.parent.db.query(sql, bindings)
        need_to_buffer = {}
        for d, s, gs, js in ret:
            logger.debug('gs=%r, gridspec=%r', gs, gridspec)
            need_to_buffer[d] = {'gridspec':gs,'jobs':js,'job_index':0}
        if not need_to_buffer:
            # nothing to buffer
            logger.info('nothing to buffer')
            return

        with (yield self.parent.db.acquire_lock('queue')):
            # remove already buffered jobs
            sql = 'select dataset_id,count(*) from job '
            sql += ' where dataset_id in ('
            sql += ','.join(['?' for _ in need_to_buffer])
            sql += ') group by dataset_id'
            bindings = tuple(need_to_buffer)
            ret = yield self.parent.db.query(sql, bindings)
            for d, num in ret:
                need_to_buffer[d]['job_index'] = num

            # get task_rels for buffering datasets
            task_rel_ids = {}
            task_rel_reqs = {}
            try:
                sql = 'select task_rel_id,dataset_id,task_index,name,depends,requirements from task_rel '
                sql += 'where dataset_id in ('
                sql += ','.join('?' for _ in need_to_buffer)+')'
                bindings = tuple(need_to_buffer)
                ret = yield self.parent.db.query(sql, bindings)
                for tr_id, dataset_id, index, name, deps, reqs in ret:
                    if 'task_rels' not in need_to_buffer[dataset_id]:
                        need_to_buffer[dataset_id]['task_rels'] = {}
                    need_to_buffer[dataset_id]['task_rels'][tr_id] = (index,name,deps)
                    task_rel_ids[tr_id] = (dataset_id,index,deps)
                    task_rel_reqs[tr_id] = json_decode(reqs) if reqs else None
            except Exception as e:
                logger.info('error getting task_rels', exc_info=True)
                raise
            
            dataset_configs = {}
            try:
                sql = 'select dataset_id,config_data from config '
                sql += ' where dataset_id in ('
                sql += ','.join(['?' for _ in need_to_buffer])
                sql += ')'
                bindings = tuple(need_to_buffer)
                ret = yield self.parent.db.query(sql, bindings)
                for dataset_id, config in ret:
                    dataset_configs[dataset_id] = json_decode(config)
            except Exception as e:
                logger.info('error getting dataset configs', exc_info=True)
                raise
            
            # buffer for each dataset
            # TODO: use priorities to do this better
            for dataset in random.sample(list(need_to_buffer),len(need_to_buffer)):
                try:
                    job_index = need_to_buffer[dataset]['job_index']
                    total_jobs = need_to_buffer[dataset]['jobs']
                    task_rels = need_to_buffer[dataset]['task_rels']
                    sorted_task_rels = sorted(task_rels,
                            key=lambda k: task_rels[k][0])
                    sorted_task_rel_values = sorted(task_rels.values(),
                            key=lambda v: v[0])
                    gs = need_to_buffer[dataset]['gridspec']
                    
                    logger.debug('buffering dataset %s, job index %d',
                                 dataset, job_index)

                    db_updates_sql = []
                    db_updates_bindings = []

                    while num_jobs > 0 and job_index < total_jobs:
                        # figure out the task dependencies for the tasks in
                        # the current job
                        depends = []
                        try:
                            for i, x in enumerate(sorted_task_rel_values):
                                index, name, deps = x
                                logger.debug('checking depends: %r',x)
                                task_deps = ([],[])
                                for d in deps.split(','):
                                    if not d:
                                        continue
                                    if d in task_rels:
                                        # linking within job
                                        if i == sorted_task_rels.index(d):
                                            raise Exception('cannot depend on ourself')
                                        task_deps[0].append(task_rels[d][0])
                                        continue
                                    # linking to another dataset
                                    if d not in task_rel_ids:
                                        sql = 'select dataset_id,task_index,depends from task_rel '
                                        sql += 'where task_rel_id = ?'
                                        bindings = (d,)
                                        ret = yield self.parent.db.query(sql, bindings)
                                        for dataset_id, index, deps in ret:
                                            task_rel_ids[d] = (dataset_id,index,deps)
                                    if d not in task_rel_ids:
                                        logger.error('cannot find task_rel_id %r',d)
                                        raise Exception('dependency not found')
                                    
                                    sql = 'select job_id, task_id from search where dataset_id = ?'
                                    bindings = (task_rel_ids[d][0],)
                                    ret = yield self.parent.db.query(sql, bindings)
                                    jobs = {}
                                    for j, t in ret:
                                        if j not in jobs:
                                            jobs[j] = [t]
                                        else:
                                            jobs[j].append(t)
                                    sql = 'select job_id,job_index from job where '
                                    sql += 'job_index = ? and job_id in ('
                                    sql += ','.join('?' for _ in jobs) + ')'
                                    bindings = (job_index,)+tuple(jobs)
                                    ret = yield self.parent.db.query(sql, bindings)
                                    if (not ret) or not ret[0]:
                                        raise Exception('job_index not found')
                                    tasks = sorted(jobs[ret[0][0]], key=lambda k: GlobalID.char2int(k))
                                    task_deps[1].append(tasks[task_rel_ids[d][1]])
                                depends.append(task_deps)
                        except Exception:
                            logger.warning('missing dependency when buffering dataset')
                            raise

                        # make job
                        job_id = yield self.parent.db.increment_id('job')
                        sql = 'insert into job (job_id, dataset_id, status, job_index, '
                        sql += 'status_changed) values (?,?,?,?,?)'
                        bindings = (job_id, dataset, 'processing', job_index, now)
                        db_updates_sql.append(sql)
                        db_updates_bindings.append(bindings)

                        # make tasks
                        task_ids = []
                        for _ in task_rels:
                            x = yield self.parent.db.increment_id('task')
                            task_ids.append(x)
                        sql = 'insert into task (task_id,status,prev_status,'
                        sql += 'status_changed,submit_dir,grid_queue_id,'
                        sql += 'failures,evictions,walltime,walltime_err,walltime_err_n,'
                        sql += 'depends,requirements,task_rel_id) values '
                        sql += '(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
                        sql2 = 'insert into search (task_id,job_id,dataset_id,gridspec,'
                        sql2 += 'name,task_status) values (?,?,?,?,?,?)'
                        for index, task_rel_id in enumerate(sorted_task_rels):
                            deps = [task_ids[i] for i in depends[index][0]]
                            deps.extend(depends[index][1])

                            if not task_rel_reqs[task_rel_id]:
                                reqs = ''
                            else:
                                # parse requirements
                                cfg = dict(dataset_configs[dataset])
                                cfg['options']['job'] = job_index
                                cfg['options']['iter'] = 0
                                cfg['options']['jobs_submitted'] = total_jobs
                                reqs = Config(config=cfg).parseObject(task_rel_reqs[task_rel_id], {})
                                
                                # only store job-specific requirements if they
                                # are distinct from the value in task_rel
                                if reqs != task_rel_reqs[task_rel_id]:
                                    reqs = json_encode(reqs)
                                else:
                                    reqs = ''

                            # task table
                            bindings = (task_ids[index], 'idle', 'idle', now,
                                        '', '', 0, 0, 0.0, 0.0, 0,
                                        ','.join(deps), reqs, task_rel_id)
                            db_updates_sql.append(sql)
                            db_updates_bindings.append(bindings)

                            # search table
                            name = task_rels[task_rel_id][1]
                            bindings2 = (task_ids[index], job_id, dataset, gs, name, 'idle')
                            db_updates_sql.append(sql2)
                            db_updates_bindings.append(bindings2)

                        job_index += 1
                        num_jobs -= 1

                    # write to database
                    yield self.parent.db.query(db_updates_sql, db_updates_bindings)
                    for i in range(len(db_updates_sql)):
                        sql = db_updates_sql[i]
                        bindings = db_updates_bindings[i]
                        if self._is_master():
                            master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                            sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                            bindings3 = (master_update_history_id,sql.split()[2],bindings[0],now)
                            try:
                                yield self.parent.db.query(sql3, bindings3)
                            except Exception:
                                logger.info('error updating master_update_history',
                                            exc_info=True)
                        else:
                            yield self._send_to_master((sql.split()[2],bindings[0],now,sql,bindings))
                except Exception:
                    logger.warning('error buffering dataset %s', dataset, exc_info=True)
                    continue

    @tornado.gen.coroutine
    def queue_get_queueing_datasets(self, gridspec=None):
        """
        Get datasets that are currently in processing status on gridspec.

        Args:
            gridspec (str): The gridspec

        Returns:
            dict: {dataset_id: dataset info}
        """
        bindings = []
        sql = 'select * from dataset where status in ("processing","truncated") '
        if gridspec:
            sql += ' and gridspec like ?'
            bindings.append('%'+gridspec+'%')
        bindings = tuple(bindings)
        ret = yield self.parent.db.query(sql, bindings)
        datasets = {}
        for row in ret:
            d = self._list_to_dict('dataset',row)
            datasets[d['dataset_id']] = d
        raise tornado.gen.Return(datasets)

    @tornado.gen.coroutine
    def queue_get_queueing_tasks(self, dataset_prios, num=20,
                                 resources=None, gridspec_assignment=None,
                                 global_queueing=False):
        """
        Get tasks to queue based on dataset priorities.

        Args:
            dataset_prios (dict): {dataset_id:priority} where sum(priorities)=1
            num (int): (optional) number of tasks to queue
            resources (dict): (optional) available resources on grid
            gridspec_assignment (str): (optional) the grid to assign the tasks to
            global_queueing (bool): Global queueing mode (default: False)

        Returns:
            dict: {task_id:task}
        """
        if dataset_prios is None or not isinstance(dataset_prios,dict):
            raise Exception('dataset_prios not a dict')
        logger.info('queue() num=%r, global=%r, prios=%r, gridspec_assign=%r, resources=%r',
                     num, global_queueing, dataset_prios, gridspec_assignment, resources)

        with (yield self.parent.db.acquire_lock('queue')):
            # get all tasks for processing datasets so we can do dependency check
            try:
                sql = 'select task_rel_id, dataset_id, requirements from task_rel '
                sql += 'where dataset_id in (%s)'
                task_rel_ids = {}
                for f in self._bulk_select(sql, dataset_prios):
                    for task_rel_id, dataset_id, reqs in (yield f):
                        if reqs:
                            reqs = json_decode(reqs)
                        task_rel_ids[task_rel_id] = (dataset_id, reqs)
                if not task_rel_ids:
                    raise tornado.gen.Return({})

                sql = 'select task_id, status, depends, requirements, task_rel_id '
                sql += 'from task where task_rel_id in (%s) '
                if global_queueing:
                    sql += ' and status = "idle" '
                else:
                    sql += ' and status in ("idle","waiting") '
                sql += ' limit '+str(num)
                tasks = {}
                datasets = {k:{} for k in dataset_prios}
                for f in self._bulk_select(sql, task_rel_ids, num=1):
                    for task_id, status, depends, reqs, task_rel_id in (yield f):
                        dataset, task_rel_reqs = task_rel_ids[task_rel_id]
                        tasks[task_id] = {'dataset':dataset, 'status':status}
                        if (status == 'idle' or
                            ((not global_queueing) and status == 'waiting')):
                            for dep in depends.split(','):
                                if dep in tasks and tasks[dep]['status'] != 'complete':
                                    break
                            else:
                                if reqs:
                                    reqs = json_decode(reqs)
                                else:
                                    reqs = task_rel_reqs
                                datasets[dataset][task_id] = (depends,reqs,task_rel_id)
            except Exception:
                logger.info('error getting processing tasks', exc_info=True)
                raise

            # get actual tasks
            task_prio = {}
            for dataset in dataset_prios:
                limit = num
                dataset_task_prio = []
                logger.info('queue() dataset %s, limit is %d, available is %d',
                             dataset, limit, len(datasets[dataset]))
                def sort_key(k):
                    if datasets[dataset][k][0]:
                        return datasets[dataset][k][-1]
                    else:
                        return ''
                for task_id in sorted(datasets[dataset], key=sort_key, reverse=True):
                    depends = datasets[dataset][task_id][0]
                    reqs = datasets[dataset][task_id][1]
                    logger.info('now examining %r, with %r %r',task_id,depends,reqs)
                    satisfied = True
                    if depends == 'unknown': # depends not yet computed
                        satisfied = False
                        logger.info('task %r has unknown depends', task_id)
                    elif depends:
                        for dep in depends.split(','):
                            if dep not in tasks:
                                logger.info('look up depend status: %r',dep)
                                sql = 'select task_status from search where task_id = ?'
                                bindings = (dep,)
                                try:
                                    ret = yield self.parent.db.query(sql, bindings)
                                except Exception:
                                    logger.info('error getting depend task status for %s',
                                                dep, exc_info=True)
                                    satisfied = False
                                    break
                                if (not ret) or len(ret[0]) < 0:
                                    logger.info('bad depend task status result: %r',ret)
                                    satisfied = False
                                    break
                                elif ret[0][0] != 'complete':
                                    logger.info('depends not yet satisfied: %r', task_id)
                                    satisfied = False
                                    break
                            elif tasks[dep]['status'] != 'complete':
                                logger.info('depends not yet satisfied: %r', task_id)
                                satisfied = False
                                break
                    if satisfied and reqs and resources:
                        # now match based on resources
                        try:
                            for r in reqs:
                                if r not in resources:
                                    logger.info('reqs not satisfied: %r', task_id)
                                    satisfied = False
                                    break
                        except Exception:
                            logger.info('failed to check resources',
                                        exc_info=True)
                    if satisfied:
                        # task can be queued now
                        dataset_task_prio.append(task_id)
                        limit -= 1
                        if limit <= 0:
                            break

                task_prio[dataset] = dataset_task_prio

            logger.info('queue() %d tasks can queue',
                        sum(len(task_prio[t]) for t in task_prio))
            if not task_prio:
                raise tornado.gen.Return({})

            # grab tasks from task_prio in order of dataset priority
            dataset_ids = set()
            tasks = set()
            num_to_queue = num
            while num_to_queue > 0 and task_prio:
                for dataset in sorted(task_prio, key=lambda k:dataset_prios[k], reverse=True):
                    if not task_prio[dataset]:
                        del task_prio[dataset]
                        continue
                    dataset_ids.add(dataset)
                    tasks.add(task_prio[dataset].pop())
                    num_to_queue -= 1

            sql = 'select dataset_id, jobs_submitted, debug from dataset '
            sql += ' where dataset_id in (%s)'
            try:
                dataset_debug = {}
                for f in self._bulk_select(sql, dataset_ids):
                    for d_id,js,debug in (yield f):
                        dataset_debug[d_id] = (js,bool(debug))
            except Exception:
                logger.debug('error getting dataset debug', exc_info=True)
                raise
            sql = 'select * from search where task_id in (%s)'
            try:
                ret = []
                for f in self._bulk_select(sql, tasks):
                    ret2 = yield f
                    ret.extend(ret2)
            except Exception:
                logger.debug('error queueing tasks', exc_info=True)
                raise
            tasks = {}
            job_ids = {}
            for row in ret:
                tmp = self._list_to_dict('search',row)
                if tmp['dataset_id'] not in dataset_debug:
                    logger.warning('found a bad dataset: %r', tmp['dataset_id'])
                    continue
                tmp['jobs_submitted'] = dataset_debug[tmp['dataset_id']][0]
                tmp['debug'] = dataset_debug[tmp['dataset_id']][1]
                tmp['reqs'] = datasets[tmp['dataset_id']][tmp['task_id']][1]
                tasks[tmp['task_id']] = tmp
                if tmp['job_id'] not in job_ids:
                    job_ids[tmp['job_id']] = [tmp['task_id']]
                else:
                    job_ids[tmp['job_id']].append(tmp['task_id'])
            if job_ids:
                # get the job index for each task
                sql = 'select job_id,job_index from job where job_id in ('
                sql += ','.join('?' for _ in job_ids)+')'
                bindings = tuple(job_ids)
                ret = yield self.parent.db.query(sql, bindings)
                if (not ret) or not ret[0]:
                    logger.info('sql %r',sql)
                    logger.info('bindings %r',bindings)
                    logger.info('ret %r',ret)
                    logger.warning('failed to find job with known job_id %r for task_id %r',
                                job_ids, list(tasks.keys()))
                    raise Exception('no job_index')
                for job_id,job_index in ret:
                    for task_id in job_ids[job_id]:
                        tasks[task_id]['job'] = job_index
            if tasks:
                # update status
                new_status = 'waiting' if global_queueing else 'queued'
                now = nowstr()
                sql = 'update search set task_status = ? '
                bindings = [new_status]
                if gridspec_assignment:
                    sql += ', gridspec = ? '
                    bindings.append(gridspec_assignment)
                sql += 'where task_id in ('
                sql += ','.join('?' for _ in tasks)
                sql += ')'
                bindings.extend(tasks)
                bindings = tuple(bindings)
                sql2 = 'update task set prev_status = status, '
                sql2 += 'status = ?, '
                sql2 += 'status_changed = ? '
                bindings2 = [new_status, now]
                sql2 += 'where task_id in ('
                sql2 += ','.join('?' for _ in tasks)
                sql2 += ')'
                bindings2.extend(tasks)
                bindings2 = tuple(bindings2)
                yield self.parent.db.query([sql,sql2], [bindings,bindings2])
                if self._is_master():
                    for t in tasks:
                        sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                        master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                        bindings3 = (master_update_history_id,'search',t,now)
                        master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                        bindings4 = (master_update_history_id,'task',t,now)
                        try:
                            yield self.parent.db.query([sql3,sql3], [bindings3,bindings4])
                        except Exception:
                            logger.info('error updating master_update_history',
                                        exc_info=True)
                else:
                    sql = 'update search set task_status=? '
                    if gridspec_assignment:
                        sql += ', gridspec = ? '
                    sql += 'where task_id = ?'
                    sql2 = 'update task set prev_status = status, '
                    sql2 += 'status = ?, '
                    sql2 += 'status_changed = ? '
                    sql2 += 'where task_id = ?'
                    for t in tasks:
                        if gridspec_assignment:
                            bindings = (new_status,gridspec_assignment,t)
                        else:
                            bindings = (new_status,t)
                        bindings2 = (new_status,now,t)
                        yield self._send_to_master(('search',t,now,sql,bindings))
                        yield self._send_to_master(('task',t,now,sql2,bindings2))

        for t in tasks:
            tasks[t]['task_status'] = new_status
            if gridspec_assignment:
                tasks[t]['gridspec'] = gridspec_assignment

        raise tornado.gen.Return(tasks)

    @tornado.gen.coroutine
    def queue_new_pilot_ids(self, num):
        """
        Get new ids for pilots.

        A pre-cursor to :func:`queue_add_pilot`.

        Args:
            num (int): The number of ids to get.

        Returns:
            list: A list of pilot ids.
        """
        try:
            ret = []
            for _ in range(num):
                x = yield self.parent.db.increment_id('pilot')
                ret.append(x)
        except Exception:
            logger.info('new pilot_ids error', exc_info=True)
            raise
        else:
            raise tornado.gen.Return(ret)

    @tornado.gen.coroutine
    def queue_add_pilot(self, pilot):
        """
        Add a pilot to the DB

        Args:
            pilot: The pilot dict.
        """
        try:
            now = nowstr()
            s  = 'insert into pilot (pilot_id, grid_queue_id, submit_time, '
            s += 'submit_dir, tasks, requirements, avail_cpu, avail_gpu, '
            s += 'avail_memory, avail_disk, avail_time, claim_cpu, claim_gpu, '
            s += 'claim_memory, claim_disk, claim_time) values (?,?,?,?,?,?,'
            s += '0,0,0.0,0.0,0.0,0,0,0.0,0.0,0.0)'
            sql = []
            bindings = []
            for i,pilot_id in enumerate(pilot['pilot_ids']):
                grid_queue_id = str(pilot['grid_queue_id'])+'.'+str(i)
                sql.append(s)
                reqs = json_encode(pilot['reqs'])
                bindings.append((pilot_id, grid_queue_id, now, pilot['submit_dir'],'',reqs))
            yield self.parent.db.query(sql, bindings)
        except Exception as e:
            logger.debug('error adding pilot', exc_info=True)
            raise

    @tornado.gen.coroutine
    def queue_get_pilots(self, active=None):
        """
        Get pilot information.

        When `active=True`, get only pilots with tasks.
        When `active=False`, get only idle pilots.
        By default, get all pilots

        Args:
            active (bool): Get only pilots with active tasks (default: None).

        Returns:
            list: [pilot dict]
        """
        sql = 'select * from pilot'
        if active is not None:
            sql += ' where tasks '+('!=' if active else '=')+' "" '
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        pilots = []
        for row in ret:
            tmp = self._list_to_dict('pilot',row)
            tmp['submit_time'] = str2datetime(tmp['submit_time'])
            tmp['tasks'] = tmp['tasks'].split(',')
            if tmp['requirements']:
                tmp['requirements'] = json_decode(tmp['requirements'])
            pilots.append(tmp)
        raise tornado.gen.Return(pilots)

    @tornado.gen.coroutine
    def queue_del_pilots(self, pilots):
        """
        Remove pilots from the DB.

        Args:
            pilots (iterable): List of pilot_ids
        """
        now = nowstr()
        if not isinstance(pilots,list):
            pilots = list(pilots)

        task_ids = set()
        try:
            # work in batches of 900
            while pilots:
                p = pilots[:900]
                pilots = pilots[900:]

                sql = 'select tasks from pilot where pilot_id in ('
                sql += ','.join('?' for _ in p)+')'
                bindings = tuple(p)
                ret = yield self.parent.db.query(sql, bindings)
                for row in ret:
                    tasks = row[0].strip()
                    if tasks:
                        task_ids.update(x for x in row[0].split(',') if x)

                sql = 'delete from pilot where pilot_id in ('
                sql += ','.join('?' for _ in p)+')'
                bindings = tuple(p)
                yield self.parent.db.query(sql, bindings)
        except Exception:
            logger.debug('error deleting pilots', exc_info=True)
            raise
        if task_ids:
            with (yield self.parent.db.acquire_lock('queue')):
                sql = 'select task_id from search '
                sql += 'where task_status = "processing" and task_id in (%s)'
                reset_tasks = set()
                for f in self._bulk_select(sql, task_ids):
                    reset_tasks.update([row[0] for row in (yield f)])
                yield self.queue_set_task_status(reset_tasks,'reset')

    @tornado.gen.coroutine
    def queue_get_cfg_for_task(self, task_id):
        """
        Get a config for a task.

        Args:
            task_id (str): A task id

        Returns:
            str: config as a json blob
        """
        if not task_id:
            raise Exception('bad task_id')
        sql = 'select task_id,dataset_id from search where task_id = ?'
        bindings = (task_id,)
        ret = yield self.parent.db.query(sql, bindings)
        if not ret or len(ret) < 1 or len(ret[0]) < 2:
            raise Exception('get_cfg_for_task did not return a dataset_id')
        else:
            ret = yield self.queue_get_cfg_for_dataset(ret[0][1])
            raise tornado.gen.Return(ret)

    @tornado.gen.coroutine
    def queue_get_cfg_for_dataset(self, dataset_id):
        """
        Get a config for a dataset.

        Args:
            dataset_id (str): A dataset id

        Returns:
            str: config as a json blob
        """
        if not dataset_id:
            raise Exception('bad dataset_id')
        sql = 'select dataset_id,config_data from config where dataset_id = ?'
        bindings = (dataset_id,)
        ret = yield self.parent.db.query(sql, bindings)
        if not ret or len(ret) < 1 or len(ret[0]) < 2:
            raise Exception('get_cfg_for_dataset did not return a config')
        else:
            logger.debug('config for dataset: %r',ret)
            data = None
            for dataset_id,config_data in ret:
                data = config_data
            raise tornado.gen.Return(data)

    @tornado.gen.coroutine
    def queue_add_task_lookup(self, tasks):
        """
        Add the tasks currently available for lookup by pilots.

        Args:
            tasks (dict): dict of {task_id: resources}
        """
        now = time.time()
        keys = next(iter(tasks.values()))
        sql = 'replace into task_lookup (task_id,queue,insert_time,'
        sql += ','.join('req_'+k for k in keys)
        sql += ') values (?,?,?,'
        sql += ','.join('?' for k in keys)+')'
        bindings = []
        for t in tasks:
            reqs = tasks[t]
            queue = task_queue.get_queue(reqs)
            bindings.append((t,queue,now)+tuple(reqs[k] for k in keys))
        yield self.parent.db.query([sql for _ in bindings], bindings)

    @tornado.gen.coroutine
    def queue_get_task_lookup(self):
        """
        Get the resources for all tasks in the lookup.

        Returns:
            dict: {task_id: resources}
        """
        with (yield self.parent.db.acquire_lock('task_lookup')):
            # get tasks from lookup
            sql = 'select * from task_lookup'
            bindings = tuple()
            ret = yield self.parent.db.query(sql, bindings)
            task_ids = {}
            for row in ret:
                row = self._list_to_dict('task_lookup',row)
                tid = row.pop('task_id')
                task_ids[tid] = {k.replace('req_',''):row[k] for k in row if k.startswith('req_')}

            # check that these are still valid
            sql = 'select task_id from search where task_id in (%s) and task_status = ?'
            bindings = ('queued',)
            ret = {}
            for f in self._bulk_select(sql, task_ids, extra_bindings=bindings):
                for row in (yield f):
                    tid = row[0]
                    ret[tid] = task_ids[tid]
            invalid_tasks = set(task_ids).difference(ret)
            if invalid_tasks:
                logger.info('tasks not valid, remove from task_lookup: %s',
                            invalid_tasks)
                sql = 'delete from task_lookup where task_id in (%s)'
                for f in self._bulk_select(sql, invalid_tasks):
                    yield f

            reset_tasks = set(ret).difference(task_ids)
            if reset_tasks:
                logger.info('tasks queued, but not in task_lookup: %s',
                            reset_tasks)
                yield self.parent.service['queue_set_task_status'](reset_tasks,'waiting')

            raise tornado.gen.Return(ret)
