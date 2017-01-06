"""
Queue database methods
"""

import logging
from datetime import datetime
from functools import partial,reduce
import operator
from collections import OrderedDict, Iterable
import math

import tornado.gen

from iceprod.core.util import Node_Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server import GlobalID
from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime,nowstr

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
        except:
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
                        except:
                            try:
                                queues[k]['resources'] = old_queues[k]['resources']
                            except:
                                queues[k]['resources'] = {}
                    queues = json_encode(queues)
                except:
                    logger.warn('set_site_queues(): cannot encode queues to json')
                    raise
                sql = 'update site set queues = ? where site_id = ?'
                bindings = (queues,site_id)
            else:
                # add a new site entry
                try:
                    queues = json_encode(queues)
                except:
                    logger.warn('set_site_queues(): cannot encode queues to json')
                    raise
                sql = 'insert into site (site_id,queues) values (?,?)'
                bindings = (site_id,queues)
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

    @tornado.gen.coroutine
    def queue_get_active_tasks(self, gridspec=None):
        """
        Get a dict of active tasks (waiting,queued,processing,reset,resume).

        Args:
            gridspec (str): The gridspec (None for master)

        Returns:
            dict: {status:{task_id:task}}
        """
        with (yield self.parent.db.acquire_lock('queue')):
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
                        if status not in task_groups:
                            task_groups[status] = {}
                        task_groups[status][task_id] = tasks[task_id]
            except:
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
        with (yield self.parent.db.acquire_lock('queue')):
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
                        task_ret.append({
                            'task_id': task_id,
                            'grid_queue_id': ret[task_id]['grid_queue_id'],
                            'submit_time': ret[task_id]['status_changed'],
                            'submit_dir': ret[task_id]['submit_dir'],
                        })
            except:
                logger.info('error getting grid tasks', exc_info=True)
                raise
            else:
                raise tornado.gen.Return(task_ret)

    @tornado.gen.coroutine
    def queue_set_task_status(self, task, status):
        """
        Set the status of a task.

        Args:
            task (str or iterable): task_id or iterable of task_ids
            status (str): status to set
        """
        now = nowstr()
        if isinstance(task,String):
            task = [task]
        elif not isinstance(task,Iterable):
            raise Exception('unknown type for task')
        
        msql = 'update search set task_status = ? '
        msql += ' where task_id = ?'
        msql2 = 'update task set prev_status = status, '
        msql2 += ' status = ?, status_changed = ? where task_id = ?'

        # process in batches of 900
        if not isinstance(task,list):
            task = list(task)
        try:
            while task:
                t = task[:900]
                task = task[900:]
                b = ','.join('?' for _ in t)
                sql = 'update search set task_status = ? '
                sql += ' where task_id in ('+b+')'
                sql2 = 'update task set prev_status = status, '
                sql2 += ' status = ?, status_changed = ? where task_id in ('+b+')'
                bindings = (status,)+tuple(t)
                bindings2 = (status,now)+tuple(t)
                yield self.parent.db.query([sql,sql2],[bindings,bindings2])
                
                for tt in t:
                    if self._is_master():
                        sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                        bindings3 = ('search',tt,now)
                        bindings4 = ('task',tt,now)
                        yield self.parent.db.query([sql3,sql3],[bindings3,bindings4])
                    else:
                        bindings = (status,tt)
                        bindings2 = (status,now,tt)
                        yield self._send_to_master(('search',tt,now,msql,bindings))
                        yield self._send_to_master(('task',tt,now,msql2,bindings2))
        except:
            logger.info('error updating task status', exc_info=True)
            raise

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
            sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
            bindings3 = ('task',task,nowstr())
            try:
                yield self.parent.db.query(sql3, bindings3)
            except:
                logger.info('error updating master_update_history',
                            exc_info=True)
        else:
            yield self._send_to_master(('task',task,nowstr(),sql,bindings))

    @tornado.gen.coroutine
    def queue_buffer_jobs_tasks(self, gridspec=None, num_tasks=100):
        """
        Create a buffer of jobs and tasks ahead of queueing.

        Args:
            gridspec (str or iterable): Single or multiple gridspecs to match.
                                        `None` for global queueing.
            num_tasks (int): Number of tasks to buffer (rounds up to buffer
                             a full job at once).
        """
        with (yield self.parent.db.acquire_lock('queue')):
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

            # remove already buffered jobs
            sql = 'select dataset_id,job_id from search '
            sql += ' where dataset_id in ('
            sql += ','.join(['?' for _ in need_to_buffer])
            sql += ')'
            bindings = tuple(need_to_buffer)
            ret = yield self.parent.db.query(sql, bindings)
            already_buffered = {}
            for d, job_id in ret:
                if d not in already_buffered:
                    already_buffered[d] = set()
                already_buffered[d].add(job_id)
            for d in need_to_buffer:
                if d in already_buffered:
                    need_to_buffer[d]['job_index'] = len(already_buffered[d])

            # get task_rels for buffering datasets
            task_rel_ids = {}
            try:
                sql = 'select task_rel_id,dataset_id,task_index,name,depends from task_rel '
                sql += 'where dataset_id in ('
                sql += ','.join('?' for _ in need_to_buffer)+')'
                bindings = tuple(need_to_buffer)
                ret = yield self.parent.db.query(sql, bindings)
                for tr_id, dataset_id, index, name, deps in ret:
                    if 'task_rels' not in need_to_buffer[dataset_id]:
                        need_to_buffer[dataset_id]['task_rels'] = {}
                    need_to_buffer[dataset_id]['task_rels'][tr_id] = (index,name,deps)
                    task_rel_ids[tr_id] = (dataset_id,index,deps)
            except Exception as e:
                logger.info('error getting task_rels', exc_info=True)
                raise

            # buffer for each dataset
            # for now, do the stupid thing and just buffer in order
            # TODO: use priorities to do this better
            for dataset in need_to_buffer:
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

                    while num_tasks > 0 and job_index < total_jobs:
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
                            logger.warn('missing dependency when buffering dataset')
                            raise

                        # make job
                        job_id = yield self.parent.db.increment_id('job')
                        sql = 'insert into job (job_id, status, job_index, '
                        sql += 'status_changed) values (?,?,?,?)'
                        bindings = (job_id, 'processing', job_index, now)
                        db_updates_sql.append(sql)
                        db_updates_bindings.append(bindings)

                        # make tasks
                        task_ids = [(yield self.parent.db.increment_id('task'))
                                    for _ in task_rels]
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

                            # task table
                            bindings = (task_ids[index], 'idle', 'idle', now,
                                        '', '', 0, 0, 0.0, 0.0, 0,
                                        ','.join(deps), '', task_rel_id)
                            db_updates_sql.append(sql)
                            db_updates_bindings.append(bindings)

                            # search table
                            name = task_rels[task_rel_id][1]
                            bindings2 = (task_ids[index], job_id, dataset, gs, name, 'idle')
                            db_updates_sql.append(sql2)
                            db_updates_bindings.append(bindings2)

                        job_index += 1
                        num_tasks -= len(task_rels)

                    # write to database
                    yield self.parent.db.query(db_updates_sql, db_updates_bindings)
                    for i in range(len(db_updates_sql)):
                        sql = db_updates_sql[i]
                        bindings = db_updates_bindings[i]
                        if self._is_master():
                            sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                            bindings3 = (sql.split()[2],bindings[0],now)
                            try:
                                yield self.parent.db.query(sql3, bindings3)
                            except:
                                logger.info('error updating master_update_history',
                                            exc_info=True)
                        else:
                            yield self._send_to_master((sql.split()[2],bindings[0],now,sql,bindings))
                except Exception:
                    logger.warn('error buffering dataset %s', dataset, exc_info=True)
                    continue

    @tornado.gen.coroutine
    def queue_get_queueing_datasets(self, gridspec=None):
        """
        Get datasets that are currently in processing status on gridspec.

        Args:
            gridspec (str): The gridspec

        Returns:
            list: dataset_ids
        """
        bindings = []
        sql = 'select * from dataset where status = "processing" '
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
    def queue_get_queueing_tasks(self, dataset_prios, gridspec=None, num=20,
                                 resources=None, gridspec_assignment=None,
                                 global_queueing=False):
        """
        Get tasks to queue based on dataset priorities.

        Args:
            dataset_prios (dict): {dataset_id:priority} where sum(priorities)=1
            gridspec (str): (optional) the grid to queue on
            num (int): (optional) number of tasks to queue
            resources (dict): (optional) available resources on grid
            gridspec_assignment (str): (optional) the grid to assign the tasks to
            global_queueing (bool): Global queueing mode (default: False)

        Returns:
            dict: {task_id:task}
        """
        if dataset_prios is None or not isinstance(dataset_prios,dict):
            raise Exception('dataset_prios not a dict')
        logger.info('queue() num=%r, global=%r, prios=%r, gridspec=%r, gridspec_assign=%r, resources=%r',
                     num, global_queueing, dataset_prios, gridspec, gridspec_assignment, resources)
        
        with (yield self.parent.db.acquire_lock('queue')):
            # get all tasks for processing datasets so we can do dependency check
            try:
                sql = 'select dataset_id, task_id, task_status '
                sql += 'from search where dataset_id in ('
                sql += ','.join(['?' for _ in dataset_prios]) + ')'
                bindings = tuple(dataset_prios)
                if gridspec:
                    sql += 'and gridspec = ? '
                    bindings += (gridspec,)
                ret = yield self.parent.db.query(sql, bindings)
                tasks = {}
                for dataset, task_id, status in ret:
                    tasks[task_id] = {'dataset':dataset, 'status':status}
                if not tasks:
                    raise tornado.gen.Return({})
                task_rel_ids = {}
                sql = 'select task_rel_id, requirements from task_rel '
                sql += 'where dataset_id in (%s)'
                for f in self._bulk_select(sql, dataset_prios):
                    for task_rel_id, reqs in (yield f):
                        task_rel_ids[task_rel_id] = reqs
                sql = 'select task_id, depends, requirements, task_rel_id '
                sql += 'from task where task_rel_id in (%s)'
                datasets = {k:OrderedDict() for k in dataset_prios}
                for f in self._bulk_select(sql, task_rel_ids):
                    for task_id, depends, reqs, task_rel_id in (yield f):
                        dataset = tasks[task_id]['dataset']
                        status = tasks[task_id]['status']
                        if (status == 'idle' or
                            ((not global_queueing) and status == 'waiting')):
                            reqs = ''
                            if reqs:
                                reqs = json_decode(reqs)
                            elif task_rel_ids[task_rel_id]:
                                reqs = json_decode(task_rel_ids[task_rel_id])
                            datasets[dataset][task_id] = [depends,reqs,task_rel_id]
            except:
                logger.info('error getting processing tasks', exc_info=True)
                raise

            # get actual tasks
            task_prio = []
            for dataset in dataset_prios:
                limit = int(math.ceil(dataset_prios[dataset]*num))
                logger.debug('queue() dataset %s, limit is %d, available is %d',
                             dataset, limit, len(datasets[dataset]))
                for task_id in sorted(datasets[dataset],key=lambda k:(datasets[dataset][k][-1],k)):
                    depends, reqs = datasets[dataset][task_id]
                    satisfied = True
                    if depends == 'unknown': # depends not yet computed
                        satisfied = False
                    elif depends:
                        for dep in depends.split(','):
                            if dep not in tasks:
                                sql = 'select task_status from search where task_id = ?'
                                bindings = (dep,)
                                try:
                                    ret = yield self.parent.db.query(sql, bindings)
                                except:
                                    logger.info('error getting depend task status for %s',
                                                dep, exc_info=True)
                                    satisfied = False
                                    break
                                if (not ret) or len(ret[0]) < 0:
                                    logger.info('bad depend task status result: %r',ret)
                                    satisfied = False
                                    break
                                elif ret[0][0] != 'complete':
                                    satisfied = False
                                    break
                            elif tasks[dep]['status'] != 'complete':
                                satisfied = False
                                break
                    if satisfied and reqs and resources:
                        # now match based on resources
                        try:
                            for r in reqs:
                                if r not in resources:
                                    satisfied = False
                                    break
                        except:
                            logger.info('failed to check resources',
                                        exc_info=True)
                    if satisfied:
                        # task can be queued now
                        task_prio.append((dataset_prios[dataset],dataset,task_id))
                        limit -= 1
                        if limit <= 0:
                            break
            logger.debug('queue() %d tasks can queue', len(task_prio))
            if not task_prio:
                raise tornado.gen.Return({})
            # sort by prio, low to high (so when we pop we get higher first)
            task_prio.sort(key=operator.itemgetter(0), reverse=True)
            # return first num tasks
            dataset_ids = set()
            tasks = set()
            for p,d,t in task_prio:
                dataset_ids.add(d)
                tasks.add(t)
                if len(tasks) >= num:
                    break
            sql = 'select dataset_id, jobs_submitted, debug from dataset '
            sql += ' where dataset_id in (%s)'
            try:
                dataset_debug = {}
                for f in self._bulk_select(sql, dataset_ids):
                    for d_id,js,debug in (yield f):
                        dataset_debug[d_id] = (js,bool(debug))
            except:
                logger.debug('error getting dataset debug', exc_info=True)
                raise
            sql = 'select * from search where task_id in (%s)'
            try:
                ret = []
                for f in self._bulk_select(sql, tasks):
                    ret.extend((yield f))
            except:
                logger.debug('error queueing tasks', exc_info=True)
                raise
            tasks = {}
            job_ids = {}
            for row in ret:
                tmp = self._list_to_dict('search',row)
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
                    logger.warn('failed to find job with known job_id')
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
                        sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                        bindings3 = ('search',t,now)
                        bindings4 = ('task',t,now)
                        try:
                            yield self.parent.db.query([sql3,sql3], [bindings3,bindings4])
                        except:
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
            ret = [(yield self.parent.db.increment_id('pilot')) for _ in range(num)]
        except:
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
            s += 'submit_dir, tasks) values (?,?,?,?,?)'
            sql = []
            bindings = []
            for i,pilot_id in enumerate(pilot['pilot_ids']):
                grid_queue_id = str(pilot['grid_queue_id'])+'.'+str(i)
                sql.append(s)
                bindings.append((pilot_id, grid_queue_id, now, pilot['submit_dir'],''))
            yield self.parent.db.query(sql, bindings)
        except Exception as e:
            logger.debug('error adding pilot', exc_info=True)
            raise

    @tornado.gen.coroutine
    def queue_get_pilots(self):
        """
        Get all active pilots.

        Returns:
            list: [pilot dict]
        """
        sql = 'select * from pilot'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        pilots = []
        for row in ret:
            tmp = self._list_to_dict('pilot',row)
            tmp['submit_time'] = str2datetime(tmp['submit_time'])
            tmp['tasks'] = tmp['tasks'].split(',')
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
        except:
            logger.debug('error deleting pilots', exc_info=True)
            raise
        if task_ids:
            yield self.queue_set_task_status(task_ids,'reset')

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
            tasks (dict): dict of {task_id: Node_Resources}
        """
        sql = 'replace into task_lookup (task_id,'
        sql += ','.join('req_'+k for k in tasks.values()[0])
        sql += ') values (?,'
        sql += ','.join('?' for k in tasks.values()[0])+')'
        bindings = [(task_id,)+tuple(tasks[task_id].values()) for task_id in tasks]
        yield self.parent.db.query([sql for _ in bindings], bindings)

    @tornado.gen.coroutine
    def queue_get_task_lookup(self):
        """
        Get all the tasks in the lookup.

        Returns:
            dict: tasks
        """
        with (yield self.parent.db.acquire_lock('queue')):
            # get tasks from lookup
            sql = 'select * from task_lookup'
            bindings = tuple()
            ret = yield self.parent.db.query(sql, bindings)
            task_ids = {}
            for row in ret:
                row = self._list_to_dict('task_lookup',row)
                tid = row.pop('task_id')
                task_ids[tid] = {k.replace('req_',''):row[k] for k in row}

            # verify that they are valid
            tasks = {}
            while task_ids:
                tids = set(task_ids.keys()[:900])
                sql = 'select task_id from search where task_id in ('
                sql += ','.join('?' for _ in tids)
                sql += ') and task_status = ?'
                bindings = tuple(tids)+('queued',)
                ret = yield self.parent.db.query(sql, bindings)
                valid = set()
                for row in ret:
                    valid.add(row[0])
                remove = tids ^ valid
                if remove:
                    logger.info('tasks %r not valid, remove from task_lookup',
                                remove)
                    sql = 'delete from task_lookup where task_id in ('
                    sql +=  ','.join('?' for _ in remove)+')'
                    bindings = tuple(remove)
                    yield self.parent.db.query(sql, bindings)
                    yield self.queue_set_task_status(remove,'reset')
                for tid in valid:
                    tasks[tid] = task_ids[tid]
                task_ids = {t:task_ids[t] for t in task_ids if t not in tids}

            raise tornado.gen.Return(tasks)
