"""
Queue database methods
"""

import logging
from datetime import datetime
from functools import partial,reduce
import operator
from collections import OrderedDict, Iterable
import math

from iceprod.core.util import Node_Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import dbmethod,_Methods_Base,datetime2str,str2datetime,nowstr

logger = logging.getLogger('dbmethods.queue')

class queue(_Methods_Base):
    """
    The Queue DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    def _queue_get_task_from_ret(self,ret):
        tasks = OrderedDict()
        try:
            for row in ret:
                tasks[row[0]] = self._list_to_dict('task',row)
        except:
            return {}
        else:
            return tasks

    @dbmethod
    def queue_get_site_id(self,callback=None):
        """Get the current site_id"""
        sql = 'select site_id from setting'
        bindings = tuple()
        cb = partial(self._queue_get_site_id_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_get_site_id_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            elif ret is None or len(ret) < 1 or len(ret[0]) < 1:
                callback(Exception('no site id'))
            else:
                callback(ret[0][0])

    @dbmethod
    def queue_set_site_queues(self,site_id,queues,callback=None):
        """Set the site queues"""
        cb = partial(self._queue_set_site_queues_blocking,site_id,queues,
                     callback=callback)
        self.db.blocking_task('queue',cb)
    def _queue_set_site_queues_blocking(self,site_id,queues,callback=None):
        conn,archive_conn = self.db._dbsetup()
        sql = 'select * from site where site_id = ?'
        bindings = (site_id,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif len(ret) > 0 and len(ret[0]) > 0:
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
            except Exception as e:
                logger.warn('set_site_queues(): cannot encode queues to json')
                callback(e)
                return
            sql = 'update site set queues = ? where site_id = ?'
            bindings = (queues,site_id)
        else:
            # add a new site entry
            try:
                queues = json_encode(queues)
            except Exception as e:
                logger.warn('set_site_queues(): cannot encode queues to json')
                callback(e)
                return
            sql = 'insert into site (site_id,queues) values (?,?)'
            bindings = (site_id,queues)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('site',site_id,nowstr())
                try:
                    self.db._db_write(conn,sql3,bindings3,None,None,None)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                self._send_to_master(('site',site_id,nowstr(),sql,bindings))
            callback(True)

    @dbmethod
    def queue_get_active_tasks(self,gridspec,callback=None):
        """Get a dict of active tasks (queued,processing,reset,resume) on this site and plugin,
           returning {status:{tasks}} where each task = join of search and task tables"""
        if not callback:
            return
        sql = 'select task_id from search '
        sql += 'where gridspec like ? '
        sql += ' and task_status in ("queued","processing","reset","resume")'
        bindings = ('%'+gridspec+'%',)
        cb = partial(self._queue_get_active_tasks_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_get_active_tasks_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            tasks = set(row[0] for row in ret)
            sql = 'select * from task where task_id in ('
            sql += ','.join('?' for _ in tasks)+')'
            bindings = tuple(tasks)
            cb = partial(self._queue_get_active_tasks_callback2,callback=callback)
            self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_get_active_tasks_callback2(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            task_groups = {}
            if ret:
                tasks = self._queue_get_task_from_ret(ret)
                for task_id in tasks:
                    status = tasks[task_id]['status']
                    if status not in task_groups:
                        task_groups[status] = {}
                    task_groups[status][task_id] = tasks[task_id]
            callback(task_groups)

    @dbmethod
    def queue_set_task_status(self,task,status,callback=None):
        """Set the status of a task"""
        if not isinstance(task,Iterable):
            raise Exception('task is not a str or iterable')
        cb = partial(self._queue_set_task_status_blocking,task,status,
                     callback=callback)
        self.db.non_blocking_task(cb)
    def _queue_set_task_status_blocking(self,task,status,callback=None):
        conn,archive_conn = self.db._dbsetup()
        now = nowstr()
        if isinstance(task,String):
            # single task
            sql = 'update search set task_status = ? '
            sql += ' where task_id = ?'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id = ?'
            bindings = (status,task)
            bindings2 = (status,now,task)
        elif isinstance(task,Iterable):
            b = ','.join(['?' for _ in xrange(len(task))])
            sql = 'update search set task_status = ? '
            sql += ' where task_id in ('+b+')'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id in ('+b+')'
            bindings = (status,)+tuple(task)
            bindings2 = (status,now)+tuple(task)
        else:
            callback(Exception('unknown type for task'))
            return
        try:
            ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if isinstance(task,String):
                task=(task,)
            for t in task:
                sql = 'update search set task_status = ? '
                sql += ' where task_id = ?'
                sql2 = 'update task set prev_status = status, '
                sql2 += ' status = ?, status_changed = ? where task_id = ?'
                bindings = (status,t)
                bindings2 = (status,now,t)
                if self._is_master():
                    sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                    bindings3 = ('search',t,now)
                    bindings4 = ('task',t,now)
                    try:
                        self.db._db_write(conn,[sql3,sql3],[bindings3,bindings4],None,None,None)
                    except Exception as e:
                        logger.info('error updating master_update_history',
                                    exc_info=True)
                else:
                    self._send_to_master(('search',t,now,sql,bindings))
                    self._send_to_master(('task',t,now,sql2,bindings2))
            callback(True)

    @dbmethod
    def queue_reset_tasks(self,reset=[],fail=[],callback=None):
        """Reset and fail specified tasks"""
        def cb(ret=None):
            if isinstance(ret,Exception):
                callback(ret)
                return
            if fail:
                self.queue_set_task_status(fail,'failed',callback=callback)
            else:
                callback()
        if reset:
            self.queue_set_task_status(reset,'reset',callback=cb)
        else:
            cb(True)

    @dbmethod
    def queue_get_task(self,task_id=None,callback=None):
        """Get tasks specified by task_id (can be id or list of ids).
           Returns either a single task, or a dict of many tasks."""
        def cb(ret):
            if isinstance(ret,Exception):
                callback(ret)
            elif ret is None:
                callback(Exception('error getting task: ret is None'))
            else:
                tasks = self._queue_get_task_from_ret(ret)
                if isinstance(task_id,str):
                    try:
                        callback(tasks.values()[0])
                    except:
                        callback(None)
                else:
                    callback(tasks)

        if isinstance(task_id,str):
            # single task
            sql = 'select * from task where task_id = ?'
            bindings = (task_id,)
        elif isinstance(task_id,Iterable):
            # multiple tasks
            b = ','.join(['?' for _ in xrange(len(task_id))])
            sql = 'select * from task where task_id in ('+b+')'
            bindings = tuple(task_id)
        else:
            raise Exception('task_id is not a str or iterable')

        self.db.sql_read_task(sql,bindings,callback=cb)

    @dbmethod
    def queue_get_task_by_grid_queue_id(self,grid_queue_id,callback=None):
        """Get tasks specified by grid_queue_id (can be id or list of ids)"""
        def cb(ret):
            if isinstance(ret,Exception):
                callback(ret)
            elif ret is None:
                callback(Exception('error getting task: ret is None'))
            else:
                tasks = self._queue_get_task_from_ret(ret)
                if isinstance(grid_queue_id,str):
                    try:
                        callback(tasks.values()[0])
                    except:
                        callback(None)
                else:
                    callback(tasks)

        if isinstance(grid_queue_id,str):
            # single task
            sql = 'select * from task where grid_queue_id = ?'
            bindings = (grid_queue_id,)
        elif isinstance(grid_queue_id,Iterable):
            # multiple tasks
            b = ','.join(['?' for _ in xrange(len(grid_queue_id))])
            sql = 'select * from task where grid_queue_id in ('+b+')'
            bindings = tuple(grid_queue_id)
        else:
            raise Exception('grid_queue_id is not a str or iterable')

        self.db.sql_read_task(sql,bindings,callback=cb)

    @dbmethod
    def queue_set_submit_dir(self,task,submit_dir,callback=None):
        """Set the submit_dir of a task"""
        if not task:
            raise Exception('No task')
        sql = 'update task set submit_dir = ? '
        sql += ' where task_id = ?'
        bindings = (submit_dir,task)
        def cb(ret):
            if not isinstance(ret,Exception):
                if self._is_master():
                    sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                    bindings3 = ('task',task,nowstr())
                    try:
                        self.db._db_write(conn,sql3,bindings3,None,None,None)
                    except Exception as e:
                        logger.info('error updating master_update_history',
                                    exc_info=True)
                else:
                    self._send_to_master(('task',task,nowstr(),sql,bindings))
            callback(ret)
        self.db.sql_write_task(sql,bindings,callback=cb)

    @dbmethod
    def queue_buffer_jobs_tasks(self, gridspec=None, num_tasks=100, callback=None):
        """
        Create a buffer of jobs and tasks ahead of queueing.

        Args:
            gridspec (str or iterable): Single or multiple gridspecs to match.
                                        `None` for global queueing.
            num_tasks (int): Number of tasks to buffer (rounds up to buffer
                             a full job at once).
        """
        cb = partial(self._queue_buffer_jobs_tasks_blocking,gridspec,
                     num_tasks,callback=callback)
        self.db.blocking_task('queue',cb)
    def _queue_buffer_jobs_tasks_get_datasets(self, gridspec, conn):
        """Helper to get datasets to buffer new jobs/tasks"""

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
        ret = self.db._db_read(conn,sql,bindings,None,None,None)
        if isinstance(ret, Exception):
            raise ret
        need_to_buffer = {}
        for d, s, gs, js in ret:
            logger.debug('gs=%r, gridspec=%r', gs, gridspec)
            need_to_buffer[d] = {'gridspec':gs,'jobs':js,'job_index':0}
        if not need_to_buffer:
            # nothing to buffer
            logger.info('nothing to buffer')
            return None

        # remove already buffered jobs
        sql = 'select dataset_id,job_id from search '
        sql += ' where dataset_id in ('
        sql += ','.join(['?' for _ in need_to_buffer])
        sql += ')'
        bindings = tuple(need_to_buffer)
        ret = self.db._db_read(conn,sql,bindings,None,None,None)
        if isinstance(ret,Exception):
            raise ret
        already_buffered = {}
        for d, job_id in ret:
            if d not in already_buffered:
                already_buffered[d] = set()
            already_buffered[d].add(job_id)
        for d in need_to_buffer:
            if d in already_buffered:
                need_to_buffer[d]['job_index'] = len(already_buffered[d])
        return need_to_buffer
    def _queue_buffer_jobs_tasks_blocking(self,gridspec,num_tasks,callback=None):
        conn,archive_conn = self.db._dbsetup()
        now = nowstr()

        try:
            need_to_buffer = self._queue_buffer_jobs_tasks_get_datasets(gridspec, conn)
        except Exception as e:
            logger.info('error getting datasets to buffer', exc_info=True)
            callback(e)
            return
        if not need_to_buffer:
            # nothing to buffer
            logger.info('nothing to buffer (cb2)')
            callback(True)
            return

        # get task_rels for buffering datasets
        task_rel_ids = {}
        try:
            sql = 'select task_rel_id,dataset_id,task_index,name,depends from task_rel '
            sql += 'where dataset_id in ('
            sql += ','.join('?' for _ in need_to_buffer)+')'
            bindings = tuple(need_to_buffer)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            for tr_id, dataset_id, index, name, deps in ret:
                if 'task_rels' not in need_to_buffer[dataset_id]:
                    need_to_buffer[dataset_id]['task_rels'] = {}
                need_to_buffer[dataset_id]['task_rels'][tr_id] = (index,name,deps)
                task_rel_ids[tr_id] = (dataset_id,index,deps)
        except Exception as e:
            logger.info('error getting task_rels', exc_info=True)
            callback(e)
            return

        def get_task_rels_by_id(task_rel_id):
            """
            Get task_rels for a specific id.
            """
            sql = 'select task_rel_id,dataset_id,task_index,depends from task_rel '
            sql += 'where task_rel_id = ?'
            bindings = (task_rel_id,)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            for tr_id, dataset_id, index, deps in ret:
                task_rel_ids[tr_id] = (dataset_id,index,deps)

        def get_task_by_index(dataset, job_index, task_index):
            """Get a task id referenced by a dataset, job_index, and task_index."""
            logger.info('get_task_by_index: %s, %d, %d', dataset, job_index,
                        task_index)
            sql = 'select job_id, task_id from search where dataset_id = ?'
            bindings = (dataset,)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
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
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            if not ret:
                raise Exception('job_index not found')
            job_id = ret[0][0]
            return sorted(jobs[job_id])[task_index]

        # buffer for each dataset
        # for now, do the stupid thing and just buffer in order
        # TODO: use priorities to do this better
        for dataset in need_to_buffer:
            try:
                job_index = need_to_buffer[dataset]['job_index']
                total_jobs = need_to_buffer[dataset]['jobs']
                task_rels = need_to_buffer[dataset]['task_rels']
                sorted_task_rels = sorted(task_rels)
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
                        for i, x in enumerate(sorted(task_rels.values(),key=lambda a:a[0])):
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
                                    get_task_rels_by_id(d)
                                task_deps[1].append(get_task_by_index(
                                        dataset=task_rel_ids[d][0],
                                        job_index=job_index,
                                        task_index=task_rel_ids[d][1]))
                            depends.append(task_deps)
                    except Exception:
                        logger.warn('missing dependency when buffering dataset')
                        raise

                    # make job
                    job_id = self.db._increment_id_helper('job',conn)
                    sql = 'insert into job (job_id, status, job_index, '
                    sql += 'status_changed) values (?,?,?,?)'
                    bindings = (job_id, 'processing', job_index, now)
                    db_updates_sql.append(sql)
                    db_updates_bindings.append(bindings)

                    # make tasks
                    task_ids = [self.db._increment_id_helper('task',conn)
                                for _ in task_rels]
                    sql = 'insert into task (task_id,status,prev_status,'
                    sql += 'error_message,status_changed,submit_dir,grid_queue_id,'
                    sql += 'failures,evictions,depends,requirements) values '
                    sql += '(?,?,?,?,?,?,?,?,?,?,?)'
                    sql2 = 'insert into search (task_id,job_id,dataset_id,gridspec,'
                    sql2 += 'name,task_status) values (?,?,?,?,?,?)'
                    for index, task_rel_id in enumerate(sorted_task_rels):
                        deps = [task_ids[i] for i in depends[index][0]]
                        deps.extend(depends[index][1])

                        # task table
                        bindings = (task_ids[index], 'idle', 'idle', '', now,
                                    '', '', 0, 0, ','.join(deps), '')
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
                try:
                    ret = self.db._db_write(conn,db_updates_sql,db_updates_bindings,None,None,None)
                except Exception as e:
                    ret = e
                if isinstance(ret,Exception):
                    callback(ret)
                    return
                else:
                    for i in range(len(db_updates_sql)):
                        sql = db_updates_sql[i]
                        bindings = db_updates_bindings[i]
                        if self._is_master():
                            sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                            bindings3 = (sql.split()[2],bindings[0],now)
                            try:
                                self.db._db_write(conn,sql3,bindings3,None,None,None)
                            except Exception as e:
                                logger.info('error updating master_update_history',
                                            exc_info=True)
                        else:
                            self._send_to_master((sql.split()[2],bindings[0],now,sql,bindings))

            except Exception:
                logger.warn('error buffering dataset %s', dataset, exc_info=True)
                continue

        # done buffering
        callback(True)

    @dbmethod
    def queue_get_queueing_datasets(self, gridspec=None, callback=None):
        """Get datasets that are currently in processing status on gridspec.
           Returns a list of dataset_ids"""
        def cb(ret):
            if isinstance(ret,Exception):
                callback(ret)
            else:
                datasets = {}
                if ret is not None:
                    for row in ret:
                        d = self._list_to_dict('dataset',row)
                        datasets[d['dataset_id']] = d
                callback(datasets)
        bindings = []
        sql = 'select * from dataset where status = "processing" '
        if gridspec:
            sql += ' and gridspec like ?'
            bindings.append('%'+gridspec+'%')
        bindings = tuple(bindings)
        self.db.sql_read_task(sql,bindings,callback=cb)

    @dbmethod
    def queue_get_queueing_tasks(self, dataset_prios, gridspec=None, num=20,
                                 resources=None, gridspec_assignment=None,
                                 global_queueing=False, callback=None):
        """Get tasks to queue based on dataset priorities.

        :param dataset_prios: a dict of {dataset_id:priority} where sum(priorities)=1
        :param gridspec: (optional) the grid to queue on
        :param num: (optional) number of tasks to queue
        :param resources: (optional) available resources on grid
        :param gridspec_assignment: (optional) the grid to assign the tasks to
        :param global_queueing: Global queueing mode (default: False)
        :returns: {task_id:task}
        """
        if callback is None:
            raise Exception('need a callback')
        if dataset_prios is None or not isinstance(dataset_prios,dict):
            raise Exception('dataset_prios not a dict')
        cb = partial(self._queue_get_queueing_tasks_blocking,
                     dataset_prios, gridspec, num, resources,
                     gridspec_assignment, global_queueing=global_queueing,
                     callback=callback)
        self.db.non_blocking_task(cb)
    def _queue_get_queueing_tasks_blocking(self, dataset_prios, gridspec,
                                           num, resources,
                                           gridspec_assignment,
                                           global_queueing,
                                           callback=None):
        logger.debug('queue() num=%r, global=%r, prios=%r, gridspec=%r, gridspec_assign=%r, resources=%r',
                     num, global_queueing, dataset_prios, gridspec, gridspec_assignment, resources)
        conn,archive_conn = self.db._dbsetup()
        # get all tasks for processing datasets so we can do dependency check
        try:
            sql = 'select dataset_id, task_id, task_status '
            sql += 'from search where dataset_id in ('
            sql += ','.join(['?' for _ in dataset_prios]) + ')'
            bindings = tuple(dataset_prios)
            if gridspec:
                sql += 'and gridspec = ? '
                bindings += (gridspec,)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            tasks = {}
            for dataset, task_id, status in ret:
                tasks[task_id] = {'dataset':dataset, 'status':status}
            if not tasks:
                callback({})
                return
            sql = 'select task_id, depends, requirements, task_rel_id '
            sql += 'from task where task_id in ('
            sql += ','.join('?' for _ in tasks) + ')'
            bindings = tuple(tasks)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            datasets = {k:OrderedDict() for k in dataset_prios}
            task_rel_ids = {}
            for task_id, depends, reqs, task_rel_id in ret:
                dataset = tasks[task_id]['dataset']
                status = tasks[task_id]['status']
                tasks[task_id]['task_rel_id'] = task_rel_id
                if reqs:
                    reqs = json_decode(reqs)
                if (status == 'idle' or
                    ((not global_queueing) and status == 'waiting')):
                    datasets[dataset][task_id] = [depends,reqs]
                if not reqs:
                    task_rel_ids[task_rel_id] = None
            sql = 'select task_rel_id, requirements from task_rel '
            sql += 'where task_rel_id in ('
            sql += ','.join('?' for _ in task_rel_ids) + ')'
            bindings = tuple(task_rel_ids)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            for task_rel_id, reqs in ret:
                task_rel_ids[task_rel_id] = reqs
            for d in datasets:
                for task_id in datasets[d]:
                    task_rel_reqs = task_rel_ids[tasks[task_id]['task_rel_id']]
                    if task_rel_reqs and (not datasets[d][task_id][1]):
                        if task_rel_reqs:
                            task_rel_reqs = json_decode(task_rel_reqs)
                        datasets[d][task_id][1] = task_rel_reqs
        except Exception as e:
            logger.debug('error getting processing tasks', exc_info=True)
            callback(e)
            return

        # get actual tasks
        task_prio = []
        for dataset in dataset_prios:
            limit = int(math.ceil(dataset_prios[dataset]*num))
            logger.debug('queue() dataset %s, limit is %d, available is %d',
                         dataset, limit, len(datasets[dataset]))
            for task_id in datasets[dataset]:
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
                                ret = self.db._db_read(conn,sql,bindings,None,None,None)
                            except Exception as e:
                                logger.info('error getting depend task status for %s',
                                            dep, exc_info=True)
                                satisfied = False
                                break
                            if not ret or len(ret[0]) < 0:
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
            callback({})
            return
        # sort by prio, low to high (so when we pop we get higher first)
        task_prio.sort(key=operator.itemgetter(0),reverse=True)
        # return first num tasks
        dataset_ids = set()
        tasks = set()
        for p,d,t in task_prio:
            dataset_ids.add(d)
            tasks.add(t)
            if len(tasks) >= num:
                break
        sql = 'select dataset_id, debug from dataset where dataset_id in ('
        sql += ','.join('?' for _ in dataset_ids)+')'
        bindings = tuple(dataset_ids)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            logger.debug('error getting dataset debug',exc_info=True)
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        dataset_debug = {d_id: debug for d_id,debug in ret}
        sql = 'select * from search where task_id in ('
        sql += ','.join('?' for _ in tasks)+')'
        bindings = tuple(tasks)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            logger.debug('error queueing tasks',exc_info=True)
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if ret:
            tasks = {}
            for row in ret:
                tmp = self._list_to_dict('search',row)
                tmp['debug'] = dataset_debug[tmp['dataset_id']]
                tmp['reqs'] = datasets[tmp['dataset_id']][tmp['task_id']][1]
                tasks[tmp['task_id']] = tmp
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
                try:
                    ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
                except Exception as e:
                    logger.debug('error setting status',exc_info=True)
                    ret = e
                if isinstance(ret,Exception):
                    callback(ret)
                    return
                else:
                    if self._is_master():
                        for t in tasks:
                            sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                            bindings3 = ('search',t,now)
                            bindings4 = ('task',t,now)
                            try:
                                self.db._db_write(conn,[sql3,sql3],[bindings3,bindings4],None,None,None)
                            except Exception as e:
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
                            self._send_to_master(('search',t,now,sql,bindings))
                            self._send_to_master(('task',t,now,sql2,bindings2))
                for t in tasks:
                    tasks[t]['task_status'] = new_status
                    if gridspec_assignment:
                        tasks[t]['gridspec'] = gridspec_assignment
        callback(tasks)

    @dbmethod
    def queue_add_pilot(self,pilot,callback=None):
        """Add a pilot to the DB"""
        cb = partial(self._queue_add_pilot_blocking, pilot, callback=callback)
        self.db.non_blocking_task(cb)
    def _queue_add_pilot_blocking(self,pilot,callback=None):
        conn,archive_conn = self.db._dbsetup()
        now = nowstr()
        n = 1
        if 'num' in pilot:
            n = pilot['num']
        sql = []
        bindings = []
        for i in range(n):
            pilot_id = self.db._increment_id_helper('pilot',conn)
            grid_queue_id = str(pilot['grid_queue_id'])+'.'+str(i)
            sql.append('insert into pilot (pilot_id, grid_queue_id, '
                       'submit_time, submit_dir) values (?,?,?,?)')
            bindings.append((pilot_id, grid_queue_id, now, pilot['submit_dir']))
        
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            logger.debug('error setting status',exc_info=True)
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        else:
            for i,b in enumerate(bindings):
                if self._is_master():
                    sql2 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                    bindings2 = ('pilot',b[0],now)
                    try:
                        self.db._db_write(conn,sql2,bindings2,None,None,None)
                    except Exception as e:
                        logger.info('error updating master_update_history',
                                    exc_info=True)
                else:
                    self._send_to_master(('pilot',b[0],now,sql[i],b))
        callback(True)

    @dbmethod
    def queue_get_active_pilots(self,callback=None):
        # TODO: implement this
        callback({})

    @dbmethod
    def queue_get_cfg_for_task(self,task_id,callback=None):
        """Get a cfg for a task"""
        if not task_id:
            raise Exception('bad task_id')
        sql = 'select task_id,dataset_id from search where task_id = ?'
        bindings = (task_id,)
        cb = partial(self._queue_get_cfg_for_task_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_get_cfg_for_task_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or len(ret) < 1 or len(ret[0]) < 2:
            callback(Exception('get_cfg_for_task did not return a dataset_id'))
        else:
            dataset_id = ret[0][1]
            self.queue_get_cfg_for_dataset(dataset_id,callback=callback)

    @dbmethod
    def queue_get_cfg_for_dataset(self,dataset_id,callback=None):
        """Get a cfg for a dataset"""
        if not dataset_id:
            raise Exception('bad dataset_id')
        sql = 'select dataset_id,config_data from config where dataset_id = ?'
        bindings = (dataset_id,)
        cb = partial(self._queue_get_cfg_for_dataset_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_get_cfg_for_dataset_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or len(ret) < 1 or len(ret[0]) < 2:
            callback(Exception('get_cfg_for_dataset did not return a config'))
        else:
            logger.debug('config for dataset: %r',ret)
            data = None
            for dataset_id,config_data in ret:
                data = config_data
            callback(data)
