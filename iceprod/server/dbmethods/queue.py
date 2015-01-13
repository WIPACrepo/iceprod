"""
Queue database methods
"""

import logging
from datetime import datetime
from functools import partial,reduce
import operator
from collections import OrderedDict, Iterable
import math

from iceprod.core.util import Resources
from iceprod.core.dataclasses import Number,String
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime

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
                    for kk in set(old_queues[k]) - set(queues[k]):
                        queues[k][kk] = old_queues[k][kk]
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
            callback(True)
    
    def queue_get_active_tasks(self,gridspec,callback=None):
        """Get a dict of active tasks (queued,processing,reset,resume) on this site and plugin, 
           returning {status:{tasks}} where each task = join of search and task tables"""
        sql = 'select task.* from search join task on search.task_id = task.task_id '
        sql += 'where search.gridspec like ? '
        sql += ' and search.task_status in ("queued","processing","reset","resume")'
        bindings = ('%'+gridspec+'%',)
        cb = partial(self._queue_get_active_tasks_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_get_active_tasks_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                task_groups = {}
                if ret is not None:
                    tasks = self._queue_get_task_from_ret(ret)
                    for task_id in tasks:
                        status = tasks[task_id]['status']
                        if status not in task_groups:
                            task_groups[status] = {}
                        task_groups[status][task_id] = tasks[task_id]
                callback(task_groups)
    
    def queue_set_task_status(self,task,status,callback=None):
        """Set the status of a task"""
        if not isinstance(task,Iterable):
            raise Exception('task is not a str or iterable')
        cb = partial(self._queue_set_task_status_blocking,task,status,
                     callback=callback)
        self.db.non_blocking_task(cb)
    def _queue_set_task_status_blocking(self,task,status,callback=None):
        conn,archive_conn = self.db._dbsetup()
        now = datetime.utcnow()
        if isinstance(task,String):
            # single task
            sql = 'update search set task_status = ? '
            sql += ' where task_id = ?'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id = ?'
            bindings = (status,task)
            bindings2 = (status,datetime2str(now),task)
        elif isinstance(task,Iterable):
            b = ','.join(['?' for _ in xrange(len(task))])
            sql = 'update search set task_status = ? '
            sql += ' where task_id in ('+b+')'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id in ('+b+')'
            bindings = (status,)+tuple(task)
            bindings2 = (status,datetime2str(now))+tuple(task)
        try:
            ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(True)
    
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
    
    def queue_set_submit_dir(self,task,submit_dir,callback=None):
        """Set the submit_dir of a task"""
        if not task:
            raise Exception('No task')
        sql = 'update task set submit_dir = ? '
        sql += ' where task_id = ?'
        bindings = (submit_dir,task)
        self.db.sql_write_task(sql,bindings,callback=callback)
    
    def queue_buffer_jobs_tasks(self,gridspec,num_tasks,callback=None):
        """Create a buffer of jobs and tasks ahead of queueing"""
        sql = 'select dataset_id,status,gridspec,jobs_submitted,'
        sql += 'tasks_submitted from dataset where '
        if isinstance(gridspec,String):
            sql += 'gridspec like "%?%"'
            bindings = (gridspec,)
            gridspec = [gridspec]
        elif isinstance(gridspec,Iterable):
            if len(gridspec) < 1:
                logger.info('in buffer_jobs_tasks, no gridspec %r',gridspec)
                raise Exception('no gridspec defined')
            sql += '('+(' or '.join(['gridspec like ?' for _ in gridspec]))+')'
            bindings = tuple(['%'+g+'%' for g in gridspec])
        else:
            logger.info('in buffer_jobs_tasks, unknown gridspec %r',gridspec)
            raise Exception('unknown gridspec type')
        sql += ' and status = ?'
        bindings += ('processing',)
        logger.debug('in buffer_jobs_tasks, buffering on %r',gridspec)
        cb = partial(self._queue_buffer_jobs_tasks_callback,gridspec,
                     num_tasks,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_buffer_jobs_tasks_callback(self,gridspec,num_tasks,ret,callback=None):
        logger.debug('in _buffer_jobs_tasks_callback, ret = %r',ret)
        possible_datasets = {}
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for d,s,gs,js,ts in ret:
                logger.debug('cb:gs=%r,gridspec=%r',gs,gridspec)
                try:
                    gs = json_decode(gs)
                except:
                    logger.debug('gs not a json object, must be str')
                    if gs not in gridspec:
                        continue # not a local dataset
                else:
                    logger.debug('gs is json object %r',gs)
                    for g in gs.values():
                        if g not in gridspec:
                            continue # not a local dataset
                possible_datasets[d] = {'gridspec':gs,'jobs':js,'tasks':ts}
        if len(possible_datasets) < 1:
            # nothing to buffer
            logger.info('nothing to buffer (cb)')
            callback(True)
            return
        sql = 'select dataset_id,job_id,task_id,gridspec,task_status from search '
        sql += ' where dataset_id in ('
        sql += ','.join(['?' for _ in possible_datasets])
        sql += ')'
        bindings = tuple(possible_datasets)
        cb = partial(self._queue_buffer_jobs_tasks_callback2,gridspec,
                     num_tasks,possible_datasets,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _queue_buffer_jobs_tasks_callback2(self,gridspec,num_tasks,possible_datasets,
                                     ret,callback=None):
        logger.debug('in _buffer_jobs_tasks_callback2, ret = %r',ret)
        already_buffered = {}
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            job_ids = set()
            for d,j,t,gs,st in ret:
                logger.debug('cb2:gs=%r,gridspec=%r',gs,gridspec)
                if gs not in gridspec:
                    continue
                if d not in already_buffered:
                    already_buffered[d] = {'jobs_submitted':0,
                                           'tasks_submitted':0,
                                           'tasks_buffered':0,
                                          }
                already_buffered[d]['tasks_submitted'] += 1
                if j not in job_ids:
                    already_buffered[d]['jobs_submitted'] += 1
                    job_ids.add(j)
                if st == 'waiting':
                    already_buffered[d]['tasks_buffered'] += 1
        need_to_buffer = {}
        for d in possible_datasets:
            total_jobs = possible_datasets[d]['jobs']
            total_tasks = possible_datasets[d]['tasks']
            tasks_per_job = float(total_tasks/total_jobs)
            if d in already_buffered:
                total_tasks -= already_buffered[d]['tasks_submitted']
                total_jobs -= already_buffered[d]['jobs_submitted']
            n = num_tasks
            poss = total_tasks
            if poss < n:
                n = poss
            if n > 0:
                n = int(math.ceil(num_tasks/tasks_per_job))
                poss = total_jobs
                if poss < n:
                    n = poss
                if n > 0:
                    need_to_buffer[d] = n
        if not need_to_buffer:
            # nothing to buffer
            logger.info('nothing to buffer (cb2)')
            callback(True)
        else:
            # create jobs and tasks
            cb = partial(self._queue_buffer_jobs_tasks_blocking,possible_datasets,
                         need_to_buffer,callback=callback)
            self.db.non_blocking_task(cb)
    def _queue_buffer_jobs_tasks_blocking(self,possible_datasets,need_to_buffer,
                                    callback=None):
        logger.debug('in _buffer_jobs_tasks_blocking')
        conn,archive_conn = self.db._dbsetup()
        now = datetime2str(datetime.utcnow())
        
        # get dataset config
        sql = 'select dataset_id,config_data from config where dataset_id in '
        sql += '('+(','.join(['?' for _ in need_to_buffer]))+')'
        bindings = tuple(need_to_buffer)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        for d,c in ret:
            try:
                possible_datasets[d]['config'] = serialization.serialize_json.loads(c)
            except:
                logger.info('config for dataset %s not loaded',d,exc_info=True)
        
        for dataset in need_to_buffer:
            logger.debug('buffering dataset %s',dataset)
            total_jobs = possible_datasets[dataset]['jobs']
            total_tasks = possible_datasets[dataset]['tasks']
            tasks_per_job = int(total_tasks/total_jobs)
            
            # make jobs
            jobs = []
            sql_bindings = []
            for _ in xrange(need_to_buffer[dataset]):
                jobs.append((self.db._increment_id_helper('job',conn),
                             'processing',now))
                sql_bindings.append('(?,?,?)')
            sql = 'insert into job (job_id,status,status_changed) values '
            sql += ','.join(sql_bindings)
            bindings = reduce(lambda a,b:a+b,jobs)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            
            # make tasks
            task_names = []
            task_depends = []
            if 'config' in possible_datasets[dataset]:
                tt = possible_datasets[dataset]['config']['tasks']
                if len(tt) != tasks_per_job:
                    logger.warn('config tasks len != tasks_per_job for '
                                 'dataset %s. ignoring...',dataset)
                    for t in xrange(tasks_per_job):
                        task_names.append(str(t))
                        task_depends.append([])
                else:
                    task_names = [t['name'] if t['name'] else i for i,t in enumerate(tt)]
                    try:
                        for t in tt:
                            task_depends.append([task_names.index(d) 
                                                 for d in t['depends']])
                    except ValueError:
                        logger.error('task dependency not in tasks for '
                                     'dataset %s. skipping dataset',dataset)
                        continue
            else:
                logger.info('config for dataset %s does not exist or '
                             'does not have proper task names. ignoring...',
                             dataset)
                for t in xrange(tasks_per_job):
                    task_names.append(str(t))
                    task_depends.append([])
            tasks = []
            search = []
            task_bindings = []
            search_bindings = []
            q_10 = '('+(','.join(['?' for _ in xrange(10)]))+')'
            q_6 = '('+(','.join(['?' for _ in xrange(6)]))+')'
            for job_id,job_status,n in jobs:
                task_ids = [self.db._increment_id_helper('task',conn) 
                            for t in xrange(tasks_per_job)]
                for t in xrange(tasks_per_job):
                    task_id = task_ids[t]
                    depends = ','.join([task_ids[n] for n in task_depends[t]])
                    tasks.append((task_id, 'waiting', 'waiting', '', now,
                                 '', '', 0, 0, depends))
                    task_bindings.append(q_10)
                    name = task_names[t]
                    if isinstance(possible_datasets[dataset]['gridspec'],String):
                        gs = possible_datasets[dataset]['gridspec']
                    else:
                        try:
                            gs = possible_datasets[dataset]['gridspec'][str(name)]
                        except:
                            logger.error('cannot find task name in dataset '
                                        'gridspec def: %r %r',dataset,name)
                            continue
                    if isinstance(gs,(list,tuple)):
                        gs = ','.join(gs)
                    search.append((task_id, job_id, dataset, gs,
                                   name, 'waiting'))
                    search_bindings.append(q_6)
            sql = 'insert into task (task_id,status,prev_status,'
            sql += 'error_message,status_changed,submit_dir,grid_queue_id,'
            sql += 'failures,evictions,depends) values '
            sql += ','.join(task_bindings)
            bindings = reduce(lambda a,b:a+b,tasks)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            sql = 'insert into search (task_id,job_id,dataset_id,gridspec,'
            sql += 'name,task_status) values '
            sql += ','.join(search_bindings)
            bindings = reduce(lambda a,b:a+b,search)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
        
        # done buffering
        callback(True)
    
    def queue_get_queueing_datasets(self,gridspec,callback=None):
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
        sql = 'select dataset.* from dataset join search on '
        sql += 'search.dataset_id = dataset.dataset_id where '
        sql += 'dataset.status = "processing" and '
        sql += 'search.gridspec = ? and '
        sql += 'search.task_status = "waiting" '
        bindings = (gridspec,)
        self.db.sql_read_task(sql,bindings,callback=cb)
    
    def queue_get_queueing_tasks(self,dataset_prios,gridspec,num=20,callback=None):
        """Get tasks to queue based on dataset priorities.
        
        :param dataset_prios: a dict of {dataset_id:priority} where sum(priorities)=1
        :param gridspec: the grid to queue on
        :param num: number of tasks to queue
        :returns: {task_id:task}
        """
        if callback is None:
            raise Exception('need a callback')
        if dataset_prios is None or not isinstance(dataset_prios,dict):
            raise Exception('dataset_prios not a dict')
        if not gridspec:
            callback({})
            return
        cb = partial(self._queue_get_queueing_tasks_blocking,dataset_prios,
                     gridspec,num,callback=callback)
        self.db.non_blocking_task(cb)
    def _queue_get_queueing_tasks_blocking(self,dataset_prios,gridspec,num,
                                     callback=None):
        conn,archive_conn = self.db._dbsetup()
        # get all tasks for processing datasets so we can do dependency check
        sql = 'select search.dataset_id,task.task_id,task.depends,task.status '
        sql += ' from search join task on search.task_id = task.task_id '
        sql += ' where dataset_id in ('+','.join(['?' for _ in dataset_prios])
        sql += ') and gridspec = ?'
        bindings = tuple(dataset_prios)+(gridspec,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        tasks = {}
        datasets = {k:OrderedDict() for k in dataset_prios}
        if ret:
            for dataset,id,depends,status in ret:
                if status == 'waiting':
                    datasets[dataset][id] = depends
                tasks[id] = status
        # get actual tasks
        task_prio = []
        for dataset in dataset_prios:
            limit = int(math.ceil(dataset_prios[dataset]*num))
            for task_id in datasets[dataset]:
                depends = datasets[dataset][task_id]
                satisfied = True
                if depends:
                    for dep in depends.split(','):
                        if dep not in tasks or tasks[dep] != 'complete':
                            satisfied = False
                            break
                if satisfied:
                    # task can be queued now
                    task_prio.append((dataset_prios[dataset],task_id))
                    limit -= 1
                    if limit <= 0:
                        break
        if not task_prio:
            callback({})
            return
        # sort by prio, low to high (so when we pop we get higher first)
        task_prio.sort(key=operator.itemgetter(0),reverse=True)
        # return first num tasks
        task_prio = [t for p,t in task_prio[:num]]
        tasks = {}
        sql = 'select search.*,dataset.debug from search '
        sql += ' join dataset on search.dataset_id = dataset.dataset_id '
        sql += ' where search.task_id in ('
        sql += ','.join(['?' for _ in task_prio])+')'
        bindings = tuple(task_prio)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        if ret:
            tasks = {}
            for row in ret:
                tmp = self._list_to_dict('search',row[:-1])
                tmp['debug'] = row[-1]
                tasks[tmp['task_id']] = tmp
        callback(tasks)

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
            self.get_cfg_for_dataset(dataset_id,callback=callback)
    
    def queue_get_cfg_for_dataset(self,dataset_id,callback=None):
        """Get a cfg for a dataset"""
        if not dataset_id:
            raise Exception('bad datset_id')
        sql = 'select config_id,config_data from config where dataset_id = ?'
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
            values = {}
            for config_id,config_data in ret:
                values = {'config_id':config_id,
                          'config_data':config_data,
                         }
            if 'config_data' in values:
                callback(values['config_data'])
            else:
                callback(None)
    