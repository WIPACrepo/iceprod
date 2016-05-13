"""
RPC database methods
"""

import logging
from datetime import datetime, timedelta
from functools import partial
from collections import OrderedDict
import math

from iceprod.core.util import Node_Resources
from iceprod.core import dataclasses
from iceprod.core import serialization
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import GlobalID, calc_datasets_prios

from iceprod.server.dbmethods import dbmethod,_Methods_Base,datetime2str,str2datetime,nowstr

logger = logging.getLogger('dbmethods.rpc')


class rpc(_Methods_Base):
    """
    The RPC DB methods.

    :param parent: the parent DBMethods class instance
    """

    @dbmethod
    def rpc_echo(self,value,callback=None):
        """Echo a single value. Just a test to see if rpc is working"""
        return value

    @dbmethod
    def rpc_new_task(self, gridspec=None, callback=None, **kwargs):
        """Get a new task from the queue specified by the gridspec,
           based on the platform, hostname, network interfaces, python unicode.
           Save plaform,hostname,network in nodes table.
           Returns a runnable config file with site content.
        """
        if not gridspec:
            raise Exception('gridspec is not given')
        args = {'gridspec':gridspec,
                'platform':None,
                'hostname':None,
                'domain':None,
                'ifaces':None,
                'python_unicode':None,
               }
        args.update(kwargs)
        self.parent.node_update(**args) # non-blocking node update
        cb2 = partial(self._rpc_new_task_callback,args,callback=callback)
        cb = partial(self._rpc_new_task_blocking,args,callback=cb2)
        self.db.blocking_task('queue',cb)
    def _rpc_new_task_blocking(self,args,callback=None):
        """This executes in a single thread regardless of the number of
           parallel requests for a new task.
        """
        # check resource requirements
        reqs = {}
        for k in Node_Resources:
            if k in args and args[k] != Node_Resources[k]:
                reqs[k] = args[k]
        conn,archive_conn = self.db._dbsetup()
        
        while True:
            sql = 'select task_id from task_lookup '
            if reqs:
                sql += 'where '+' and '.join('req_'+k+' <= ?' for k in reqs)
            sql += ' limit 1'
            if reqs:
                bindings = tuple(reqs.values())
            else:
                bindings = tuple()
            try:
                ret = self.db._db_read(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            elif not ret or not ret[0]:
                callback(None)
                return
            task_id = ret[0][0]
            sql = 'select * from search where task_id = ? and task_status = ?'
            bindings = (task_id,'queued')
            try:
                ret = self.db._db_read(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            elif not ret:
                logger.info('task %s not valid, remove from task_lookup',
                            task_id)
                sql3 = 'delete from task_lookup where task_id = ?'
                bindings3 = (task_id,)
                try:
                    ret = self.db._db_write(conn,sql,bindings,None,None,None)
                except Exception as e:
                    ret = e
                if isinstance(ret,Exception):
                    callback(ret)
                    return
            else:
                break # we found a valid task
        
        newtask = {}
        logger.debug('new task: %r',ret)
        try:
            newtask = self._list_to_dict('search',ret[0])
        except:
            logger.warn('error converting search results',exc_info=True)
            pass
        if not newtask:
            callback(newtask)
            return
        sql = 'select job_index from job where job_id = ?'
        bindings = (newtask['job_id'],)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or ret[0] is None:
            logger.warn('failed to find job with known job_id %r',
                        newtask['job_id'])
            callback(None)
            return
        newtask['job'] = ret[0][0]
        sql = 'select jobs_submitted, debug from dataset where dataset_id = ?'
        bindings = (newtask['dataset_id'],)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif not ret or not ret[0]:
            logger.warn('failed to find dataset with known dataset_id')
            callback(None)
            return
        for js, debug in ret:
            newtask['jobs_submitted'] = js
            newtask['debug'] = bool(debug)
            break

        now = nowstr()
        sql = 'update search set task_status = ? '
        sql += ' where task_id = ?'
        bindings = ('processing',newtask['task_id'])
        sql2 = 'update task set prev_status = status, '
        sql2 += ' status = ?, status_changed = ? where task_id = ?'
        bindings2 = ('processing',now,newtask['task_id'])
        sql3 = 'delete from task_lookup where task_id = ?'
        bindings3 = (newtask['task_id'],)
        try:
            ret = self.db._db_write(conn,[sql,sql2,sql3],[bindings,bindings2,bindings3],None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('search',newtask['task_id'],now)
                bindings4 = ('task',newtask['task_id'],now)
                try:
                    self.db._db_write(conn,[sql3,sql3],[bindings3,bindings4],None,None,None)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                self._send_to_master(('search',newtask['task_id'],now,sql,bindings))
                self._send_to_master(('task',newtask['task_id'],now,sql2,bindings2))
            callback(newtask)
    def _rpc_new_task_callback(self,args,task,callback=None):
        if isinstance(task,Exception):
            callback(task)
        elif not task:
            callback(None)
        else:
            cb = partial(self._rpc_new_task_callback2,task,callback=callback)
            self.parent.queue_get_cfg_for_dataset(task['dataset_id'],
                                                  callback=cb)
    def _rpc_new_task_callback2(self,task,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            config = json_decode(ret)
            if 'options' not in config:
                config['options'] = {}
            config['options']['task_id'] = task['task_id']
            config['options']['task'] = task['name']
            config['options']['dataset_id'] = task['dataset_id']
            config['options']['job'] = task['job']
            config['options']['jobs_submitted'] = task['jobs_submitted']
            config['options']['debug'] = task['debug']
            callback(config)

    @dbmethod
    def rpc_set_processing(self,task,callback=None):
        """Set a task to the processing status"""
        return self.parent.queue_set_task_status(task,'processing',
                                                 callback=callback)

    @dbmethod
    def rpc_finish_task(self,task,stats={},callback=None):
        """Do task completion operations.
        Takes a task_id and a stats dict as input.
        """
        stats = json_encode(stats)
        cb = partial(self._rpc_finish_task_blocking,task,stats,callback=callback)
        self.db.blocking_task('queue',cb)
    def _rpc_finish_task_blocking(self,task,stats,callback=None):
        conn,archive_conn = self.db._dbsetup()

        # update task status
        now = nowstr()
        sql = 'update search set task_status = ? '
        sql += ' where task_id = ?'
        sql2 = 'update task set prev_status = status, '
        sql2 += ' status = ?, status_changed = ? where task_id = ?'
        bindings = ('complete',task)
        bindings2 = ('complete',now,task)
        try:
            ret = self.db._db_write(conn,[sql,sql2],[bindings,bindings2],None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('search',newtask['task_id'],now)
                bindings4 = ('task',newtask['task_id'],now)
                try:
                    self.db._db_write(conn,[sql3,sql3],[bindings3,bindings4],None,None,None)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                self._send_to_master(('search',task,now,sql,bindings))
                self._send_to_master(('task',task,now,sql2,bindings2))

        # update task statistics
        sql = 'select task_stat_id,task_id from task_stat where task_id = ?'
        bindings = (task,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        task_stat_id = None
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for ts,t in ret:
                task_stat_id = ts
        if task_stat_id:
            logger.debug('replace previous task_stat')
            sql = 'update task_stat set stat = ? where task_stat_id = ?'
            bindings = (stats,task_stat_id)
        else:
            logger.debug('insert new task_stat')
            task_stat_id = self.db._increment_id_helper('task_stat',conn)
            sql = 'replace into task_stat (task_stat_id,task_id,stat) values '
            sql += ' (?, ?, ?)'
            bindings = (task_stat_id,task,stats)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
            return
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('task_stat',task_stat_id,now)
                try:
                    self.db._db_write(conn,sql3,bindings3,None,None,None)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                self._send_to_master(('task_stat',task_stat_id,now,sql,bindings))

        # check if whole job is finished
        sql = 'select search.dataset_id,job_id,jobs_submitted,tasks_submitted '
        sql += ' from search '
        sql += ' join dataset on search.dataset_id = dataset.dataset_id '
        sql += ' where task_id = ?'
        bindings = (task,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        dataset_id = None
        job_id = None
        total_jobs = None
        total_tasks = None
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for d_id,j_id,njobs,ntasks in ret:
                dataset_id = d_id
                job_id = j_id
                total_jobs = njobs
                total_tasks = ntasks
        if not dataset_id or not job_id or not total_jobs or not total_tasks:
            callback(Exception('cannot find dataset or job id'))
            return
        tasks_per_job = int(total_tasks/total_jobs)
        sql = 'select task_id,task_status from search where job_id = ?'
        bindings = (job_id,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        task_statuses = set()
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret and len(ret) == tasks_per_job:
            logger.debug('tasks_per_job = %d, len(ret) = %d',tasks_per_job,len(ret))
            # require that all tasks for this job are in our db
            # means that distributed jobs can only complete at the master
            for t,s in ret:
                task_statuses.add(s)
        job_status = None
        if task_statuses and not task_statuses&{'waiting','queued','processing','resume','reset'}:
            if not task_statuses-{'complete'}:
                job_status = 'complete'
            elif not task_statuses-{'complete','failed'}:
                job_status = 'errors'
            elif not task_statuses-{'complete','failed','suspended'}:
                job_status = 'suspended'
        if job_status:
            # update job status
            logger.info('job %s marked as %s',job_id,job_status)
            sql = 'update job set status = ?, status_changed = ? '
            sql += ' where job_id = ?'
            bindings = (job_status,now,job_id)
            try:
                ret = self.db._db_write(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret = e
            if isinstance(ret,Exception):
                callback(ret)
                return
            else:
                if self._is_master():
                    sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                    bindings3 = ('job',job_id,now)
                    try:
                        self.db._db_write(conn,sql3,bindings3,None,None,None)
                    except Exception as e:
                        logger.info('error updating master_update_history',
                                    exc_info=True)
                else:
                    self._send_to_master(('job',job_id,now,sql,bindings))

            if job_status == 'complete':
                # TODO: collate task stats
                pass

        callback(True)

    @dbmethod
    def rpc_task_error(self,task,error_info=None,callback=None):
        """Mark task as ERROR"""
        if not task:
            raise Exception('no task specified')
        cb = partial(self._rpc_task_error_blocking,task,error_info,
                     callback=callback)
        self.db.non_blocking_task(cb)
    def _rpc_task_error_blocking(self,task,error_info,callback=None):
        conn,archive_conn = self.db._dbsetup()
        sql = 'select task_id,failures from task where task_id = ?'
        bindings = (task,)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret:
            callback(Exception('sql error in task_error'))
        else:
            sql = 'select search.task_id,debug from search '
            sql += 'join dataset on search.dataset_id = dataset.dataset_id '
            sql += 'where task_id = ?'
            bindings = (task,)
            try:
                ret2 = self.db._db_read(conn,sql,bindings,None,None,None)
            except Exception as e:
                ret2 = e
            if isinstance(ret2,Exception):
                callback(ret2)
            elif not ret2:
                callback(Exception('sql error in task_error'))
            else:
                task = None
                failures=0
                for t,f in ret:
                    task = t
                    failures = f
                    break
                failures += 1
                debug = False
                for t,d in ret2:
                    debug = (d == True)
                if debug:
                    status = 'suspended'
                elif failures >= self.db.cfg['queue']['max_resets']:
                    status = 'failed'
                else:
                    status = 'reset'

                now = nowstr()
                sql = 'update search set task_status = ? '
                sql += ' where task_id = ?'
                bindings = (status,task)
                sql2 = 'update task set prev_status = status, '
                sql2 += ' status = ?, failures = ?, status_changed = ? where task_id = ?'
                bindings2 = (status,failures,now,task)
                sql3 = 'replace into task_stat (task_stat_id, task_id, stat) '
                sql3 += 'values (?,?,?)'
                task_stat_id = self.db._increment_id_helper('task_stat',conn)
                stat = {'error_'+now: error_info}
                bindings3 = (task_stat_id, task, json_encode(stat))
                try:
                    ret = self.db._db_write(conn,[sql,sql2,sql3],[bindings,bindings2,bindings3],None,None,None)
                except Exception as e:
                    ret = e
                if isinstance(ret,Exception):
                    callback(ret)
                else:
                    if self._is_master():
                        msql = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                        mbindings1 = ('search',task,now)
                        mbindings2 = ('task',task,now)
                        mbindings3 = ('task_stat',task_stat_id,now)
                        try:
                            self.db._db_write(conn,[msql,msql,msql],[mbindings1,mbindings2,mbindings3],None,None,None)
                        except Exception as e:
                            logger.info('error updating master_update_history',
                                        exc_info=True)
                    else:
                        self._send_to_master(('search',task,now,sql,bindings))
                        self._send_to_master(('task',task,now,sql2,bindings2))
                        self._send_to_master(('task_stat',task_stat_id,now,sql3,bindings3))
                    callback(True)

    @dbmethod
    def rpc_upload_logfile(self,task,name,data,callback=None):
        """Uploading of a logfile from a task"""
        cb2 = partial(self._rpc_upload_logfile_callback,callback=callback)
        cb = partial(self._rpc_upload_logfile_blocking,task,name,data,callback=cb2)
        self.db.blocking_task('logfile',cb)
    def _rpc_upload_logfile_blocking(self,task,name,data,callback=None):
        conn,archive_conn = self.db._dbsetup()
        sql = 'select task_log_id,task_id from task_log where '
        sql += ' task_id = ? and name = ?'
        bindings = (task,name)
        try:
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        task_log_id = None
        if isinstance(ret,Exception):
            callback(ret)
            return
        elif ret:
            for ts,t in ret:
                task_log_id = ts
        if task_log_id:
            logger.debug('replace previous task_log')
            sql = 'update task_log set data = ? where task_log_id = ?'
            bindings = (data,task_log_id)
        else:
            logger.debug('insert new task_log')
            task_log_id = self.db._increment_id_helper('task_log',conn)
            sql = 'insert into task_log (task_log_id,task_id,name,data) '
            sql += ' values (?,?,?,?)'
            bindings = (task_log_id,task,name,data)
        try:
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('task_log',task_log_id,nowstr())
                try:
                    self.db._db_write(conn,sql3,bindings3,None,None,None)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                self._send_to_master(('task_log',task_log_id,nowstr(),sql,bindings))
            callback(True)
    def _rpc_upload_logfile_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif ret is None:
            callback(Exception('sql error in upload_logfile'))
        else:
            callback(True)

    @dbmethod
    def rpc_stillrunning(self,task,callback=None):
        """Check that the task is still in a running state"""
        sql = 'select task_id,status from task where task_id = ?'
        bindings = (task,)
        cb = partial(self._rpc_stillrunning_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _rpc_stillrunning_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif not ret or not ret[0]:
            callback(Exception('sql error in stillrunning'))
        else:
            if ret[0][1] in ('queued','processing'):
                callback(True)
            else:
                callback(False)

    @dbmethod
    def rpc_submit_dataset(self,data,difplus='',description='',gridspec='',
                           njobs=1,stat_keys=[],debug=False,
                           callback=None):
        """Submit a dataset"""
        if gridspec is None:
            gridspec = ''
        cb = partial(self._rpc_submit_dataset_blocking,data,difplus,description,
                     gridspec,njobs,stat_keys,debug,
                     callback=callback)
        self.db.blocking_task('submit_dataset',cb)
    def _rpc_submit_dataset_blocking(self,config,difplus,description,gridspec,
                                     njobs,stat_keys,debug,
                                     callback=None):
        conn,archive_conn = self.db._dbsetup()
        # make sure we have a serialized and deserialized copy of config
        if isinstance(config, dict):
            try:
                config = serialization.dict_to_dataclasses(config)
            except Exception:
                logger.info('error converting config: %r', config,
                            exc_info=True)
                callback(e)
                return
        elif not isinstance(config, dataclasses.Job):
            try:
                config = serialization.serialize_json.loads(config)
            except Exception as e:
                logger.info('error deserializing config: %r', config,
                            exc_info=True)
                callback(e)
                return

        # check the number of jobs, tasks, and trays
        try:
            njobs = int(njobs)
            ntasks = len(config['tasks'])*njobs
        except Exception as e:
            logger.info('error reading ntasks from submitting config',
                        exc_info=True)
            callback(e)
            return
        
        # join categories as csv
        category_csv = ''
        try:
            category_csv = ','.join(config['categories'])
        except:
            pass

        # look up dependencies
        task_rels = []
        depends = []
        try:
            dataset_depends = {}
            for task in config['tasks']:
                for dep in task['depends']:
                    if '.' in dep:
                        dataset_depends[dep.split('.')[0]] = []
            if dataset_depends:
                sql = 'select task_rel_id,dataset_id,task_index,name from task_rel '
                sql += 'where dataset_id in ('
                sql += ','.join('?' for _ in dataset_depends)+')'
                bindings = tuple(dataset_depends)
                ret = self.db._db_read(conn,sql,bindings,None,None,None)
                if isinstance(ret,Exception):
                    raise ret
                for tid,did,task_index,name in ret:
                    dataset_depends[did].append({'task_rel_id':tid,
                                                 'task_index':task_index,
                                                 'name':name})
            for task in config['tasks']:
                task_dep = []
                task_rel_id = self.db._increment_id_helper('task_rel',conn)
                for dep in task['depends']:
                    if '.' in dep:
                        did, dep = dep.split('.')
                        if dep.isdigit():
                            dep = int(dep)
                        for rel in dataset_depends[did]:
                            if rel['task_index'] == dep or rel['name'] == dep:
                                task_dep.append(rel['task_rel_id'])
                                break
                        else:
                            raise Exception('missing a dataset dependency')
                    else:
                        if dep.isdigit():
                            task_dep.append(task_rels[int(dep)])
                        else:
                            for i,task in enumerate(config['tasks']):
                                if dep == task['name']:
                                    task_dep.append(task_rels[i])
                                    break
                            else:
                                raise Exception('missing a dependency')
                depends.append(','.join(task_dep))
                task_rels.append(task_rel_id)
        except Exception as e:
            logger.warn('task dependency error', exc_info=True)
            callback(Exception('Task dependency error'))
            return

        # start constructing sql
        db_updates_sql = []
        db_updates_bindings = []
        now = nowstr()

        try:
            # add dataset
            if isinstance(gridspec,dict):
                gridspec = json_encode(gridspec)
            dataset_id = self.db._increment_id_helper('dataset',conn)
            config['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
            stat_keys = json_encode(stat_keys)
            bindings = (dataset_id,'name',description,gridspec,'processing',
                        'user','institution','localhost',0,njobs,ntasks,
                        now,'','','','',stat_keys,
                        category_csv,debug)
            sql = 'insert into dataset (dataset_id,name,description,gridspec,'
            sql += 'status,username,institution,submit_host,priority,'
            sql += 'jobs_submitted,tasks_submitted,start_date,end_date,'
            sql += 'temporary_storage,global_storage,parent_id,stat_keys,'
            sql += 'categoryvalue_ids,debug)'
            sql += ' values ('+','.join(['?' for _ in bindings])+')'
            db_updates_sql.append(sql)
            db_updates_bindings.append(bindings)
            
            # add config
            try:
                config_data = serialization.serialize_json.dumps(config)
            except:
                logger.info('error serializing config: %r', config,
                            exc_info=True)
                callback(e)
                return
            sql = 'insert into config (dataset_id,config_data,difplus_data)'
            sql += ' values (?,?,?)'
            bindings = (dataset_id,config_data,difplus)
            db_updates_sql.append(sql)
            db_updates_bindings.append(bindings)

            # add task_rel
            for i,task in enumerate(config['tasks']):
                try:
                    reqs = serialization.serialize_json.dumps(task['requirements'])
                except Exception as e:
                    logger.info('cannot serialize requirements',exc_info=True)
                    callback(e)
                    return
                task_name = task['name']
                if not task_name:
                    task_name = str(i)
                sql = 'insert into task_rel (task_rel_id,dataset_id,task_index,'
                sql += 'name,depends,requirements) values (?,?,?,?,?,?)'
                bindings = (task_rels[i],dataset_id,i,task_name,depends[i],reqs)
                db_updates_sql.append(sql)
                db_updates_bindings.append(bindings)

            # write to database
            ret = self.db._db_write(conn,db_updates_sql,db_updates_bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
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
        except Exception as e:
            logger.warn('submit error', exc_info=True)
            callback(e)
            return

        callback(True)

    @dbmethod
    def rpc_update_dataset_config(self,dataset_id,data,callback=None):
        """Update a dataset config"""
        if isinstance(data,dict):
            try:
                data = serialization.serialize_json.dumps(data)
            except:
                logger.info('error serializing config: %r', config,
                            exc_info=True)
                callback(e)
                return

        sql = 'update config set config_data = ? where dataset_id = ?'
        bindings = (data,dataset_id)
        cb = partial(self._rpc_update_dataset_config_callback,
                     dataset_id, sql, bindings, callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _rpc_update_dataset_config_callback(self,dataset_id,sql,bindings,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('dataset',dataset_id,nowstr())
                try:
                    self.db._db_write(conn,sql3,bindings3,None,None,None)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                self._send_to_master(('dataset',dataset_id,nowstr(),sql,bindings))
            callback(True)

    @dbmethod
    def rpc_queue_master(self,resources=None,
                         queueing_factor_priority=1.0,
                         queueing_factor_dataset=1.0,
                         queueing_factor_tasks=1.0,
                         callback=None):
        """
        Handle global queueing request from a site.

        For a task to queue on a site, it must be matched in the dataset
        gridspec list (or the list should be empty to match all), and
        the necessary resources should be available on the site.

        :param resources: (optional) the available resources on the site
        :param queueing_factor_priority: (optional) queueing factor for priority
        :param queueing_factor_dataset: (optional) queueing factor for dataset id
        :param queueing_factor_tasks: (optional) queueing factor for number of tasks
        :returns: (via callback) dict of table entries to be merged
        """
        # priority factors
        qf_p = queueing_factor_priority
        qf_d = queueing_factor_dataset
        qf_t = queueing_factor_tasks

        def cb3(tasks):
            if isinstance(tasks,Exception):
                callback(tasks)
            elif not isinstance(tasks,dict):
                callback(Exception('queue_get_queueing_tasks() did not return a dict'))
            elif not tasks:
                logger.debug('rpc_queue_master(): tasks: %r',tasks)
                callback({})
            else:
                logger.debug('rpc_queue_master(): tasks: %r',tasks)
                self.parent.misc_get_tables_for_task(tasks,callback=callback)
        def cb2(datasets):
            if isinstance(datasets,Exception):
                callback(datasets)
            elif not datasets:
                callback({})
            elif not isinstance(datasets,dict):
                callback(Exception('queue_get_queueing_datasets() did not return a dict'))
            else:
                dataset_prios = calc_datasets_prios(datasets,qf_p,qf_d,qf_t)
                logger.debug('rpc_queue_master(): dataset prios: %r',dataset_prios)
                self.parent.queue_get_queueing_tasks(dataset_prios,
                                                     resources=resources,
                                                     global_queueing=True,
                                                     callback=cb3)
        def cb(ret):
            self.parent.queue_get_queueing_datasets(callback=cb2)
        # buffer tasks before queueing
        self.parent.queue_buffer_jobs_tasks(callback=cb)

    @dbmethod
    def rpc_master_update(self, updates, callback=None):
        def cb(ret):
            if isinstance(ret, Exception):
                callback(ret)
            elif updates:
                self.parent.misc_update_master_db(*updates.pop(0),callback=cb)
            else:
                callback()
        cb(None)

    @dbmethod
    def rpc_stop_module(self, module_name, callback=None):
        self.db.messaging.daemon.stop(mod = module_name, callback=callback)

    @dbmethod
    def rpc_start_module(self, module_name, callback=None):
        self.db.messaging.daemon.start(mod = module_name, callback=callback)

    @dbmethod
    def rpc_update_config(self, config_text, callback=None):
        self.db.messaging.config.set_config_string(config_text = config_text, callback=callback)


    ### Public Methods ###

    @dbmethod
    def rpc_public_get_graphs(self, start, callback=None):
        """
        Get the graph data for a length of time.

        :param start: Amount of minutes in the past to start grabbing
        """
        t = datetime2str(datetime.utcnow()-timedelta(minutes=start))
        sql = 'select * from graph where timestamp >= ?'
        bindings = (t,)
        cb = partial(self._rpc_public_get_graphs_cb,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _rpc_public_get_graphs_cb(self, ret, callback=None):
        if isinstance(ret, Exception):
            callback(ret)
            return
        data = []
        for gid, name, value, timestamp in ret:
            value = json_decode(value)
            data.append({'name':name, 'value':value, 'timestamp':timestamp})
        callback(sorted(data, key=lambda r:r['timestamp']))
