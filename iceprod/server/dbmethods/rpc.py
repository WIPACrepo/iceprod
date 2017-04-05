"""
RPC database methods
"""

import logging
from datetime import datetime, timedelta
from functools import partial
from collections import OrderedDict
import math

import tornado.gen

from iceprod.core.resources import Resources
from iceprod.core import dataclasses
from iceprod.core import serialization
from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import GlobalID
from iceprod.server import dataset_prio

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime,nowstr

logger = logging.getLogger('dbmethods.rpc')


class rpc(_Methods_Base):
    """
    The RPC DB methods.
    """

    def rpc_echo(self,value,callback=None):
        """Echo a single value. Just a test to see if rpc is working"""
        return value

    @tornado.gen.coroutine
    def rpc_new_task(self, gridspec=None, **kwargs):
        """

        Get a new task from the queue specified by the gridspec,
        based on the platform, hostname, network interfaces, python unicode.
        Save plaform,hostname,network in nodes table.

        Returns:
            dict: a job config
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
        #if args['hostname']:
        #    self.parent.statsd.incr('new_task.hostname.'+args['hostname'].replace('.','_'))
        if args['domain']:
            self.parent.statsd.incr('new_task.domain.'+args['domain'].replace('.','_'))
        yield self.parent.service['node_update'](**args)

        logger.info('acquiring queue lock')
        with (yield self.parent.db.acquire_lock('queue')):
            logger.info('queue lock granted')
            # check resource requirements
            reqs = {}
            for k in Resources.defaults:
                default = Resources.defaults[k]
                if isinstance(default, list):
                    default = len(default)
                if k in args and args[k] != default:
                    reqs[k] = args[k]
                else:
                    reqs[k] = default
            logger.info('new task for resources: %r', reqs)

            for _ in range(100):
                sql = 'select * from task_lookup '
                if reqs:
                    sql += 'where '+' and '.join('req_'+k+' <= ?' for k in reqs)
                sql += ' limit 1'
                if reqs:
                    bindings = tuple(reqs.values())
                else:
                    bindings = tuple()
                ret = yield self.parent.db.query(sql, bindings)
                task_id = None
                resources = {}
                for row in ret:
                    row = self._list_to_dict('task_lookup',row)
                    task_id = row.pop('task_id')
                    for k in row:
                        resources[k.replace('req_','')] = row[k]
                if not task_id:
                    logger.info('no tasks found matching resources available')
                    raise tornado.gen.Return(None)
                sql = 'select * from search where task_id = ? and task_status = ?'
                bindings = (task_id,'queued')
                ret = yield self.parent.db.query(sql, bindings)
                if not ret:
                    logger.info('task %s not valid, remove from task_lookup',
                                task_id)
                    sql = 'delete from task_lookup where task_id = ?'
                    bindings = (task_id,)
                    yield self.parent.db.query(sql, bindings)
                else:
                    break # we found a valid task
            else:
                logger.warn('100 iterations searching for new_task')
                raise tornado.gen.Return(None)

            newtask = self._list_to_dict('search',ret[0])
            if not newtask:
                logger.info('error: no task to allocate')
                raise tornado.gen.Return(None)
            sql = 'select job_index from job where job_id = ?'
            bindings = (newtask['job_id'],)
            ret = yield self.parent.db.query(sql, bindings)
            if (not ret) or not ret[0]:
                logger.warn('failed to find job with known job_id %r',
                            newtask['job_id'])
                raise tornado.gen.Return(None)
            newtask['job'] = ret[0][0]
            sql = 'select jobs_submitted, debug from dataset where dataset_id = ?'
            bindings = (newtask['dataset_id'],)
            ret = yield self.parent.db.query(sql, bindings)
            if (not ret) or not ret[0]:
                logger.warn('failed to find dataset with known dataset_id')
                raise tornado.gen.Return(None)
            for js, debug in ret:
                newtask['jobs_submitted'] = js
                newtask['debug'] = bool(debug)
                break
            newtask['resources'] = resources

            now = nowstr()
            sql = 'update search set task_status = ? '
            sql += ' where task_id = ?'
            bindings = ('processing',newtask['task_id'])
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ? where task_id = ?'
            bindings2 = ('processing',now,newtask['task_id'])
            sql3 = 'delete from task_lookup where task_id = ?'
            bindings3 = (newtask['task_id'],)
            yield self.parent.db.query([sql,sql2,sql3], [bindings,bindings2,bindings3])
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('search',newtask['task_id'],now)
                bindings4 = ('task',newtask['task_id'],now)
                try:
                    yield self.parent.db.query([sql3,sql3], [bindings3,bindings4])
                except:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                yield self._send_to_master(('search',newtask['task_id'],now,sql,bindings))
                yield self._send_to_master(('task',newtask['task_id'],now,sql2,bindings2))

        ret = yield self.parent.service['queue_get_cfg_for_dataset'](newtask['dataset_id'])

        # now make the job config
        config = json_decode(ret)
        if 'options' not in config:
            config['options'] = {}
        config['options']['task_id'] = newtask['task_id']
        config['options']['task'] = newtask['name']
        config['options']['dataset_id'] = newtask['dataset_id']
        config['options']['job'] = newtask['job']
        config['options']['jobs_submitted'] = newtask['jobs_submitted']
        config['options']['debug'] = newtask['debug']
        config['options']['resources'] = newtask['resources']
        raise tornado.gen.Return(config)

    def rpc_set_processing(self, task_id):
        """
        Set a task to the processing status

        Args:
            task_id (str): task_id
        """
        logger.info('rpc_set_processing for %r', task_id)
        return self.parent.service['queue_set_task_status'](task_id, 'processing')

    @tornado.gen.coroutine
    def rpc_finish_task(self, task_id, stats):
        """
        Do task completion operations.

        Args:
            task_id (str): task_id
            stats (dict): statistics from task
        """
        time_used = 0
        try:
            if 'time_used' in stats and stats['time_used']:
                time_used = float(stats['time_used'])
        except:
            logger.warn('bad time_used', exc_info=True)
        #if 'hostname' in stats and stats['hostname']:
        #    self.parent.statsd.incr('finish_task.hostname.'+stats['hostname'].replace('.','_'),
        #                            count=int(time_used) if time_used else 1)
        if 'domain' in stats and stats['domain']:
            self.parent.statsd.incr('finish_task.domain.'+stats['domain'].replace('.','_'),
                                    count=int(time_used) if time_used else 1)

        with (yield self.parent.db.acquire_lock('queue')):
            # update task status
            now = nowstr()
            sql = 'update search set task_status = ? '
            sql += ' where task_id = ?'
            sql2 = 'update task set prev_status = status, '
            sql2 += ' status = ?, status_changed = ?'
            bindings = ('complete',task_id)
            bindings2 = ['complete',now]
            if time_used:
                logger.info('time_used: %r', time_used)
                sql2 += ', walltime = ? '
                bindings2.append(time_used)
            sql2 += ' where task_id = ?'
            bindings2 = tuple(bindings2+[task_id])
            yield self.parent.db.query([sql,sql2],[bindings,bindings2])
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('search',newtask['task_id'],now)
                bindings4 = ('task',newtask['task_id'],now)
                try:
                    yield self.parent.db.query([sql3,sql3], [bindings3,bindings4])
                except:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                yield self._send_to_master(('search',task_id,now,sql,bindings))
                yield self._send_to_master(('task',task_id,now,sql2,bindings2))

        # update task statistics
        logger.debug('insert new task_stat')
        task_stat_id = yield self.parent.db.increment_id('task_stat')
        sql = 'replace into task_stat (task_stat_id,task_id,stat) values '
        sql += ' (?, ?, ?)'
        bindings = (task_stat_id, task_id, json_encode(stats))
        yield self.parent.db.query(sql, bindings)
        if self._is_master():
            sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
            bindings3 = ('task_stat',task_stat_id,now)
            try:
                yield self.parent.db.query(sql3, bindings3)
            except:
                logger.info('error updating master_update_history',
                            exc_info=True)
        else:
            yield self._send_to_master(('task_stat',task_stat_id,now,sql,bindings))

        # check if whole job is finished
        sql = 'select dataset_id, job_id from search where task_id = ?'
        bindings = (task_id,)
        ret = yield self.parent.db.query(sql, bindings)
        dataset_id = None
        job_id = None
        for d_id,j_id in ret:
            dataset_id = d_id
            job_id = j_id
        if (not dataset_id) or (not job_id):
            raise Exception('cannot find dataset or job id')
        sql = 'select jobs_submitted,tasks_submitted from dataset '
        sql += 'where dataset_id = ?'
        bindings = (dataset_id,)
        ret = yield self.parent.db.query(sql, bindings)
        total_jobs = None
        total_tasks = None
        for njobs,ntasks in ret:
            total_jobs = njobs
            total_tasks = ntasks
        if (not total_jobs) or (not total_tasks):
            raise Exception('cannot find total_jobs or total_tasks')

        tasks_per_job = int(total_tasks/total_jobs)
        sql = 'select task_id,task_status from search where job_id = ?'
        bindings = (job_id,)
        ret = yield self.parent.db.query(sql, bindings)
        task_statuses = set()
        if ret and len(ret) == tasks_per_job:
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
            yield self.parent.db.query(sql, bindings)
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('job',job_id,now)
                try:
                    yield self.parent.db.query(sql3, bindings3)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                yield self._send_to_master(('job',job_id,now,sql,bindings))

            if job_status == 'complete':
                # TODO: collate task stats

                # clean dagtemp
                if 'site_temp' in self.parent.cfg['queue']:
                    temp_dir = self.parent.cfg['queue']['site_temp']
                    dataset = GlobalID.localID_ret(dataset_id, type='int')
                    sql = 'select job_index from job where job_id = ?'
                    bindings = (job_id,)
                    try:
                        ret = yield self.parent.db.query(sql, bindings)
                        job = ret[0][0]
                        dagtemp = os.path.join(temp_dir, str(dataset), str(job))
                        logger.info('cleaning site_temp %r', dagtemp)
                        yield self._executor_wrapper(partial(functions.delete, dagtemp))
                    except Exception as e:
                        logger.warn('failed to clean site_temp', exc_info=True)

                # TODO: if not master, delete tasks

    @tornado.gen.coroutine
    def rpc_task_error(self, task_id, error_info=None):
        """
        Mark task as ERROR and possibly adjust resources.

        Args:
            task_id (str): task id
            error_info (dict): error information
        """
        logger.info('task_reset: %r',task_id)
        if not error_info:
            error_info = {}
        time_used = 0
        try:
            if 'time_used' in error_info and error_info['time_used']:
                time_used = float(error_info['time_used'])
        except:
            logger.warn('bad time_used', exc_info=True)
        #if 'hostname' in error_info and error_info['hostname']:
        #    self.parent.statsd.incr('task_error.hostname.'+error_info['hostname'].replace('.','_'),
        #                            count=int(time_used) if time_used else 1)
        if 'domain' in error_info and error_info['domain']:
            self.parent.statsd.incr('task_error.domain.'+error_info['domain'].replace('.','_'),
                                    count=int(time_used) if time_used else 1)
        if 'error_summary' in error_info:
            err = error_info['error_summary']
            logger.info('task killed because: %s', err)
            self.parent.statsd.incr('task_error.killed')
            if 'SIGTERM' in err:
                self.parent.statsd.incr('task_error.kill.sigterm')
            elif 'Resource overuse' in err:
                resource_name = err.split(':',1)[0].split(' ')[-1]
                self.parent.statsd.incr('task_error.kill.'+resource_name+'_overuse')
                if time_used:
                    self.parent.statsd.incr('task_error.kill.'+resource_name+'_overuse_sum',
                                            count=int(time_used))
            elif 'failed to create task' in err:
                self.parent.statsd.incr('task_error.kill.create_failure')
        with (yield self.parent.db.acquire_lock('queue')):
            try:
                sql = 'select failures, requirements, task_rel_id from task '
                sql += 'where task_id = ?'
                bindings = (task_id,)
                ret = yield self.parent.db.query(sql, bindings)
                if (not ret) or not ret[0]:
                    raise Exception('did not get failures')
                failures = 0
                task_reqs = {}
                task_rel_id = None
                for row in ret:
                    failures = row[0] + 1
                    task_rel_id = row[2]
                    try:
                        task_reqs = json_decode(row[1])
                    except Exception:
                        pass
                sql = 'select requirements from task_rel where task_rel_id = ?'
                bindings = (task_rel_id,)
                ret = yield self.parent.db.query(sql, bindings)
                if (not ret) or not ret[0]:
                    raise Exception('did not get task_rel requirements')
                for row in ret:
                    try:
                        if not row[0]:
                            continue
                        reqs = json_decode(row[0])
                        for r in reqs:
                            if r not in task_reqs:
                                task_reqs[r] = r
                    except Exception:
                        logger.warn('could not decode task_rel requirements: %r',
                                    row, exc_info=True)
                sql = 'select dataset_id from search where task_id = ?'
                bindings = (task_id,)
                ret = yield self.parent.db.query(sql, bindings)
                if (not ret) or not ret[0]:
                    raise Exception('did not get dataset_id')
                dataset_id = ret[0][0]
                sql = 'select debug from dataset where dataset_id = ?'
                bindings = (dataset_id,)
                ret = yield self.parent.db.query(sql, bindings)
                if (not ret) or not ret[0]:
                    raise Exception('did not get debug')
                debug = ret[0][0] in (True, 1, 'true', 'T')
                if debug:
                    status = 'suspended'
                elif failures >= self.parent.db.cfg['queue']['max_resets']:
                    status = 'failed'
                else:
                    status = 'reset'

                now = nowstr()
                sql = 'update search set task_status = ? '
                sql += ' where task_id = ?'
                bindings = (status,task_id)
                sql2 = 'update task set prev_status = status, '
                sql2 += ' status = ?, failures = ?, status_changed = ? '
                bindings2 = [status,failures,now]
                if time_used:
                    logger.info('time_used: %r', time_used)
                    sql2 += ', walltime_err = walltime_err + ?, walltime_err_n = walltime_err_n + 1 '
                    bindings2.append(time_used)
                if 'resources' in error_info:
                    logger.info('error_resources: %r', error_info['resources'])
                    logger.info('old task_reqs: %r', task_reqs)
                    # update requirements
                    for req in error_info['resources']:
                        if req not in Resources.defaults or req in ('cpu','gpu'):
                            logger.info('skipping update for req %r', req)
                            continue
                        req_value = error_info['resources'][req]
                        if isinstance(req_value, dataclasses.Number):
                            default = Resources.defaults[req]
                            if isinstance(default, list):
                                default = len(default)
                            if isinstance(default, int):
                                req_value = int(req_value)
                            elif isinstance(default, float):
                                req_value = round(req_value*1.5, 1)
                            if req_value <= default:
                                continue
                            if (req not in task_reqs or task_reqs[req] < req_value
                                or not isinstance(task_reqs[req], dataclasses.Number)):
                                task_reqs[req] = req_value
                        elif req not in task_reqs:
                            task_reqs[req] = req_value
                    logger.info('new task_reqs: %r', task_reqs)
                    sql2 += ', requirements = ? '
                    bindings2.append(json_encode(task_reqs))
                sql2 += ' where task_id = ?'
                bindings2 = tuple(bindings2+[task_id])
                sql3 = 'replace into task_stat (task_stat_id, task_id, stat) '
                sql3 += 'values (?,?,?)'
                task_stat_id = yield self.parent.db.increment_id('task_stat')
                error_info['error'] = True
                error_info['time'] = now
                bindings3 = (task_stat_id, task_id, json_encode(error_info))
                yield self.parent.db.query([sql,sql2,sql3], [bindings,bindings2,bindings3])
                if self._is_master():
                    msql = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                    mbindings1 = ('search',task_id,now)
                    mbindings2 = ('task',task_id,now)
                    mbindings3 = ('task_stat',task_stat_id,now)
                    try:
                        yield self.parent.db.query([msql,msql,msql], [mbindings1,mbindings2,mbindings3])
                    except:
                        logger.info('error updating master_update_history',
                                    exc_info=True)
                else:
                    yield self._send_to_master(('search',task_id,now,sql,bindings))
                    yield self._send_to_master(('task',task_id,now,sql2,bindings2))
                    yield self._send_to_master(('task_stat',task_stat_id,now,sql3,bindings3))
            except:
                logger.warn('error in task_error', exc_info=True)
                raise

    @tornado.gen.coroutine
    def rpc_upload_logfile(self,task,name,data,callback=None):
        """Uploading of a logfile from a task"""
        with (yield self.parent.db.acquire_lock('logfile')):
            logger.info('uploading logfile')
            sql = 'select task_log_id,task_id from task_log where '
            sql += ' task_id = ? and name = ?'
            bindings = (task,name)
            ret = yield self.parent.db.query(sql, bindings)
            task_log_id = None
            for ts,t in ret:
                task_log_id = ts
            if task_log_id:
                logger.debug('replace previous task_log: %r', task_log_id)
                sql = 'update task_log set data = ? where task_log_id = ?'
                bindings = (data,task_log_id)
            else:
                task_log_id = yield self.parent.db.increment_id('task_log')
                logger.info('insert new task_log: %r', task_log_id)
                sql = 'insert into task_log (task_log_id,task_id,name,data) '
                sql += ' values (?,?,?,?)'
                bindings = (task_log_id,task,name,data)
            ret = yield self.parent.db.query(sql, bindings)
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                bindings3 = ('task_log',task_log_id,nowstr())
                try:
                    yield self.parent.db.query(sql3, bindings3)
                except:
                    logger.info('error updating master_update_history',
                                exc_info=True)
            else:
                yield self._send_to_master(('task_log',task_log_id,nowstr(),sql,bindings))
            logger.info('finished uploading logfile')

    @tornado.gen.coroutine
    def rpc_stillrunning(self, task_id):
        """
        Check that the task is still in a running state.

        Running states are "queued" or "processing". Queued is allowed because
        of possible race conditions around changing status to processing.

        Args:
            task_id: task id

        Returns:
            bool: True or False
        """
        sql = 'select status from task where task_id = ?'
        bindings = (task_id,)
        ret = yield self.parent.db.query(sql, bindings)
        if not ret:
            raise Exception('task %s does not exist'%(task_id,))
        ret = ret[0][0] in ('queued','processing')
        raise tornado.gen.Return(ret)

    @tornado.gen.coroutine
    def rpc_update_pilot(self, pilot_id, **kwargs):
        """
        Update the pilot table.

        Args:
            pilot_id (str): Id of the pilot to update.
            **kwargs (dict): {key:value}
        """
        logger.info('update pilot: %s',str(kwargs))
        if not kwargs:
            raise Exception('no update given')
        sql = 'update pilot set '
        bindings = []
        for name in self.parent.db.tables['pilot']:
            if name in kwargs:
                sql += name+'=? '
                bindings.append(kwargs[name])
        sql += ' where pilot_id = ?'
        bindings.append(pilot_id)
        yield self.parent.db.query(sql, tuple(bindings))

    @tornado.gen.coroutine
    def rpc_submit_dataset(self, config, difplus='', description='', gridspec='',
                           njobs=1, stat_keys=[], debug=False):
        """
        Submit a dataset.

        Args:
            config (dict): A config object
            difplus (str): A serialized difplus
            description (str): The dataset description
            gridspec (str): The grid to run on
            njobs (int): Number of jobs to submit
            stat_keys (list): Statistics to keep
            debug (bool): Debug flag (default False)
        """
        if gridspec is None:
            gridspec = ''

        # make sure we have a serialized and deserialized copy of config
        if isinstance(config, dict):
            try:
                config = serialization.dict_to_dataclasses(config)
            except:
                logger.info('error converting config: %r', config,
                            exc_info=True)
                raise
        elif not isinstance(config, dataclasses.Job):
            try:
                config = serialization.serialize_json.loads(config)
            except:
                logger.info('error deserializing config: %r', config,
                            exc_info=True)
                raise

        # check the number of jobs, tasks, and trays
        try:
            njobs = int(njobs)
            ntasks = len(config['tasks'])*njobs
        except:
            logger.info('error reading ntasks from submitting config',
                        exc_info=True)
            raise

        # join categories as csv
        category_csv = ''
        try:
            category_csv = ','.join(config['categories'])
        except:
            pass

        with (yield self.parent.db.acquire_lock('dataset')):
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
                    ret = yield self.parent.db.query(sql, bindings)
                    for tid,did,task_index,name in ret:
                        dataset_depends[did].append({'task_rel_id':tid,
                                                     'task_index':task_index,
                                                     'name':name})
                for task in config['tasks']:
                    task_dep = []
                    task_rel_id = yield self.parent.db.increment_id('task_rel')
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
                raise Exception('Task dependency error')

            # start constructing sql
            db_updates_sql = []
            db_updates_bindings = []
            now = nowstr()

            try:
                # add dataset
                if isinstance(gridspec,dict):
                    gridspec = json_encode(gridspec)
                dataset_id = yield self.parent.db.increment_id('dataset')
                config['dataset'] = GlobalID.localID_ret(dataset_id, type='int')
                stat_keys = json_encode(stat_keys)
                bindings = (dataset_id,'name',description,gridspec,'processing',
                            'user','institution','localhost',0,njobs,ntasks,
                            now,'','','','',stat_keys,
                            category_csv,debug)
                sql = 'insert into dataset (dataset_id,name,description,gridspec,'
                sql += 'status,username,institution,submit_host,priority,'
                sql += 'jobs_submitted,tasks_submitted,start_date,end_date,'
                sql += 'temporary_storage,global_storage,groups_id,stat_keys,'
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
                    raise
                sql = 'insert into config (dataset_id,config_data,difplus_data)'
                sql += ' values (?,?,?)'
                bindings = (dataset_id,config_data,difplus)
                db_updates_sql.append(sql)
                db_updates_bindings.append(bindings)

                # add task_rel
                for i,task in enumerate(config['tasks']):
                    try:
                        reqs = serialization.serialize_json.dumps(task['requirements'])
                    except:
                        logger.info('cannot serialize requirements %r',
                                    task['requirements'], exc_info=True)
                        raise
                    task_name = task['name']
                    if not task_name:
                        task_name = str(i)
                    sql = 'insert into task_rel (task_rel_id,dataset_id,task_index,'
                    sql += 'name,depends,requirements) values (?,?,?,?,?,?)'
                    bindings = (task_rels[i],dataset_id,i,task_name,depends[i],reqs)
                    db_updates_sql.append(sql)
                    db_updates_bindings.append(bindings)

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
            except:
                logger.warn('submit error', exc_info=True)
                raise
            raise tornado.gen.Return(dataset_id)


    @tornado.gen.coroutine
    def rpc_update_dataset_config(self, dataset_id, config):
        """
        Update a dataset config

        Args:
            dataset_id (str): dataset id
            config (str or dict): config
        """
        if isinstance(config,dict):
            try:
                config = serialization.dict_to_dataclasses(config)
            except:
                logger.info('error converting config: %r', config,
                            exc_info=True)
                raise
            try:
                config = serialization.serialize_json.dumps(config)
            except:
                logger.info('error serializing config: %r', config,
                            exc_info=True)
                raise

        sql = 'update config set config_data = ? where dataset_id = ?'
        bindings = (config,dataset_id)
        yield self.parent.db.query(sql, bindings)
        if self._is_master():
            sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
            bindings3 = ('dataset',dataset_id,nowstr())
            try:
                yield self.parent.db.query(sql3, bindings3)
            except:
                logger.info('error updating master_update_history',
                            exc_info=True)
        else:
            yield self._send_to_master(('dataset',dataset_id,nowstr(),sql,bindings))

    @tornado.gen.coroutine
    def rpc_get_groups(self):
        """
        Get all the groups.

        Returns:
            dict: {group_id: group}
        """
        sql = 'select * from groups'
        ret = yield self.parent.db.query(sql, tuple())
        groups = {}
        for row in ret:
            r = self._list_to_dict('groups',row)
            groups[r['groups_id']] = r
        raise tornado.gen.Return(groups)

    @tornado.gen.coroutine
    def rpc_set_groups(self, user=None, groups=None):
        """
        Set all the groups.

        Args:
            user (str): user_id for authorization
            groups (dict): groups to update
        """
        with (yield self.parent.db.acquire_lock('groups')):
            try:
                # check user authorization to set groups

                # get groups
                sql = 'select * from groups'
                ret = yield self.parent.db.query(sql, tuple())

                # set groups
                updates_sql = []
                updates_bindings = []

                delete_ids = set()
                update_ids = {}
                existing_ids = set()
                for groups_id,name,desc,prio in ret:
                    if groups_id not in groups:
                        delete_ids.add(groups_id)
                    else:
                        existing_ids.add(groups_id)
                        g = groups[groups_id]
                        if (g['name'] != name or g['description'] != desc or
                            g['priority'] != prio):
                            update_ids[groups_id] = g

                create_ids = set(groups) - existing_ids
                if delete_ids:
                    updates_sql.append('delete from groups where groups_id in ('+(','.join('?' for _ in delete_ids)+')'))
                    updates_bindings.append(tuple(delete_ids))
                for ids in update_ids:
                    updates_sql.append('update groups set name=?, description=?, priority=? where groups_id=?')
                    updates_bindings.append((groups[ids]['name'],groups[ids]['description'],groups[ids]['priority'],ids))
                for ids in create_ids:
                    i = yield self.parent.db.increment_id('groups')
                    updates_sql.append('insert into groups (groups_id,name,description,priority) values (?,?,?,?)')
                    updates_bindings.append((i,groups[ids]['name'],groups[ids]['description'],groups[ids]['priority']))
                yield self.parent.db.query(updates_sql, updates_bindings)
            except:
                logger.warn('failed to set groups', exc_info=True)
                raise

    @tornado.gen.coroutine
    def rpc_get_user_roles(self, username):
        """
        Get the roles a username belongs to.

        Args:
            username (str): user name

        Returns:
            dict: {role_id: role}
        """
        if username is None:
            raise Exception('no username')

        sql = 'select roles from user where username=?'
        ret = yield self.parent.db.query(sql, (username,))
        if (not ret) or (not ret[0]):
            raise Exception('cannot find username %s'%username)
        elif not ret[0][0]:
            raise tornado.gen.Return({}) # no roles
        else:
            roles = ret[0][0].split(',')
            sql = 'select * from roles where roles_id in ('
            sql += ','.join('?' for _ in roles)+')'
            ret = yield self.parent.db.query(sql, tuple(roles))
            
            roles = {}
            for row in ret:
                r = self._list_to_dict('roles',row)
                roles[r['roles_id']] = r
            raise tornado.gen.Return(roles)

    @tornado.gen.coroutine
    def rpc_set_user_roles(self, user=None, username=None, roles=None):
        """
        Set the roles of a username.

        Args:
            user (str): user id for authorization
            username (str): user name to modify roles on
            roles (iterable): roles to set
        """
        try:
            # check user authorization to set roles

            # set roles
            sql = 'update user set roles = ? where username = ?'
            bindings = (','.join(roles), username)
            yield self.parent.db.query(sql, bindings)
        except:
            logger.warn('failed to set roles for username %s', username,
                        exc_info=True)
            raise

    @tornado.gen.coroutine
    def rpc_queue_master(self,resources=None,
                         filters=None,
                         queueing_factor_priority=1.0,
                         queueing_factor_dataset=1.0,
                         queueing_factor_tasks=1.0,
                         num=100):
        """
        Handle global queueing request from a site.

        For a task to queue on a site, it must be matched in the dataset
        gridspec list (or the list should be empty to match all), and
        the necessary resources should be available on the site.

        Args:
            resources (dict): (optional) the available resources on the site
            filters (dict): (optional) group filters on the site
            queueing_factor_priority (float): (optional) queueing factor for priority
            queueing_factor_dataset (float): (optional) queueing factor for dataset id
            queueing_factor_tasks (float): (optional) queueing factor for number of tasks
            num (int): (optional) number of tasks to queue

        Returns:
            dict: table entries to be merged
        """
        # priority factors
        qf_p = queueing_factor_priority
        qf_d = queueing_factor_dataset
        qf_t = queueing_factor_tasks

        datasets = yield self.parent.service['queue_get_queueing_datasets']()
        if not datasets:
            raise tornado.gen.Return({})
        elif not isinstance(datasets,dict):
            raise Exception('queue_get_queueing_datasets() did not return a dict')
        
        groups = yield self.rpc_get_groups()
        datasets = dataset_prio.apply_group_prios(datasets,
                groups=groups, filters=filters)
        dataset_prios = dataset_prio.calc_datasets_prios(datasets,
                queueing_factor_priority=qf_p,
                queueing_factor_dataset=qf_d,
                queueing_factor_tasks=qf_t)
        logger.debug('rpc_queue_master(): dataset prios: %r',dataset_prios)

        tasks = yield self.parent.service['queue_get_queueing_tasks'](
                dataset_prios, num=num, resources=resources,
                global_queueing=True)
        if not isinstance(tasks,dict):
            raise Exception('queue_get_queueing_tasks() did not return a dict')
        elif len(tasks) < num/2:
            logger.info('only %d tasks queued, buffering to get more',
                        len(tasks))
            num -= len(tasks)
            yield self.parent.service['queue_buffer_jobs_tasks'](num_tasks=num)
            logger.info('done buffering')
            tasks2 = yield self.parent.service['queue_get_queueing_tasks'](
                    dataset_prios, num=num, resources=resources,
                    global_queueing=True)
            if not isinstance(tasks2,dict):
                raise Exception('queue_get_queueing_tasks() did not return a dict')
            tasks.update(tasks2)
        logger.info('rpc_queue_master(): num tasks: %d', len(tasks))
        logger.debug('rpc_queue_master(): tasks: %r', tasks)

        tables = yield self.parent.service['misc_get_tables_for_task'](tasks)
        raise tornado.gen.Return(tables)

    @tornado.gen.coroutine
    def rpc_master_update(self, updates):
        while updates:
            u = updates.pop(0)
            try:
                yield self.parent.service['misc_update_master_db'](*u)
            except:
                logger.warn('failed to apply update: %r', u, exc_info=True)
                raise

    def rpc_stop_module(self, module_name):
        self.parent.modules[module_name]['stop']()

    def rpc_start_module(self, module_name):
        self.parent.modules[module_name]['start']()

    def rpc_update_config(self, config_text):
        self.parent.config.load_string(config_text)
        self.parent.modules['daemon']['reload']()

    @tornado.gen.coroutine
    def rpc_reset_task(self, task):
        yield self.parent.service['queue_set_task_status'](task=task, status='reset')
        sql = 'update task set failures=0 where task_id = ?'
        bindings = (task,)
        yield self.parent.db.query(sql,bindings)

    def rpc_resume_task(self, task):
        return self.parent.service['queue_set_task_status'](task=task, status='resume')

    def rpc_suspend_task(self, task):
        return self.parent.service['queue_set_task_status'](task=task, status='suspended')

    @tornado.gen.coroutine
    def rpc_reset_dataset(self, dataset_id):
        sql = 'select task_id from search '
        sql += 'where task_status != "complete" and dataset_id = ?'
        ret = yield self.parent.db.query(sql, bindings)
        tasks = [row[0] for row in ret]

        sql = 'update search set task_status="idle" '
        sql += 'where task_status != "complete" and dataset_id = ?'
        bindings = (dataset_id,)
        yield self.parent.db.query(sql, bindings)

        sql = 'update task set status="idle" '
        sql += 'where status != "complete" and task_id in (%s)'
        for f in self._bulk_select(sql, tasks):
            yield f

        sql = 'update dataset set status="processing" where dataset_id = ?'
        bindings = (dataset_id,)
        yield self.parent.db.query(sql, bindings)

    @tornado.gen.coroutine
    def rpc_hard_reset_dataset(self, dataset_id):
        sql = 'select task_id from search '
        sql += 'where dataset_id = ?'
        ret = yield self.parent.db.query(sql, bindings)
        tasks = [row[0] for row in ret]

        sql = 'update search set task_status="idle" '
        sql += 'where dataset_id = ?'
        bindings = (dataset_id,)
        yield self.parent.db.query(sql, bindings)

        sql = 'update task set status="idle" '
        sql += 'where task_id in (%s)'
        for f in self._bulk_select(sql, tasks):
            yield f

        sql = 'update dataset set status="processing" where dataset_id = ?'
        bindings = (dataset_id,)
        yield self.parent.db.query(sql, bindings)
        
    @tornado.gen.coroutine
    def rpc_suspend_dataset(self, dataset_id):
        sql = 'update dataset set status="suspended" where dataset_id = ?'
        bindings = (dataset_id,)
        yield self.parent.db.query(sql, bindings)

    ### Public Methods ###

    @tornado.gen.coroutine
    def rpc_public_get_graphs(self, start, callback=None):
        """
        Get the graph data for a length of time.

        Args:
            start (int): Amount of minutes in the past to start grabbing

        Returns:
            list: [{name, value, timestamp}]
        """
        t = datetime2str(datetime.utcnow()-timedelta(minutes=start))
        sql = 'select * from graph where timestamp >= ?'
        bindings = (t,)
        ret = yield self.parent.db.query(sql, bindings)
        data = []
        for gid, name, value, timestamp in ret:
            value = json_decode(value)
            data.append({'name':name, 'value':value, 'timestamp':timestamp})
        ret = sorted(data, key=lambda r:r['timestamp'])
        raise tornado.gen.Return(ret)
