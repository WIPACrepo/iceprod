"""
Cron database methods
"""

import os
import logging
from datetime import datetime, timedelta
from functools import partial
from collections import defaultdict, OrderedDict, Counter

import tornado.gen

from iceprod.core import functions
from iceprod.core.gridftp import GridFTP
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor
from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime, nowstr
from iceprod.server import GlobalID
from iceprod.server.master_communication import send_master

logger = logging.getLogger('dbmethods.cron')

class cron(_Methods_Base):
    """
    The scheduled (cron) DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    @tornado.gen.coroutine
    def cron_dataset_completion(self):
        """Check for newly completed datasets and mark them as such"""
        with (yield self.parent.db.acquire_lock('dataset')):
            sql = 'select dataset_id,jobs_submitted,tasks_submitted '
            sql += ' from dataset where status = ? '
            bindings = ('processing',)
            ret = yield self.parent.db.query(sql, bindings)
            datasets = OrderedDict()
            for dataset_id,njobs,ntasks in ret:
                datasets[dataset_id] = {'jobs_submitted':njobs,
                                        'tasks_submitted':ntasks,
                                        'task_status':set(),
                                        'ntasks':0}
            if not datasets:
                return
            sql = 'select dataset_id,task_status from search '
            sql += ' where dataset_id in ('
            sql += ','.join(['?' for _ in datasets])
            sql += ')'
            bindings = tuple(datasets.keys())
            ret = yield self.parent.db.query(sql, bindings)
            for dataset_id,task_status in ret:
                datasets[dataset_id]['ntasks'] += 1
                datasets[dataset_id]['task_status'].add(task_status)

            dataset_status = {}
            for dataset_id in datasets:
                total_tasks = datasets[dataset_id]['tasks_submitted']
                #tasks_per_job = int(total_tasks/total_jobs)
                ntasks = datasets[dataset_id]['ntasks']
                if ntasks < total_tasks:
                    continue # not all tasks accounted for
                task_statuses = datasets[dataset_id]['task_status']
                if not task_statuses&{'waiting','queued','processing','resume','reset'}:
                    logger.info('dataset %s task statues %r',dataset_id,task_statuses)
                    if not task_statuses-{'complete'}:
                        dataset_status[dataset_id] = 'complete'
                    elif not task_statuses-{'complete','failed'}:
                        dataset_status[dataset_id] = 'errors'
                    elif not task_statuses-{'complete','failed','suspended'}:
                        dataset_status[dataset_id] = 'suspended'
            if dataset_status:
                # update dataset statuses
                now = nowstr()
                statuses = {}
                for dataset_id in dataset_status:
                    status = dataset_status[dataset_id]
                    logger.info('dataset %s marked as %s',dataset_id,status)
                    if status not in statuses:
                        statuses[status] = set()
                    statuses[status].add(dataset_id)
                multi_sql = []
                multi_bindings = []
                master_sql = []
                master_bindings = []
                for s in statuses:
                    bindings = (s,)
                    sql = 'update dataset set status = ?'
                    if s == 'complete':
                        sql += ', end_date = ? '
                        bindings += (now,)
                    sql += ' where dataset_id in ('
                    sql += ','.join(['?' for _ in statuses[s]])
                    sql += ')'
                    bindings += tuple([d for d in statuses[s]])
                    multi_sql.append(sql)
                    multi_bindings.append(bindings)
                    # now prepare individual master sqls
                    bindings = (s,)
                    sql = 'update dataset set status = ?'
                    if s == 'complete':
                        sql += ', end_date = ? '
                        bindings += (now,)
                    sql += ' where dataset_id = ? '
                    for d in statuses[s]:
                        master_sql.append(sql)
                        master_bindings.append(bindings+(d,))
                yield self.parent.db.query(multi_sql, multi_bindings)

                if self._is_master():
                    master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                    sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                    for sql,bindings in zip(master_sql,master_bindings):
                        bindings3 = (master_update_history_id, 'dataset',bindings[-1],now)
                        try:
                            yield self.parent.db.query(sql3, bindings3)
                        except Exception:
                            logger.info('error updating master_update_history',
                                        exc_info=True)
                else:
                    for sql,bindings in zip(master_sql,master_bindings):
                        yield self._send_to_master(('dataset',bindings[-1],now,sql,bindings))

                # TODO: consolidate dataset statistics

    @tornado.gen.coroutine
    def cron_job_completion(self, delete_jobs=False):
        """
        Check for job status changes.

        If this is the master, mark jobs complete, suspended, or failed
        as necessary.  Completed jobs also delete the job temp space.

        If this is not the master, and if all tasks in a job are not in
        an active state, then delete the job and tasks.
        """

        sql = 'select dataset_id,status,jobs_submitted,tasks_submitted '
        sql += ' from dataset '
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        datasets = {}
        for dataset_id,status,njobs,ntasks in ret:
            try:
                datasets[dataset_id] = {
                    'status': status,
                    'tasks': int(ntasks)//int(njobs),
                }
            except ValueError:
                logger.info('something strange with dataset %s', dataset_id,
                            exc_info=True)
        if not datasets:
            return

        # filter by jobs that need updating
        sql = 'select job_id from job where status = "processing" '
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        job_ids = [row[0] for row in ret]

        # get the jobs by status and number of tasks
        sql = 'select dataset_id, job_id, task_status from search '
        sql += ' where job_id in (%s)'
        jobs = defaultdict(lambda:[Counter(),None])
        for f in self._bulk_select(sql, job_ids):
            for dataset_id,job_id,task_status in (yield f):
                jobs[job_id][0][task_status] += 1
                jobs[job_id][1] = dataset_id

        complete_jobs = []
        errors_jobs = []
        suspended_jobs = []
        clean_jobs = []
        for job_id in job_ids:
            if job_id not in jobs:
                logger.error('unknown job id: %r', job_id)
                if self._is_master():
                    suspended_jobs.append(job_id)
                else:
                    clean_jobs.append(job_id)
                continue

            statuses = jobs[job_id][0]
            dataset_id = jobs[job_id][1]
            have_all_jobs = sum(statuses.values()) >= datasets[dataset_id]['tasks']
            statuses = set(statuses)
            if (datasets[dataset_id]['status'] in ('suspended','errors') and
                not statuses&{'processing'}):
                if self._is_master():
                    if not have_all_jobs:
                        logger.error('not all tasks in job %r buffered',job_id)
                        continue
                    if not statuses-{'complete'}:
                        complete_jobs.append(job_id)
                    elif statuses&{'failed'}:
                        errors_jobs.append(job_id)
                    else:
                        suspended_jobs.append(job_id)
                else:
                    logger.info('job %r can be removed', job_id)
                    clean_jobs.append(job_id)
            elif not statuses&{'waiting','queued','processing','resume','reset'}:
                if self._is_master():
                    if not have_all_jobs:
                        logger.error('not all tasks in job %r buffered',job_id)
                        continue
                    if not statuses-{'complete'}:
                        complete_jobs.append(job_id)
                    elif not statuses-{'complete','failed'}:
                        errors_jobs.append(job_id)
                    elif not statuses-{'complete','failed','suspended'}:
                        suspended_jobs.append(job_id)
                else:
                    logger.info('job %r can be removed', job_id)
                    clean_jobs.append(job_id)

        if delete_jobs and (not self._is_master()) and clean_jobs:
            # we are not the master, and just cleaning these jobs
            with (yield self.parent.db.acquire_lock('queue')):
                sql = 'select task_id from search where job_id in (%s)'
                task_ids = set()
                for f in self._bulk_select(sql, clean_jobs):
                    task_ids.update([row[0] for row in (yield f)])

                if task_ids:
                    sql = 'delete from search where task_id in (%s)'
                    for f in self._bulk_select(sql, task_ids):
                        yield f
                    sql = 'delete from task where task_id in (%s)'
                    for f in self._bulk_select(sql, task_ids):
                        yield f
                    sql = 'delete from task_stat where task_id in (%s)'
                    for f in self._bulk_select(sql, task_ids):
                        yield f
                    sql = 'delete from task_log where task_id in (%s)'
                    for f in self._bulk_select(sql, task_ids):
                        yield f
                    sql = 'delete from task_lookup where task_id in (%s)'
                    for f in self._bulk_select(sql, task_ids):
                        yield f
                sql = 'delete from job where job_id in (%s)'
                for f in self._bulk_select(sql, clean_jobs):
                    yield f
                sql = 'delete from job_stat where job_id in (%s)'
                for f in self._bulk_select(sql, clean_jobs):
                    yield f

        else:
            # we are the master, and are updating job statuses
            now = nowstr()

            # errors jobs
            sql = 'update job set status = "errors", status_changed = ? '
            sql += ' where job_id = ?'
            for job_id in errors_jobs:
                # update job status
                logger.info('job %s marked as errors',job_id)
                bindings = (now,job_id)
                yield self.parent.db.query(sql, bindings)
                master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                bindings3 = (master_update_history_id,'job',job_id,now)
                try:
                    yield self.parent.db.query(sql3, bindings3)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)

            # suspended jobs
            sql = 'update job set status = "suspended", status_changed = ? '
            sql += ' where job_id = ?'
            for job_id in suspended_jobs:
                # update job status
                logger.info('job %s marked as suspended',job_id)
                bindings = (now,job_id)
                yield self.parent.db.query(sql, bindings)
                master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                sql3 = 'insert into master_update_history (master_update_history_id, table_name,update_index,timestamp) values (?,?,?,?)'
                bindings3 = (master_update_history_id,'job',job_id,now)
                try:
                    yield self.parent.db.query(sql3, bindings3)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)

            # complete jobs
            sql = 'update job set status = "complete", status_changed = ? '
            sql += ' where job_id = ?'
            for job_id in complete_jobs:
                dataset_id = jobs[job_id][1]

                # update job status
                logger.info('job %s marked as complete',job_id)
                bindings = (now,job_id)
                yield self.parent.db.query(sql, bindings)
                master_update_history_id = yield self.parent.db.increment_id('master_update_history')
                sql3 = 'insert into master_update_history (master_update_history_id,table_name,update_index,timestamp) values (?,?,?,?)'
                bindings3 = (master_update_history_id,'job',job_id,now)
                try:
                    yield self.parent.db.query(sql3, bindings3)
                except Exception as e:
                    logger.info('error updating master_update_history',
                                exc_info=True)

                # TODO: collate task stats

                # clean dagtemp
                if 'site_temp' in self.parent.cfg['queue']:
                    temp_dir = self.parent.cfg['queue']['site_temp']
                    dataset = GlobalID.localID_ret(dataset_id, type='int')
                    sql2 = 'select job_index from job where job_id = ?'
                    bindings = (job_id,)
                    try:
                        ret = yield self.parent.db.query(sql2, bindings)
                        job = ret[0][0]
                        dagtemp = os.path.join(temp_dir, str(dataset), str(job))
                        logger.info('cleaning site_temp %r', dagtemp)
                        yield self._executor_wrapper(partial(functions.delete, dagtemp))
                    except Exception as e:
                        logger.warning('failed to clean site_temp', exc_info=True)

    @tornado.gen.coroutine
    def cron_clean_completed_jobs(self):
        """Check old files in the dagtemp from completed jobs"""
        if 'site_temp' not in self.parent.cfg['queue']:
            return

        sql = 'select job_id,job_index from job where status = "complete"'
        sql += ' or (status in ("suspended","errors") and status_changed < ?)'
        timelimit = datetime.utcnow() - timedelta(days=30)
        bindings = (timelimit.isoformat(),)
        ret = yield self.parent.db.query(sql, bindings)
        jobs = {job_id:str(index) for job_id,index in ret}

        sql = 'select dataset_id, job_id from search '
        sql += ' where task_status != "idle"'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        datasets = defaultdict(set)
        for dataset_id,job_id in ret:
            if job_id in jobs:
                dataset = str(GlobalID.localID_ret(dataset_id, type='int'))
                datasets[dataset].add(jobs[job_id])

        # get all the job_ids currently in tmp
        temp_dir = self.parent.cfg['queue']['site_temp']
        dataset_dirs = yield self._executor_wrapper(partial(GridFTP.list, temp_dir))
        for d in dataset_dirs:
            job_dirs = yield self._executor_wrapper(partial(GridFTP.list, os.path.join(temp_dir, d)))            
            for j in job_dirs:
                if d in datasets and j in datasets[d]:
                    try:
                        dagtemp = os.path.join(temp_dir, d, j)
                        logger.info('cleaning site_temp %r', dagtemp)
                        yield self._executor_wrapper(partial(functions.delete, dagtemp))
                    except Exception as e:
                        logger.warning('failed to clean site_temp', exc_info=True)

    def cron_remove_old_passkeys(self):
        now = nowstr()
        sql = 'delete from passkey where expire < ?'
        bindings = (now,)
        return self.parent.db.query(sql, bindings)

    @tornado.gen.coroutine
    def cron_generate_web_graphs(self):
        sql = 'select task_status, count(*) from search '
        sql += 'where task_status not in (?,?,?) group by task_status'
        bindings = ('idle','waiting','complete')
        ret = yield self.parent.db.query(sql, bindings)

        now = nowstr()
        results = {}
        for status, count in ret:
            results[status] = count
        graph_id = yield self.parent.db.increment_id('graph')
        sql = 'insert into graph (graph_id, name, value, timestamp) '
        sql += 'values (?,?,?,?)'
        bindings = (graph_id, 'active_tasks', json_encode(results), now)
        yield self.parent.db.query(sql, bindings)
        
        time_interval = datetime2str(datetime.utcnow()-timedelta(minutes=1))
        sql = 'select count(*) from task where status = ? and '
        sql += 'status_changed > ?'
        bindings = ('complete', time_interval)
        ret = yield self.parent.db.query(sql, bindings)

        now = nowstr()
        results = {'completions':ret[0][0] if ret and ret[0] else 0}
        graph_id = yield self.parent.db.increment_id('graph')
        sql = 'insert into graph (graph_id, name, value, timestamp) '
        sql += 'values (?,?,?,?)'
        bindings = (graph_id, 'completed_tasks', json_encode(results), now)
        yield self.parent.db.query(sql, bindings)

    @tornado.gen.coroutine
    def cron_pilot_monitoring(self):
        sql = 'select sum(avail_cpu), sum(avail_gpu), sum(avail_memory), '
        sql += 'sum(avail_disk), sum(avail_time), sum(claim_cpu), '
        sql += 'sum(claim_gpu), sum(claim_memory), sum(claim_disk), '
        sql += 'sum(claim_time), count(*) from pilot'
        ret = yield self.parent.db.query(sql, tuple())
        for (avail_cpu, avail_gpu, avail_memory, avail_disk, avail_time,
             claim_cpu, claim_gpu, claim_memory, claim_disk, claim_time,
             num) in ret:
            self.parent.statsd.gauge('pilot_resources.available.cpu', avail_cpu if avail_cpu and avail_cpu > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.available.gpu', avail_gpu if avail_gpu and avail_gpu > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.available.memory', avail_memory if avail_memory and avail_memory > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.available.disk', avail_disk if avail_disk and avail_disk > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.available.time', avail_time if avail_time and avail_time > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.claimed.cpu', claim_cpu if claim_cpu and claim_cpu > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.claimed.gpu', claim_gpu if claim_gpu and claim_gpu > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.claimed.memory', claim_memory if claim_memory and claim_memory > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.claimed.disk', claim_disk if claim_disk and claim_disk > 0 else 0)
            self.parent.statsd.gauge('pilot_resources.claimed.time', claim_time if claim_time and claim_time > 0 else 0)
            self.parent.statsd.gauge('pilot_count', num if num and num > 0 else 0)
            break

    @tornado.gen.coroutine
    def cron_dataset_update(self):
        """Update the dataset table on clients"""
        if 'master_updater' in self.parent.modules:
            ret = yield send_master(self.parent.cfg, 'master_get_tables',
                                    tablenames=['dataset'])
            if ret:
                yield self.parent.service['misc_update_tables'](ret)

    @tornado.gen.coroutine
    def cron_suspend_overusage_tasks(self):
        """Suspend very high resource usage tasks"""
        with (yield self.parent.db.acquire_lock('task_lookup')):
            sql = 'select task_id, req_memory, req_time '
            sql += ' from task_lookup where req_memory > 50 or req_time > 24'
            ret = yield self.parent.db.query(sql, tuple())
            task_ids_all = []
            task_ids_mem = []
            task_ids_time = []
            for task_id, mem, time in ret:
                task_ids_all.append(task_id)
                if mem > 50:
                    task_ids_mem.append(task_id)
                elif time > 24:
                    task_ids_time.append(task_id)
            if task_ids_all:
                sql = 'delete from task_lookup where task_id in (%s)'
                for f in self._bulk_select(sql, task_ids_all):
                    yield f
        # release lock

        if task_ids_all:
            yield self.parent.service['queue_set_task_status'](task_ids_all,'suspended')
            now = nowstr()
            def add_log(task_ids, data):
                sql = 'insert into task_log (task_log_id,task_id,name,data) '
                sql += ' values (?,?,?,?)'
                for task_id in task_ids:
                    task_log_id = yield self.parent.db.increment_id('task_log')
                    bindings = (task_log_id,task_id,'stderr',data)
                    ret = yield self.parent.db.query(sql, bindings)
                    yield self._send_to_master(('task_log',task_log_id,now,sql,bindings))
            if task_ids_mem:
                data = json_compressor.compress(b'task held: requested >50GB memory')
                add_log(task_ids_mem, data)
            if task_ids_time:
                data = json_compressor.compress(b'task held: requested >24hr time')
                add_log(task_ids_time, data)

    @tornado.gen.coroutine
    def cron_check_active_pilots_tasks(self):
        """
        Reset processing tasks that are not listed as running by
        an active pilot.
        """
        sql = 'select task_id from search where task_status="processing"'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        task_ids = {row[0] for row in ret}

        tasks = yield self.parent.service['queue_get_pilots'](active=True)
        for task in tasks:
            if task['tasks']:
                task_ids -= set(task['tasks'])

        if task_ids:
            yield self.parent.service['queue_set_task_status'](task_ids,'reset')
            now = nowstr()
            sql = 'insert into task_log (task_log_id,task_id,name,data) '
            sql += ' values (?,?,?,?)'
            data = json_compressor.compress(b'task reset: not running in an active pilot')
            for task_id in task_ids:
                task_log_id = yield self.parent.db.increment_id('task_log')
                bindings = (task_log_id,task_id,'stderr',data)
                ret = yield self.parent.db.query(sql, bindings)
                yield self._send_to_master(('task_log',task_log_id,now,sql,bindings))

    @tornado.gen.coroutine
    def cron_dataset_status_monitoring(self):
        """
        Monitor all datasets for job/task status summary.
        """
        sql = 'select dataset_id from dataset where status = "processing"'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        processing_datasets = {row[0] for row in ret}

        sql = 'select dataset_id, status, count(*) from job group by dataset_id,status'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        for dataset_id, status, num in ret:
            dataset_num = GlobalID.localID_ret(dataset_id,type='int')
            if dataset_id not in processing_datasets:
                num = 0
            self.parent.statsd.gauge('datasets.{}.jobs.{}'.format(dataset_num,status), num)

        sql = 'select dataset_id, name, task_status, count(*) from search group by dataset_id,name,task_status'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        for dataset_id, name, status, num in ret:
            dataset_num = GlobalID.localID_ret(dataset_id,type='int')
            if dataset_id not in processing_datasets:
                num = 0
            self.parent.statsd.gauge('datasets.{}.tasks.{}.{}'.format(dataset_num,name,status), num)

    @tornado.gen.coroutine
    def cron_task_stat_monitoring(self, limit=1000):
        """
        Monitor task statistics in ES.
        """
        sql = 'select task_stat_id from task_stat'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        task_stat_ids = {row[0] for row in ret}

        task_stat_updates = []
        for ts_id in task_stat_ids:
            if not self.parent.elasticsearch.head('task_stat',ts_id):
                task_stat_updates.append(ts_id)
                if len(task_stat_updates) >= limit:
                    logger.info('task_stat_monitoring hit limit')
                    break

        if task_stat_updates:
            sql = 'select * from task_stat where task_stat_id in (%s)'
            for f in self._bulk_select(sql, task_stat_updates):
                ret = yield f
                for task_stat, task_id, data in ret:
                    data = json.loads(data)
                    data['task_id'] = task_id
                    if 'task_stats' in data:
                        del data['task_stats']
                    self.parent.elasticsearch.put('task_stat', ts_id, data)

