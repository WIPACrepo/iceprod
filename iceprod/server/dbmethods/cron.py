"""
Cron database methods
"""

import os
import logging
from datetime import datetime, timedelta
from functools import partial
from collections import defaultdict, OrderedDict

import tornado.gen

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime, nowstr
from iceprod.server import GlobalID

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
                    sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                    for sql,bindings in zip(master_sql,master_bindings):
                        bindings3 = ('dataset',bindings[-1],now)
                        try:
                            yield self.parent.db.query(sql, bindings3)
                        except Exception:
                            logger.info('error updating master_update_history',
                                        exc_info=True)
                else:
                    for sql,bindings in zip(master_sql,master_bindings):
                        yield self._send_to_master(('dataset',bindings[-1],now,sql,bindings))

                # TODO: consolidate dataset statistics

    @tornado.gen.coroutine
    def cron_job_completion(self):
        """
        Check for job status changes.

        If this is the master, mark jobs complete, suspended, or failed
        as necessary.  Completed jobs also delete the job temp space.

        If this is not the master, and if all tasks in a job are not in
        an active state, then delete the job and tasks.
        """

        sql = 'select dataset_id,jobs_submitted,tasks_submitted '
        sql += ' from dataset where status = ? '
        bindings = ('processing',)
        ret = yield self.parent.db.query(sql, bindings)
        datasets = {}
        for dataset_id,njobs,ntasks in ret:
            datasets[dataset_id] = ntasks//njobs
        if not datasets:
            return

        sql = 'select dataset_id, job_id, task_status, count(*) from search '
        sql += ' where dataset_id in ('
        sql += ','.join(['?' for _ in datasets])
        sql += ') group by job_id, task_status'
        bindings = tuple(datasets)
        ret = yield self.parent.db.query(sql, bindings)

        jobs = defaultdict(lambda:[{},None])
        for dataset_id,job_id,task_status,num in ret:
            jobs[job_id][0][task_status] = num
            jobs[job_id][1] = dataset_id

        complete_jobs = []
        errors_jobs = []
        suspended_jobs = []
        clean_jobs = []
        for job_id in jobs:
            statuses = jobs[job_id][0]
            dataset_id = jobs[job_id][1]
            have_all_jobs = sum(statuses.values()) >= datasets[dataset_id]
            statuses = set(statuses)
            if not statuses&{'waiting','queued','processing','resume','reset'}:
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

        if clean_jobs:
            # we are not the master, and just cleaning these jobs
            with (yield self.parent.db.acquire_lock('queue')):
                sql = 'select task_id from search where job_id in (%s)'
                task_ids = set()
                for f in self._bulk_select(sql, clean_jobs):
                    task_ids.update([row[0] for row in (yield f)])

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
            sql = 'select job_id from job where status = "processing" and job_id in (%s)'
            bindings = complete_jobs+errors_jobs+suspended_jobs
            job_ids = set()
            for f in self._bulk_select(sql, bindings):
                for row in (yield f):
                    job_ids.add(row[0])
            complete_jobs = [j for j in complete_jobs if j in job_ids]
            errors_jobs = [j for j in errors_jobs if j in job_ids]
            suspended_jobs = [j for j in suspended_jobs if j in job_ids]

            now = nowstr()
            sql = 'update job set status = "complete", status_changed = ? '
            sql += ' where job_id = ?'
            for job_id in job_ids:
                dataset_id = jobs[job_id][1]

                # update job status
                logger.info('job %s marked as complete',job_id)
                bindings = (now,job_id)
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
                        logger.warn('failed to clean site_temp', exc_info=True)

    @tornado.gen.coroutine
    def cron_clean_completed_jobs(self):
        """Check old files in the dagtemp from completed jobs"""
        if 'site_temp' not in self.parent.cfg['queue']:
            return

        sql = 'select job_id,job_index from job where status = "complete"'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        jobs = {job_id:index for job_id,index in ret}

        sql = 'select dataset_id, job_id from search '
        sql += ' where task_status = "complete"'
        bindings = tuple()
        ret = yield self.parent.db.query(sql, bindings)
        datasets = {job_id:dataset_id for dataset_id,job_id in ret}

        for job_id in jobs:
            dataset_id = datasets[job_id]
            job_index = jobs[job_id]

            # clean dagtemp
            temp_dir = self.parent.cfg['queue']['site_temp']
            dataset = GlobalID.localID_ret(dataset_id, type='int')
            try:
                dagtemp = os.path.join(temp_dir, str(dataset), str(job_index))
                logger.info('cleaning site_temp %r', dagtemp)
                yield self._executor_wrapper(partial(functions.delete, dagtemp))
            except Exception as e:
                logger.warn('failed to clean site_temp', exc_info=True)

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
