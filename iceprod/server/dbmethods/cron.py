"""
Cron database methods
"""

import logging
from datetime import datetime, timedelta
from functools import partial
from collections import OrderedDict

from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server.dbmethods import dbmethod,_Methods_Base,datetime2str,str2datetime, nowstr

logger = logging.getLogger('dbmethods.cron')

class cron(_Methods_Base):
    """
    The scheduled (cron) DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """

    @dbmethod
    def cron_dataset_completion(self,callback=None):
        """Check for newly completed datasets and mark them as such"""
        sql = 'select dataset_id,jobs_submitted,tasks_submitted '
        sql += ' from dataset where status = ? '
        bindings = ('processing',)
        cb = partial(self._cron_dataset_completion_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _cron_dataset_completion_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            datasets = OrderedDict()
            for dataset_id,njobs,ntasks in ret:
                datasets[dataset_id] = {'jobs_submitted':njobs,
                                        'tasks_submitted':ntasks,
                                        'task_status':set(),
                                        'ntasks':0}
            if not datasets:
                callback(True)
                return
            sql = 'select dataset_id,task_status from search '
            sql += ' where dataset_id in ('
            sql += ','.join(['?' for _ in datasets])
            sql += ')'
            bindings = tuple(datasets.keys())
            cb = partial(self._cron_dataset_completion_callback2,datasets,
                         callback=callback)
            self.db.sql_read_task(sql,bindings,callback=cb)
    def _cron_dataset_completion_callback2(self,datasets,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
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
                cb = partial(self._cron_dataset_completion_callback3,
                             master_sql,master_bindings,now,
                             callback=callback)
                self.db.sql_write_task(multi_sql,multi_bindings,callback=cb)
            else:
                callback(True)
    def _cron_dataset_completion_callback3(self,master_sql,
                                           master_bindings,now,ret,
                                           callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            if self._is_master():
                sql3 = 'replace into master_update_history (table_name,update_index,timestamp) values (?,?,?)'
                for sql,bindings in zip(master_sql,master_bindings):
                    bindings3 = ('dataset',bindings[-1],now)
                    try:
                        self.db._db_write(conn,sql3,bindings3,None,None,None)
                    except Exception as e:
                        logger.info('error updating master_update_history',
                                    exc_info=True)
            else:
                for sql,bindings in zip(master_sql,master_bindings):
                    self._send_to_master(('dataset',bindings[-1],now,sql,bindings))
            # TODO: consolidate dataset statistics
            callback(True)

    @dbmethod
    def cron_remove_old_passkeys(self,callback=None):
        now = nowstr()
        sql = 'delete from passkey where expire < ?'
        bindings = (now,)
        cb = partial(self._cron_remove_old_passkeys_cb,callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _cron_remove_old_passkeys_cb(self,ret,callback=None):
        callback(ret)

    @dbmethod
    def cron_generate_web_graphs(self,callback=None):
        sql = 'select task_status, count(*) from search '
        sql += 'where task_status not in (?,?,?) group by task_status'
        bindings = ('idle','waiting','complete')
        cb = partial(self._cron_generate_web_graphs_cb,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _cron_generate_web_graphs_cb(self,ret,callback=None):
        if isinstance(ret, Exception):
            callback(ret)
            return
        now = nowstr()
        results = {}
        for status, count in ret:
            results[status] = count
        graph_id = self.db.increment_id('graph')
        sql = 'insert into graph (graph_id, name, value, timestamp) '
        sql += 'values (?,?,?,?)'
        bindings = (graph_id, 'active_tasks', json_encode(results), now)
        cb = partial(self._cron_generate_web_graphs_cb2,callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _cron_generate_web_graphs_cb2(self,ret,callback=None):
        if isinstance(ret, Exception):
            callback(ret)
            return
        time_interval = datetime2str(datetime.utcnow()-timedelta(minutes=1))
        sql = 'select count(*) from task where status = ? and '
        sql += 'status_changed > ?'
        bindings = ('complete', time_interval)
        cb = partial(self._cron_generate_web_graphs_cb3,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _cron_generate_web_graphs_cb3(self,ret,callback=None):
        if isinstance(ret, Exception):
            callback(ret)
            return
        now = nowstr()
        results = {'completions':ret[0][0] if ret and ret[0] else 0}
        graph_id = self.db.increment_id('graph')
        sql = 'insert into graph (graph_id, name, value, timestamp) '
        sql += 'values (?,?,?,?)'
        bindings = (graph_id, 'completed_tasks', json_encode(results), now)
        cb = partial(self._cron_generate_web_graphs_cb4,callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _cron_generate_web_graphs_cb4(self,ret,callback=None):
        callback(ret)

