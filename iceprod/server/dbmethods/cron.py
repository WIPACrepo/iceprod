"""
Cron database methods
"""

import logging
from datetime import datetime
from functools import partial
from collections import OrderedDict

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime

logger = logging.getLogger('dbmethods.cron')

class cron(_Methods_Base):
    """
    The scheduled (cron) DB methods.
    
    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """
    
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
                now = datetime2str(datetime.utcnow())
                statuses = {}
                for dataset_id in dataset_status:
                    status = dataset_status[dataset_id]
                    logger.info('dataset %s marked as %s',dataset_id,status)
                    if status not in statuses:
                        statuses[status] = set()
                    statuses[status].add(dataset_id)
                multi_sql = []
                multi_bindings = []
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
                cb = partial(self._cron_dataset_completion_callback3,
                             callback=callback)
                self.db.sql_write_task(multi_sql,multi_bindings,callback=cb)
            else:
                callback(True)
    def _cron_dataset_completion_callback3(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            # TODO: consolidate dataset statistics
            callback(True)
    
    