import logging
import json
import uuid
import math
from collections import defaultdict

import tornado.web
import pymongo
import motor

from iceprod.core import dataclasses
from iceprod.core.resources import Resources
from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr, task_statuses, task_status_sort

logger = logging.getLogger('rest.tasks')

def setup(config, *args, **kwargs):
    """
    Setup method for Tasks REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('tasks',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).tasks
    if 'task_id_index' not in db.tasks.index_information():
        db.tasks.create_index('task_id', name='task_id_index', unique=True)
    if 'dataset_id_index' not in db.tasks.index_information():
        db.tasks.create_index('dataset_id', name='dataset_id_index', unique=False)
    if 'job_id_index' not in db.tasks.index_information():
        db.tasks.create_index('job_id', name='job_id_index', unique=False)
    if 'status_index' not in db.tasks.index_information():
        db.tasks.create_index('status', name='status_index', unique=False)
    if 'priority_index' not in db.tasks.index_information():
        db.tasks.create_index([('status',pymongo.ASCENDING),('priority',pymongo.DESCENDING)], name='priority_index', unique=False)

    if 'dataset_id_index' not in db.dataset_files.index_information():
        db.dataset_files.create_index('dataset_id', name='dataset_id_index', unique=False)
    if 'task_id_index' not in db.dataset_files.index_information():
        db.dataset_files.create_index('task_id', name='task_id_index', unique=False)

    handler_cfg = RESTHandlerSetup(config, *args, **kwargs)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(**db_cfg).tasks,
    })

    return [
        (r'/tasks', MultiTasksHandler, handler_cfg),
        (r'/tasks/(?P<task_id>\w+)', TasksHandler, handler_cfg),
        (r'/tasks/(?P<task_id>\w+)/status', TasksStatusHandler, handler_cfg),
        (r'/task_actions/queue', TasksActionsQueueHandler, handler_cfg),
        (r'/task_actions/bulk_status/(?P<status>\w+)', TaskBulkStatusHandler, handler_cfg),
        (r'/task_actions/process', TasksActionsProcessingHandler, handler_cfg),
        (r'/task_counts/status', TaskCountsStatusHandler, handler_cfg),
        (r'/tasks/(?P<task_id>\w+)/task_actions/reset', TasksActionsErrorHandler, handler_cfg),
        (r'/tasks/(?P<task_id>\w+)/task_actions/complete', TasksActionsCompleteHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks', DatasetMultiTasksHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)', DatasetTasksHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/status', DatasetTasksStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/task_summaries/status', DatasetTaskSummaryStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/task_counts/status', DatasetTaskCountsStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/task_counts/name_status', DatasetTaskCountsNameStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/task_actions/bulk_status/(?P<status>\w+)', DatasetTaskBulkStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/task_actions/bulk_requirements/(?P<name>\w+)', DatasetTaskBulkRequirementsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/task_stats', DatasetTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/files', DatasetMultiFilesHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/files/(?P<task_id>\w+)', DatasetTaskFilesHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiTasksHandler(BaseHandler):
    """
    Handle multi tasks requests.
    """
    @authorization(roles=['admin','system','client'])
    async def get(self):
        """
        Get task entries.

        Params (optional):
            status: | separated list of task status to filter by
            keys: | separated list of keys to return for each task
            sort: | separated list of sort key=values, with values of 1 or -1
            limit: number of tasks to return

        Returns:
            dict: {'tasks': [<task>]}
        """
        filters = {}

        status = self.get_argument('status', None)
        if status:
            filters['status'] = {'$in': status.split('|')}

        sort = self.get_argument('sort', None)
        mongo_sort = []
        if sort:
            for s in sort.split('|'):
                if '=' in s:
                    name, order = s.split('=', 1)
                    if order == '-1':
                        mongo_sort.append((name, pymongo.DESCENDING))
                    else:
                        mongo_sort.append((name, pymongo.ASCENDING))
                else:
                    mongo_sort.append((s, pymongo.ASCENDING))

        limit = self.get_argument('limit', 0)
        if limit:
            try:
                limit = int(limit)
            except Exception:
                limit = 0

        projection = {x:True for x in self.get_argument('keys','').split('|') if x}
        projection['_id'] = False

        ret = []
        async for row in self.db.tasks.find(filters, projection=projection, sort=mongo_sort, limit=limit):
            ret.append(row)
        self.write({'tasks': ret})

    @authorization(roles=['admin','system','client'])
    async def post(self):
        """
        Create a task entry.

        Body should contain the task data.

        Returns:
            dict: {'result': <task_id>}
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'dataset_id': str,
            'job_id': str,
            'task_index': int,
            'job_index': int,
            'name': str,
            'depends': list,
            'requirements': dict,
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key {} should be of type {}'.format(k, req_fields[k])
                raise tornado.web.HTTPError(400, reason=r)

        # set some fields
        task_id = uuid.uuid1().hex
        data.update({
            'task_id': task_id,
            'status_changed': nowstr(),
            'failures': 0,
            'evictions': 0,
            'walltime': 0.0,
            'walltime_err': 0.0,
            'walltime_err_n': 0,
            'site': '',
        })
        if 'status' not in data:
            data['status'] = 'waiting'
        if 'priority' not in data:
            data['priority'] = 1.

        ret = await self.db.tasks.insert_one(data)
        self.set_status(201)
        self.write({'result': task_id})
        self.finish()

class TasksHandler(BaseHandler):
    """
    Handle single task requests.
    """
    @authorization(roles=['admin','client','system','pilot'])
    async def get(self, task_id):
        """
        Get a task entry.

        Args:
            task_id (str): the task id

        Returns:
            dict: task entry
        """
        ret = await self.db.tasks.find_one({'task_id':task_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Task not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin','client','system','pilot'])
    async def patch(self, task_id):
        """
        Update a task entry.

        Body should contain the task data to update.  Note that this will
        perform a merge (not replace).

        Args:
            task_id (str): the task id

        Returns:
            dict: updated task entry
        """
        data = json.loads(self.request.body)
        if not data:
            raise tornado.web.HTTPError(400, reason='Missing update data')

        ret = await self.db.tasks.find_one_and_update({'task_id':task_id},
                {'$set':data},
                projection={'_id':False},
                return_document=pymongo.ReturnDocument.AFTER)
        if not ret:
            self.send_error(404, reason="Task not found")
        else:
            self.write(ret)
            self.finish()

class TasksStatusHandler(BaseHandler):
    """
    Handle single task requests.
    """
    @authorization(roles=['admin','client','system', 'pilot'])
    async def put(self, task_id):
        """
        Set a task status.

        Body should have {'status': <new_status>}

        Args:
            task_id (str): the task id

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if (not data) or 'status' not in data:
            raise tornado.web.HTTPError(400, reason='Missing status in body')
        if data['status'] not in task_statuses:
            raise tornado.web.HTTPError(400, reason='Bad status')
        update_data = {
            'status': data['status'],
            'status_changed': nowstr(),
        }

        ret = await self.db.tasks.update_one({'task_id':task_id},
                {'$set':update_data})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Task not found")
        else:
            self.write({})
            self.finish()

class TaskCountsStatusHandler(BaseHandler):
    """
    Handle task summary grouping by status.
    """
    @authorization(roles=['admin','client','system'])
    async def get(self):
        """
        Get the task counts for all tasks, group by status.

        Returns:
            dict: {<status>: num}
        """
        ret = {}
        for status in task_statuses:
            ret[status] = await self.db.tasks.count_documents({"status":status})

        ret2 = {}
        for k in sorted(ret, key=task_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()

class DatasetMultiTasksHandler(BaseHandler):
    """
    Handle multi tasks requests.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get task entries.

        Params (optional):
            status: | separated list of task status to filter by
            job_id: job_id to filter by
            job_index: job_index to filter by
            keys: | separated list of keys to return for each task

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {'task_id': {task data}}
        """
        filters = {'dataset_id':dataset_id}

        status = self.get_argument('status', None)
        if status:
            filters['status'] = {'$in': status.split('|')}

        job_id = self.get_argument('job_id', None)
        if job_id:
            filters['job_id'] = job_id

        job_index = self.get_argument('job_index', None)
        if job_index:
            try:
                filters['job_index'] = int(job_index)
            except ValueError:
                raise tornado.web.HTTPError(400, reason='Bad argument "job_index": must be integer')

        projection = {'_id': False}
        keys = self.get_argument('keys','')
        if keys:
            projection.update({x:True for x in keys.split('|') if x})
            projection['task_id'] = True

        ret = {}
        async for row in self.db.tasks.find(filters, projection=projection):
            ret[row['task_id']] = row
        self.write(ret)

class DatasetTasksHandler(BaseHandler):
    """
    Handle single task requests.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id, task_id):
        """
        Get a task entry.

        Args:
            dataset_id (str): dataset id
            task_id (str): the task id

        Params (optional):
            keys: | separated list of keys to return for each task

        Returns:
            dict: task entry
        """
        projection = {'_id': False}
        keys = self.get_argument('keys','')
        if keys:
            projection.update({x:True for x in keys.split('|') if x})
            projection['task_id'] = True

        ret = await self.db.tasks.find_one({'task_id':task_id,'dataset_id':dataset_id},
                projection=projection)
        if not ret:
            self.send_error(404, reason="Task not found")
        else:
            self.write(ret)
            self.finish()

class DatasetTasksStatusHandler(BaseHandler):
    """
    Handle single task requests.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:write'])
    async def put(self, dataset_id, task_id):
        """
        Set a task status.

        Body should have {'status': <new_status>}

        Args:
            dataset_id (str): dataset id
            task_id (str): the task id

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if (not data) or 'status' not in data:
            raise tornado.web.HTTPError(400, reason='Missing status in body')
        if data['status'] not in task_statuses:
            raise tornado.web.HTTPError(400, reason='Bad status')
        update_data = {
            'status': data['status'],
            'status_changed': nowstr(),
        }
        if data['status'] == 'reset':
            update_data['failures'] = 0

        ret = await self.db.tasks.update_one({'task_id':task_id,'dataset_id':dataset_id},
                {'$set':update_data})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Task not found")
        else:
            self.write({})
            self.finish()

class DatasetTaskSummaryStatusHandler(BaseHandler):
    """
    Handle task summary grouping by status.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get the task summary for all tasks in a dataset, group by status.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<status>: [<task_id>,]}
        """
        cursor = self.db.tasks.find({'dataset_id':dataset_id},
                projection={'_id':False,'status':True,'task_id':True})
        ret = defaultdict(list)
        async for row in cursor:
            ret[row['status']].append(row['task_id'])
        ret2 = {}
        for k in sorted(ret, key=task_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()

class DatasetTaskCountsStatusHandler(BaseHandler):
    """
    Handle task summary grouping by status.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get the task counts for all tasks in a dataset, group by status.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<status>: num}
        """
        cursor = self.db.tasks.aggregate([
            {'$match':{'dataset_id':dataset_id}},
            {'$group':{'_id':'$status', 'total': {'$sum':1}}},
        ])
        ret = {}
        async for row in cursor:
            ret[row['_id']] = row['total']
        ret2 = {}
        for k in sorted(ret, key=task_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()

class DatasetTaskCountsNameStatusHandler(BaseHandler):
    """
    Handle task summary grouping by name and status.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get the task counts for all tasks in a dataset, group by name,status.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<name>: {<status>: num}}
        """
        cursor = self.db.tasks.aggregate([
            {'$match':{'dataset_id':dataset_id}},
            {'$group':{
                '_id':{'name':'$name','status':'$status'},
                'ordering':{'$first':'$task_index'},
                'total': {'$sum':1}
            }},
        ])
        ret = defaultdict(dict)
        ordering = {}
        async for row in cursor:
            ret[row['_id']['name']][row['_id']['status']] = row['total']
            ordering[row['_id']['name']] = row['ordering']
        ret2 = {}
        for k in sorted(ordering, key=lambda n:ordering[n]):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()

class DatasetTaskStatsHandler(BaseHandler):
    """
    Handle task stats
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get the task statistics for all tasks in a dataset, group by name.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<name>: {<stat>: <value>}}
        """
        cursor = self.db.tasks.aggregate([
            {'$match':{'dataset_id':dataset_id, 'status':'complete'}},
            {'$group':{'_id':'$name',
                'count': {'$sum': 1},
                'gpu': {'$sum': '$requirements.gpu'},
                'total_hrs': {'$sum': '$walltime'},
                'total_err_hrs': {'$sum': '$walltime_err'},
                'avg_hrs': {'$avg': '$walltime'},
                'stddev_hrs': {'$stdDevSamp': '$walltime'},
                'min_hrs': {'$min': '$walltime'},
                'max_hrs': {'$max': '$walltime'},
                'ordering': {'$first': '$task_index'},
            }},
        ])
        ret = {}
        ordering = {}
        async for row in cursor:
            denom = row['total_hrs'] + row['total_err_hrs']
            row['efficiency'] = row['total_hrs']/denom if denom > 0 else 0.0
            name = row.pop('_id')
            ordering[name] = row.pop('ordering')
            ret[name] = row
        ret2 = {}
        for k in sorted(ordering, key=lambda n:ordering[n]):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()

class TasksActionsQueueHandler(BaseHandler):
    """
    Handle task action for waiting -> queued.
    """
    @authorization(roles=['admin','client'])
    async def post(self):
        """
        Take a number of waiting tasks and queue them.

        Order by priority.

        Body args (json):
            num_tasks: int

        Returns:
            dict: {queued: num tasks queued}
        """
        data = json.loads(self.request.body)
        num_tasks = data.get('num_tasks', 100)

        query = {'status': 'waiting'}
        val = {'$set': {'status': 'queued'}}

        queued = 0
        while queued < num_tasks:
            ret = await self.db.tasks.find_one_and_update(query, val,
                    projection={'_id':False,'task_id':True},
                    sort=[('priority', -1)])
            if not ret:
                logger.debug('no more tasks to queue')
                break
            queued += 1
        logger.info(f'queued {queued} tasks')
        self.write({'queued': queued})

class TasksActionsProcessingHandler(BaseHandler):
    """
    Handle task action for queued -> processing.
    """
    @authorization(roles=['admin','client','pilot'])
    async def post(self):
        """
        Take one queued task, set its status to processing, and return it.

        Body args (json):
            requirements: dict
            query_params: (optional) dict of mongodb params

        Returns:
            dict: <task dict>
        """
        filter_query = {'status':'queued'}
        sort_by = [('priority',-1)]
        site = 'unknown'
        if self.request.body:
            data = json.loads(self.request.body)
            # handle requirements
            reqs = data.get('requirements', {})
            req_filters = []
            for k in reqs:
                if k == 'gpu' and reqs[k] > 0:
                    val = {'$lte': reqs[k], '$gte': 1}
                    req_filters.append({'requirements.'+k: val})
                    continue
                elif isinstance(reqs[k], (int,float)):
                    val = {'$lte': reqs[k]}
                else:
                    val = reqs[k]
                req_filters.append({'$or': [
                    {'requirements.'+k: {'$exists': False}},
                    {'requirements.'+k: val},
                ]})
            if req_filters:
                filter_query['$and'] = req_filters
            if 'site' in reqs:
                site = reqs['site']
            # handle query_params
            params = data.get('query_params', {})
            for k in params:
                if k in filter_query:
                    raise tornado.web.HTTPError(400, reason=f'param {k} would override an already set filter')
                filter_query[k] = params[k]
        print('filter_query', filter_query)
        ret = await self.db.tasks.find_one_and_update(filter_query,
                {'$set':{'status':'processing'}},
                projection={'_id':False},
                sort=sort_by,
                return_document=pymongo.ReturnDocument.AFTER)
        if not ret:
            logger.info('filter_query: %r', filter_query)
            self.send_error(404, reason="Task not found")
        else:
            self.module.statsd.incr('site.{}.task_processing'.format(site))
            self.write(ret)
            self.finish()

class TasksActionsErrorHandler(BaseHandler):
    """
    Handle task action on error (* -> reset).
    """
    @authorization(roles=['admin','client','pilot'])
    async def post(self, task_id):
        """
        Take one task, set its status to reset.

        Args:
            task_id (str): task id

        Body args (json):
            time_used (int): (optional) time used to run task, in seconds
            resources (dict): (optional) resources used by task
            site (str): (optional) site the task was running at
            reason (str): (optional) reason for error

        Returns:
            dict: {}  empty dict
        """
        filter_query = {'task_id': task_id, 'status': {'$ne': 'complete'}}
        update_query = defaultdict(dict,{
            '$set': {
                'status': 'reset',
                'status_changed': nowstr(),
            },
            '$inc': {
                'failures': 1,
            },
        })
        if self.request.body:
            data = json.loads(self.request.body)
            task = await self.db.tasks.find_one(filter_query)
            if 'time_used' in data:
                update_query['$inc']['walltime_err_n'] = 1
                update_query['$inc']['walltime_err'] = data['time_used']/3600.
            elif 'resources' in data and 'time' in data['resources']:
                update_query['$inc']['walltime_err_n'] = 1
                update_query['$inc']['walltime_err'] = data['resources']['time']
            for k in ('cpu','memory','disk','time'):
                if 'resources' in data and k in data['resources']:
                    try:
                        new_val = float(data['resources'][k])
                        old_val = task['resources'][k] if k in task['resources'] else Resources.defaults[k]
                        if k == 'cpu': # special handling for cpu
                            if new_val <= 1.1 or new_val > 20:
                                continue
                            if new_val < old_val*1.1:
                                continue
                            new_val = old_val+1 # increase linearly
                        else:
                            new_val *= 1.5 # increase new request by 1.5
                        if isinstance(Resources.defaults[k], (int, list)):
                            new_val = math.ceil(new_val)
                    except Exception:
                        logger.info('error converting requirement %r',
                                    data['resources'][k], exc_info=True)
                    else:
                        update_query['$max']['requirements.'+k] = new_val
            site = 'unknown'
            if 'site' in data:
                site = data['site']
                update_query['$set']['site'] = site
            if self.module and self.module.statsd and 'reason' in data and data['reason']:
                reason = 'other'
                reasons = [
                    ('Exception: failed to download', 'download_failure'),
                    ('Exception: failed to upload', 'upload_failure'),
                    ('Exception: module failed', 'module_failure'),
                    ('Resource overusage for cpu', 'cpu_overuse'),
                    ('Resource overusage for gpu', 'gpu_overuse'),
                    ('Resource overusage for memory', 'memory_overuse'),
                    ('Resource overusage for disk', 'disk_overuse'),
                    ('Resource overusage for time', 'time_overuse'),
                    ('pilot SIGTERM', 'sigterm'),
                    ('killed', 'killed'),
                ]
                for text,r in reasons:
                    if text in data['reason']:
                        reason = r
                        break
                self.module.statsd.incr('site.{}.task_reset.{}'.format(site, reason))
        ret = await self.db.tasks.find_one_and_update(filter_query,
                update_query,
                projection={'_id':False})
        if not ret:
            logger.info('filter_query: %r', filter_query)
            self.send_error(404, reason="Task not found")
        else:
            self.write(ret)
            self.finish()

class TasksActionsCompleteHandler(BaseHandler):
    """
    Handle task action on processing -> complete.
    """
    @authorization(roles=['admin','client','pilot'])
    async def post(self, task_id):
        """
        Take one task, set its status to complete.

        Args:
            task_id (str): task id

        Body args (json):
            time_used (int): (optional) time used to run task, in seconds
            site (str): (optional) site the task was running at

        Returns:
            dict: {}  empty dict
        """
        filter_query = {'task_id': task_id, 'status': 'processing'}
        update_query = {
            '$set': {
                'status': 'complete',
                'status_changed': nowstr(),
            },
        }
        if self.request.body:
            data = json.loads(self.request.body)
            if 'time_used' in data:
                update_query['$set']['walltime'] = data['time_used']/3600.
            site = 'unknown'
            if 'site' in data:
                site = data['site']
                update_query['$set']['site'] = site
            self.module.statsd.incr('site.{}.task_complete'.format(site))
        ret = await self.db.tasks.find_one_and_update(filter_query,
                update_query,
                projection={'_id':False})
        if not ret:
            logger.info('filter_query: %r', filter_query)
            self.send_error(404, reason="Task not found or not processing")
        else:
            self.write(ret)
            self.finish()

class TaskBulkStatusHandler(BaseHandler):
    """
    Update the status of multiple tasks at once.
    """
    @authorization(roles=['admin','client','system'])
    async def post(self, status):
        """
        Set multiple tasks' status.

        Body should have {'tasks': [<task_id>, <task_id>, ...]}

        Args:
            status (str): the status

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if (not data) or 'tasks' not in data or not data['tasks']:
            raise tornado.web.HTTPError(400, reason='Missing tasks in body')
        tasks = list(data['tasks'])
        if len(tasks) > 100000:
            raise tornado.web.HTTPError(400, reason='Too many tasks specified (limit: 100k)')
        if status not in task_statuses:
            raise tornado.web.HTTPError(400, reason='Bad status')
        query = {
            'task_id': {'$in': tasks},
        }
        update_data = {
            'status': status,
            'status_changed': nowstr(),
        }
        if status == 'reset':
            update_data['failures'] = 0

        ret = await self.db.tasks.update_many(query, {'$set':update_data})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Tasks not found")
        else:
            self.write({})
            self.finish()

class DatasetTaskBulkStatusHandler(BaseHandler):
    """
    Update the status of multiple tasks at once.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:write'])
    async def post(self, dataset_id, status):
        """
        Set multiple tasks' status.

        Body should have {'tasks': [<task_id>, <task_id>, ...]}

        Args:
            dataset_id (str): dataset id
            status (str): the status

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if (not data) or 'tasks' not in data or not data['tasks']:
            raise tornado.web.HTTPError(400, reason='Missing tasks in body')
        tasks = list(data['tasks'])
        if len(tasks) > 100000:
            raise tornado.web.HTTPError(400, reason='Too many tasks specified (limit: 100k)')
        if status not in task_statuses:
            raise tornado.web.HTTPError(400, reason='Bad status')
        query = {
            'dataset_id': dataset_id,
            'task_id': {'$in': tasks},
        }
        update_data = {
            'status': status,
            'status_changed': nowstr(),
        }
        if status == 'reset':
            update_data['failures'] = 0

        ret = await self.db.tasks.update_many(query, {'$set':update_data})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Tasks not found")
        else:
            self.write({})
            self.finish()

class DatasetTaskBulkRequirementsHandler(BaseHandler):
    """
    Update the requirements of multiple tasks at once.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:write'])
    async def patch(self, dataset_id, name):
        """
        Set multiple tasks' requirements. Sets for all tasks in a dataset
        with the specified name.

        Body should have {<resource>: <requirement>}.

        Args:
            dataset_id (str): dataset id
            name (str): the task name

        Returns:
            dict: empty dict
        """
        valid_req_keys = set(Resources.defaults)
        valid_req_keys.add('os')
        valid_req_keys.add('site')
        
        data = json.loads(self.request.body)
        if (not data):
            raise tornado.web.HTTPError(400, reason='Missing body')
        elif set(data) - valid_req_keys:
            raise tornado.web.HTTPError(400, reason='Invalid resource types')

        reqs = {}
        for key in valid_req_keys.intersection(data):
            val = data[key]
            if key == 'os':
                if not isinstance(val, list):
                    raise tornado.web.HTTPError(400, reason='Bad type for {}, should be list'.format(key))
            elif key in Resources.defaults and isinstance(Resources.defaults[key], (int, list)):
                if not isinstance(val, int):
                    raise tornado.web.HTTPError(400, reason='Bad type for {}, should be int'.format(key))
            elif key in Resources.defaults and isinstance(Resources.defaults[key], float):
                if not isinstance(val, (int,float)):
                    raise tornado.web.HTTPError(400, reason='Bad type for {}, should be float'.format(key))
            else:
                val = str(val)
            reqs['requirements.'+key] = val

        query = {
            'dataset_id': dataset_id,
            'name': name,
        }
        ret = await self.db.tasks.update_many(query,
                {'$max':reqs})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Tasks not found")
        else:
            self.write({})
            self.finish()

class DatasetMultiFilesHandler(BaseHandler):
    """
    Handle multi files requests, by dataset.
    """
    @authorization(roles=['admin','system','client'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get dataset_files entries.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {'files': [<file>]}
        """
        filters = {'dataset_id': dataset_id}
        projection = {'_id':False, 'dataset_id':False, 'task_id':False}
        ret = []
        async for row in self.db.dataset_files.find(filters, projection=projection):
            ret.append(row)
        self.write({'files': ret})

    @authorization(roles=['admin','system','client'], attrs=['dataset_id:write'])
    async def post(self, dataset_id):
        """
        Create a dataset_files entry.

        Body should contain the file data.

        Parameters:
            filename (str): the full url filename
            movement (str): [input | output | both]
            job_index (int): the job index to add to
            task_name (str): the name of the task
            local (str): (optional) the local filename the task sees
            transfer (str): whether to transfer the file (can be bool or str)
            compression (str): whether to automatically compress/decompress the file

        Returns:
            dict: {'result': <task_id>}
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'filename': str,
            'movement': str,
            'job_index': int,
            'task_name': str,
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key {} should be of type {}'.format(k, req_fields[k])
                raise tornado.web.HTTPError(400, reason=r)

        # find the task referred to
        filters = {
            'dataset_id': dataset_id,
            'job_index': data['job_index'],
            'name': data['task_name'],
        }
        ret = await self.db.tasks.find_one(filters)
        if not ret:
            raise tornado.web.HTTPError(400, reason='task referred to not found')

        # set some fields
        file_data = dataclasses.Data()
        file_data.update({
            'task_id': ret['task_id'],
            'dataset_id': dataset_id,
            'remote': data['filename'],
            'movement': data['movement'],
        })
        if 'local' in data:
            if not isinstance(data['local'], str):
                r = 'key {} should be of type {}'.format('local', str)
                raise tornado.web.HTTPError(400, reason=r)
            file_data['local'] = data['local']
        if 'transfer' in data:
            if not isinstance(data['transfer'], (str,bool)):
                r = 'key {} should be of type {}'.format('transfer', str)
                raise tornado.web.HTTPError(400, reason=r)
            file_data['transfer'] = data['transfer']
        if 'compression' in data:
            if not isinstance(data['compression'], (str,bool)):
                r = 'key {} should be of type {}'.format('compression', str)
                raise tornado.web.HTTPError(400, reason=r)
            file_data['compression'] = data['compression']
        if not file_data.valid():
            raise tornado.web.HTTPError(400, reason='invalid file data')

        ret = await self.db.dataset_files.insert_one(dict(file_data))
        self.set_status(201)
        self.write({'result': file_data['task_id']})
        self.finish()

class DatasetTaskFilesHandler(BaseHandler):
    """
    Handle multi files requests, by task.
    """
    @authorization(roles=['admin','system','client','pilot'], attrs=['dataset_id:read'])
    async def get(self, dataset_id, task_id):
        """
        Get dataset_files entries.

        Args:
            dataset_id (str): dataset id
            task_id (str): task_id

        Returns:
            dict: {'files': [<file>]}
        """
        filters = {'dataset_id': dataset_id, 'task_id': task_id}
        projection = {'_id':False, 'dataset_id':False, 'task_id':False}
        ret = []
        async for row in self.db.dataset_files.find(filters, projection=projection):
            ret.append(row)
        self.write({'files': ret})

    @authorization(roles=['admin','system','client'], attrs=['dataset_id:write'])
    async def post(self, dataset_id, task_id):
        """
        Create a dataset_files entry.

        Body should contain the file data.

        Parameters:
            filename (str): the full url filename
            movement (str): [input | output | both]
            local (str): (optional) the local filename the task sees
            transfer (str): whether to transfer the file (can be bool or str)
            compression (str): whether to automatically compress/decompress the file

        Returns:
            dict: {}
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'filename': str,
            'movement': str,
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key {} should be of type {}'.format(k, req_fields[k])
                raise tornado.web.HTTPError(400, reason=r)

        # set some fields
        file_data = dataclasses.Data()
        file_data.update({
            'task_id': task_id,
            'dataset_id': dataset_id,
            'remote': data['filename'],
            'movement': data['movement'],
        })
        if 'local' in data:
            if not isinstance(data['local'], str):
                r = 'key {} should be of type {}'.format('local', str)
                raise tornado.web.HTTPError(400, reason=r)
            file_data['local'] = data['local']
        if 'transfer' in data:
            if not isinstance(data['transfer'], (str,bool)):
                r = 'key {} should be of type {}'.format('transfer', str)
                raise tornado.web.HTTPError(400, reason=r)
            file_data['transfer'] = data['transfer']
        if 'compression' in data:
            if not isinstance(data['compression'], (str,bool)):
                r = 'key {} should be of type {}'.format('compression', str)
                raise tornado.web.HTTPError(400, reason=r)
            file_data['compression'] = data['compression']
        if not file_data.valid():
            raise tornado.web.HTTPError(400, reason='invalid file data')

        ret = await self.db.dataset_files.insert_one(dict(file_data))
        self.set_status(201)
        self.write({})
        self.finish()

    @authorization(roles=['admin','system','client'], attrs=['dataset_id:write'])
    async def delete(self, dataset_id, task_id):
        """
        Delete dataset_files entries.

        Args:
            dataset_id (str): dataset id
            task_id (str): task_id

        Returns:
            dict: {}
        """
        filters = {'dataset_id': dataset_id, 'task_id': task_id}
        await self.db.dataset_files.delete_many(filters)
        self.write({})
