import logging
import json
import uuid
from collections import defaultdict

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.tasks')

def setup(config):
    """
    Setup method for Tasks REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_auth = config.get('rest',{}).get('tasks',{})
    db_name = cfg_auth.get('database','mongodb://localhost:27017')

    # add indexes
    db = pymongo.MongoClient(db_name).tasks
    if 'task_id_index' not in db.tasks.index_information():
        db.tasks.create_index('task_id', name='task_id_index', unique=True)
    if 'dataset_id_index' not in db.tasks.index_information():
        db.tasks.create_index('dataset_id', name='dataset_id_index', unique=False)

    handler_cfg = RESTHandlerSetup(config)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(db_name).tasks,
    })

    return [
        (r'/tasks', MultiTasksHandler, handler_cfg),
        (r'/tasks/(?P<task_id>\w+)', TasksHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks', DatasetMultiTasksHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)', DatasetTasksHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/status', DatasetTasksStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/task_summaries/status', DatasetTaskSummaryStatusHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiTasksHandler(BaseHandler):
    """
    Handle multi tasks requests.
    """
    @authorization(roles=['admin','system'])
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
            'task_index': int,
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
        data.update({
            'task_id': uuid.uuid1().hex,
            'status': 'idle',
            'status_changed': nowstr(),
            'failures': 0,
            'evictions': 0,
            'walltime': 0.0,
            'walltime_err': 0.0,
            'walltime_err_n': 0,
        })

        ret = await self.db.tasks.insert_one(data)
        self.set_status(201)
        self.write({'result': data['task_id']})
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

    @authorization(roles=['admin','client','system'])
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

class DatasetMultiTasksHandler(BaseHandler):
    """
    Handle multi tasks requests.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get all tasks for a dataset.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {'uuid': {pilot_data}}
        """
        cursor = self.db.tasks.find({'dataset_id':dataset_id},
                projection={'_id':False})
        ret = {}
        async for row in cursor:
            ret[row['task_id']] = row
        self.write(ret)
        self.finish()

class DatasetTasksHandler(BaseHandler):
    """
    Handle single task requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
    async def get(self, dataset_id, task_id):
        """
        Get a task entry.

        Args:
            dataset_id (str): dataset id
            task_id (str): the task id

        Returns:
            dict: task entry
        """
        ret = await self.db.tasks.find_one({'task_id':task_id,'dataset_id':dataset_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Task not found")
        else:
            self.write(ret)
            self.finish()

class DatasetTasksStatusHandler(BaseHandler):
    """
    Handle single task requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:write'])
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
        if data['status'] not in ('idle','waiting','queued','processing','reset','failed','suspended'):
            raise tornado.web.HTTPError(400, reason='Bad status')
        update_data = {
            'status': data['status'],
            'status_changed': nowstr(),
        }

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
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
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
        self.write(ret)
        self.finish()
