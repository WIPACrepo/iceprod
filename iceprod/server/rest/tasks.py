import logging
import json
import uuid

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
