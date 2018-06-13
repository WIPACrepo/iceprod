import logging
import json
import uuid

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.task_stats')

def setup(config):
    """
    Setup method for TaskStats REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_auth = config.get('rest',{}).get('task_stats',{})
    db_name = cfg_auth.get('database','mongodb://localhost:27017')

    # add indexes
    db = pymongo.MongoClient(db_name).task_stats
    if 'task_stat_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('task_stat_id', name='task_stat_id_index', unique=True)
    if 'task_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('task_id', name='task_id_index', unique=False)
    if 'dataset_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('dataset_id', name='dataset_id_index', unique=False)

    handler_cfg = RESTHandlerSetup(config)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(db_name).task_stats,
    })

    return [
        (r'/tasks/(?P<task_id>\w+)/task_stats', MultiTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/task_stats', DatasetsMultiTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/task_stats/(?P<task_stat_id>\w+)', DatasetsTaskStatsHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiTaskStatsHandler(BaseHandler):
    """
    Handle multi task_stats requests.
    """
    @authorization(roles=['admin','client','pilot'])
    async def post(self, task_id):
        """
        Create a task_stat entry.

        Body should contain the task_stat data.

        Args:
            task_id (str): the task id for this task_stat

        Returns:
            dict: {'result': <task_stat_id>}
        """
        stat_data = json.loads(self.request.body)
        if 'dataset_id' not in stat_data:
            raise tornado.web.HTTPError(400, reason='Missing dataset_id in body')

        # set some fields
        data = {
            'task_stat_id': uuid.uuid1().hex,
            'task_id': task_id,
            'dataset_id': stat_data['dataset_id'],
            'create_date': nowstr(),
            'stats': stat_data,
        }

        ret = await self.db.task_stats.insert_one(data)
        self.set_status(201)
        self.write({'result': data['task_stat_id']})
        self.finish()

class DatasetsMultiTaskStatsHandler(BaseHandler):
    """
    Handle multi task_stats requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
    async def get(self, dataset_id, task_id):
        """
        Get all task_stats for a dataset and task.

        Args:
            dataset_id (str): dataset id
            task_id (str): task id

        Returns:
            dict: {<task_stat_id>: stats}
        """
        ret = await self.db.task_stats.find({'dataset_id':dataset_id,'task_id':task_id},
                projection={'_id':False}).to_list(10000)
        self.write({row['task_stat_id']:row for row in ret})
        self.finish()

class DatasetsTaskStatsHandler(BaseHandler):
    """
    Handle single task_stat requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
    async def get(self, dataset_id, task_id, task_stat_id):
        """
        Get a task_stat entry.

        Args:
            dataset_id (str): dataset id
            task_id (str): task id
            task_stat_id (str): the task_stat id

        Returns:
            dict: task_stat entry
        """
        ret = await self.db.task_stats.find_one({'dataset_id':dataset_id,'task_id':task_id,'task_stat_id':task_stat_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Task stat not found")
        else:
            self.write(ret)
            self.finish()
