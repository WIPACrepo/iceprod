import logging
import json
import uuid

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, IceProdRestConfig, authorization, lookup_auth
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.tokens')


def setup(config, **kwargs):
    """
    Setup method for Tokens REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('task_stats',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).task_stats
    if 'task_stat_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('task_stat_id', name='task_stat_id_index', unique=True)
    if 'task_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('task_id', name='task_id_index', unique=False)
    if 'dataset_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('dataset_id', name='dataset_id_index', unique=False)

    database = motor.motor_tornado.MotorClient(**db_cfg).task_stats
    handler_cfg = IceProdRestConfig(config, database=database, **kwargs)

    return [
        (r'/tasks/(?P<task_id>\w+)/task_stats', MultiTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/task_stats', DatasetsMultiTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/bulk/task_stats', DatasetsBulkTaskStatsHandler, handler_cfg),
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

        Body should contain the task stat data.

        Args:
            task_id (str): the task id for this task_stat

        Returns:
            dict: {'result': <task_stat_id>}
        """
        stat_data = json.loads(self.request.body)
        if 'dataset_id' not in stat_data:
            raise tornado.web.HTTPError(400, reason='Missing dataset_id in body')

        # set some fields
        task_stat_id = uuid.uuid1().hex
        data = {
            'task_stat_id': task_stat_id,
            'task_id': task_id,
            'dataset_id': stat_data['dataset_id'],
            'create_date': nowstr(),
            'stats': stat_data,
        }

        ret = await self.db.task_stats.insert_one(data)
        self.set_status(201)
        self.write({'result': task_stat_id})
        self.finish()
