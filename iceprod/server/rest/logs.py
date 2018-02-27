import logging
import json
import uuid

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization, catch_error

logger = logging.getLogger('rest.logs')

def setup(config):
    """
    Setup method for Logs REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_auth = config.get('rest',{}).get('logs',{})
    db_name = cfg_auth.get('database','mongodb://localhost:27017')

    # add indexes
    db = pymongo.MongoClient(db_name).logs
    if 'log_id_index' not in db.logs.index_information():
        db.logs.create_index('log_id', name='log_id_index', unique=True)

    handler_cfg = RESTHandlerSetup(config)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(db_name).logs,
    })

    return [
        (r'/logs', MultiLogsHandler, handler_cfg),
        (r'/logs/(?P<log_id>\w+)', LogsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/logs', DatasetMultiLogsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/logs/(?P<log_id>\w+)', DatasetLogsHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiLogsHandler(BaseHandler):
    """
    Handle logs requests.
    """
    @authorization(roles=['admin','pilot'])
    async def post(self):
        """
        Create a log entry.

        Body should contain the log message.

        Returns:
            dict: {'result': <log_id>}
        """
        data = {
            'log_id': uuid.uuid1().hex,
            'data': self.request.body,
        }
        ret = await self.db.logs.insert_one(data)
        self.set_status(201)
        self.write({'result': data['log_id']})
        self.finish()

class LogsHandler(BaseHandler):
    """
    Handle logs requests.
    """
    @authorization(roles=['admin'])
    async def get(self, log_id):
        """
        Get a log entry.

        Args:
            log_id (str): the log id of the entry

        Returns:
            str: log entry
        """
        ret = await self.db.logs.find_one({'log_id':log_id},
                projection={'_id':False, 'data':True})
        if not ret:
            self.send_error(404, reason="Log not found")
        else:
            self.write(ret['data'])
            self.finish()

class DatasetMultiLogsHandler(BaseHandler):
    """
    Handle logs requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:write'])
    async def post(self, dataset_id):
        """
        Create a log entry.

        Body should contain the log message.

        Args:
            dataset_id (str): the dataset id of the config

        Returns:
            dict: {'result': <log_id>}
        """
        data = {
            'log_id': uuid.uuid1().hex,
            'data': self.request.body,
        }
        ret = await self.db.logs.insert_one(data)
        self.set_status(201)
        self.write({'result': data['log_id']})
        self.finish()

class DatasetLogsHandler(BaseHandler):
    """
    Handle logs requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
    async def get(self, dataset_id, log_id):
        """
        Get a config.

        Args:
            dataset_id (str): the dataset id of the config
            log_id (str): the log id of the entry

        Returns:
            dict: config
        """
        ret = await self.db.logs.find_one({'log_id':log_id},
                projection={'_id':False, 'data':True})
        if not ret:
            self.send_error(404, reason="Log not found")
        else:
            self.write(ret['data'])
            self.finish()
