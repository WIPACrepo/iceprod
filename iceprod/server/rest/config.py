import logging
import json
import uuid

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization, catch_error

logger = logging.getLogger('rest.config')

def setup(config, *args, **kwargs):
    """
    Setup method for Config REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for config, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('config',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).config
    if 'dataset_id_index' not in db.config.index_information():
        db.config.create_index('dataset_id', name='dataset_id_index', unique=True)

    handler_cfg = RESTHandlerSetup(config, *args, **kwargs)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(**db_cfg).config,
    })

    return [
        (r'/config/(?P<dataset_id>\w+)', ConfigHandler, handler_cfg),
    ]

class ConfigHandler(RESTHandler):
    """
    Handle config requests.
    """
    def initialize(self, database=None, **kwargs):
        super(ConfigHandler, self).initialize(**kwargs)
        self.db = database

    @authorization(roles=['admin','client','system','pilot'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get a config.

        Args:
            dataset_id (str): the dataset id of the config

        Returns:
            dict: config
        """
        ret = await self.db.config.find_one({'dataset_id':dataset_id},
                projection={'_id':False, 'dataset_id':False})
        if not ret:
            self.send_error(404, reason="Config not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin'], attrs=['dataset_id:write'])
    async def put(self, dataset_id):
        """
        Set a config.

        Body should contain the config.

        Args:
            dataset_id (str): the dataset id of the config

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'dataset_id' not in data:
            data['dataset_id'] = dataset_id
        elif data['dataset_id'] != dataset_id:
            raise tornado.web.HTTPError(400, reason='dataset_id mismatch')
        ret = await self.db.config.replace_one({'dataset_id':dataset_id},
                data, upsert=True)
        self.write({})
        self.finish()
