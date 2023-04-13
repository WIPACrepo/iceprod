import logging
import json

import tornado.web

from ..base_handler import APIBase
from ..auth import authorization, attr_auth

logger = logging.getLogger('rest.config')


def setup(handler_cfg):
    """
    Setup method for Config REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            (r'/config/(?P<dataset_id>\w+)', ConfigHandler, handler_cfg),
        ],
        'database': 'config',
        'indexes': {
            'config': {
                'dataset_id_index': {'keys': 'dataset_id', 'unique': True},
            }
        }
    }


class ConfigHandler(APIBase):
    """
    Handle config requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id):
        """
        Get a config.

        Args:
            dataset_id (str): the dataset id of the config

        Returns:
            dict: config
        """
        ret = await self.db.config.find_one(
            {'dataset_id':dataset_id},
            projection={'_id':False, 'dataset_id':False}
        )
        if not ret:
            self.send_error(404, reason="Config not found")
        else:
            self.write(ret)

    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
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
        await self.db.config.replace_one({'dataset_id':dataset_id}, data, upsert=True)
        self.write({})
