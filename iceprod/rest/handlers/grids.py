import logging
import json
import uuid

import pymongo
import tornado.web

from ..base_handler import APIBase
from ..auth import authorization
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.grids')


def setup(handler_cfg):
    """
    Setup method for Grids REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            (r'/grids', MultiGridsHandler, handler_cfg),
            (r'/grids/(?P<grid_id>\w+)', GridsHandler, handler_cfg),
        ],
        'database': 'grids',
        'indexes': {
            'grids': {
                'grid_id_index': {'keys': 'grid_id', 'unique': True},
            }
        }
    }


class MultiGridsHandler(APIBase):
    """
    Handle multi grids requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self):
        """
        Get grid entries.

        Returns:
            dict: {'uuid': {grid_data}}
        """
        ret = await self.db.grids.find(projection={'_id':False}).to_list(1000)
        self.write({row['grid_id']:row for row in ret})
        self.finish()

    @authorization(roles=['admin', 'system'])
    async def post(self):
        """
        Create a grid entry.

        Body should contain the grid data.

        Returns:
            dict: {'result': <grid_id>}
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'host': str,
            'queues': dict,  # dict of {name: type}
            'version': str,  # iceprod version
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key {} should be of type {}'.format(k, req_fields[k])
                raise tornado.web.HTTPError(400, reason=r)

        # set some fields
        grid_id = uuid.uuid1().hex
        data['grid_id'] = grid_id
        data['start_date'] = nowstr()
        data['last_update'] = data['start_date']
        if 'debug' not in data:
            data['debug'] = False

        await self.db.grids.insert_one(data)
        self.set_status(201)
        self.write({'result': grid_id})
        self.finish()


class GridsHandler(APIBase):
    """
    Handle single grid requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, grid_id):
        """
        Get a grid entry.

        Args:
            grid_id (str): the grid id

        Returns:
            dict: grid entry
        """
        ret = await self.db.grids.find_one({'grid_id':grid_id}, projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Grid not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin', 'system'])
    async def patch(self, grid_id):
        """
        Update a grid entry.

        Body should contain the grid data to update.  Note that this will
        perform a merge (not replace).

        Args:
            grid_id (str): the grid id

        Returns:
            dict: updated grid entry
        """
        data = json.loads(self.request.body)
        if not data:
            raise tornado.web.HTTPError(400, reason='Missing update data')
        data['last_update'] = nowstr()

        ret = await self.db.grids.find_one_and_update(
            {'grid_id':grid_id},
            {'$set':data},
            projection={'_id':False},
            return_document=pymongo.ReturnDocument.AFTER
        )
        if not ret:
            self.send_error(404, reason="Grid not found")
        else:
            self.write(ret)
            self.finish()
