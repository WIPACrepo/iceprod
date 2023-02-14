import logging
import json
import uuid

import pymongo
import tornado.web

from ..base_handler import APIBase
from ..auth import authorization
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.pilots')


def setup(handler_cfg):
    """
    Setup method for Pilots REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            (r'/pilots', MultiPilotsHandler, handler_cfg),
            (r'/pilots/(?P<pilot_id>\w+)', PilotsHandler, handler_cfg),
        ],
        'database': 'pilots',
        'indexes': {
            'jobs': {
                'pilot_id_index': {'keys': 'pilot_id', 'unique': True},
            }
        }
    }


class MultiPilotsHandler(APIBase):
    """
    Handle multi pilots requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self):
        """
        Get pilot entries.

        Params (optional):
            queue_host: queue_host to filter by
            queue_version: queue_version to filter by
            host: host to filter by
            version: version to filter by
            keys: | separated list of keys to return for each pilot

        Returns:
            dict: {'uuid': {pilot_data}}
        """
        filters = {}
        for k in ('queue_host','queue_version','host','version'):
            tmp = self.get_argument(k, None)
            if tmp:
                filters[k] = tmp

        projection = {'_id': False}
        keys = self.get_argument('keys','')
        if keys:
            projection.update({x:True for x in keys.split('|') if x})
            projection['pilot_id'] = True

        ret = {}
        async for row in self.db.pilots.find(filters,projection=projection):
            ret[row['pilot_id']] = row
        self.write(ret)

    @authorization(roles=['admin', 'system'])
    async def post(self):
        """
        Create a pilot entry.

        Body should contain the pilot data.

        Returns:
            dict: {'result': <pilot_id>}
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'queue_host': str,
            'queue_version': str,  # iceprod version
            'resources': dict,  # min resources requested
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key {} should be of type {}'.format(k, req_fields[k])
                raise tornado.web.HTTPError(400, reason=r)

        # set some fields
        pilot_id = uuid.uuid1().hex
        data['pilot_id'] = pilot_id
        data['submit_date'] = nowstr()
        data['start_date'] = ''
        data['last_update'] = data['submit_date']
        if 'tasks' not in data:
            data['tasks'] = []
        if 'host' not in data:
            data['host'] = ''
        if 'site' not in data:
            data['site'] = ''
        if 'version' not in data:
            data['version'] = ''
        if 'grid_queue_id' not in data:
            data['grid_queue_id'] = ''
        if 'resources_available' not in data:
            data['resources_available'] = {}
        if 'resources_claimed' not in data:
            data['resources_claimed'] = {}

        await self.db.pilots.insert_one(data)
        self.set_status(201)
        self.write({'result': pilot_id})
        self.finish()


class PilotsHandler(APIBase):
    """
    Handle single pilot requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, pilot_id):
        """
        Get a pilot entry.

        Args:
            pilot_id (str): the pilot id

        Returns:
            dict: pilot entry
        """
        ret = await self.db.pilots.find_one({'pilot_id':pilot_id}, projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Pilot not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin', 'system'])
    async def patch(self, pilot_id):
        """
        Update a pilot entry.

        Body should contain the pilot data to update.  Note that this will
        perform a merge (not replace).

        Args:
            pilot_id (str): the pilot id

        Returns:
            dict: updated pilot entry
        """
        data = json.loads(self.request.body)
        if not data:
            raise tornado.web.HTTPError(400, reason='Missing update data')
        data['last_update'] = nowstr()

        ret = await self.db.pilots.find_one_and_update(
            {'pilot_id':pilot_id},
            {'$set':data},
            projection={'_id':False},
            return_document=pymongo.ReturnDocument.AFTER
        )
        if not ret:
            self.send_error(404, reason="Pilot not found")
        else:
            if 'site' in ret and ret['site']:
                self.module.statsd.incr('site.{}.pilot'.format(ret['site']))
            self.write(ret)
            self.finish()

    @authorization(roles=['admin', 'system'])
    async def delete(self, pilot_id):
        """
        Delete a pilot entry.

        Args:
            pilot_id (str): the pilot id

        Returns:
            dict: empty dict
        """
        ret = await self.db.pilots.find_one_and_delete({'pilot_id':pilot_id})
        if not ret:
            self.send_error(404, reason="Pilot not found")
        else:
            if 'site' in ret and ret['site']:
                self.module.statsd.incr('site.{}.pilot_delete'.format(ret['site']))
            self.write({})
