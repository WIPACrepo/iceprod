import logging
import json
import uuid

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.pilots')

def setup(config, *args, **kwargs):
    """
    Setup method for Pilots REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('pilots',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).pilots
    if 'pilot_id_index' not in db.pilots.index_information():
        db.pilots.create_index('pilot_id', name='pilot_id_index', unique=True)

    handler_cfg = RESTHandlerSetup(config, *args, **kwargs)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(**db_cfg).pilots,
    })

    return [
        (r'/pilots', MultiPilotsHandler, handler_cfg),
        (r'/pilots/(?P<pilot_id>\w+)', PilotsHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiPilotsHandler(BaseHandler):
    """
    Handle multi pilots requests.
    """
    @authorization(roles=['admin','client','system'])
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

    @authorization(roles=['admin','client'])
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
            'queue_version': str, # iceprod version
            'resources': dict, # min resources requested
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

        ret = await self.db.pilots.insert_one(data)
        self.set_status(201)
        self.write({'result': pilot_id})
        self.finish()

class PilotsHandler(BaseHandler):
    """
    Handle single pilot requests.
    """
    @authorization(roles=['admin','client','pilot'])
    async def get(self, pilot_id):
        """
        Get a pilot entry.

        Args:
            pilot_id (str): the pilot id

        Returns:
            dict: pilot entry
        """
        ret = await self.db.pilots.find_one({'pilot_id':pilot_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Pilot not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin','client','pilot'])
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

        ret = await self.db.pilots.find_one_and_update({'pilot_id':pilot_id},
                {'$set':data},
                projection={'_id':False},
                return_document=pymongo.ReturnDocument.AFTER)
        if not ret:
            self.send_error(404, reason="Pilot not found")
        else:
            if 'site' in ret and ret['site']:
                self.module.statsd.incr('site.{}.pilot'.format(ret['site']))
            self.write(ret)
            self.finish()

    @authorization(roles=['admin','client','pilot'])
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
