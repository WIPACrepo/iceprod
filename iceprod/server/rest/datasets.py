import logging
import json
import uuid
from collections import defaultdict

import tornado.web
import tornado.httpclient
from tornado.platform.asyncio import to_asyncio_future
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr, status_sort

logger = logging.getLogger('rest.datasets')

def setup(config, *args, **kwargs):
    """
    Setup method for Dataset REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for dataset, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('datasets',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).datasets
    if 'dataset_id_index' not in db.datasets.index_information():
        db.datasets.create_index('dataset_id', name='dataset_id_index', unique=True)

    handler_cfg = RESTHandlerSetup(config, *args, **kwargs)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(**db_cfg).datasets,
    })

    return [
        (r'/datasets', MultiDatasetHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)', DatasetHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/description', DatasetDescriptionHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/status', DatasetStatusHandler, handler_cfg),
        (r'/dataset_summaries/status', DatasetSummariesStatusHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    """
    Base handler for Dataset REST API. 
    """
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiDatasetHandler(BaseHandler):
    """
    Handle multi-group requests.
    """
    @authorization(roles=['admin','user']) #TODO: figure out how to do auth for each dataset in the list
    async def get(self):
        """
        Get a dict of datasets.

        Params (optional):
            keys: | separated list of keys to return for each dataset

        Returns:
            dict: {<dataset_id>: metadata}
        """
        projection = {'_id': False}
        keys = self.get_argument('keys', None)
        if keys:
            projection.update({x:True for x in keys.split('|') if x})

        ret = {}
        async for row in self.db.datasets.find(projection=projection):
            k = row['dataset_id']
            ret[k] = row
        self.write(ret)
        self.finish()

    @authorization(roles=['admin','user']) # anyone should be able to create a dataset
    async def post(self):
        """
        Add a dataset.

        Body should contain all necessary fields for a dataset.
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'description': str,
            'jobs_submitted': int,
            'tasks_submitted': int,
            'group': str
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key "{}" should be of type {}'.format(k, req_fields[k].__name__)
                raise tornado.web.HTTPError(400, reason=r)

        # generate dataset number
        ret = await self.db.settings.find_one_and_update(
            {'name': 'dataset_num'},
            {'$inc': {'num': 1}},
            projection={'num': True, '_id': False},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER)
        dataset_num = ret['num']

        # set some fields
        data['dataset_id'] = uuid.uuid1().hex
        data['dataset'] = dataset_num
        data['status'] = 'processing'
        data['start_date'] = nowstr()
        data['username'] = self.auth_data['username']
        if 'priority' not in data:
            data['priority'] = 0
        if 'debug' not in data:
            data['debug'] = False

        # insert
        ret = await self.db.datasets.insert_one(data)

        # set auth rules
        url = self.auth_url+'/auths/'+data['dataset_id']
        http_client = tornado.httpclient.AsyncHTTPClient()
        auth_data = {
            'read_groups':[data['group']],
            'write_groups':[data['group']],
        }
        logger.info('Authorization header: %s', 'bearer '+self.module_auth_key)
        await to_asyncio_future(http_client.fetch(url,
                method='PUT', body=json.dumps(auth_data),
                headers={'Authorization': 'bearer '+self.module_auth_key}))

        # return success
        self.set_status(201)
        self.set_header('Location', '/datasets/'+data['dataset_id'])
        self.write({'result': '/datasets/'+data['dataset_id']})
        self.finish()

class DatasetHandler(BaseHandler):
    """
    Handle dataset requests.
    """
    @authorization(roles=['admin','client','system','pilot'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get a dataset.

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: dataset metadata
        """
        ret = await self.db.datasets.find_one({'dataset_id':dataset_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write(ret)
            self.finish()

class DatasetDescriptionHandler(BaseHandler):
    """
    Handle dataset description updates.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:write'])
    async def put(self, dataset_id):
        """
        Set a dataset description.

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'description' not in data:
            raise tornado.web.HTTPError(400, reason='missing description')
        elif not isinstance(data['description'],str):
            raise tornado.web.HTTPError(400, reason='bad description')

        ret = await self.db.datasets.find_one_and_update({'dataset_id':dataset_id},
                {'$set':{'description': data['description']}},
                projection=['_id'])
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write({})
            self.finish()

class DatasetStatusHandler(BaseHandler):
    """
    Handle dataset status updates.
    """
    @authorization(roles=['admin','system','client'], attrs=['dataset_id:write'])
    async def put(self, dataset_id):
        """
        Set a dataset status.

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'status' not in data:
            raise tornado.web.HTTPError(400, reason='missing status')
        elif data['status'] not in ('processing','suspended','errors','truncated','complete'):
            raise tornado.web.HTTPError(400, reason='bad status')

        ret = await self.db.datasets.find_one_and_update({'dataset_id':dataset_id},
                {'$set':{'status': data['status']}},
                projection=['_id'])
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write({})
            self.finish()

class DatasetSummariesStatusHandler(BaseHandler):
    """
    Handle dataset summary grouping by status.
    """
    @authorization(roles=['admin','system','client','user']) #TODO: figure out how to do auth for each dataset in the list
    async def get(self):
        """
        Get the dataset summary for all datasets, group by status.

        Returns:
            dict: {<status>: [<dataset_id>,]}
        """
        cursor = self.db.datasets.find(
                projection={'_id':False,'status':True,'dataset_id':True})
        ret = defaultdict(list)
        async for row in cursor:
            ret[row['status']].append(row['dataset_id'])
        ret2 = {}
        for k in sorted(ret, key=status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()
