import logging
import json
import uuid
from collections import defaultdict

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.jobs')

def setup(config):
    """
    Setup method for Jobs REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_auth = config.get('rest',{}).get('jobs',{})
    db_name = cfg_auth.get('database','mongodb://localhost:27017')

    # add indexes
    db = pymongo.MongoClient(db_name).jobs
    if 'job_id_index' not in db.jobs.index_information():
        db.jobs.create_index('job_id', name='job_id_index', unique=True)
    if 'dataset_id_index' not in db.jobs.index_information():
        db.jobs.create_index('dataset_id', name='dataset_id_index', unique=False)

    handler_cfg = RESTHandlerSetup(config)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(db_name).jobs,
    })

    return [
        (r'/jobs', MultiJobsHandler, handler_cfg),
        (r'/jobs/(?P<job_id>\w+)', JobsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/jobs', DatasetMultiJobsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/jobs/(?P<job_id>\w+)', DatasetJobsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/jobs/(?P<job_id>\w+)/status', DatasetJobsStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/job_summaries/status', DatasetJobSummaryStatusHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiJobsHandler(BaseHandler):
    """
    Handle multi jobs requests.
    """
    @authorization(roles=['admin','system'])
    async def post(self):
        """
        Create a job entry.

        Body should contain the job data.

        Returns:
            dict: {'result': <job_id>}
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'dataset_id': str,
            'job_index': int,
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key {} should be of type {}'.format(k, req_fields[k])
                raise tornado.web.HTTPError(400, reason=r)

        # set some fields
        data.update({
            'job_id': uuid.uuid1().hex,
            'status': 'processing',
            'status_changed': nowstr(),
        })

        ret = await self.db.jobs.insert_one(data)
        self.set_status(201)
        self.write({'result': data['job_id']})
        self.finish()

class JobsHandler(BaseHandler):
    """
    Handle single job requests.
    """
    @authorization(roles=['admin','client','system','pilot'])
    async def get(self, job_id):
        """
        Get a job entry.

        Args:
            job_id (str): the job id

        Returns:
            dict: job entry
        """
        ret = await self.db.jobs.find_one({'job_id':job_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Job not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin','client','system'])
    async def patch(self, job_id):
        """
        Update a job entry.

        Body should contain the job data to update.  Note that this will
        perform a merge (not replace).

        Args:
            job_id (str): the job id

        Returns:
            dict: updated job entry
        """
        data = json.loads(self.request.body)
        if not data:
            raise tornado.web.HTTPError(400, reason='Missing update data')

        ret = await self.db.jobs.find_one_and_update({'job_id':job_id},
                {'$set':data},
                projection={'_id':False},
                return_document=pymongo.ReturnDocument.AFTER)
        if not ret:
            self.send_error(404, reason="Job not found")
        else:
            self.write(ret)
            self.finish()

class DatasetMultiJobsHandler(BaseHandler):
    """
    Handle multi jobs requests.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get all jobs for a dataset.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {'uuid': {pilot_data}}
        """
        cursor = self.db.jobs.find({'dataset_id':dataset_id},
                projection={'_id':False})
        ret = {}
        async for row in cursor:
            ret[row['job_id']] = row
        self.write(ret)
        self.finish()

class DatasetJobsHandler(BaseHandler):
    """
    Handle single job requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
    async def get(self, dataset_id, job_id):
        """
        Get a job entry.

        Args:
            dataset_id (str): dataset id
            job_id (str): the job id

        Returns:
            dict: job entry
        """
        ret = await self.db.jobs.find_one({'job_id':job_id,'dataset_id':dataset_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Job not found")
        else:
            self.write(ret)
            self.finish()

class DatasetJobsStatusHandler(BaseHandler):
    """
    Handle single job requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:write'])
    async def put(self, dataset_id, job_id):
        """
        Set a job status.

        Body should have {'status': <new_status>}

        Args:
            dataset_id (str): dataset id
            job_id (str): the job id

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if (not data) or 'status' not in data:
            raise tornado.web.HTTPError(400, reason='Missing status in body')
        if data['status'] not in ('idle','waiting','queued','processing','reset','failed','suspended'):
            raise tornado.web.HTTPError(400, reason='Bad status')
        update_data = {
            'status': data['status'],
            'status_changed': nowstr(),
        }

        ret = await self.db.jobs.update_one({'job_id':job_id,'dataset_id':dataset_id},
                {'$set':update_data})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Job not found")
        else:
            self.write({})
            self.finish()

class DatasetJobSummaryStatusHandler(BaseHandler):
    """
    Handle job summary grouping by status.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get the job summary for all jobs in a dataset, group by status.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<status>: [<job_id>,]}
        """
        cursor = self.db.jobs.find({'dataset_id':dataset_id},
                projection={'_id':False,'status':True,'job_id':True})
        ret = defaultdict(list)
        async for row in cursor:
            ret[row['status']].append(row['job_id'])
        self.write(ret)
        self.finish()
