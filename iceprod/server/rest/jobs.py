import logging
import json
import uuid
from collections import defaultdict

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr, job_statuses, job_status_sort

logger = logging.getLogger('rest.jobs')

def setup(config, *args, **kwargs):
    """
    Setup method for Jobs REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('jobs',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).jobs
    if 'job_id_index' not in db.jobs.index_information():
        db.jobs.create_index('job_id', name='job_id_index', unique=True)
    if 'dataset_id_index' not in db.jobs.index_information():
        db.jobs.create_index('dataset_id', name='dataset_id_index', unique=False)

    handler_cfg = RESTHandlerSetup(config, *args, **kwargs)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(**db_cfg).jobs,
    })

    return [
        (r'/jobs', MultiJobsHandler, handler_cfg),
        (r'/jobs/(?P<job_id>\w+)', JobsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/jobs', DatasetMultiJobsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/jobs/(?P<job_id>\w+)', DatasetJobsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/jobs/(?P<job_id>\w+)/status', DatasetJobsStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/job_actions/bulk_status/(?P<status>\w+)', DatasetJobBulkStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/job_summaries/status', DatasetJobSummariesStatusHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/job_counts/status', DatasetJobCountsStatusHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiJobsHandler(BaseHandler):
    """
    Handle multi jobs requests.
    """
    @authorization(roles=['admin','system','client'])
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

        # validate job_id if given
        if 'job_id' in data:
            try:
                job_id = uuid.UUID(hex=data['job_id']).hex
            except Exception:
                raise tornado.web.HTTPError(400, reason='job_id should be a valid uuid')
        else:
            job_id = uuid.uuid1().hex

        # set some fields
        data.update({
            'job_id': job_id,
            'status': 'processing',
            'status_changed': nowstr(),
        })

        ret = await self.db.jobs.insert_one(data)
        self.set_status(201)
        self.write({'result': job_id})
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

        Params (optional):
            job_index: job_index to filter by
            status: | separated list of task status to filter by
            keys: | separated list of keys to return for each task

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {'job_id':{job_data}}
        """
        filters = {'dataset_id':dataset_id}
        status = self.get_argument('status', None)
        if status:
            filters['status'] = {'$in': status.split('|')}

        job_index = self.get_argument('job_index', None)
        if job_index:
            try:
                filters['job_index'] = int(job_index)
            except ValueError:
                raise tornado.web.HTTPError(400, reason='Bad argument "job_index": must be integer')

        projection = {'_id': False}
        keys = self.get_argument('keys','')
        if keys:
            projection.update({x:True for x in keys.split('|') if x})
            projection['job_id'] = True

        cursor = self.db.jobs.find(filters, projection=projection)
        ret = {}
        async for row in cursor:
            ret[row['job_id']] = row
        self.write(ret)
        self.finish()

class DatasetJobsHandler(BaseHandler):
    """
    Handle single job requests.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
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
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:write'])
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
        if data['status'] not in job_statuses:
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
            self.finish

class DatasetJobBulkStatusHandler(BaseHandler):
    """
    Update the status of multiple jobs at once.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:write'])
    async def post(self, dataset_id, status):
        """
        Set multiple jobs' status.

        Body should have {'jobs': [<job_id>, <job_id>, ...]}

        Args:
            dataset_id (str): dataset id
            status (str): the status

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if (not data) or 'jobs' not in data or not data['jobs']:
            raise tornado.web.HTTPError(400, reason='Missing jobs in body')
        jobs = list(data['jobs'])
        if len(jobs) > 100000:
            raise tornado.web.HTTPError(400, reason='Too many jobs specified (limit: 100k)')
        if status not in job_statuses:
            raise tornado.web.HTTPError(400, reason='Bad status')
        query = {
            'dataset_id': dataset_id,
            'job_id': {'$in': jobs},
        }
        update_data = {
            'status': status,
            'status_changed': nowstr(),
        }

        ret = await self.db.jobs.update_many(query, {'$set':update_data})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Jobs not found")
        else:
            self.write({})
            self.finish()

class DatasetJobSummariesStatusHandler(BaseHandler):
    """
    Handle job summary grouping by status.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
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
        ret2 = {}
        for k in sorted(ret, key=job_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()

class DatasetJobCountsStatusHandler(BaseHandler):
    """
    Handle job counts by status.
    """
    @authorization(roles=['admin','client','system'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get the job counts for all jobs in a dataset, group by status.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<status>: [<job_id>,]}
        """
        cursor = self.db.jobs.aggregate([
            {'$match':{'dataset_id':dataset_id}},
            {'$group':{'_id':'$status', 'total': {'$sum':1}}},
        ])
        ret = {}
        async for row in cursor:
            ret[row['_id']] = row['total']
        ret2 = {}
        for k in sorted(ret, key=job_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()
