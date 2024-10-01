import logging
import json
import uuid
from collections import defaultdict

import pymongo
import tornado.web

from ..base_handler import APIBase
from ..auth import authorization, attr_auth
from iceprod.server.util import nowstr
from iceprod.server.states import JOB_STATUS, JOB_STATUS_START, job_prev_statuses, job_status_sort

logger = logging.getLogger('rest.jobs')


def setup(handler_cfg):
    """
    Setup method for Jobs REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            (r'/jobs', MultiJobsHandler, handler_cfg),
            (r'/jobs/(?P<job_id>\w+)', JobsHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/jobs', DatasetMultiJobsHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/jobs/(?P<job_id>\w+)', DatasetJobsHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/jobs/(?P<job_id>\w+)/status', DatasetJobsStatusHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/job_actions/bulk_status/(?P<status>\w+)', DatasetJobBulkStatusHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/job_actions/bulk_suspend', DatasetJobBulkSuspendHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/job_actions/bulk_reset', DatasetJobBulkResetHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/job_actions/bulk_hard_reset', DatasetJobBulkHardResetHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/job_summaries/status', DatasetJobSummariesStatusHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/job_counts/status', DatasetJobCountsStatusHandler, handler_cfg),
        ],
        'database': 'jobs',
        'indexes': {
            'jobs': {
                'job_id_index': {'keys': 'job_id', 'unique': True},
                'dataset_id_index': {'keys': 'dataset_id', 'unique': False},
            }
        }
    }


class MultiJobsHandler(APIBase):
    """
    Handle multi jobs requests.
    """
    @authorization(roles=['admin', 'system'])
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

        opt_fields = {
            'job_id': str,
            'status': str,
        }
        for k in opt_fields:
            if k in data and not isinstance(data[k], opt_fields[k]):
                r = 'key "{}" should be of type {}'.format(k, opt_fields[k].__name__)
                raise tornado.web.HTTPError(400, reason=r)

        bad_fields = set(data).difference(set(opt_fields).union(req_fields))
        if bad_fields:
            r = 'invalid keys found'
            raise tornado.web.HTTPError(400, reason=r)

        if 'job_id' in data:
            try:
                uuid.UUID(hex=data['job_id']).hex
            except Exception:
                raise tornado.web.HTTPError(400, reason='job_id should be a valid uuid')

        if 'status' in data and data['status'] not in JOB_STATUS:
            raise tornado.web.HTTPError(400, reason='unknown status')

        # set some fields
        if 'job_id' not in data:
            data['job_id'] = uuid.uuid1().hex
        if 'status' not in data:
            data['status'] = JOB_STATUS_START
        data['status_changed'] = nowstr()

        await self.db.jobs.insert_one(data)
        self.set_status(201)
        self.write({'result': data['job_id']})
        self.finish()


class JobsHandler(APIBase):
    """
    Handle single job requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, job_id):
        """
        Get a job entry.

        Args:
            job_id (str): the job id

        Returns:
            dict: job entry
        """
        ret = await self.db.jobs.find_one({'job_id':job_id}, projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Job not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin', 'system'])
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

        ret = await self.db.jobs.find_one_and_update(
            {'job_id':job_id},
            {'$set':data},
            projection={'_id':False},
            return_document=pymongo.ReturnDocument.AFTER
        )
        if not ret:
            self.send_error(404, reason="Job not found")
        else:
            self.write(ret)
            self.finish()


class DatasetMultiJobsHandler(APIBase):
    """
    Handle multi jobs requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id):
        """
        Get all jobs for a dataset.

        Params (optional):
            job_index: job_index to filter by
            status: | separated list of job status to filter by
            keys: | separated list of keys to return for each task

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {'job_id':{job_data}}
        """
        filters = {'dataset_id':dataset_id}
        status = self.get_argument('status', None)
        if status:
            status_list = status.split('|')
            if any(s not in JOB_STATUS for s in status_list):
                raise tornado.web.HTTPError(400, reason='Unknown status')
            filters['status'] = {'$in': status_list}

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


class DatasetJobsHandler(APIBase):
    """
    Handle single job requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id, job_id):
        """
        Get a job entry.

        Args:
            dataset_id (str): dataset id
            job_id (str): the job id

        Returns:
            dict: job entry
        """
        ret = await self.db.jobs.find_one(
            {'job_id':job_id,'dataset_id':dataset_id},
            projection={'_id':False}
        )
        if not ret:
            self.send_error(404, reason="Job not found")
        else:
            self.write(ret)
            self.finish()


class DatasetJobsStatusHandler(APIBase):
    """
    Handle single job requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def put(self, dataset_id, job_id):
        """
        Set a job status, following possible state transitions.

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
        if data['status'] not in JOB_STATUS:
            raise tornado.web.HTTPError(400, reason='Bad status')
        update_data = {
            'status': data['status'],
            'status_changed': nowstr(),
        }

        ret = await self.db.jobs.update_one(
            {'job_id': job_id, 'dataset_id': dataset_id, 'status': {'$in': job_prev_statuses(data['status'])}},
            {'$set': update_data}
        )
        if (not ret) or ret.modified_count < 1:
            ret = await self.db.jobs.find_one({'job_id': job_id, 'dataset_id': dataset_id})
            if not ret:
                self.send_error(404, reason="Job not found")
                return
            elif ret['status'] != data['status']:
                self.send_error(400, reason="Bad state transition for status")
                return

        self.write({})
        self.finish()


class DatasetJobBulkStatusHandler(APIBase):
    """
    Update the status of multiple jobs at once.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id, status):
        """
        Set multiple jobs' status, following possible state transitions.

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
        if status not in JOB_STATUS:
            raise tornado.web.HTTPError(400, reason='Bad status')
        query = {
            'dataset_id': dataset_id,
            'job_id': {'$in': jobs},
            'status': {'$in': job_prev_statuses(status)}
        }
        update_data = {
            'status': status,
            'status_changed': nowstr(),
        }

        ret = await self.db.jobs.update_many(query, {'$set': update_data})
        if (not ret) or ret.modified_count < 1:
            self.send_error(404, reason="Jobs not found")
        else:
            self.write({})
            self.finish()


class DatasetJobBulkSuspendHandler(APIBase):
    """
    Suspend jobs in a dataset.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id):
        """
        Do a suspend on jobs.

        Body may have {'jobs': [<job_id>, <job_id>, ...]}
        If it does not, all jobs in a dataset are suspended.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: empty dict
        """
        query = {
            'dataset_id': dataset_id,
            'status': {'$in': job_prev_statuses('suspended')},
        }
        update_data = {
            'status': 'suspended',
            'status_changed': nowstr(),
        }

        if self.request.body:
            data = json.loads(self.request.body)
            if data and 'jobs' in data:
                jobs = list(data['jobs'])
                if len(jobs) > 100000:
                    raise tornado.web.HTTPError(400, reason='Too many jobs specified (limit: 100k)')
                query['job_id'] = {'$in': jobs}

        await self.db.jobs.update_many(query, {'$set': update_data})
        self.write({})
        self.finish()


class DatasetJobBulkResetHandler(APIBase):
    """
    Reset jobs in a dataset.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id):
        """
        Do a reset on jobs.

        Body may have {'jobs': [<job_id>, <job_id>, ...]}
        If it does not, all jobs in a dataset are reset.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: empty dict
        """
        query = {
            'dataset_id': dataset_id,
            'status': {'$in': job_prev_statuses(JOB_STATUS_START)},
        }
        update_data = {
            'status': JOB_STATUS_START,
            'status_changed': nowstr(),
        }

        if self.request.body:
            data = json.loads(self.request.body)
            if data and 'jobs' in data:
                jobs = list(data['jobs'])
                if len(jobs) > 100000:
                    raise tornado.web.HTTPError(400, reason='Too many jobs specified (limit: 100k)')
                query['job_id'] = {'$in': jobs}

        logger.info('bulk reset query: %r', query)
        await self.db.jobs.update_many(query, {'$set': update_data})
        self.write({})
        self.finish()


class DatasetJobBulkHardResetHandler(APIBase):
    """
    Hard reset jobs in a dataset.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id):
        """
        Do a hard reset on jobs.

        Body may have {'jobs': [<job_id>, <job_id>, ...]}
        If it does not, all jobs in a dataset are hard reset.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: empty dict
        """
        query = {
            'dataset_id': dataset_id,
        }
        update_data = {
            'status': JOB_STATUS_START,
            'status_changed': nowstr(),
        }

        if self.request.body:
            data = json.loads(self.request.body)
            if data and 'jobs' in data:
                jobs = list(data['jobs'])
                if len(jobs) > 100000:
                    raise tornado.web.HTTPError(400, reason='Too many jobs specified (limit: 100k)')
                query['job_id'] = {'$in': jobs}

        await self.db.jobs.update_many(query, {'$set': update_data})
        self.write({})
        self.finish()


class DatasetJobSummariesStatusHandler(APIBase):
    """
    Handle job summary grouping by status.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id):
        """
        Get the job summary for all jobs in a dataset, group by status.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<status>: [<job_id>,]}
        """
        cursor = self.db.jobs.find(
            {'dataset_id':dataset_id},
            projection={'_id':False,'status':True,'job_id':True}
        )
        ret = defaultdict(list)
        async for row in cursor:
            ret[row['status']].append(row['job_id'])
        ret2 = {}
        for k in sorted(ret, key=job_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()


class DatasetJobCountsStatusHandler(APIBase):
    """
    Handle job counts by status.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id):
        """
        Get the job counts for all jobs in a dataset, group by status.

        Args:
            dataset_id (str): dataset id

        Returns:
            dict: {<status>: [<job_id>,]}
        """
        cursor = self.db.jobs.aggregate([
            {'$match': {'dataset_id': dataset_id}},
            {'$group': {'_id':'$status', 'total': {'$sum':1}}},
        ])
        ret = {}
        async for row in cursor:
            ret[row['_id']] = row['total']
        ret2 = {}
        for k in sorted(ret, key=job_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()
