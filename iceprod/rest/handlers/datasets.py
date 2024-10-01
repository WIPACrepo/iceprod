import logging
import json
import uuid
from collections import defaultdict

import pymongo
import tornado.web

from ..base_handler import APIBase
from ..auth import authorization, attr_auth
from iceprod.server.util import nowstr
from iceprod.server.states import DATASET_STATUS, DATASET_STATUS_START, dataset_prev_statuses, dataset_status_sort

logger = logging.getLogger('rest.datasets')


def setup(handler_cfg):
    """
    Setup method for Dataset REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            (r'/datasets', MultiDatasetHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)', DatasetHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/description', DatasetDescriptionHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/status', DatasetStatusHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/priority', DatasetPriorityHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/jobs_submitted', DatasetJobsSubmittedHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/dataset_actions/hard_reset', DatasetHardResetHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/dataset_actions/truncate', DatasetTruncateHandler, handler_cfg),
            (r'/dataset_summaries/status', DatasetSummariesStatusHandler, handler_cfg),
        ],
        'database': 'datasets',
        'indexes': {
            'datasets': {
                'dataset_id_index': {'keys': 'dataset_id', 'unique': True},
            }
        },
    }


class MultiDatasetHandler(APIBase):
    """
    Handle multi-dataset requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    async def get(self):
        """
        Get a dict of datasets.

        Params (optional):
            status: | separated list of status filters
            groups: | separated list of groups to filter on
            users: | separated list of users to filter on
            keys: | separated list of keys to return for each dataset

        Returns:
            dict: {<dataset_id>: metadata}
        """
        query = {}
        status = self.get_argument('status', None)
        if status:
            status_list = status.split('|')
            if any(s not in DATASET_STATUS for s in status_list):
                raise tornado.web.HTTPError(400, reason='unknown status')
            query['status'] = {'$in': status_list}
        groups = self.get_argument('groups', None)
        if groups:
            query['group'] = {'$in': groups.split('|')}
        users = self.get_argument('users', None)
        if users:
            query['username'] = {'$in': users.split('|')}

        projection = {'_id': False}
        keys = self.get_argument('keys', None)
        if keys:
            projection.update({x:True for x in keys.split('|') if x})
        projection['dataset_id'] = True  # must be in projection

        ret = {}
        async for row in self.db.datasets.find(query, projection=projection):
            k = row['dataset_id']
            if await self.manual_attr_auth('dataset_id', k, 'read'):
                ret[k] = row
        self.write(ret)
        self.finish()

    @authorization(roles=['admin', 'user'])
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
            'tasks_per_job': int,
            'group': str,
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key "{}" should be of type {}'.format(k, req_fields[k].__name__)
                raise tornado.web.HTTPError(400, reason=r)

        opt_fields = {
            'priority': float,
            'debug': bool,
            'jobs_immutable': bool,
            'status': str,
            'auth_groups_read': list,
        }
        for k in opt_fields:
            if k in data and not isinstance(data[k], opt_fields[k]):
                r = 'key "{}" should be of type {}'.format(k, opt_fields[k].__name__)
                raise tornado.web.HTTPError(400, reason=r)

        bad_fields = set(data).difference(set(opt_fields).union(req_fields))
        if bad_fields:
            r = 'invalid keys found'
            raise tornado.web.HTTPError(400, reason=r)

        read_groups = data.pop('auth_groups_read') if 'auth_groups_read' in data else ['users']

        if data['jobs_submitted'] == 0 and data['tasks_per_job'] <= 0:
            r = '"tasks_per_job" must be > 0'
            raise tornado.web.HTTPError(400, reason=r)
        elif data['tasks_submitted'] != 0 and data['tasks_submitted'] / data['jobs_submitted'] != data['tasks_per_job']:
            r = '"tasks_per_job" does not match "tasks_submitted"/"jobs_submitted"'
            raise tornado.web.HTTPError(400, reason=r)

        if 'status' in data and data['status'] not in DATASET_STATUS:
            raise tornado.web.HTTPError(400, reason='unknown status')

        # generate dataset number
        ret = await self.db.settings.find_one_and_update(
            {'name': 'dataset_num'},
            {'$inc': {'num': 1}},
            projection={'num': True, '_id': False},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER)
        dataset_num = ret['num']

        # set some fields
        dataset_id = uuid.uuid1().hex
        data['dataset_id'] = dataset_id
        data['dataset'] = dataset_num
        if 'status' not in data:
            data['status'] = DATASET_STATUS_START
        data['start_date'] = nowstr()
        data['username'] = self.current_user
        if 'priority' not in data:
            data['priority'] = 0.5
        if 'debug' not in data:
            data['debug'] = False
        if 'jobs_immutable' not in data:
            data['jobs_immutable'] = False
        data['truncated'] = False

        # insert
        ret = await self.db.datasets.insert_one(data)

        # set auth rules
        write_groups = list({'admin', data['group']}) if data['group'] != 'users' else ['admin']
        await self.set_attr_auth(
            'dataset_id',
            data['dataset_id'],
            read_groups=list({'admin', data['group']} | set(read_groups)),
            write_groups=write_groups,
            read_users=[data['username']],
            write_users=[data['username']],
        )

        # make sure user prio is set
        try:
            await self.add_user(self.current_user)
        except pymongo.errors.DuplicateKeyError:
            # ignore already added users
            pass

        # return success
        self.set_status(201)
        self.set_header('Location', f'/datasets/{dataset_id}')
        self.write({'result': f'/datasets/{dataset_id}'})
        self.finish()


class DatasetHandler(APIBase):
    """
    Handle dataset requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id):
        """
        Get a dataset.

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: dataset metadata
        """
        ret = await self.db.datasets.find_one({'dataset_id':dataset_id}, projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write(ret)
            self.finish()


class DatasetDescriptionHandler(APIBase):
    """
    Handle dataset description updates.
    """
    @authorization(roles=['admin', 'user'])
    @attr_auth(arg='dataset_id', role='write')
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

        ret = await self.db.datasets.find_one_and_update(
            {'dataset_id':dataset_id},
            {'$set':{'description': data['description']}},
            projection=['_id']
        )
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write({})
            self.finish()


class DatasetStatusHandler(APIBase):
    """
    Handle dataset status updates.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def put(self, dataset_id):
        """
        Set a dataset status, following possible state transitions.

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'status' not in data:
            raise tornado.web.HTTPError(400, reason='missing status')
        elif data['status'] not in DATASET_STATUS:
            raise tornado.web.HTTPError(400, reason='bad status')

        logging.debug('%r %r', dataset_prev_statuses, data['status'])
        prev_statuses = dataset_prev_statuses(data['status'])
        logging.debug('prev_statuses: %r', prev_statuses)
        ret = await self.db.datasets.find_one_and_update(
            {'dataset_id': dataset_id, 'status': {'$in': prev_statuses}},
            {'$set': {'status': data['status']}},
            projection=['_id']
        )
        if not ret:
            ret = await self.db.datasets.find_one({'dataset_id': dataset_id})
            if not ret:
                self.send_error(404, reason="Dataset not found")
                return
            elif ret['status'] != data['status']:
                self.send_error(400, reason="Bad state transition for status")
                return

        self.write({})
        self.finish()


class DatasetPriorityHandler(APIBase):
    """
    Handle dataset priority updates.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def put(self, dataset_id):
        """
        Set a dataset priority.

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'priority' not in data:
            raise tornado.web.HTTPError(400, reason='missing priority')
        elif not isinstance(data['priority'], (int, float)):
            raise tornado.web.HTTPError(400, reason='priority is not a number')

        ret = await self.db.datasets.find_one_and_update(
            {'dataset_id':dataset_id},
            {'$set':{'priority': data['priority']}},
            projection=['_id']
        )
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write({})
            self.finish()


class DatasetJobsSubmittedHandler(APIBase):
    """
    Handle dataset jobs_submitted updates.
    """
    @authorization(roles=['admin', 'user'])
    @attr_auth(arg='dataset_id', role='write')
    async def put(self, dataset_id):
        """
        Set a dataset's jobs_submitted.

        Only allows increases, if the jobs_immutable flag is not set.

        Args:
            dataset_id (str): the dataset

        Json body:
            jobs_submitted (int): the number of jobs submitted

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'jobs_submitted' not in data:
            raise tornado.web.HTTPError(400, reason='missing jobs_submitted')
        try:
            jobs_submitted = int(data['jobs_submitted'])
        except Exception:
            raise tornado.web.HTTPError(400, reason='jobs_submitted is not an int')

        ret = await self.db.datasets.find_one({'dataset_id':dataset_id})
        if not ret:
            raise tornado.web.HTTPError(404, reason='Dataset not found')
        if ret['jobs_immutable']:
            raise tornado.web.HTTPError(400, reason='jobs_submitted is immutable')
        if ret['jobs_submitted'] > jobs_submitted:
            raise tornado.web.HTTPError(400, reason='jobs_submitted must be larger than before')
        if 'tasks_per_job' not in ret or ret['tasks_per_job'] <= 0:
            raise tornado.web.HTTPError(400, reason='tasks_per_job not valid')

        ret = await self.db.datasets.find_one_and_update(
            {'dataset_id':dataset_id},
            {'$set':{
                'jobs_submitted': jobs_submitted,
                'tasks_submitted': int(jobs_submitted*ret['tasks_per_job']),
            }},
            projection=['_id']
        )
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write({})
            self.finish()


class DatasetHardResetHandler(APIBase):
    """
    Do a hard reset on a dataset.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id):
        """
        Do a hard reset on a dataset.

        Returns:
            dict: empty dict
        """
        ret = await self.db.datasets.find_one_and_update(
            {'dataset_id': dataset_id},
            {'$set': {'status': DATASET_STATUS_START}},
            projection=['_id']
        )
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write({})
            self.finish()


class DatasetTruncateHandler(APIBase):
    """
    Truncate a dataset.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id):
        """
        Truncate a dataset.

        Returns:
            dict: empty dict
        """
        ret = await self.db.datasets.find_one_and_update(
            {'dataset_id': dataset_id, 'status': {'$ne': 'complete'}},
            {'$set': {'truncated': True}},
            projection=['_id']
        )
        if not ret:
            ret = await self.db.datasets.find_one({'dataset_id': dataset_id})
            if not ret:
                self.send_error(404, reason="Dataset not found")
                return
            elif ret['status'] == 'complete':
                self.send_error(400, reason="Cannot truncate complete dataset")
                return

        self.write({})
        self.finish()


class DatasetSummariesStatusHandler(APIBase):
    """
    Handle dataset summary grouping by status.
    """
    @authorization(roles=['admin', 'user', 'system'])
    async def get(self):
        """
        Get the dataset summary for all datasets, group by status.

        Returns:
            dict: {<status>: [<dataset_id>,]}
        """
        cursor = self.db.datasets.find(projection={'_id': False, 'status': True, 'dataset_id': True})
        ret = defaultdict(list)
        async for row in cursor:
            if await self.manual_attr_auth('dataset_id', row['dataset_id'], 'read'):
                ret[row['status']].append(row['dataset_id'])
        ret2 = {}
        for k in sorted(ret, key=dataset_status_sort):
            ret2[k] = ret[k]
        self.write(ret2)
        self.finish()
