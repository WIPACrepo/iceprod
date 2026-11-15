import json
import logging
import uuid

import tornado.web

from iceprod.core.jsonUtil import json_encode
from iceprod.server.util import nowstr

from ..auth import attr_auth, authorization
from ..base_handler import APIBase

logger = logging.getLogger('rest.task_stats')


def setup(handler_cfg):
    """
    Setup method for Task Stats REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            (r'/tasks/(?P<task_id>\w+)/task_stats', MultiTaskStatsHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/bulk/task_stats', DatasetsBulkTaskStatsHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/task_stats', DatasetsMultiTaskStatsHandler, handler_cfg),
            (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/task_stats/(?P<task_stat_id>\w+)', DatasetsTaskStatsHandler, handler_cfg),
        ],
        'database': 'task_stats',
        'indexes': {
            'jobs': {
                'task_stat_id_index': {'keys': 'task_stat_id', 'unique': True},
                'task_id_index': {'keys': 'task_id', 'unique': False},
                'dataset_id_index': {'keys': 'dataset_id', 'unique': False},
            }
        }
    }


class MultiTaskStatsHandler(APIBase):
    """
    Handle multi task_stats requests.
    """
    @authorization(roles=['admin', 'system'])
    async def post(self, task_id):
        """
        Create a task_stat entry.

        Body should contain the task stat data.

        Args:
            task_id (str): the task id for this task_stat

        Returns:
            dict: {'result': <task_stat_id>}
        """
        stat_data = json.loads(self.request.body)
        if 'dataset_id' not in stat_data:
            raise tornado.web.HTTPError(400, reason='Missing dataset_id in body')

        # set some fields
        task_stat_id = uuid.uuid1().hex
        data = {
            'task_stat_id': task_stat_id,
            'task_id': task_id,
            'dataset_id': stat_data.pop('dataset_id', ''),
            'create_date': nowstr(),
            'stats': stat_data,
        }

        await self.db.task_stats.insert_one(data)
        self.set_status(201)
        self.write({'result': task_stat_id})
        self.finish()


class DatasetsBulkTaskStatsHandler(APIBase):
    """
    Handle a dataset bulk task_stats requests.

    Stream the output of all stats.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id):
        """
        Get task_stats for a dataset and task.

        Args:
            dataset_id (str): dataset id

        Params (optional):
            last: bool (True: only last task_stat.  False: all task_stats)
            after: only return stats created more recently than this date
            task_ids: | separated list of task_ids to filter by (max 100)
            keys: | separated list of keys to return for each task_stat (task_stat_id and task_id are always returned)
            buffer_size: number of records to buffer before flushing (default 1000)

        Returns:
            newline-delimited JSON task_stat entries, grouped by task_id
            and sorted by create_date within each task
        """
        last = self.get_argument('last', 'f').lower() in ('true','t','1','yes','y')

        query: dict = {'dataset_id':dataset_id}
        after = self.get_argument('after', None)
        if after:
            query['create_date'] = {"$gte": after}
        task_ids = self.get_id_list('task_ids')
        if task_ids:
            query['task_id'] = {'$in': task_ids}

        projection = {'_id': False}
        # output keys, always including the ids so streamed rows can be identified
        out_keys: set[str] = set()
        keys = self.get_argument('keys','')
        if keys:
            out_keys = {x for x in keys.split('|') if x}
            out_keys.update(('task_stat_id', 'task_id'))
            projection.update({x:True for x in out_keys})
            projection['create_date'] = True  # needed for sorting
        buffer_size = int(self.get_argument('buffer_size', '1000'))

        task_id = None
        data = []
        n = 0

        async def _write_data():
            nonlocal n
            ret = sorted(data, key=lambda x: x['create_date'])
            if out_keys:
                ret = [{k:d[k] for k in d if k in out_keys} for d in ret]
            if last:
                ret = ret[-1:]
            for row in ret:
                self.write(json_encode(row) + '\n')
            n += 1
            if n >= buffer_size:
                n = 0
                await self.flush()

        async for row in self.db.task_stats.find(query, projection=projection).sort([('task_id',1)]):
            if row['task_id'] == task_id:
                data.append(row)
                continue
            if data:
                await _write_data()
            data = [row]
            task_id = row['task_id']

        if data:
            await _write_data()
        self.finish()


class DatasetsMultiTaskStatsHandler(APIBase):
    """
    Handle multi task_stats requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id, task_id):
        """
        Get task_stats for a dataset and task.

        Args:
            dataset_id (str): dataset id
            task_id (str): task id

        Params (optional):
            last: bool (True: only last task_stat.  False: all task_stats)
            keys: | separated list of keys to return for each task_stat

        Returns:
            dict: {<task_stat_id>: stats}
        """
        last = self.get_argument('last', 'f').lower() in ('true','t','1','yes','y')

        projection = {'_id': False}
        keys = self.get_argument('keys','')
        if keys:
            projection.update({x:True for x in keys.split('|') if x})
            projection['task_stat_id'] = True
            if last:
                projection['create_date'] = True

        ret = await self.db.task_stats.find(
            {'dataset_id':dataset_id,'task_id':task_id},
            projection=projection
        ).to_list(10000)

        if last:
            ret = sorted(ret, key=lambda x: x['create_date'])[-1:]

        self.write({row['task_stat_id']:row for row in ret})
        self.finish()


class DatasetsTaskStatsHandler(APIBase):
    """
    Handle single task_stat requests.
    """
    @authorization(roles=['admin', 'user', 'system'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id, task_id, task_stat_id):
        """
        Get a task_stat entry.

        Args:
            dataset_id (str): dataset id
            task_id (str): task id
            task_stat_id (str): the task_stat id

        Returns:
            dict: task_stat entry
        """
        ret = await self.db.task_stats.find_one(
            {'dataset_id':dataset_id,'task_id':task_id,'task_stat_id':task_stat_id},
            projection={'_id':False}
        )
        if not ret:
            self.send_error(404, reason="Task stat not found")
        else:
            self.write(ret)
            self.finish()
