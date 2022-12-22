import logging
import json
import uuid

import tornado.web
import pymongo
import motor

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr

logger = logging.getLogger('rest.task_stats')

def setup(config, *args, **kwargs):
    """
    Setup method for TaskStats REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for logs, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('task_stats',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).task_stats
    if 'task_stat_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('task_stat_id', name='task_stat_id_index', unique=True)
    if 'task_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('task_id', name='task_id_index', unique=False)
    if 'dataset_id_index' not in db.task_stats.index_information():
        db.task_stats.create_index('dataset_id', name='dataset_id_index', unique=False)

    handler_cfg = RESTHandlerSetup(config, *args, **kwargs)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(**db_cfg).task_stats,
    })

    return [
        (r'/tasks/(?P<task_id>\w+)/task_stats', MultiTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/task_stats', DatasetsMultiTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/bulk/task_stats', DatasetsBulkTaskStatsHandler, handler_cfg),
        (r'/datasets/(?P<dataset_id>\w+)/tasks/(?P<task_id>\w+)/task_stats/(?P<task_stat_id>\w+)', DatasetsTaskStatsHandler, handler_cfg),
    ]

class BaseHandler(RESTHandler):
    def initialize(self, database=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database

class MultiTaskStatsHandler(BaseHandler):
    """
    Handle multi task_stats requests.
    """
    @authorization(roles=['admin','client','pilot'])
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
            'dataset_id': stat_data['dataset_id'],
            'create_date': nowstr(),
            'stats': stat_data,
        }

        ret = await self.db.task_stats.insert_one(data)
        self.set_status(201)
        self.write({'result': task_stat_id})
        self.finish()

class DatasetsBulkTaskStatsHandler(BaseHandler):
    """
    Handle a dataset bulk task_stats requests.

    Stream the output of all stats.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
    async def get(self, dataset_id):
        """
        Get task_stats for a dataset and task.

        Args:
            dataset_id (str): dataset id

        Params (optional):
            last: bool (True: only last task_stat.  False: all task_stats)
            after: only return stats created more recently than this date
            keys: | separated list of keys to return for each task_stat
            buffer_size: number of records to buffer before flushing (default 100)

        Returns:
            dict: {<task_stat_id>: stats}
        """
        last = self.get_argument('last', 'f').lower() in ('true','t','1','yes','y')

        query = {'dataset_id':dataset_id}
        after = self.get_argument('after', None)
        if after:
            query['create_date'] = {"$gte": after}

        projection = {'_id': False}
        keys = self.get_argument('keys','')
        if keys:
            keys = keys.split('|')
            projection.update({x:True for x in keys if x})
            projection['task_stat_id'] = True
            projection['task_id'] = True
            projection['create_date'] = True
        buffer_size = int(self.get_argument('buffer_size', '1000'))

        task_id = None
        data = []
        n = 0
        async for row in self.db.task_stats.find(query, projection=projection).sort([('task_id',1)]):
            if row['task_id'] == task_id:
                data.append(row)
                continue
            if data:
                ret = sorted(data, key=lambda x: x['create_date'])
                if keys:
                    ret = [{k:d[k] for k in d if k in keys} for d in ret]
                if last:
                    self.write(ret[-1])
                    self.write('\n')
                else:
                    for ret in data:
                        self.write(ret)
                        self.write('\n')
                n += 1
                if n >= buffer_size:
                    n = 0
                    await self.flush()
            data = [row]
            task_id = row['task_id']

        if data:
            ret = sorted(data, key=lambda x: x['create_date'])
            if keys:
                ret = [{k:d[k] for k in d if k in keys} for d in ret]
            if last:
                self.write(ret[-1])
                self.write('\n')
            else:
                for ret in data:
                    self.write(ret)
                    self.write('\n')
        self.finish()

class DatasetsMultiTaskStatsHandler(BaseHandler):
    """
    Handle multi task_stats requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
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

        ret = await self.db.task_stats.find({'dataset_id':dataset_id,'task_id':task_id},
                projection=projection).to_list(10000)

        if last:
            ret = sorted(ret, key=lambda x: x['create_date'])[-1:]

        self.write({row['task_stat_id']:row for row in ret})
        self.finish()

class DatasetsTaskStatsHandler(BaseHandler):
    """
    Handle single task_stat requests.
    """
    @authorization(roles=['admin'], attrs=['dataset_id:read'])
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
        ret = await self.db.task_stats.find_one({'dataset_id':dataset_id,'task_id':task_id,'task_stat_id':task_stat_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Task stat not found")
        else:
            self.write(ret)
            self.finish()
