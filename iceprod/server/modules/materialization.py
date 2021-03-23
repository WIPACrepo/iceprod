"""
Materialization service for dataset late materialization.
"""
import logging
import importlib
import os
import time
from datetime import datetime
import json
import uuid
import asyncio
from collections import defaultdict

import pymongo
import motor
import motor.motor_asyncio
from rest_tools.server import RestServer
from rest_tools.client import RestClient

import iceprod.server
from iceprod.server import module
from iceprod.server import get_pkgdata_filename
from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization
from iceprod.server.util import nowstr, datetime2str, task_statuses
from iceprod.core.parser import ExpParser
from iceprod.core.resources import Resources
from iceprod.server.priority import Priority

logger = logging.getLogger('modules_materialization')

class materialization(module.module):
    """
    Run the late materialization module, which handles a REST service for
    dataset late materialization.

    """
    def __init__(self,*args,**kwargs):
        super(materialization,self).__init__(*args,**kwargs)

        # set up materialization service
        ms = MaterializationService(self.cfg)

        # set up the REST API
        routes, args = setup_rest(self.cfg, module=self, materialization_service=ms)
        self.server = RestServer(**args)
        for r in routes:
            self.server.add_route(*r)

        kwargs = {}
        if 'materialization' in self.cfg:
            if 'address' in self.cfg['materialization']:
                kwargs['address'] = self.cfg['materialization']['address']
            if 'port' in self.cfg['materialization']:
                kwargs['port'] = self.cfg['materialization']['port']

        self.server.startup(**kwargs)


class MaterializationService:
    """Materialization service."""
    def __init__(self, cfg):
        self.cfg = cfg

        db_cfg = cfg.get('materialization',{}).get('database',{})
        self.db = motor.motor_asyncio.AsyncIOMotorClient(**db_cfg).datasets

        rest_cfg = cfg.get('rest_api', {})
        rest_url = rest_cfg.get('url', 'https://iceprod2-api.icecube.wisc.edu')
        if 'auth_key' not in rest_cfg:
            raise Exception('no auth key for rest api')
        rest_auth_key = rest_cfg.get('auth_key')
        rest_client = RestClient(rest_url, rest_auth_key)
        self.materialize = Materialize(rest_client)

        self.start_time = time.time()
        self.last_run_time = None
        self.last_success_time = None
        asyncio.get_event_loop().create_task(self.run())

    async def run(self):
        """
        Run loop.
        """
        while True:
            try:
                self.last_run_time = time.time()

                # get next materialization from DB
                ret = await self.db.materialization.find_one_and_update(
                        {'status':'waiting'},
                        {'$set': {'status': 'processing'}},
                        projection={'_id':False},
                        sort=[('timestamp', 1)],
                        return_document=pymongo.ReturnDocument.AFTER,
                )
                if not ret:
                    logger.info('materialization service has nothing to do. sleeping')
                    await asyncio.sleep(60)
                    continue

                # run materialization
                logger.warning(f'running materialization request {ret["materialization_id"]}')
                kwargs = {}
                if 'dataset_id' in ret and ret['dataset_id']:
                    kwargs['only_dataset'] = ret['dataset_id']
                if 'num' in ret and ret['num']:
                    kwargs['num'] = ret['num']
                if 'set_status' in ret and ret['set_status']:
                    kwargs['set_status'] = ret['set_status']
                await self.materialize.run_once(**kwargs)

                await self.db.materialization.update_one(
                        {'materialization_id': ret['materialization_id']},
                        {'$set': {'status': 'complete'}},
                )
                self.last_success_time = time.time()
            except Exception:
                logger.error('error running materialization', exc_info=True)
                await self.db.materialization.update_one(
                        {'materialization_id': ret['materialization_id']},
                        {'$set': {'status': 'error'}},
                )


class Materialize:
    def __init__(self, rest_client):
        self.rest_client = rest_client

    async def run_once(self, only_dataset=None, set_status=None, num=20000, dryrun=False):
        """
        Actual materialization work.

        Args:
            only_dataset (str): dataset_id if we should only buffer a single dataset
            set_status (str): status of new tasks
            num (int): max number of jobs to buffer
            dryrun (bool): if true, do not modify DB, just log changes
        """
        if set_status and set_status not in task_statuses:
            raise Exception('set_status is not a valid task status')
        prio = Priority(self.rest_client)
        datasets = await self.rest_client.request('GET', '/dataset_summaries/status')
        if 'truncated' in datasets and only_dataset:
            datasets['processing'].extend(datasets['truncated'])
        if 'suspended' in datasets and only_dataset:
            datasets['processing'].extend(datasets['suspended'])
        if 'processing' in datasets:
            for dataset_id in datasets['processing']:
                if only_dataset and dataset_id != only_dataset:
                    continue
                try:
                    dataset = await self.rest_client.request('GET', '/datasets/{}'.format(dataset_id))
                    tasks = await self.rest_client.request('GET', '/datasets/{}/task_counts/status'.format(dataset_id))
                    if 'waiting' not in tasks or tasks['waiting'] < num or only_dataset:
                        # buffer for this dataset
                        logger.warning('buffering dataset %s', dataset_id)
                        jobs = await self.rest_client.request('GET', '/datasets/{}/jobs'.format(dataset_id))
                        jobs_to_buffer = min(num, dataset['jobs_submitted'] - len(jobs))
                        if jobs_to_buffer > 0:
                            logger.info('buffering %d jobs for dataset %s', jobs_to_buffer, dataset_id)
                            config = await self.rest_client.request('GET', '/config/{}'.format(dataset_id))
                            if 'options' not in config:
                                config['options'] = {}
                            parser = ExpParser()
                            task_names = [task['name'] if task['name'] else str(i) for i,task in enumerate(config['tasks'])]
                            job_index = max(jobs[i]['job_index'] for i in jobs)+1 if jobs else 0
                            for i in range(jobs_to_buffer):
                                # buffer job
                                logger.info('buffering dataset %s job %d', dataset_id, job_index)
                                args = {'dataset_id': dataset_id, 'job_index': job_index}
                                if dryrun:
                                    job_id = {'result': 'DRYRUN'}
                                else:
                                    job_id = await self.rest_client.request('POST', '/jobs', args)
                                # buffer tasks
                                task_ids = []
                                for task_index,name in enumerate(task_names):
                                    depends = await self.get_depends(config, job_index,
                                                                     task_index, task_ids)
                                    config['options']['job'] = job_index
                                    config['options']['task'] = task_index
                                    config['options']['dataset'] = dataset['dataset']
                                    config['options']['jobs_submitted'] = dataset['jobs_submitted']
                                    config['options']['tasks_submitted'] = dataset['tasks_submitted']
                                    config['options']['debug'] = dataset['debug']
                                    args = {
                                        'dataset_id': dataset_id,
                                        'job_id': job_id['result'],
                                        'task_index': task_index,
                                        'job_index': job_index,
                                        'name': name,
                                        'depends': depends,
                                        'requirements': self.get_reqs(config, task_index, parser),
                                    }
                                    if set_status:
                                        args['status'] = set_status
                                    if dryrun:
                                        logger.info(f'DRYRUN: POST /tasks {args}')
                                    else:
                                        ret = await self.rest_client.request('POST', '/tasks', args)
                                        task_id = ret['result']
                                        task_ids.append(task_id)
                                        p = await prio.get_task_prio(dataset_id, task_id)
                                        await self.rest_client.request('PATCH', f'/tasks/{task_id}', {'priority': p})
                                job_index += 1
                except Exception:
                    logger.error('error buffering dataset %s', dataset_id, exc_info=True)
                    if only_dataset:
                        raise

    def get_reqs(self, config, task_index, parser):
        """
        Get requirements for a task.

        Args:
            config (:py:class:`iceprod.core.dataclasses.Job`): dataset config
            task_index (int): task index
            parser (:py:class:`iceprod.core.parser.ExpParser`): parser

        Returns:
            dict: task requirements
        """
        task = config['tasks'][task_index]
        req = task['requirements'].copy() if 'requirements' in task else {}
        for k in req:
            if isinstance(req[k], list):
                req[k] = [parser.parse(val,config) for val in req[k]]
            else:
                req[k] = parser.parse(req[k],config)
        for k in Resources.defaults:
            # don't add if not needed?
            #if k not in req or not req[k]:
            #    req[k] = Resources.defaults[k]
            if k == 'gpu' and k in req and isinstance(req[k], (tuple,list)):
                req[k] = len(req[k])
        return req

    async def get_depends(self, config, job_index, task_index, task_ids):
        """
        Get dependency task_ids for a task.

        Args:
            config (:py:class:`iceprod.core.dataclasses.Job`): dataset config
            job_index (int): job index
            task_index (int): task index
            task_ids (list): list of already buffered task_ids in this job

        Returns:
            list: list of task_id dependencies
        """
        task = config['tasks'][task_index]
        if 'depends' not in task:
            return []
        all_tasks ={task['name'] if task['name'] else str(i):i for i,task in enumerate(config['tasks'])}
        ret = []
        for dep in task['depends']:
            if dep in all_tasks:
                logging.debug('dep %r in all_tasks', dep)
                dep_index = all_tasks[dep]
                if dep_index >= task_index:
                    raise Exception("bad depends: %r. can't depend on ourself or a greater index", dep)
                ret.append(task_ids[dep_index])
            elif isinstance(dep, int) and dep < len(all_tasks):
                logging.debug('dep %r is int', dep)
                dep_index = dep
                if dep_index >= task_index:
                    raise Exception("bad depends: %r. can't depend on ourself or a greater index", dep)
                ret.append(task_ids[dep_index])
            elif isinstance(dep,str) and ':' in dep:
                # try for a task in another dataset
                logging.debug('dep %r in another dataset', dep)
                dataset_id, dep = dep.split(':',1)
                try:
                    #tasks = await rest_client.request('GET', '/datasets/{}/tasks?keys=task_id|name|task_index&job_index={}'.format(dataset_id,job_index))
                    tasks = await self.rest_client.request('GET', '/datasets/{}/tasks?keys=task_id|name|task_index'.format(dataset_id))
                    for task in tasks.values():
                        if dep == task['name'] or dep == str(task['task_index']):
                            job = await self.rest_client.request('GET', '/jobs/{}'.format(task['job_id']))
                            logging.info('ext job_index=%r, my job_index=%r', job['job_index'], job_index)
                            if job['job_index'] == job_index:
                                ret.append(task['task_id'])
                                break
                    else:
                        raise Exception()
                except Exception:
                    raise Exception('bad depends: %r', dep)
            else:
                # try for a raw task_id
                logging.debug('dep %r is a raw id', dep)
                try:
                    task = await self.rest_client.request('GET', '/tasks/{}'.format(dep))
                    ret.append(task['task_id'])
                except Exception:
                    raise Exception('bad depends: %r', dep)

        return ret


def setup_rest(config, materialization_service=None, **kwargs):
    """
    Setup a REST Tornado server.

    Args:
        config (:py:class:`iceprod.server.config.Config`): An IceProd config

    Returns:
        tuple: (routes, application args)
    """
    cfg_rest = config.get('materialization',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).datasets
    if 'materialization_id_index' not in db.materialization.index_information():
        db.materialization.create_index('materialization_id', name='materialization_id_index', unique=True)
    if 'dataset_id_index' not in db.materialization.index_information():
        db.materialization.create_index('dataset_id', name='dataset_id_index')
    if 'status_timestamp_index' not in db.materialization.index_information():
        db.materialization.create_index([('status',pymongo.ASCENDING),('timestamp',pymongo.ASCENDING)], name='status_timestamp_index')

    handler_cfg = RESTHandlerSetup(config, **kwargs)
    handler_cfg['database'] = motor.motor_tornado.MotorClient(**db_cfg).datasets
    handler_cfg['materialization_service'] = materialization_service

    routes = [
        (r'/status/(?P<materialization_id>\w+)', StatusHandler, handler_cfg),
        (r'/', RequestHandler, handler_cfg),
        (r'/request/(?P<dataset_id>\w+)', RequestDatasetHandler, handler_cfg),
        (r'/healthz', HealthHandler, handler_cfg),
    ]

    logger.info('REST routes being served:')
    for r in routes:
        logger.info('  %r', r)

    kwargs = {}
    return (routes, kwargs)

class BaseHandler(RESTHandler):
    """
    Base handler for Materialization REST API. 
    """
    def initialize(self, database=None, materialization_service=None, **kwargs):
        super(BaseHandler, self).initialize(**kwargs)
        self.db = database
        self.materialization_service = materialization_service

    async def new_request(self, args):
        # validate first
        fields = {
            'dataset_id': str,
            'set_status': str,
            'num': int,
        }
        if set(args)-set(fields): # don't let random args through
            raise tornado.web.HTTPError(400, reason='invalid params')
        for k in fields:
            if k in args and not isinstance(args[k], fields[k]):
                r = 'key "{}" should be of type {}'.format(k, fields[k].__name__)
                raise tornado.web.HTTPError(400, reason=r)

        # set some fields
        data = {
            'materialization_id': uuid.uuid1().hex,
            'status': 'waiting',
            'timestamp': nowstr(),
            'creator': self.auth_data['username'],
            'role': self.auth_data['role'],
        }
        for k in fields:
            if k in args:
                data[k] = args[k]

        # insert
        await self.db.materialization.insert_one(data)
        return data

class StatusHandler(BaseHandler):
    """
    Handle materialization status requests.
    """
    @authorization(roles=['admin','client','user'])
    async def get(self, materialization_id):
        """
        Get materialization status.

        If materialization_id is invalid, returns http code 404.

        Args:
            materialization_id (str): materialization request id

        Returns:
            dict: materialization metadata
        """
        ret = await self.db.materialization.find_one({'materialization_id':materialization_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Materialization request not found")
        else:
            self.write(ret)

class RequestHandler(BaseHandler):
    """
    Handle basic materialization requests.
    """
    @authorization(roles=['admin','client'])
    async def post(self):
        """
        Create basic materialization request.

        On success, returns http code 201.

        Params:
            num: number of jobs to buffer per dataset

        Returns:
            dict: {result: materialization_id}
        """
        args = json.loads(self.request.body) if self.request.body else {}

        data = await self.new_request(args)

        # return success
        self.set_status(201)
        self.write({'result': data['materialization_id']})

class RequestDatasetHandler(BaseHandler):
    """
    Handle dataset materialization requests.
    """
    @authorization(roles=['admin','client'], attrs=['dataset_id:read'])
    async def post(self, dataset_id):
        """
        Create dataset materialization request.

        On success, returns http code 201.

        Params:
            num: number of jobs to buffer per dataset

        Args:
            dataset_id (str): dataset_id to materialize

        Returns:
            dict: {result: materialization_id}
        """
        args = json.loads(self.request.body) if self.request.body else {}
        args['dataset_id'] = dataset_id

        data = await self.new_request(args)

        # return success
        self.set_status(201)
        self.write({'result': data['materialization_id']})

class HealthHandler(BaseHandler):
    """
    Handle health requests.
    """
    async def get(self):
        """
        Get health status.

        Returns based on exit code, 200 = ok, 400 = failure
        """
        now = time.time()
        status = {
            'now': now,
            'start_time': datetime2str(datetime.utcfromtimestamp(self.materialization_service.start_time)),
            'last_run_time': "",
            'last_success_time': "",
            'num_requests': -1,
        }
        try:
            if self.materialization_service.last_run_time is None and self.materialization_service.start_time + 3600 < now:
                self.send_error(500, reason='materialization was never run')
                return
            if self.materialization_service.last_run_time is not None:
                if self.materialization_service.last_run_time + 3600 < now:
                    self.send_error(500, reason='materialization has stopped running')
                    return
                status['last_run_time'] = datetime2str(datetime.utcfromtimestamp(self.materialization_service.last_run_time))
            if self.materialization_service.last_success_time is None and self.materialization_service.start_time + 86400 < now:
                self.send_error(500, reason='materialization was never successful')
                return
            if self.materialization_service.last_success_time is not None:
                if self.materialization_service.last_success_time + 86400 < now:
                    self.send_error(500, reason='materialization has stopped being successful')
                    return
                status['last_success_time'] = datetime2str(datetime.utcfromtimestamp(self.materialization_service.last_success_time))
        except Exception:
            logger.info('error from materialization service', exc_info=True)
            self.send_error(500, reason='error from materialization service')
            return

        try:
            ret = await self.db.materialization.count_documents({}, maxTimeMS=1000)
        except Exception:
            logger.info('bad db request', exc_info=True)
            self.send_error(500, reason='bad db request')
            return
        if ret is None:
            self.send_error(500, reason='bad db result')
        else:
            status['num_requests'] = ret
        self.write(status)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Materialize a dataset')
    parser.add_argument('dataset_id')
    parser.add_argument('--rest_url', default='https://iceprod2-api.icecube.wisc.edu')
    parser.add_argument('-t', '--rest_token', default=None)
    parser.add_argument('--set_status', default=None,help='initial task status')
    parser.add_argument('-n','--num', default=100,type=int,help='number of jobs to materialize')
    parser.add_argument('--debug',action='store_true')
    parser.add_argument('--dryrun',action='store_true',help='do not modify database, just log changes')
    args = parser.parse_args()
    if not args.rest_token:
        raise Exception('no token for rest api')
    logging.basicConfig(level=(logging.DEBUG if args.debug else logging.INFO))

    rest_client = RestClient(args.rest_url, args.rest_token)
    materialize = Materialize(rest_client)
    asyncio.run(materialize.run_once(only_dataset=args.dataset_id, set_status=args.set_status, num=args.num, dryrun=args.dryrun))
