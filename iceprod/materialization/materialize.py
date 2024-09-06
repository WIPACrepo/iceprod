import argparse
import asyncio
import logging
import time

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.core.parser import ExpParser
from iceprod.core.resources import Resources
from iceprod.server.priority import Priority
from iceprod.server.states import TASK_STATUS

logger = logging.getLogger('materialize')

DATASET_CYCLE_TIMEOUT = 120


class Materialize:
    def __init__(self, rest_client):
        self.rest_client = rest_client
        self.config_cache = {}
        self.prio = None

    async def run_once(self, only_dataset=None, set_status=None, num=10000, dryrun=False):
        """
        Actual materialization work.

        Args:
            only_dataset (str): dataset_id if we should only buffer a single dataset
            set_status (str): status of new tasks
            num (int): max number of jobs to buffer
            dryrun (bool): if true, do not modify DB, just log changes
        """
        if set_status and set_status not in TASK_STATUS:
            raise Exception('set_status is not a valid task status')
        self.config_cache = {}  # clear config cache
        self.prio = Priority(self.rest_client)  # clear priority cache

        ret = True

        if only_dataset:
            datasets = [only_dataset]
        else:
            ret = await self.rest_client.request('GET', '/dataset_summaries/status')
            datasets = ret.get('processing', [])

        for dataset_id in datasets:
            try:
                start_time = time.time()
                dataset = await self.rest_client.request('GET', '/datasets/{}'.format(dataset_id))
                if dataset.get('truncated', False) and not only_dataset:
                    logger.info('ignoring truncated dataset %s', dataset_id)
                    continue
                job_counts = await self.rest_client.request('GET', '/datasets/{}/job_counts/status'.format(dataset_id))
                tasks = await self.rest_client.request('GET', '/datasets/{}/task_counts/status'.format(dataset_id))
                if 'waiting' not in tasks or job_counts.get('processing', 0) < num or only_dataset:
                    # buffer for this dataset
                    logger.warning('checking dataset %s', dataset_id)
                    jobs = await self.rest_client.request('GET', '/datasets/{}/jobs'.format(dataset_id), {'keys': 'job_id|job_index'})

                    # check that last job was buffered correctly
                    job_index = max(jobs[i]['job_index'] for i in jobs)+1 if jobs else 0
                    num_tasks = sum(tasks.values())
                    while num_tasks % dataset['tasks_per_job'] != 0 and job_index > 0:
                        # a job must have failed to buffer, so check in reverse order
                        job_index -= 1
                        job_tasks = await self.rest_client.request('GET', f'/datasets/{dataset_id}/tasks',
                                                                   {'job_index': job_index, 'keys': 'task_id|job_id|task_index'})
                        if len(job_tasks) != dataset['tasks_per_job']:
                            logger.info('fixing buffer of job %d for dataset %s', job_index, dataset_id)
                            ret = await self.rest_client.request('GET', f'/datasets/{dataset_id}/jobs',
                                                                 {'job_index': job_index, 'keys': 'job_id'})
                            job_id = list(ret.keys())[0]
                            tasks_buffered = await self.buffer_job(dataset, job_index, job_id=job_id,
                                                                   tasks=list(job_tasks.values()),
                                                                   set_status=set_status, dryrun=dryrun)
                            num_tasks += tasks_buffered

                    # now try buffering new tasks
                    job_index = max(jobs[i]['job_index'] for i in jobs)+1 if jobs else 0
                    jobs_to_buffer = min(num, dataset['jobs_submitted'] - len(jobs))
                    if jobs_to_buffer > 0:
                        logger.info('buffering %d jobs for dataset %s', jobs_to_buffer, dataset_id)
                        for i in range(jobs_to_buffer):
                            await self.buffer_job(dataset, job_index, set_status=set_status, dryrun=dryrun)
                            job_index += 1

                            if only_dataset is None and time.time() - start_time > DATASET_CYCLE_TIMEOUT:
                                logger.warning('dataset cycle timeout for dataset %s', dataset_id)
                                ret = False
                                break
            except Exception:
                logger.error('error buffering dataset %s', dataset_id, exc_info=True)
                if only_dataset:
                    raise

        return ret

    async def buffer_job(self, dataset, job_index, job_id=None, tasks=None, set_status=None, dryrun=False):
        """
        Buffer a single job for a dataset

        Args:
            dataset (dict): dataset info
            job_index (int): job index
            job_id (str): job id (if filling in remaining)
            tasks (list): existing tasks (if filling in remaining)
            set_status (str): status of new tasks
            dryrun (bool): set to True if this is a dry run

        Returns:
            int: number of tasks buffered
        """
        dataset_id = dataset['dataset_id']
        logger.info('buffering dataset %s job %d', dataset_id, job_index)

        config = await self.get_config(dataset_id)
        parser = ExpParser()
        task_names = [task['name'] if task['name'] else str(i) for i,task in enumerate(config['tasks'])]
        if len(task_names) != dataset['tasks_per_job']:
            raise Exception('config num tasks does not match dataset tasks_per_job')

        args = {'dataset_id': dataset_id, 'job_index': job_index}
        if dryrun:
            job_id = {'result': 'DRYRUN'}
            task_ids = []
            task_iter = enumerate(task_names)
        elif job_id:
            task_ids = [task['task_id'] for task in tasks] if tasks else []
            task_indexes = {task['task_index'] for task in tasks} if tasks else {}
            task_iter = [(i,name) for i,name in enumerate(task_names) if i not in task_indexes]
            if not task_iter:
                raise Exception('no task names to create')
        else:
            ret = await self.rest_client.request('POST', '/jobs', args)
            job_id = ret['result']
            task_ids = []
            task_iter = enumerate(task_names)

        # buffer tasks
        for task_index,name in task_iter:
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
                'job_id': job_id,
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
                task_ids.append(job_index*1000000+task_index)
            else:
                ret = await self.rest_client.request('POST', '/tasks', args)
                task_id = ret['result']
                task_ids.append(task_id)
                p = await self.prio.get_task_prio(dataset_id, task_id)
                await self.rest_client.request('PATCH', f'/tasks/{task_id}', {'priority': p})

        return len(task_ids)

    async def get_config(self, dataset_id):
        """Get dataset config"""
        if dataset_id in self.config_cache:
            return self.config_cache[dataset_id]

        config = await self.rest_client.request('GET', '/config/{}'.format(dataset_id))
        if 'options' not in config:
            config['options'] = {}
        self.config_cache[dataset_id] = config
        return config

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
        all_tasks = {task['name'] if task['name'] else str(i):i for i,task in enumerate(config['tasks'])}
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
                    tasks = await rest_client.request('GET', '/datasets/{}/tasks?keys=task_id|name|task_index&job_index={}'.format(dataset_id,job_index))
                    for task in tasks.values():
                        if dep == task['name'] or dep == str(task['task_index']):
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Materialize a dataset')
    parser.add_argument('dataset_id')
    add_auth_to_argparse(parser)
    parser.add_argument('--set_status', default=None, help='initial task status')
    parser.add_argument('-n', '--num', default=100, type=int, help='number of jobs to materialize')
    parser.add_argument('--job_index', type=int, help='specific job index to buffer')
    parser.add_argument('--job_id', default=None, help='specific job id to buffer tasks into')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dryrun', action='store_true', help='do not modify database, just log changes')
    args = parser.parse_args()
    logging.basicConfig(level=(logging.DEBUG if args.debug else logging.INFO))

    rest_client = create_rest_client(args)
    materialize = Materialize(rest_client)
    if args.job_index is not None:
        logging.warning('manually buffering a job for dataset %s job %d', args.dataset_id, args.job_index)

        async def run():
            dataset = await rest_client.request('GET', '/datasets/{}'.format(args.dataset_id))
            materialize.prio = Priority(rest_client)
            await materialize.buffer_job(dataset, args.job_index, job_id=args.job_id)

        asyncio.run(run())
    else:
        asyncio.run(materialize.run_once(only_dataset=args.dataset_id, set_status=args.set_status, num=args.num, dryrun=args.dryrun))
