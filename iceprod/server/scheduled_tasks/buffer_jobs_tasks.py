"""
Buffer jobs and tasks into the queue.

Also known as "late materialization" of jobs and tasks,
this method finds datasets that are not fully materialized and
buffers a few more jobs and tasks into existence so they can be queued.

Initial delay: rand(10 minutes)
Periodic delay: 10 minutes
"""

import logging
import random
import time

from tornado.ioloop import IOLoop

from iceprod.core.resources import Resources

logger = logging.getLogger('buffer_jobs_tasks')

def buffer_jobs_tasks(module):
    """
    Initial entrypoint.

    Args:
        module (:py:class:`iceprod.server.modules.schedule`): schedule module
    """
    # initial delay
    IOLoop.current().call_later(random.randint(10,60*10), run, module.rest_client)

async def run(rest_client, debug=False):
    """
    Actual runtime / loop.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest client
        debug (bool): debug flag to propagate exceptions
    """
    start_time = time.time()
    datasets = await rest_client.request('GET', '/dataset_summaries/status')
    if 'processing' in datasets:
        for dataset_id in datasets['processing']:
            try:
                dataset = await rest_client.request('GET', '/datasets/{}'.format(dataset_id))
                tasks = await rest_client.request('GET', '/datasets/{}/task_counts/status'.format(dataset_id))
                if 'waiting' not in tasks or tasks['waiting'] < 1000:
                    # buffer for this dataset
                    jobs = await rest_client.request('GET', '/datasets/{}/jobs'.format(dataset_id))
                    jobs_to_buffer = min(1000, dataset['jobs_submitted'] - len(jobs))
                    if jobs_to_buffer > 0:
                        config = await rest_client.request('GET', '/config/{}'.format(dataset_id))
                        task_names = [task['name'] if task['name'] else str(i) for i,task in enumerate(config['tasks'])]
                        job_index = max(jobs[i]['job_index'] for i in jobs)+1 if jobs else 0
                        for i in range(jobs_to_buffer):
                            # buffer job
                            job_index += 1
                            args = {'dataset_id': dataset_id, 'job_index': job_index}
                            job_id = await rest_client.request('POST', '/jobs', args)
                            # buffer tasks
                            task_ids = []
                            for task_index,name in enumerate(task_names):
                                depends = await get_depends(rest_client, config, job_index,
                                                            task_index, task_ids)
                                args = {
                                    'dataset_id': dataset_id,
                                    'job_id': job_id['result'],
                                    'task_index': task_index,
                                    'name': name,
                                    'depends': depends,
                                    'requirements': get_reqs(config, task_index),
                                }
                                task_id = await rest_client.request('POST', '/tasks', args)
                                task_ids.append(task_id['result'])
            except Exception:
                logger.error('error buffering dataset %s', dataset_id, exc_info=True)
                if debug:
                    raise

    # run again after 10 minute delay
    stop_time = time.time()
    delay = max(60*10 - (stop_time-start_time), 60)
    IOLoop.current().call_later(delay, run, rest_client)

def get_reqs(config, index):
    """
    Get requirements for a task.

    Args:
        config (:py:class:`iceprod.core.dataclasses.Job`): dataset config
        index (int): task index

    Returns:
        dict: task requirements
    """
    task = config['tasks'][index]
    req = task['requirements'].copy()
    for k in Resources.defaults:
        if k not in req or not req[k]:
            req[k] = Resources.defaults[k]
    return req

async def get_depends(rest_client, config, job_index, task_index, task_ids):
    """
    Get dependency task_ids for a task.

    Args:
        rest_client (:py:class:`iceprod.core.rest_client.Client`): rest api client
        config (:py:class:`iceprod.core.dataclasses.Job`): dataset config
        job_index (int): job index
        task_index (int): task index
        task_ids (list): list of already buffered task_ids in this job

    Returns:
        list: list of task_id dependencies
    """
    task = config['tasks'][task_index]
    all_tasks ={task['name'] if task['name'] else str(i):i for i,task in enumerate(config['tasks'])}
    ret = []
    for dep in task['depends']:
        if dep in all_tasks:
            dep_index = all_tasks[dep]
            if dep_index >= task_index:
                raise Exception("bad depends: %r. can't depend on ourself or a greater index", dep)
            ret.append(task_ids[dep_index])
        elif isinstance(dep, int) and dep < len(all_tasks):
            dep_index = dep
            if dep_index >= task_index:
                raise Exception("bad depends: %r. can't depend on ourself or a greater index", dep)
            ret.append(task_ids[dep_index])
        elif isinstance(dep,str) and ':' in dep:
            # try for a task in another dataset
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
            try:
                task = await rest_client.request('GET', '/tasks/{}'.format(dep))
                ret.append(task['task_id'])
            except Exception:
                raise Exception('bad depends: %r', dep)

    return ret
