import logging

import tornado.web
from cachetools.func import ttl_cache
from rest_tools.client import RestClient

from iceprod.roles_groups import GROUPS
from iceprod.server.states import DATASET_STATUS

from .base import PublicHandler, authenticated

logger = logging.getLogger('website-dataset')

#: dataset statuses hidden from the browse page unless explicitly filtered on
HIDDEN_DATASET_STATUS = ('obsolete',)


class UsernamesMixin:
    """Provide the list of known usernames for filters"""
    #: provided by PublicHandler
    system_rest_client: RestClient

    @ttl_cache(maxsize=256, ttl=600)
    async def get_usernames(self):
        ret = await self.system_rest_client.request('GET', '/users')
        return [x['username'] for x in ret['results']]


class DatasetBrowse(UsernamesMixin, PublicHandler):
    """Handle /dataset urls"""

    @authenticated
    async def get(self):
        assert self.rest_client
        usernames = await self.get_usernames()
        filter_options = {
            'status': list(DATASET_STATUS),
            'groups': list(GROUPS.keys()),
            'users': usernames,
        }
        filter_results = {n: self.get_arguments(n) for n in filter_options}

        args = {'keys': 'dataset_id|dataset|status|description|group|username'}
        for name in filter_results:
            val = filter_results[name]
            if not val:
                continue
            if any(v not in filter_options[name] for v in val):
                raise tornado.web.HTTPError(400, reason='Bad filter '+name+' value')
            args[name] = '|'.join(val)
        if not filter_results['status']:
            # hide obsolete datasets by default
            args['status'] = '|'.join(s for s in DATASET_STATUS if s not in HIDDEN_DATASET_STATUS)

        url = '/datasets'

        ret = await self.rest_client.request('GET', url, args)
        datasets = sorted(ret.values(), key=lambda x:x.get('dataset',0), reverse=True)
        logger.debug('datasets: %r', datasets)
        datasets = filter(lambda x: 'dataset' in x, datasets)
        self.render(
            'dataset_browse.html',
            datasets=datasets,
            filter_options=filter_options,
            filter_results=filter_results,
            hidden_status=HIDDEN_DATASET_STATUS,
        )


class Dataset(PublicHandler):
    """Handle /dataset urls"""
    @authenticated
    async def get(self, dataset_id):
        assert self.rest_client
        if dataset_id.isdigit():
            try:
                d_num = int(dataset_id)
                if d_num < 10000000:
                    all_datasets = await self.rest_client.request('GET', '/datasets', {'keys': 'dataset_id|dataset'})
                    for d in all_datasets.values():
                        if d['dataset'] == d_num:
                            dataset_id = d['dataset_id']
                            break
            except Exception:
                pass
        try:
            dataset = await self.rest_client.request('GET',f'/datasets/{dataset_id}')
        except Exception:
            raise tornado.web.HTTPError(404, reason='Dataset not found')
        dataset_num = dataset['dataset']

        jobs = await self.rest_client.request('GET',f'/datasets/{dataset_id}/job_counts/status')
        tasks = await self.rest_client.request('GET',f'/datasets/{dataset_id}/task_counts/status')
        task_info = await self.rest_client.request('GET',f'/datasets/{dataset_id}/task_counts/name_status')
        task_stats = await self.rest_client.request('GET',f'/datasets/{dataset_id}/task_stats')
        try:
            config = await self.rest_client.request('GET',f'/config/{dataset_id}')
        except Exception:
            config = {}
        for t in task_info:
            logger.info('task_info[%s] = %r', t, task_info[t])
            type_ = 'UNK'
            for task in config.get('tasks', []):
                if 'name' in task and task['name'] == t:
                    type_ = 'GPU' if 'requirements' in task and 'gpu' in task['requirements'] and task['requirements']['gpu'] else 'CPU'
                    break
            task_info[t] = {
                'name': t,
                'type': type_,
                'idle': task_info[t].get('idle', 0),
                'waiting': task_info[t].get('waiting', 0),
                'queued': task_info[t].get('queued', 0),
                'running': task_info[t].get('processing', 0),
                'complete': task_info[t].get('complete', 0),
                'error': task_info[t].get('failed', 0) + task_info[t].get('suspended', 0),
            }
        self.render('dataset_detail.html', dataset_id=dataset_id, dataset_num=dataset_num,
                    dataset=dataset, jobs=jobs, tasks=tasks, task_info=task_info,
                    task_stats=task_stats)
