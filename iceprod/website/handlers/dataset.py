import logging

from cachetools.func import ttl_cache
import tornado.web

from iceprod.roles_groups import GROUPS
from .base import authenticated, PublicHandler

logger = logging.getLogger('website-dataset')


class DatasetBrowse(PublicHandler):
    """Handle /dataset urls"""
    @ttl_cache(maxsize=256, ttl=600)
    async def get_usernames(self):
        ret = await self.system_rest_client.request('GET', '/users')
        return [x['username'] for x in ret['results']]

    @authenticated
    async def get(self):
        assert self.rest_client
        usernames = await self.get_usernames()
        filter_options = {
            'status': ['processing', 'suspended', 'errors', 'complete', 'truncated'],
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
            dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
        except Exception:
            raise tornado.web.HTTPError(404, reason='Dataset not found')
        dataset_num = dataset['dataset']

        passkey = self.auth_access_token

        jobs = await self.rest_client.request('GET','/datasets/{}/job_counts/status'.format(dataset_id))
        tasks = await self.rest_client.request('GET','/datasets/{}/task_counts/status'.format(dataset_id))
        task_info = await self.rest_client.request('GET','/datasets/{}/task_counts/name_status'.format(dataset_id))
        task_stats = await self.rest_client.request('GET','/datasets/{}/task_stats'.format(dataset_id))
        try:
            config = await self.rest_client.request('GET','/config/{}'.format(dataset_id))
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
                    task_stats=task_stats, passkey=passkey)
