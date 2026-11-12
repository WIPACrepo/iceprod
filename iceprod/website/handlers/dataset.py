import asyncio
import logging
from collections import Counter
from urllib.parse import urlencode

import tornado.web
from cachetools.func import ttl_cache
from rest_tools.client import RestClient

from iceprod.roles_groups import GROUPS
from iceprod.server.states import JOB_STATUS, TASK_STATUS

from .base import PublicHandler, authenticated

logger = logging.getLogger('website-dataset')


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


#: dataset statuses shown on the overview page (datasets that may have active tasks)
OVERVIEW_DATASET_STATUS = ['processing', 'suspended', 'errors']

#: max dataset ids per REST multi-dataset counts request (matches REST MAX_DATASET_IDS)
OVERVIEW_CHUNK_SIZE = 100


class UserOverview(UsernamesMixin, PublicHandler):
    """Handle /overview urls: task and job counts across a user's active datasets"""
    def initialize(self, *args, grafana_url: str = '', **kwargs):  # type: ignore[override]
        """
        Args:
            grafana_url (str): optional Grafana user overview dashboard url
        """
        super().initialize(*args, **kwargs)
        self.grafana_url = grafana_url

    async def get_counts(self, url, dataset_ids):
        """
        Get multi-dataset counts, chunking requests to the REST API limit.

        Args:
            url (str): REST multi-dataset counts url
            dataset_ids (list): dataset ids

        Returns:
            dict: {<dataset_id>: {<status>: num}}
        """
        assert self.rest_client
        chunks = [dataset_ids[i:i+OVERVIEW_CHUNK_SIZE] for i in range(0, len(dataset_ids), OVERVIEW_CHUNK_SIZE)]
        rets = await asyncio.gather(*[
            self.rest_client.request('GET', url, {'dataset_ids': '|'.join(c)}) for c in chunks
        ])
        ret = {}
        for r in rets:
            ret.update(r)
        return ret

    @authenticated
    async def get(self):
        assert self.rest_client
        usernames = await self.get_usernames()
        filter_options = {
            'status': OVERVIEW_DATASET_STATUS,
            'groups': list(GROUPS.keys()),
            'users': usernames,
        }
        filter_results = {n: self.get_arguments(n) for n in filter_options}
        for name, val in filter_results.items():
            if any(v not in filter_options[name] for v in val):
                raise tornado.web.HTTPError(400, reason='Bad filter '+name+' value')
        # default to the current user's datasets
        if not filter_results['users'] and not filter_results['groups']:
            filter_results['users'] = [self.current_user]

        args = {
            'keys': 'dataset_id|dataset|status|description|group|username',
            'status': '|'.join(filter_results['status'] or OVERVIEW_DATASET_STATUS),
        }
        if filter_results['users']:
            args['users'] = '|'.join(filter_results['users'])
        if filter_results['groups']:
            args['groups'] = '|'.join(filter_results['groups'])

        ret = await self.rest_client.request('GET', '/datasets', args)
        datasets = sorted((d for d in ret.values() if 'dataset' in d), key=lambda x: x['dataset'], reverse=True)
        dataset_ids = [d['dataset_id'] for d in datasets]

        task_counts, job_counts = {}, {}
        if dataset_ids:
            task_counts, job_counts = await asyncio.gather(
                self.get_counts('/task_counts/dataset_status', dataset_ids),
                self.get_counts('/job_counts/dataset_status', dataset_ids),
            )

        task_totals: Counter = Counter()
        job_totals: Counter = Counter()
        for d in dataset_ids:
            task_totals.update(task_counts.get(d, {}))
            job_totals.update(job_counts.get(d, {}))

        # link to the Grafana dashboard filtered to the selected users/groups
        grafana_url = ''
        if self.grafana_url:
            params = [('var-username', u) for u in filter_results['users']]
            params += [('var-group', g) for g in filter_results['groups']]
            grafana_url = self.grafana_url + ('&' if '?' in self.grafana_url else '?') + urlencode(params)

        self.render(
            'user_overview.html',
            datasets=datasets,
            task_counts=task_counts,
            job_counts=job_counts,
            task_totals=task_totals,
            job_totals=job_totals,
            task_statuses=list(TASK_STATUS),
            job_statuses=list(JOB_STATUS),
            grafana_url=grafana_url,
            filter_options=filter_options,
            filter_results=filter_results,
        )
