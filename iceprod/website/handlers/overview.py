"""
User overview page: task and job counts across a user's active datasets.
"""

import asyncio
import logging
from collections import Counter
from urllib.parse import urlencode

import tornado.web

from iceprod.roles_groups import GROUPS
from iceprod.server.states import JOB_STATUS, TASK_STATUS

from .base import PublicHandler, authenticated
from .dataset import UsernamesMixin

logger = logging.getLogger('website-overview')


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
