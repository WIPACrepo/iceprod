from collections import defaultdict
import logging

from .base import authenticated, PublicHandler

logger = logging.getLogger('website-task')


class TaskBrowse(PublicHandler):
    """Handle /task urls"""
    @authenticated
    async def get(self, dataset_id):
        assert self.rest_client
        status = self.get_argument('status',default=None)
        if status:
            tasks = await self.rest_client.request('GET','/datasets/{}/tasks?status={}'.format(dataset_id,status))
            for t in tasks:
                if 'job_index' not in tasks[t]:
                    job = await self.rest_client.request('GET', '/datasets/{}/jobs/{}'.format(dataset_id, tasks[t]['job_id']))
                    tasks[t]['job_index'] = job['job_index']
            passkey = self.auth_access_token
            self.render('task_browse.html',tasks=tasks, passkey=passkey)
        else:
            status = await self.rest_client.request('GET','/datasets/{}/task_counts/status'.format(dataset_id))
            self.render('tasks.html',status=status)


class Task(PublicHandler):
    """Handle /task urls"""
    @authenticated
    async def get(self, dataset_id, task_id):
        assert self.rest_client
        status = self.get_argument('status', default=None)
        passkey = self.auth_access_token

        dataset = await self.rest_client.request('GET', '/datasets/{}'.format(dataset_id))
        task_details = await self.rest_client.request('GET','/datasets/{}/tasks/{}?status={}'.format(dataset_id, task_id, status))
        task_stats = await self.rest_client.request('GET','/datasets/{}/tasks/{}/task_stats?last=true'.format(dataset_id, task_id))
        if task_stats:
            task_stats = list(task_stats.values())[0]
        try:
            ret = await self.rest_client.request('GET','/datasets/{}/tasks/{}/logs?group=true'.format(dataset_id, task_id))
            logs = ret['logs']
            # logger.info("logs: %r", logs)
            ret2 = await self.rest_client.request('GET','/datasets/{}/tasks/{}/logs?keys=log_id|name|timestamp'.format(dataset_id, task_id))
            logs2 = ret2['logs']
            logger.info("logs2: %r", logs2)
            log_by_name = defaultdict(list)
            for log in sorted(logs2,key=lambda lg:lg['timestamp'] if 'timestamp' in lg else '',reverse=True):
                log_by_name[log['name']].append(log)
            for log in logs:
                if 'data' not in log or not log['data']:
                    log['data'] = ''
                log_by_name[log['name']][0] = log
        except Exception:
            log_by_name = {}
        self.render('task_detail.html', dataset=dataset, task=task_details, task_stats=task_stats, logs=log_by_name, passkey=passkey)
