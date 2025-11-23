import logging

from .base import authenticated, PublicHandler

logger = logging.getLogger('website-job')


class JobBrowse(PublicHandler):
    """Handle /job urls"""
    @authenticated
    async def get(self, dataset_id):
        assert self.rest_client
        status = self.get_argument('status',default=None)
        passkey = self.auth_access_token

        jobs = await self.rest_client.request('GET', '/datasets/{}/jobs'.format(dataset_id))
        if status:
            for t in list(jobs):
                if jobs[t]['status'] != status:
                    del jobs[t]
                    continue
        self.render('job_browse.html', jobs=jobs, passkey=passkey)


class Job(PublicHandler):
    """Handle /job urls"""
    @authenticated
    async def get(self, dataset_id, job_id):
        assert self.rest_client
        status = self.get_argument('status',default=None)
        passkey = self.auth_access_token

        dataset = await self.rest_client.request('GET', '/datasets/{}'.format(dataset_id))
        job = await self.rest_client.request('GET', '/datasets/{}/jobs/{}'.format(dataset_id,job_id))
        args = {'job_id': job_id}
        if status:
            args['status'] = status
        tasks = await self.rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args)
        job['tasks'] = list(tasks.values())
        job['tasks'].sort(key=lambda x:x['task_index'])
        self.render('job_detail.html', dataset=dataset, job=job, passkey=passkey)
