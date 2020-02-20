"""
Functions used to calculate dataset and task priority.
"""


class Priority:
    def __init__(self, rest_client):
        self.rest_client = rest_client
        self.dataset_cache = {}
        self.user_cache = {}
        self.group_cache = {}

    async def _populate_dataset_cache(self):
        args = {
            'keys': 'dataset_id|priority|jobs_submitted|tasks_submitted|group|username',
            'status': 'processing|truncated',
        }
        self.dataset_cache = await self.rest_client.request('GET', '/datasets', args)
        for dataset_id in self.dataset_cache:
            args = {
                'keys': 'task_id|job_index|task_index',
                'status': 'waiting|queued|processing|reset',
            }
            ret = await self.rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args)
            self.dataset_cache[dataset_id]['tasks'] = ret

    async def _populate_dataset_task_cache(self, dataset_id, task_id):
        if dataset_id not in self.dataset_cache:
            await self._populate_dataset_cache()
        if task_id not in self.dataset_cache[dataset_id]['tasks']:
            args = {
                'keys': 'task_id|job_index|task_index',
            }
            ret = await self.rest_client.request('GET', f'/datasets/{dataset_id}/tasks/{task_id}', args)
            self.dataset_cache[dataset_id]['tasks'][task_id] = ret

    async def _populate_user_cache(self):
        ret = await self.rest_client.request('GET', '/users')
        for user in ret['results']:
            self.user_cache[user['username']] = user

    async def _populate_group_cache(self):
        ret = await self.rest_client.request('GET', '/groups')
        for group in ret['results']:
            self.group_cache[group['name']] = group

    async def _get_dataset(self, dataset_id):
        if not self.dataset_cache:
            await self._populate_dataset_cache()
        return self.dataset_cache[dataset_id]

    async def _get_max_dataset_prio_user(self, user):
        if not self.dataset_cache:
            await self._populate_dataset_cache()
        try:
            return max(d['priority'] for d in self.dataset_cache.values() if d['username'] == user)
        except ValueError:
            return 1.

    async def _get_user_prio(self, user):
        if not self.user_cache:
            await self._populate_user_cache()
        return self.user_cache[user]['priority']

    async def _get_group_prio(self, group):
        if not self.group_cache:
            await self._populate_group_cache()
        return self.group_cache[group]['priority']

    async def _get_max_user_prio_group(self, group):
        if not self.group_cache:
            await self._populate_group_cache()
        try:
            return max(g['priority'] for g in self.group_cache.values() if g['name'] == group)
        except ValueError:
            return 1.

    async def _get_max_group_prio(self):
        if not self.group_cache:
            await self._populate_group_cache()
        try:
            return max(g['priority'] for g in self.group_cache.values())
        except ValueError:
            return 1

    async def _get_num_tasks(self, dataset_id=None):
        if not self.dataset_cache:
            await self._populate_dataset_cache()
        num = 0
        for d in self.dataset_cache:
            if dataset_id is None or dataset_id == d:
                try:
                    num += len(self.dataset_cache[d]['tasks'])
                except ValueError:
                    pass
        return num

    async def _get_dataset(self, dataset_id):
        if not self.dataset_cache:
            await self._populate_dataset_cache()
        return self.dataset_cache[dataset_id]

    async def get_dataset_prio(self, dataset_id):
        """
        Calculate priority for a dataset.

        Args:
            dataset_id: dataset id

        Returns:
            float: priority between 0 and 1
        """
        dataset = await self._get_dataset(dataset_id)
        dataset_prio = dataset['priority']
        user = dataset['username']
        group = dataset['group']
        max_dataset_prio = await self._get_max_dataset_prio_user(user)

        user_prio = await self._get_user_prio(user)
        max_user_prio = await self._get_max_user_prio_group(group)

        group_prio = await self._get_group_prio(group)
        max_group_prio = await self._get_max_group_prio()

        num_all_tasks = await self._get_num_tasks()
        num_dataset_tasks = await self._get_num_tasks(dataset_id)

        # general priority
        priority = 1. * dataset_prio / max_dataset_prio * user_prio / max_user_prio * group_prio / max_group_prio

        # bias against large datasets
        priority -= (1. * num_dataset_tasks / num_all_tasks) / 3.

        if priority < 0.:
            priority = 0.
        elif priority > 1.:
            priority = 1.

        return priority

    async def get_task_prio(self, dataset_id, task_id):
        """
        Calculate priority for a task.

        Args:
            dataset_id: dataset id
            task_id: task id

        Returns:
            float: priority between 0 and 1
        """
        await self._populate_dataset_task_cache(dataset_id, task_id)

        priority = await self.get_dataset_prio(dataset_id)
    
        dataset = await self._get_dataset(dataset_id)
        task = dataset['tasks'][task_id]

        tasks_per_job = dataset['jobs_submitted'] / dataset['tasks_submitted']

        # bias towards finishing jobs
        priority += (1. * task['task_index'] / tasks_per_job) / 10.

        # bias towards first jobs in dataset
        priority += (1. * (dataset['jobs_submitted'] - task['job_index']) / dataset['jobs_submitted']) / 100.

        # boost towards first 100 jobs (or small datasets)
        if task['job_index'] < 100:
            priority += (100. - task['job_index']) / 100.

        if priority < 0.:
            priority = 0.
        elif priority > 1.:
            priority = 1.

        return priority
