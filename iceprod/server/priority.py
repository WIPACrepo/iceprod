"""
Functions used to calculate dataset and task priority.
"""
import argparse
import asyncio
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.roles_groups import GROUP_PRIORITIES

logger = logging.getLogger('priority')


class Priority:
    def __init__(self, rest_client):
        self.rest_client = rest_client
        self.dataset_cache = {}
        self.user_cache = {}
        self.group_cache = {}

    async def _populate_dataset_cache(self):
        args = {
            'keys': 'dataset_id|priority|jobs_submitted|tasks_submitted|group|username',
            'status': 'processing',
        }
        self.dataset_cache = await self.rest_client.request('GET', '/datasets', args)
        dataset_ids = list(self.dataset_cache)
        args = {
            'keys': 'task_id|dataset_id|job_index|task_index',
            'status': 'idle|waiting|queued|processing',
        }
        futures = set()
        while dataset_ids:
            if len(futures) >= 20:
                done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
                futures = pending
                for f in done:
                    ret = await f
                    if ret:
                        d = list(ret.values())[0]['dataset_id']
                        self.dataset_cache[d]['tasks'] = {
                            k: {'task_index': ret[k]['task_index'], 'job_index': ret[k]['job_index']}
                            for k in ret
                        }

            dataset_id = dataset_ids.pop()
            self.dataset_cache[dataset_id]['tasks'] = {}
            t = asyncio.create_task(self.rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args))
            futures.add(t)
        while futures:
            done, pending = await asyncio.wait(futures)
            futures = pending
            for f in done:
                ret = await f
                try:
                    d = ret.pop('dataset_id')
                except KeyError:
                    continue
                self.dataset_cache[d]['tasks'] = ret

    async def _populate_dataset_task_cache(self, dataset_id, task_id):
        if dataset_id not in self.dataset_cache:
            return
        if task_id not in self.dataset_cache[dataset_id]['tasks']:
            args = {
                'keys': 'task_id|job_index|task_index',
            }
            ret = await self.rest_client.request('GET', f'/datasets/{dataset_id}/tasks/{task_id}', args)
            self.dataset_cache[dataset_id]['tasks'][task_id] = ret

    async def _populate_user_cache(self):
        ret = await self.rest_client.request('GET', '/users')
        for user in ret['results']:
            if 'priority' not in user:
                user['priority'] = 0.5
            self.user_cache[user['username']] = user

    async def _get_dataset(self, dataset_id):
        if not self.dataset_cache:
            await self._populate_dataset_cache()
        return self.dataset_cache[dataset_id]

    async def _get_max_dataset_prio_user(self, user):
        if not self.dataset_cache:
            await self._populate_dataset_cache()
        try:
            return max(d['priority'] for d in self.dataset_cache.values() if 'priority' in d and d['username'] == user)
        except ValueError:
            return 1.

    async def _get_max_dataset_prio_group(self, group):
        if not self.dataset_cache:
            await self._populate_dataset_cache()
        try:
            return max(d['priority'] for d in self.dataset_cache.values() if 'priority' in d and d['group'] == group)
        except ValueError:
            return 1.

    async def _get_user_prio(self, user):
        if not self.user_cache:
            await self._populate_user_cache()
        if user not in self.user_cache or 'priority' not in self.user_cache[user]:
            return 0.5
        return self.user_cache[user]['priority']

    async def _get_group_prio(self, group):
        return GROUP_PRIORITIES.get(group, 0.5)

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

    async def get_dataset_prio(self, dataset_id):
        """
        Calculate priority for a dataset.

        Args:
            dataset_id: dataset id

        Returns:
            float: priority between 0 and 1
        """
        try:
            dataset = await self._get_dataset(dataset_id)
        except KeyError:
            logger.warning(f'cannot find dataset {dataset_id}', exc_info=True)
            return 0.

        dataset_prio = dataset['priority']
        user = dataset['username']
        group = dataset['group']
        logger.debug(f'{dataset_id} dataset_prio: {dataset_prio}')
        max_dataset_prio = await self._get_max_dataset_prio_user(user)
        logger.debug(f'{dataset_id} max_dataset_prio: {max_dataset_prio}')
        max_dataset_prio_group = await self._get_max_dataset_prio_group(group)
        logger.debug(f'{dataset_id} max_dataset_prio_group: {max_dataset_prio_group}')

        if group == 'users':
            user_prio = await self._get_user_prio(user)
            logger.debug(f'{dataset_id} user_prio: {user_prio}')
            group_prio = await self._get_group_prio(group)
            logger.debug(f'{dataset_id} group_prio: {group_prio}')
        else:
            user_prio = 1.0
            group_prio = await self._get_group_prio(group)
            logger.debug(f'{dataset_id} group_prio: {group_prio}')

        num_dataset_jobs = dataset['jobs_submitted']
        logger.debug(f'{dataset_id} num_dataset_jobs: {num_dataset_jobs}')

        num_all_tasks = await self._get_num_tasks()
        logger.debug(f'{dataset_id} num_all_tasks: {num_all_tasks}')
        num_dataset_tasks = dataset['tasks_submitted']
        logger.debug(f'{dataset_id} num_dataset_tasks: {num_dataset_tasks}')
        num_dataset_tasks_avail = await self._get_num_tasks(dataset_id)
        logger.debug(f'{dataset_id} num_dataset_tasks_avail: {num_dataset_tasks_avail}')

        # general priority
        priority = 1.
        if max_dataset_prio > 0:
            priority *= dataset_prio / max_dataset_prio
            logger.info(f'{dataset_id} after dataset adjustment: {priority}')
        if group == 'users':
            logger.info('group is "users", so skipping dataset group adjustment')
        elif max_dataset_prio_group > 0:
            priority -= (1. - dataset_prio / max_dataset_prio_group) / 4.
            logger.info(f'{dataset_id} after dataset group adjustment: {priority}')
        priority *= user_prio**.5
        logger.info(f'{dataset_id} after user adjustment: {priority}')
        priority *= group_prio
        logger.info(f'{dataset_id} after group adjustment: {priority}')

        # bias against large datasets
        factor = (10000. / num_dataset_jobs)**.05 if num_dataset_jobs > 0 else 1.
        if factor > 1:
            factor = 1.
        priority *= factor
        logger.info(f'{dataset_id} after large dataset adjustment: {priority}')

        priority -= (1. * num_dataset_tasks_avail / num_all_tasks) / 4.
        logger.info(f'{dataset_id} after avail tasks adjustment: {priority}')

        if priority < 0.:
            priority = 0.
        elif priority > 1.:
            priority = 1.
        logger.info(f'{dataset_id} final priority: {priority}')

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
        try:
            dataset = await self._get_dataset(dataset_id)
        except Exception:
            logger.warning(f'cannot find dataset {dataset_id}', exc_info=True)
            return 0.
        if dataset['tasks_submitted'] < 1:
            return 0.

        await self._populate_dataset_task_cache(dataset_id, task_id)

        priority = await self.get_dataset_prio(dataset_id)

        task = dataset['tasks'][task_id]
        tasks_per_job = dataset['tasks_submitted'] / dataset['jobs_submitted']

        # bias towards finishing jobs
        priority += (1. * task['task_index'] / tasks_per_job) / 10.
        logger.info(f'{dataset_id}.{task_id} after finishing jobs adjustment: {priority}')

        # bias towards first jobs in dataset
        priority += (1. * (dataset['jobs_submitted'] - task['job_index']) / dataset['jobs_submitted']) / 100.
        logger.info(f'{dataset_id}.{task_id} after first jobs adjustment: {priority}')

        # boost towards first 100 jobs (or small datasets)
        if task['job_index'] < 100:
            priority += (100. - task['job_index']) / 100.
            logger.info(f'{dataset_id}.{task_id} after initial 100 jobs adjustment: {priority}')

        if priority < 0.:
            priority = 0.
        elif priority > 1.:
            priority = 1.
        logger.info(f'{dataset_id}.{task_id} final priority: {priority}')

        return priority


def main():
    parser = argparse.ArgumentParser(description='get priority')
    add_auth_to_argparse(parser)
    parser.add_argument('--debug', default=False, type=bool, help='debug enabled/disabled')
    parser.add_argument('dataset_id', help='dataset_id')
    parser.add_argument('--task_id', help='task_id (optional)')

    args = parser.parse_args()

    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=logging.DEBUG if args.debug else logging.INFO)

    rest_client = create_rest_client(args)

    p = Priority(rest_client)
    if args.task_id:
        ret = asyncio.run(p.get_task_prio(args.dataset_id, args.task_id))
        print('task priority', ret)
    else:
        ret = asyncio.run(p.get_dataset_prio(args.dataset_id))
        print('dataset priority', ret)


if __name__ == '__main__':
    main()
