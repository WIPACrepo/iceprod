import asyncio
from dataclasses import asdict, dataclass
import logging
from typing import Any

from tornado.web import HTTPError

from iceprod.common.mongo_queue import Message
from iceprod.server.states import TASK_STATUS, dataset_prev_statuses
from iceprod.services.actions.submit import TokenSubmitter
from iceprod.services.base import AuthData, BaseAction


logger = logging.getLogger('task_status')


@dataclass
class Fields:
    dataset_id: str
    action: str
    initial_status: str | None = None
    job_ids: list[str] | None = None
    task_ids: list[str] | None = None
    progress: int = 0


class Action(BaseAction):
    PRIORITY = 10

    async def create(self, args: dict[str, Any], *, auth_data: AuthData) -> str:
        try:
            data = Fields(**args)
        except Exception as e:
            raise HTTPError(400, reason=str(e))

        if data.action not in ('hard_reset', 'reset', 'suspend'):
            raise HTTPError(400, reason='invalid action')
        if data.initial_status and data.initial_status not in TASK_STATUS:
            raise HTTPError(400, reason='invalid initial_status')
        if data.initial_status and data.task_ids:
            raise HTTPError(400, reason='cannot define both initial_status and task_ids')
        if data.initial_status and data.job_ids:
            raise HTTPError(400, reason='cannot define both initial_status and job_ids')
        if data.task_ids and data.job_ids:
            raise HTTPError(400, reason='cannot define both task_ids and job_ids')

        # check auth
        await self._manual_attr_auth('dataset_id', data.dataset_id, 'write', auth_data=auth_data)

        return await self._push(payload=asdict(data), priority=self.PRIORITY)

    async def run(self, message: Message) -> None:  # noqa: C901
        assert self._api_client and self._cred_client

        data = Fields(**message.payload)

        if data.action == 'hard_reset':
            task_url = f'/datasets/{data.dataset_id}/task_actions/bulk_hard_reset'
            job_url = f'/datasets/{data.dataset_id}/job_actions/bulk_hard_reset'
        elif data.action == 'reset':
            task_url = f'/datasets/{data.dataset_id}/task_actions/bulk_reset'
            job_url = f'/datasets/{data.dataset_id}/job_actions/bulk_reset'
        elif data.action == 'suspend':
            task_url = f'/datasets/{data.dataset_id}/task_actions/bulk_suspend'
            job_url = f'/datasets/{data.dataset_id}/job_actions/bulk_suspend'
        else:
            raise Exception('invalid set_status')

        dataset = await self._api_client.request('GET', f'/datasets/{data.dataset_id}')

        if data.action in ('reset', 'hard_reset') and dataset['status'] in dataset_prev_statuses('processing'):
            await self._api_client.request('PUT', f'/datasets/{data.dataset_id}/status', {'status': 'processing'})
            config = await self._api_client.request('GET', f'/config/{data.dataset_id}')
            ts = TokenSubmitter(
                logger=self._logger,
                cred_client=self._cred_client,
                jobs_submitted=dataset['jobs_submitted'],
                config=config,
                username=dataset['username'],
                group=dataset['group'],
            )
            if not await ts.tokens_exist(dataset_id=data.dataset_id):
                await ts.resubmit_tokens(dataset_id=data.dataset_id)

        if data.job_ids is not None:
            # specific jobs
            job_ids = data.job_ids.copy()
            while job_ids:
                cur_jobs = job_ids[:5000]
                job_ids = job_ids[5000:]
                if data.action != 'suspend':
                    await self._api_client.request('POST', task_url, {'jobs': cur_jobs})
                await self._api_client.request('POST', job_url, {'jobs': cur_jobs})
                if job_ids:
                    await self._queue.update_payload(message.uuid, {
                        'progress': 100 - len(job_ids)*100//len(data.job_ids)
                    })
        else:
            # look up job and task ids
            job_set = set()
            task_set = set()
            if data.task_ids is not None and len(data.task_ids) < 100:
                task_set = set(data.task_ids)
                if data.action != 'suspend':
                    async with asyncio.TaskGroup() as tg:
                        futures = set()
                        for task_id in data.task_ids:
                            futures.add(tg.create_task(self._api_client.request('GET', f'/datasets/{data.dataset_id}/tasks/{task_id}', {'keys': 'task_id|job_id'})))
                    for f in futures:
                        ret = f.result()
                        job_set.add(ret['job_id'])
            else:
                args = {'keys': 'task_id|job_id'}
                if data.initial_status:
                    args['status'] = data.initial_status
                ret = await self._api_client.request('GET', f'/datasets/{data.dataset_id}/tasks', args)
                for row in ret.values():
                    if data.task_ids and row['task_id'] not in data.task_ids:
                        continue
                    job_set.add(row['job_id'])
                    task_set.add(row['task_id'])

            job_ids = list(job_set)
            task_ids = list(task_set)
            total_jobs = len(job_ids)
            total_tasks = len(task_ids)

            if data.action != 'suspend':
                # update jobs
                while job_ids:
                    cur_jobs = job_ids[:5000]
                    job_ids = job_ids[5000:]
                    await self._api_client.request('POST', job_url, {'jobs': cur_jobs})
                    if job_ids:
                        await self._queue.update_payload(message.uuid, {
                            'progress': 50 - len(job_ids)*50//total_jobs
                        })

            # update tasks
            while task_ids:
                cur_tasks = task_ids[:5000]
                task_ids = task_ids[5000:]
                await self._api_client.request('POST', task_url, {'tasks': cur_tasks})
                if task_ids:
                    await self._queue.update_payload(message.uuid, {
                        'progress': 100 - len(task_ids)*50//total_tasks
                    })

        self._logger.info("task status update complete!")
        await self._queue.update_payload(message.uuid, {
            'progress': 100
        })
