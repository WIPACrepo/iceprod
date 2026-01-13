import asyncio
from collections import defaultdict
from dataclasses import asdict, dataclass
import logging
import os
import re
from typing import Any

from tornado.web import HTTPError

from iceprod.core.jsonUtil import json_decode, json_encode
from iceprod.core.parser import ExpParser
from iceprod.common.mongo_queue import Message, Payload
from iceprod.core.config import Config as DatasetConfig, ValidationError
from iceprod.server.states import TASK_STATUS, dataset_prev_statuses
from iceprod.services.base import AuthData, BaseAction


logger = logging.getLogger('submit')


@dataclass
class Fields:
    dataset_id: str
    set_status: str
    hard: bool = False
    initial_status: str | None = None
    job_ids: list[str] | None = None
    task_ids: list[str] | None = None
    progress: int = 0


class Action(BaseAction):
    PRIORITY = 10

    async def create(self, args: dict[str, Any], *, auth_data: AuthData) -> str:
        try:
            data = Fields(**args)
            if not isinstance(data.hard, bool):
                raise Exception('hard must be a bool')
        except Exception as e:
            raise HTTPError(400, reason=str(e))

        if data.set_status not in ('reset', 'suspended'):
            raise HTTPError(400, reason='invalid set_status')
        if data.set_status != 'reset' and data.hard:
            raise HTTPError(400, reason='hard is only valid when set_status=reset')
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

    async def run(self, message: Message) -> None:
        assert self._api_client and self._cred_client

        data = Fields(**message.payload)

        if data.set_status == 'reset' and data.hard:
            task_url = f'/datasets/{data.dataset_id}/task_actions/bulk_hard_reset'
            job_url = f'/datasets/{data.dataset_id}/job_actions/bulk_hard_reset'
        elif data.set_status == 'reset':
            task_url = f'/datasets/{data.dataset_id}/task_actions/bulk_reset'
            job_url = f'/datasets/{data.dataset_id}/job_actions/bulk_reset'
        elif data.set_status == 'suspend':
            task_url = f'/datasets/{data.dataset_id}/task_actions/bulk_suspend'
            job_url = f'/datasets/{data.dataset_id}/job_actions/bulk_suspend'
        else:
            raise Exception('invalid set_status')

        dataset = await self._api_client.request('GET', f'/datasets/{data.dataset_id}')

        if data.job_ids is not None:
            # specific jobs
            job_ids = data.job_ids.copy()
            while job_ids:
                cur_jobs = job_ids[:5000]
                job_ids = job_ids[5000:]
                await self._api_client.request('POST', task_url, {'jobs': cur_jobs})
                await self._api_client.request('POST', job_url, {'jobs': cur_jobs})
                if job_ids:
                    await self._queue.update_payload(message.uuid, {
                        'progress': len(job_ids)//len(data.job_ids)
                    })
        else:
            # look up job and task ids
            job_ids = set()
            task_ids = set()
            if data.task_ids is not None and len(data.task_ids) < 100:
                async with asyncio.TaskGroup() as tg:
                    futures = set()
                    for task_id in data.task_ids:
                        futures.add(tg.create_task(self._api_client.request('GET', f'/datasets/{data.dataset_id}/tasks/{task_id}', {'keys': 'task_id|job_id'})))
                for f in futures:
                    ret = f.result()
                    job_ids.add(ret['job_id'])
                task_ids = set(data.task_ids)
            else:
                args = {'keys': 'task_id|job_id'}
                if data.initial_status:
                    args['status'] = data.initial_status
                ret = await self._api_client.request('GET', f'/datasets/{data.dataset_id}/tasks', args)
                for row in ret.values():
                    if data.task_ids and row['task_id'] not in data.task_ids:
                        continue
                    job_ids.add(row['job_id'])
                    task_ids.add(row['task_id'])

            job_ids = list(job_ids)
            task_ids = list(task_ids)
            total_jobs = len(job_ids)
            total_tasks = len(task_ids)

            # update jobs
            while job_ids:
                cur_jobs = job_ids[:5000]
                job_ids = job_ids[5000:]
                await self._api_client.request('POST', job_url, {'jobs': cur_jobs})
                if job_ids:
                    await self._queue.update_payload(message.uuid, {
                        'progress': len(job_ids)//total_jobs//2
                    })

            # update tasks
            while task_ids:
                cur_tasks = task_ids[:5000]
                task_ids = task_ids[5000:]
                await self._api_client.request('POST', task_url, {'tasks': cur_tasks})
                if task_ids:
                    await self._queue.update_payload(message.uuid, {
                        'progress': len(task_ids)//total_tasks + 50
                    })

        if data.set_status == 'suspend' and dataset['status'] in dataset_prev_statuses('suspended'):
            await self._api_client.request('PUT', f'/datasets/{data.dataset_id}/status', {'status': 'suspended'})
        elif data.set_status == 'suspend' and dataset['status'] in dataset_prev_statuses('processing'):
            await self._api_client.request('PUT', f'/datasets/{data.dataset_id}/status', {'status': 'suspended'})

        self._logger.info("task status update complete!")
        await self._queue.update_payload(message.uuid, {
            'progress': 100
        })
