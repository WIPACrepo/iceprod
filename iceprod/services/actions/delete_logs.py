"""
Delete logs for a dataset, or for a single task in a dataset.

Replaces the website's direct (streaming) calls to
`DELETE /datasets/{d}/logs` and `DELETE /datasets/{d}/tasks/{t}/logs`
with a queued action that reports progress.
"""
import asyncio
import logging
from dataclasses import asdict, dataclass
from typing import Any

import requests.exceptions
from tornado.web import HTTPError

from iceprod.common.mongo_queue import Message
from iceprod.services.base import AuthData, BaseAction

logger = logging.getLogger('delete_logs')


@dataclass
class Fields:
    dataset_id: str
    task_id: str | None = None  # None => delete all logs for the dataset
    progress: int = 0


class Action(BaseAction):
    PRIORITY = 5
    BATCH_SIZE = 1000   # log ids deleted between progress updates
    CONCURRENCY = 20    # concurrent DELETE requests within a batch

    async def create(self, args: dict[str, Any], *, auth_data: AuthData) -> str:
        try:
            data = Fields(**args)
        except Exception as e:
            raise HTTPError(400, reason=str(e))

        if not data.dataset_id or not isinstance(data.dataset_id, str):
            raise HTTPError(400, reason='invalid dataset_id')
        if data.task_id is not None and (not data.task_id or not isinstance(data.task_id, str)):
            raise HTTPError(400, reason='invalid task_id')
        data.progress = 0

        # same permission the REST log delete endpoints require
        await self._manual_attr_auth('dataset_id', data.dataset_id, 'write', auth_data=auth_data)

        # dedupe: an identical scope already queued/processing returns the existing id
        return await self._push(
            payload=asdict(data),
            filter_payload={'dataset_id': data.dataset_id, 'task_id': data.task_id},
            priority=self.PRIORITY,
        )

    async def _delete_log(self, log_id: str, sem: asyncio.Semaphore) -> None:
        """Delete one log (DB doc + S3 object). A 404 means already deleted."""
        assert self._api_client
        async with sem:
            try:
                await self._api_client.request('DELETE', f'/logs/{log_id}')
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    return
                raise

    async def run(self, message: Message) -> None:
        assert self._api_client

        data = Fields(**message.payload)

        # list log ids for the scope (system client => admin/system-only /logs is allowed)
        args = {'dataset_id': data.dataset_id, 'keys': 'log_id'}
        if data.task_id:
            args['task_id'] = data.task_id
        ret = await self._api_client.request('GET', '/logs', args)
        log_ids = list(ret.keys())
        total = len(log_ids)
        self._logger.info('deleting %d logs for dataset %s task %s', total, data.dataset_id, data.task_id)

        sem = asyncio.Semaphore(self.CONCURRENCY)
        done = 0
        while log_ids:
            batch = log_ids[:self.BATCH_SIZE]
            log_ids = log_ids[self.BATCH_SIZE:]
            await asyncio.gather(*(self._delete_log(log_id, sem) for log_id in batch))
            done += len(batch)
            if log_ids:
                await self._queue.update_payload(message.uuid, {
                    'progress': done * 100 // total
                })

        self._logger.info('delete logs complete!')
        await self._queue.update_payload(message.uuid, {
            'progress': 100
        })
