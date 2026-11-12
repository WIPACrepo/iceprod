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
    CONCURRENCY = 20    # max DELETE tasks in flight at once

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

    async def _delete_log(self, log_id: str) -> None:
        """Delete one log (DB doc + S3 object). A 404 means already deleted."""
        assert self._api_client
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
        total = len(ret)
        log_ids = iter(ret.keys())
        self._logger.info('deleting %d logs for dataset %s task %s', total, data.dataset_id, data.task_id)

        # Sliding window: at most CONCURRENCY tasks exist at any time.
        # A new task is created only after an in-flight one finishes.
        pending: set[asyncio.Task[None]] = set()
        done = 0
        next_report = self.BATCH_SIZE
        try:
            while True:
                # top up the window
                while len(pending) < self.CONCURRENCY:
                    log_id = next(log_ids, None)
                    if log_id is None:
                        break
                    pending.add(asyncio.create_task(self._delete_log(log_id)))
                if not pending:
                    break

                finished, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in finished:
                    task.result()  # re-raise a failed delete
                done += len(finished)

                # progress roughly every BATCH_SIZE deletes; 100 is written once at the end
                if next_report <= done < total:
                    await self._queue.update_payload(message.uuid, {
                        'progress': done * 100 // total
                    })
                    next_report = (done // self.BATCH_SIZE + 1) * self.BATCH_SIZE
        finally:
            # on error or cancellation, stop the remaining in-flight deletes
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending)

        self._logger.info('delete logs complete!')
        await self._queue.update_payload(message.uuid, {
            'progress': 100
        })
