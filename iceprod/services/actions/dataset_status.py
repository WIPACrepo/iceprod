from dataclasses import asdict, dataclass
import logging
from typing import Any

from tornado.web import HTTPError

from iceprod.common.mongo_queue import Message
from iceprod.server.states import dataset_prev_statuses
from iceprod.services.base import AuthData, BaseAction


logger = logging.getLogger('dataset_status')


@dataclass
class Fields:
    dataset_id: str
    action: str
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

        # check auth
        await self._manual_attr_auth('dataset_id', data.dataset_id, 'write', auth_data=auth_data)

        return await self._push(payload=asdict(data), priority=self.PRIORITY)

    async def run(self, message: Message) -> None:
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

        # look up job ids
        job_set = set()
        args = {'keys': 'job_id'}
        ret = await self._api_client.request('GET', f'/datasets/{data.dataset_id}/jobs', args)
        for row in ret.values():
            job_set.add(row['job_id'])

        # specific jobs
        job_ids = list(job_set)
        while job_ids:
            cur_jobs = job_ids[:5000]
            job_ids = job_ids[5000:]
            await self._api_client.request('POST', task_url, {'jobs': cur_jobs})
            await self._api_client.request('POST', job_url, {'jobs': cur_jobs})
            if job_ids:
                await self._queue.update_payload(message.uuid, {
                    'progress': len(job_ids)//len(job_set)
                })

        if data.action == 'suspend' and dataset['status'] in dataset_prev_statuses('suspended'):
            await self._api_client.request('PUT', f'/datasets/{data.dataset_id}/status', {'status': 'suspended'})
        elif data.action == 'suspend' and dataset['status'] in dataset_prev_statuses('processing'):
            await self._api_client.request('PUT', f'/datasets/{data.dataset_id}/status', {'status': 'processing'})

        self._logger.info("dataest status update complete!")
        await self._queue.update_payload(message.uuid, {
            'progress': 100
        })
