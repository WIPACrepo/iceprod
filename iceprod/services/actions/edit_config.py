from dataclasses import asdict, dataclass
import logging
from typing import Any

from tornado.web import HTTPError

from iceprod.common.mongo_queue import Message
from iceprod.core.config import Config as DatasetConfig, ValidationError
from iceprod.core.jsonUtil import json_decode, json_encode
from iceprod.core.parser import ExpParser
from iceprod.services.actions.submit import TokenSubmitter
from iceprod.services.base import AuthData, BaseAction


logger = logging.getLogger('edit_config')


@dataclass
class Fields:
    dataset_id: str
    config: str
    description: str | None = None


class Action(BaseAction):
    PRIORITY = 10

    async def create(self, args: dict[str, Any], *, auth_data: AuthData) -> str:
        try:
            data = Fields(**args)
        except Exception as e:
            raise HTTPError(400, reason=str(e))

        # check auth
        await self._manual_attr_auth('dataset_id', data.dataset_id, 'write', auth_data=auth_data)

        # validate config
        try:
            dc = DatasetConfig(json_decode(data.config))
            dc.fill_defaults()
            dc.validate()
            data.config = json_encode(dc.config)
        except ValidationError as e:
            raise HTTPError(400, reason=str(e))
        except Exception:
            raise HTTPError(400, reason='config is not valid')

        return await self._push(payload=asdict(data), priority=self.PRIORITY)

    async def run(self, message: Message) -> None:
        assert self._api_client and self._cred_client

        data = Fields(**message.payload)
        config = json_decode(data.config)

        dataset = await self._api_client.request('GET', f'/datasets/{data.dataset_id}')

        prev_config = await self._api_client.request('GET', f'/config/{data.dataset_id}')

        parser = ExpParser()
        config2 = config.copy()
        options = {
            'dataset': dataset['dataset'],
            'dataset_id': dataset['dataset_id'],
            'jobs_submitted': dataset['jobs_submitted']
        }
        options.update(config2['options'])
        config2['options'] = options

        # update tasks
        if len(config['tasks']) != len(prev_config['tasks']):
            raise Exception('cannot add/subtract tasks - create a new dataset')
        for task, prev_task in zip(config2['tasks'], prev_config['tasks']):
            config2['options']['task'] = task['name']
            if task['name'] != prev_task['name']:
                raise Exception('cannot edit task names - create a new dataset')
            if task['depends'] != prev_task['depends']:
                raise Exception('cannot edit task depends - create a new dataset')
            reqs = parser.parse(task['requirements'], env=config2)
            self._logger.info('new reqs: %r', reqs)
            if any('$' in val for val in reqs.values() if isinstance(val, str)):
                raise Exception('cannot update requirements - cannot parse expression')
            if reqs != prev_task['requirements']:
                await self._api_client.request('POST', f'/datasets/{data.dataset_id}/task_actions/bulk_requirements/{task["name"]}', reqs)

        # update config
        await self._api_client.request('PUT', f'/config/{data.dataset_id}', config)

        if data.description and data.description != dataset['description']:
            # update description
            args = {
                'description': data.description,
            }
            await self._api_client.request('PUT', f'/datasets/{data.dataset_id}/description', args)

        # check tokens
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

        self._logger.info('edit config complete')
