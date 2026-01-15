from dataclasses import asdict, dataclass
import json
from typing import Any

from tornado.web import HTTPError

from iceprod.common.mongo_queue import Message
from iceprod.rest.auth import attr_auth, authorization
from iceprod.services.base import AuthData, BaseAction, BaseHandler, HandlerTypes, TimeoutException
from .materialize import Materialize


class DatasetHandler(BaseHandler):
    @authorization(roles=['admin', 'system', 'user'])
    @attr_auth(arg='dataset_id', role='read')
    async def get(self, dataset_id: str):
        assert self.action
        type_ = self.action.action_type
        ret = await self.message_queue.lookup_by_payload({'type': type_, 'dataset_id': dataset_id})
        if not ret:
            self.send_error(404, reason="Request not found")
        else:
            self.write({
                'id': ret.uuid,
                'status': ret.status,
            })

    @authorization(roles=['admin', 'system', 'user'])
    @attr_auth(arg='dataset_id', role='write')
    async def post(self, dataset_id: str):
        assert self.action

        args = json.loads(self.request.body) if self.request.body else {}
        args['dataset_id'] = dataset_id

        auth = AuthData(
            username=self.current_user,
            groups=self.auth_groups,
            roles=self.auth_roles,
            token=self.auth_data,
        )

        id_ = await self.action.create(args, auth_data=auth)

        self.set_status(201)
        self.write({'result': id_})


@dataclass
class Fields:
    dataset_id: str
    set_status: str = 'idle'
    num: int = 1000


class Action(BaseAction):
    PRIORITY = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._materialize = Materialize(rest_client=self._api_client)

    def extra_handlers(self) -> HandlerTypes:
        """Return handlers"""
        return [
            (r'/datasets/(?P<dataset_id>\w+)', DatasetHandler, {}),
        ]

    async def create(self, args: dict[str, Any], *, auth_data: AuthData) -> str:
        """
        Validates a new materialization request.

        Deduplicates with existing requests with the same dataset_id.

        Arguments:
            args: dict of args for request

        Returns:
            str: request id
        """
        # validate request
        try:
            data = Fields(**args)
            if isinstance(data.num, str):
                data.num = int(data.num)
        except Exception as e:
            raise HTTPError(400, reason=str(e))

        # deduplicate on dataset_id
        return await self._push(payload=asdict(data), filter_payload={'dataset_id': data.dataset_id}, priority=self.PRIORITY)

    async def run(self, message: Message) -> None:
        """Run materialization"""
        data = message.payload
        if not self._materialize:
            return None
        self._logger.info(f'running materialization request: {data}')
        kwargs = {}
        if 'dataset_id' in data and data['dataset_id']:
            kwargs['only_dataset'] = data['dataset_id']
        if 'num' in data and data['num']:
            kwargs['num'] = data['num']
        if 'set_status' in data and data['set_status']:
            kwargs['set_status'] = data['set_status']
        ret = await self._materialize.run_once(**kwargs)
        self._logger.info('ret: %r', ret)

        if not ret:
            self._logger.warning('materialization request took too long, bumping to end of queue for another run')
            raise TimeoutException()
        return None
