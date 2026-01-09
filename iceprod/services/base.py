
from dataclasses import dataclass
from functools import cached_property
import inspect
from logging import Logger
from typing import Any

import requests
from rest_tools.client import RestClient
from tornado.web import HTTPError

from iceprod.common.mongo_queue import AsyncMongoQueue, Payload
from iceprod.rest.base_handler import APIBase


type HandlerTypes = list[tuple[str, Any, dict[str, Any]]]


class TimeoutException(Exception):
    pass


@dataclass(frozen=True)
class AuthData:
    username: str
    groups: list[str]
    roles: list[str]
    token: dict[str, Any]


class BaseAction:
    def __init__(self, queue: AsyncMongoQueue, logger: Logger, api_client: RestClient | None = None, cred_client: RestClient | None = None):
        self._queue = queue
        self._logger = logger
        self._api_client = api_client
        self._cred_client = cred_client

    @cached_property
    def action_type(self) -> str:
        mod = inspect.getmodule(self.__class__)
        if not mod or not mod.__package__:
            raise Exception('cannot get action type')
        if mod.__package__ == 'iceprod.services.actions':
            return mod.__name__.rsplit('.', 1)[1]
        else:
            return mod.__package__.rsplit('.', 1)[1]

    def extra_handlers(self) -> HandlerTypes:
        return []

    async def create(self, args: dict[str, Any], *, auth_data: AuthData) -> str:
        raise NotImplementedError()

    async def _push(self, *, payload: Payload, filter_payload: Payload | None = None, priority: int = 0) -> str:
        payload['type'] = self.action_type
        if not filter_payload:
            return await self._queue.push(payload=payload, priority=priority)
        else:
            return await self._queue.push_if_not_exists(payload=payload, filter_payload=filter_payload, priority=priority)

    async def run(self, data: Payload) -> None | Payload:
        raise NotImplementedError()


class BaseHandler(APIBase):
    def initialize(self, *args, message_queue: AsyncMongoQueue, rest_client: RestClient, action: BaseAction | None = None, **kwargs):  # type: ignore[override]
        super().initialize(*args, **kwargs)
        self.message_queue = message_queue
        self.rest_client = rest_client
        self.action = action

    async def check_attr_auth(self, arg, val, role):
        """
        Based on the request groups or username, check if they are allowed to
        access `arg`:`role`.

        Runs a remote query to the IceProd API.

        Args:
            arg (str): attribute name to check
            val (str): attribute value
            role (str): the role to check for (read|write)
        """
        assert self.rest_client
        args = {
            'name': arg,
            'value': val,
            'role': role,
            'username': self.current_user,
            'groups': self.auth_groups,
        }
        try:
            await self.rest_client.request('POST', '/auths', args)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                raise HTTPError(403, 'auth failed')
            else:
                raise HTTPError(500, 'auth could not be completed')
