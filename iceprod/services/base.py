
from dataclasses import dataclass
from functools import cached_property
import inspect
from logging import Logger
import logging
from typing import Any

import requests
from rest_tools.client import RestClient
from tornado.web import HTTPError

from iceprod.common.mongo_queue import AsyncMongoQueue, Message, Payload
from iceprod.rest.base_handler import APIBase


type HandlerTypes = list[tuple[str, Any, dict[str, Any]]]


class TimeoutException(Exception):
    def __init__(self, *args, update_payload: dict[str, Any] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.update_payload = update_payload


@dataclass(frozen=True)
class AuthData:
    username: str
    groups: list[str]
    roles: list[str]
    token: dict[str, Any]


async def check_attr_auth(arg: str, val: str, role: str, *, auth_data: AuthData, rest_client: RestClient, token_role_bypass: list[str] = ['admin', 'system']):
    """
    Manually run check_attr_auth and raise an error if we fail the auth check.

    Args:
        arg: attribute name to check
        val: attribute value
        role: the role to check for (read|write)
        auth_data: request auth data
        token_role_bypass: token roles that bypass this auth (default: admin,system)

    Raises:
        HTTPError: when not authorized
    """
    if any(r in auth_data.roles for r in token_role_bypass):
        logging.debug('token role bypass')
        return True
    args = {
        'name': arg,
        'value': val,
        'role': role,
        'username': auth_data.username,
        'groups': auth_data.groups,
    }
    try:
        await rest_client.request('POST', '/auths', args)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            raise HTTPError(403, reason='auth failed')
        else:
            raise HTTPError(500, reason='auth could not be completed')


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

    async def run(self, message: Message) -> None:
        raise NotImplementedError()

    async def _manual_attr_auth(self, arg: str, val: str, role: str, *, auth_data: AuthData):
        """
        Manually run check_attr_auth and return a boolean.

        Args:
            arg: attribute name to check
            val: attribute value
            role: the role to check for (read|write)
            auth_data: request auth data
            token_role_bypass: token roles that bypass this auth (default: admin,system)

        Raises:
            HTTPError: when not authorized
        """
        assert self._api_client
        await check_attr_auth(arg, val, role, auth_data=auth_data, rest_client=self._api_client)


class BaseHandler(APIBase):
    def initialize(self, *args, message_queue: AsyncMongoQueue, rest_client: RestClient, action: BaseAction | None = None, **kwargs):  # type: ignore[override]
        super().initialize(*args, **kwargs)
        self.message_queue = message_queue
        self.rest_client = rest_client
        self.action = action

    async def check_attr_auth(self, arg: str, val: str, role: str):
        auth = AuthData(
            username=self.current_user,
            groups=self.auth_groups,
            roles=self.auth_roles,
            token=self.auth_data,
        )
        await check_attr_auth(arg, val, role, auth_data=auth, rest_client=self.rest_client)
