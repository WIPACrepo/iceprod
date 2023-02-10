import logging
import json

import tornado.web

from ..base_handler import APIBase
from ..auth import authorization, ROLES, GROUPS

logger = logging.getLogger('rest.auth')


def setup(handler_cfg):
    """
    Setup method for Config REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, indexes
    """
    return {
        'routes': [
            ('/roles', MultiRoleHandler, handler_cfg),
            ('/groups', MultiGroupHandler, handler_cfg),
            ('/auths', AuthHandler, handler_cfg),
        ],
        'database': 'auth',
        'indexes': {
            'users': {
                'username_index': {'keys': 'username', 'unique': True},
            },
            'attr_auths': {
                'dataset_id_index': {'keys': 'dataset_id', 'unique': False},
            }
        }
    }


class MultiRoleHandler(APIBase):
    """
    Handle multi-role requests.
    """
    @authorization(roles=['admin'])
    async def get(self):
        """
        Get a list of roles.

        Returns:
            dict: {'results': list of roles}
        """
        self.write({'results': list(ROLES)})


class MultiGroupHandler(APIBase):
    """
    Handle multi-group requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self):
        """
        Get a list of groups.

        Returns:
            dict: {'results': list of groups}
        """
        self.write({'results': list(GROUPS)})


class AuthHandler(APIBase):
    """
    Handle authorization requests.
    """
    @authorization(roles=['admin', 'system'])
    async def post(self):
        """
        Do a remote auth lookup.  Raises a 403 on auth failure.

        Body Args:
            name (str): name of attr
            value (str): value of attr
            role (str): the role to check (read | write)
            username (str): username
            groups (list): groups for user

        Returns:
            dict: {result: ok}
        """
        data = json.loads(self.request.body)

        # validate first
        req_fields = {
            'name': str,
            'value': str,
            'role': str,
            'username': str,
            'groups': list,
        }
        for k in req_fields:
            if k not in data:
                raise tornado.web.HTTPError(400, reason='missing key: '+k)
            if not isinstance(data[k], req_fields[k]):
                r = 'key "{}" should be of type {}'.format(k, req_fields[k].__name__)
                raise tornado.web.HTTPError(400, reason=r)

        # check auth
        self.current_user = data['username']
        self.auth_groups = data['groups']
        await self.check_attr_auth(data['name'], data['value'], data['role'])
        self.write({})
