import logging
import json
import uuid
import time

import tornado.web

from ..base_handler import APIBase
from ..auth import authorization, attr_auth, ROLES, GROUPS

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
            ('/users', MultiUserHandler, handler_cfg),
        ],
        'indexes': {
            'users': {
                'username_index': {'keys': 'username', 'unique': True},
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


class MultiUserHandler(APIBase):
    """
    Handle multi-user requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self):
        """
        Get a list of users.

        Body args:
            username: username to filter by

        Returns:
            dict: {'results': list of users}
        """
        filters = {}
        try:
            data = json.loads(self.request.body)
            if 'username' in data:
                filters['username'] = data['username']
        except Exception:
            pass

        ret = await self.db.users.find(filters, projection={'_id':False}).to_list(length=1000)
        self.write({'results':ret})

    @authorization(roles=['admin', 'system'])
    async def post(self):
        """
        Add a user.

        Body should contain all necessary fields for a user.
        """
        data = json.loads(self.request.body)
        if 'username' not in data:
            raise tornado.web.HTTPError(400, reason='missing username')
        ret = await self.db.users.find_one({'username': data['username']})
        if ret:
            raise tornado.web.HTTPError(400, reason='duplicate username')

        if 'priority' not in data:
            data['priority'] = 0.5
        ret = await self.db.users.insert_one(data)
        self.set_status(201)
        self.write({})
