import logging
import json

import pymongo.errors
import tornado.web

from ..base_handler import APIBase
from ..auth import authorization
from iceprod.roles_groups import ROLES, GROUP_PRIORITIES

logger = logging.getLogger('rest.auth')


def setup(handler_cfg):
    """
    Setup method for Config REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            ('/roles', MultiRoleHandler, handler_cfg),
            ('/groups', MultiGroupHandler, handler_cfg),
            ('/users', MultiUserHandler, handler_cfg),
            (r'/users/(?P<username>[^\/\?\#]+)', UserHandler, handler_cfg),
            (r'/users/(?P<username>[^\/\?\#]+)/priority', UserPriorityHandler, handler_cfg),
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
        ret = []
        for name, priority in GROUP_PRIORITIES.items():
            ret.append({
                'name': name,
                'priority': priority,
            })
        self.write({'results': ret})


class MultiUserHandler(APIBase):
    """
    Handle multi-user requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self):
        """
        Get a list of users.

        Returns:
            dict: {'results': list of users}
        """
        ret = await self.db.users.find(projection={'_id': False}).to_list(length=10000)
        self.write({'results': ret})
        self.finish()


class UserHandler(APIBase):
    """
    Handle individual user requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, username):
        """
        Get a user.

        Args:
            username (str): the user to get
        Returns:
            dict: user info
        """
        ret = await self.db.users.find_one({'username': username}, projection={'_id':False})
        if not ret:
            self.send_error(404, reason="User not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin'])
    async def put(self, username):
        """
        Add a User.

        Args:
            username (str): the user to delete
        Returns:
            dict: empty dict
        """
        try:
            await self.add_user(username)
        except pymongo.errors.DuplicateKeyError:
            raise tornado.web.HTTPError(409, reason='duplicate username')
        self.write({})
        self.finish()

    @authorization(roles=['admin'])
    async def delete(self, username):
        """
        Delete a user.

        Args:
            username (str): the user to delete
        Returns:
            dict: empty dict
        """
        await self.db.users.delete_one({'username': username})
        self.write({})
        self.finish()


class UserPriorityHandler(APIBase):
    """
    Handle individual user requests.
    """
    @authorization(roles=['admin'])
    async def put(self, username):
        """
        Add a User.

        Args:
            username (str): the user to modify
        Body Args:
            priority (float): the new priority
        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'priority' not in data:
            raise tornado.web.HTTPError(400, reason='missing priority in body')
        try:
            priority = float(data['priority'])
            if not 0 <= priority <= 1:
                raise ValueError()
        except Exception:
            raise tornado.web.HTTPError(400, reason='bad priority: should be between 0 and 1')

        ret = await self.db.users.update_one({'username': username}, {'$set': {'priority': priority}})
        if ret.matched_count < 1:
            raise tornado.web.HTTPError(400, reason='bad username')
        self.write({})
        self.finish()


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
