import logging
from functools import wraps
import uuid

import pymongo
from rest_tools.server import catch_error, token_attribute_role_mapping_auth
from tornado.web import HTTPError

from iceprod.roles_groups import ROLES, GROUPS

logger = logging.getLogger('rest-auth')


class AttrAuthMixin:
    async def add_user(self, username):
        """
        Add a user to the auth database.

        Args:
            username (str): username
        """
        user_id = uuid.uuid1().hex
        data = {
            'user_id': user_id,
            'username': username,
            'priority': 0.5,
        }
        await self.auth_db.users.insert_one(data)
        return user_id

    async def set_attr_auth(self, arg, val, read_groups=None, write_groups=None,
                            read_users=None, write_users=None):
        """
        Set the auth for an attribute.  Sets both read and write roles at once.

        Args:
            arg (str): attribute name to set
            val (str): attribute value to set
            read_groups (list): group names of users that should have read access
            write_groups (list): group names of users that should have write access
            read_users (list): user names of users that should have read access
            write_users (list): user names of users that should have write access
        """
        if not read_groups:
            read_groups = []
        if not write_groups:
            write_groups = []
        if not read_users:
            read_users = []
        if not write_users:
            write_users = []
        if any(g for g in read_groups if g not in GROUPS):
            raise Exception(f'Invalid read groups: {read_groups}')
        if any(g for g in write_groups if g not in GROUPS):
            raise Exception(f'Invalid write groups: {write_groups}')

        new_attrs = {'read_groups': read_groups, 'write_groups': write_groups,
                     'read_users': read_users, 'write_users': write_users}

        logger.debug('setting attr auths: arg=%r, val=%r, auths=%r', arg, val, new_attrs)
        ret = await self.auth_db.attr_auths.find_one_and_update(
            {arg: val},
            {'$set': new_attrs},
            upsert=True,
            return_document=pymongo.ReturnDocument.AFTER,
        )
        if ret is None:
            raise RuntimeError('failed to insert auth')

    async def check_attr_auth(self, arg, val, role):
        """
        Based on the request groups or username, check if they are allowed to
        access `arg`:`role`.


        Args:
            arg (str): attribute name to check
            val (str): attribute value
            role (str): the role to check for (read|write)
        """
        try:
            ret = await self.auth_db.attr_auths.find_one({arg: val}, projection={'_id':False})
            if not ret:
                raise HTTPError(403, reason='attr not found')
            elif role+'_groups' not in ret:
                raise HTTPError(403, reason='role not found')
            elif not (set(ret.get(role+'_groups', [])) & set(self.auth_groups) or
                      self.current_user in ret.get(role+'_users', [])):
                logger.debug('arg=%r, val=%r, role=%r, auth_groups=%r, current_user=%r, auths=%r', arg, val, role, self.auth_groups, self.current_user, ret.get(role+'_users', []))
                raise HTTPError(403, reason='authorization failed')
        except (TypeError, ValueError, KeyError):
            logger.debug('arg=%r, val=%r, role=%r, auths=%r', arg, val, role, ret, exc_info=True)
            raise HTTPError(403, reason='failed auth')

    async def manual_attr_auth(self, arg, val, role, token_role_bypass=['admin', 'system']):
        """
        Manually run check_attr_auth and return a boolean.

        Args:
            arg (str): attribute name to check
            val (str): attribute value
            role (str): the role to check for (read|write)
            token_role_bypass (list): token roles that bypass this auth (default: admin,system)

        Returns:
            bool: authorized
        """
        if any(r in self.auth_roles for r in token_role_bypass):
            logger.debug('token role bypass')
            return True
        try:
            await self.check_attr_auth(arg, val, role)
        except Exception:
            logger.debug('unauthorized')
            return False
        return True


#: match token roles and groups
authorization = token_attribute_role_mapping_auth(role_attrs=ROLES, group_attrs=GROUPS)


def attr_auth(**_auth):
    """
    Check attribute auth.  Assume a user is already role-authorized.

    Args:
        arg (str): argument name to look up
        role (str): the role to append to the auth request (read|write)
        token_role_bypass (list): token roles that bypass this auth (default: admin,system)
    """
    def make_wrapper(method):
        @catch_error
        @wraps(method)
        async def wrapper(self, *args, **kwargs):
            arg = _auth['arg']
            role = _auth.get('role', '')
            token_role_bypass = _auth.get('token_role_bypass', ['admin', 'system'])

            if any(r in self.auth_roles for r in token_role_bypass):
                logger.debug('token role bypass')
            else:
                # we need to do an auth check
                val = kwargs.get(arg, None)
                if (not val) or not isinstance(val, str):
                    raise HTTPError(403, reason='authorization failed')
                await self.check_attr_auth(arg, val, role)

            return await method(self, *args, **kwargs)
        return wrapper
    return make_wrapper
