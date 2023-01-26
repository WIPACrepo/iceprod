import logging
from functools import wraps
import os

from rest_tools.server import catch_error, token_attribute_role_mapping_auth
from tornado.web import HTTPError

logger = logging.getLogger('rest-auth')


ROLES = {
    'admin': ['groups=/tokens/IceProdAdmins'],
    'user': ['groups=/institutions/IceCube.*'],
    'system': ['resource_access.iceprod.roles=iceprod-system'],
}

GROUPS = {
    'admin': ['groups=/tokens/IceProdAdmins'],
    'simprod': ['groups=/posix/simprod-submit'],
    'filtering': ['groups=/posix/i3filter'],
    'users': ['groups=/institutions/IceCube.*'],
}


class AttrAuthMixin:
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
        if any(g for g in read_groups if g not in GROUPS):
            raise Exception(f'Invalid read groups: {read_groups}')
        if any(g for g in write_groups if g not in GROUPS):
            raise Exception(f'Invalid write groups: {write_groups}')

        await self.db.attr_auths.find_one_and_update(
            {arg: val},
            {'$set': {'read_groups': read_groups, 'write_groups': write_groups,
                      'read_users': read_users, 'write_users': write_users}},
            upsert=True,
        )

    async def check_attr_auth(self, arg, val, role):
        """
        Based on the request groups or username, check if they are allowed to
        access `arg`:`role`.


        Args:
            arg (str): attribute name to check
            val (str): attribute value
            role (str): the role to check for (read|write)
        """
        ret = await self.db.attr_auths.find_one({arg: val}, projection={'_id':False})
        if not ret:
            raise HTTPError(403, reason='attr not found')
        elif role+'_groups' not in ret:
            raise HTTPError(403, reason='role not found')
        elif not (set(ret.get(role+'_groups', []))&set(self.auth_groups) or
            self.current_user in ret.get(role+'_users', [])):
            logger.debug('arg=%r, val=%r, role=%r, auth_groups=%r, current_user=%r, auths=%r', arg, val, role, self.auth_groups, self.current_user, ret.get(role+'_users', []))
            raise HTTPError(403, reason='authorization failed')


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