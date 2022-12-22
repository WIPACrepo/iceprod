import logging
from functools import wraps
import os

from rest_tools.server import catch_error, token_attribute_role_mapping_auth

logger = logging.getLogger('rest-auth')


ROLES = {
    'admin': ['groups': '/tokens/IceProdAdmins'],
    'user': ['groups': '/institutions/IceCube.*'],
    'system': ['resource_access.iceprod.roles': 'iceprod-system'],
}

GROUPS = {
    'admin': ['groups': '/tokens/IceProdAdmins'],
    'simprod': ['groups': '/posix/simprod-submit'],
    'filtering': ['groups': '/posix/i3filter'],
    'users': ['groups': '/institutions/IceCube.*'],
}


if os.environ.get('CI_TESTING', False):
    logger.warning('*** Auth is DISABLED for CI testing! ***')
    def authorization(**_auth):
        def make_wrapper(method):
            @catch_error
            @wraps(method)
            async def wrapper(self, *args, **kwargs):
                return await method(self, *args, **kwargs)
            return wrapper
        return make_wrapper
else:
    authorization = token_attribute_role_mapping_auth(role_attrs=ROLES, group_attrs=GROUPS)


class AttrAuthMixin:
    async def set_attr_auth(self, arg, val, read_groups=None, write_groups=None, read_users=None, write_users=None):
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
            raise Exception(f'Invalid read group: {g}')
        if any(g for g in write_groups if g not in GROUPS):
            raise Exception(f'Invalid write group: {g}')

        await self.db.auths_dataset.find_one_and_update(
            {arg: val},
            {'$set': {'read_groups': read_groups, 'write_groups': write_groups,
                      'read_users': read_users, 'write_users': write_users}},
            upsert=True,
        )


def attr_auth(**_auth):
    """
    Check attribute auth.  Assume a user is already role-authorized.

    Args:
        arg (str): argument name to look up
        role (str): the role to append to the auth request (read|write)
        token_role_bypass (list): token roles that bypass this auth
    """
    def make_wrapper(method):
        @catch_error
        @wraps(method)
        async def wrapper(self, *args, **kwargs):
            arg = _auth['arg']
            role = _auth.get('role', '')
            token_role_bypass = _auth.get('token_role_bypass', [])

            if any(r in self.auth_roles for r in token_role_bypass):
                logger.debug('token role bypass')
                return await method(self, *args, **kwargs)

            # we need to ask /auths about this
            val = kwargs.get(arg, None)
            if (not val) or not isinstance(val, str):
                raise tornado.web.HTTPError(403, reason='authorization failed')
            try:
                
                # use the token from the incoming request
                http_client = RestClient(self.auth_url, token=self.auth_key)
                await http_client.request('GET', f'/auths/{arg}/actions/{role}')
            except Exception as exc:
                logger.info('/auths failure', exc_info=True)
                raise tornado.web.HTTPError(403, reason='authorization failed') from exc

            return await method(self, *args, **kwargs)
        return wrapper
    return make_wrapper
