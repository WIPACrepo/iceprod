import logging
import uuid
from functools import wraps
from typing import Protocol

import pymongo
from rest_tools.server import (
    RestHandler,
    TokenAttributeRoleMappingProtocol,
    catch_error,
    token_attribute_role_mapping_auth,
)
from tornado.web import HTTPError

from iceprod.common.mongo import AsyncDatabase
from iceprod.roles_groups import GROUPS, ROLES

logger = logging.getLogger('rest-auth')

#: max number of datasets allowed in a single multi-dataset request
MAX_DATASET_IDS = 100

#: max values per `$in` query when batch checking attribute auth
ATTR_AUTH_BATCH_SIZE = 1000


class AuthProtocol(Protocol):
    auth_db: AsyncDatabase


class AttrAuthMixin(TokenAttributeRoleMappingProtocol, AuthProtocol, RestHandler):
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

    def _attr_auth_allowed(self, auth: dict, role: str) -> bool:
        """
        Check an `attr_auths` document against the requester's groups and username.

        This is the single access rule shared by all attribute auth checks.

        Args:
            auth (dict): attr_auths document
            role (str): the role to check for (read|write)

        Returns:
            bool: authorized
        """
        if role+'_groups' not in auth:
            return False
        try:
            return bool(set(auth.get(role+'_groups', [])) & set(self.auth_groups) or
                        self.current_user in auth.get(role+'_users', []))
        except (TypeError, ValueError):
            # malformed auth document
            logger.debug('bad attr auth: role=%r, auth=%r', role, auth, exc_info=True)
            return False

    async def check_attr_auth(self, arg, val, role):
        """
        Based on the request groups or username, check if they are allowed to
        access `arg`:`role`.

        Args:
            arg (str): attribute name to check
            val (str): attribute value
            role (str): the role to check for (read|write)
        """
        ret = None
        try:
            ret = await self.auth_db.attr_auths.find_one({arg: val}, projection={'_id':False})
            if not ret:
                raise HTTPError(403, reason='attr not found')
            elif role+'_groups' not in ret:
                raise HTTPError(403, reason='role not found')
            elif not self._attr_auth_allowed(ret, role):
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


    async def filter_attr_auth(self, arg, vals, role, token_role_bypass=['admin', 'system']) -> set:
        """
        Batch version of `manual_attr_auth`: find which values the requester may access.

        Uses one indexed `$in` query per `ATTR_AUTH_BATCH_SIZE` values,
        instead of one query per value.

        Args:
            arg (str): attribute name to check
            vals (iterable): attribute values
            role (str): the role to check for (read|write)
            token_role_bypass (list): token roles that bypass this auth (default: admin,system)

        Returns:
            set: the subset of `vals` that is authorized
        """
        vals = list(dict.fromkeys(vals))  # dedup, keep order
        if any(r in self.auth_roles for r in token_role_bypass):
            logger.debug('token role bypass')
            return set(vals)
        allowed = set()
        projection = {'_id': False, arg: True, role+'_groups': True, role+'_users': True}
        for i in range(0, len(vals), ATTR_AUTH_BATCH_SIZE):
            batch = vals[i:i+ATTR_AUTH_BATCH_SIZE]
            async for row in self.auth_db.attr_auths.find({arg: {'$in': batch}}, projection=projection):
                if self._attr_auth_allowed(row, role):
                    allowed.add(row[arg])
        return allowed

    async def get_readable_dataset_ids(self) -> list[str]:
        """
        Parse and authorize the `dataset_ids` argument for multi-dataset requests.

        Returns:
            list: dataset ids

        Raises:
            HTTPError 400: missing or too many dataset ids
            HTTPError 403: a dataset is not readable by the requester
        """
        dataset_ids = [d for d in self.get_argument('dataset_ids', '').split('|') if d]
        if not dataset_ids:
            raise HTTPError(400, reason='Missing dataset_ids')
        if len(dataset_ids) > MAX_DATASET_IDS:
            raise HTTPError(400, reason=f'Too many dataset_ids (max {MAX_DATASET_IDS})')
        allowed = await self.filter_attr_auth('dataset_id', dataset_ids, 'read')
        if any(d not in allowed for d in dataset_ids):
            raise HTTPError(403, reason='Unauthorized dataset_id')
        return dataset_ids


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
