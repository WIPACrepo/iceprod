from datetime import datetime
import logging

import jwt
import pymongo
import tornado.web

from ..base_handler import APIBase
from ..auth import authorization

logger = logging.getLogger('rest.credentials')


def setup(handler_cfg):
    """
    Setup method for Credentials REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, database, indexes
    """
    return {
        'routes': [
            (r'/groups/(?P<groupname>\w+)/credentials', GroupCredentialsHandler, handler_cfg),
            (r'/users/(?P<username>\w+)/credentials', UserCredentialsHandler, handler_cfg),
        ],
        'database': 'auth',
        'indexes': {
            'group_creds': {
                'group_index': {'keys': [('groupname', pymongo.DESCENDING), ('url', pymongo.DESCENDING)], 'unique': True},
            },
            'user_creds': {
                'username_index': {'keys': [('username', pymongo.DESCENDING), ('url', pymongo.DESCENDING)], 'unique': True},
            }
        }
    }


def get_expiration(token):
    """
    Find a token's expiration date and time.

    Args:
        token (str): jwt token
    Returns:
        str: ISO datetime in UTC
    """
    return datetime.utcfromtimestamp(int(jwt.decode(token, options={"verify_signature": False})['exp'])).isoformat()


class BaseCredentialsHandler(APIBase):
    async def create(self, db, base_data):
        url = self.get_json_body_argument('url', type=str, strict_type=True)
        credential_type = self.get_json_body_argument('type', type=str, choices=['s3', 'oauth'], strict_type=True)

        base_data['url'] = url
        data = base_data.copy()
        data.update({
            'url': url,
            'type': credential_type,
        })

        if credential_type == 's3':
            buckets = self.get_json_body_argument('buckets', type=list, strict_type=True)
            access_key = self.get_json_body_argument('access_key', type=str, strict_type=True)
            secret_key = self.get_json_body_argument('secret_key', type=str, strict_type=True)
            if not buckets:
                raise tornado.web.HTTPError(400, reason='must specify bucket(s)')
            data['buckets'] = buckets
            data['access_key'] = access_key
            data['secret_key'] = secret_key

        elif credential_type == 'oauth':
            access_token = self.get_json_body_argument('access_token', default='', type=str, strict_type=True)
            refresh_token = self.get_json_body_argument('refresh_token', default='', type=str, strict_type=True)
            exp = self.get_json_body_argument('expire_date', default='', type=str, strict_type=True)
            if (not access_token) and not refresh_token:
                raise tornado.web.HTTPError(400, reason='must specify either access or refresh tokens')
            if not exp:
                if refresh_token:
                    try:
                        exp = get_expiration(refresh_token)
                    except Exception:
                        logger.warning('refresh get_expiration failed: %r', refresh_token, exc_info=True)
                elif access_token:
                    try:
                        exp = get_expiration(access_token)
                    except Exception:
                        logger.warning('get_expiration failed', exc_info=True)
                if not exp:
                    raise tornado.web.HTTPError(400, 'cannot automatically determine expire_date; must be given')
            data['access_token'] = access_token
            data['refresh_token'] = refresh_token
            data['expire_date'] = exp

        else:
            raise tornado.web.HTTPError(400, 'bad credential type')

        await db.update_one(
            base_data,
            {'$set': data},
            upsert=True,
        )


class GroupCredentialsHandler(BaseCredentialsHandler):
    """
    Handle group credentials requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self, groupname):
        """
        Get a group's credentials.

        Args:
            groupname (str): group name
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise tornado.web.HTTPError(403, 'unauthorized')

        ret = {}
        async for row in self.db.group_creds.find({'groupname': groupname}, {'_id': False}):
            ret[row['url']] = row
        self.write(ret)

    @authorization(roles=['admin', 'system', 'user'])
    async def post(self, groupname):
        """
        Set a group credential.  Overwrites an existing credential for the specified url.

        Common body args:
            url (str): url of controlled resource
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            expire_date (str): access token expiration, ISO date time in UTC (optional)

        Args:
            groupname (str): group name
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise tornado.web.HTTPError(403, 'unauthorized')

        await self.create(self.db.group_creds, {'groupname': groupname})
        self.write({})

    @authorization(roles=['admin', 'system', 'user'])
    async def delete(self, groupname):
        """
        Delete a group's credentials.

        Args:
            groupname (str): groupname
        Body args:
            url (str): (optional) url of controlled resource
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and groupname not in self.auth_groups:
            raise tornado.web.HTTPError(403, 'unauthorized')

        args = {'groupname': groupname}
        url = self.get_json_body_argument('url', default='', type=str)
        if url:
            args['url'] = url

        await self.db.group_creds.delete_many(args)
        self.write({})


class UserCredentialsHandler(BaseCredentialsHandler):
    """
    Handle user credentials requests.
    """
    @authorization(roles=['admin', 'system', 'user'])
    async def get(self, username):
        """
        Get a user's credentials.

        Args:
            username (str): username
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise tornado.web.HTTPError(403, 'unauthorized')

        ret = {}
        async for row in self.db.user_creds.find({'username': username}, {'_id': False}):
            ret[row['url']] = row
        self.write(ret)

    @authorization(roles=['admin', 'system', 'user'])
    async def post(self, username):
        """
        Set a user credential.  Overwrites an existing credential for the specified url.

        Common body args:
            url (str): url of controlled resource
            type (str): credential type (`s3` or `oauth`)

        S3 body args:
            buckets (list): list of buckets for this url, or [] if using virtual-hosted buckets in the url
            access_key (str): access key
            secret_key (str): secret key

        OAuth body args:
            access_token (str): access token
            refresh_token (str): refresh token
            expire_date (str): access token expiration, ISO date time in UTC (optional)

        Args:
            username (str): username
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise tornado.web.HTTPError(403, 'unauthorized')

        await self.create(self.db.user_creds, {'username': username})
        self.write({})

    @authorization(roles=['admin', 'system', 'user'])
    async def delete(self, username):
        """
        Delete a user's credentials.

        Args:
            username (str): username
        Body args:
            url (str): (optional) url of controlled resource
        Returns:
            dict: url: credential dict
        """
        if self.auth_roles == ['user'] and username != self.current_user:
            raise tornado.web.HTTPError(403, 'unauthorized')

        args = {'username': username}
        url = self.get_json_body_argument('url', default='', type=str)
        if url:
            args['url'] = url

        await self.db.user_creds.delete_many(args)
        self.write({})
