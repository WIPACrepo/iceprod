import logging

import tornado.web

from ..base_handler import APIBase
from ..auth import authorization

logger = logging.getLogger('rest.tokens')


def setup(handler_cfg):
    """
    Setup method for Tokens REST API.

    Args:
        handler_cfg (dict): args to pass to the route

    Returns:
        dict: routes, indexes
    """
    return {
        'routes': [
            (r'/users/(?P<username>\w+)/tokens', UserTokensHandler, handler_cfg),
        ],
        'database': 'tokens',
        'indexes': {
            'tokens': {
                'username_index': {'keys': 'username', 'unique': True},
            }
        }
    }


class UserTokensHandler(APIBase):
    """
    Handle user tokens requests.
    """
    @authorization(roles=['admin', 'system'])
    async def get(self, username):
        """
        Get a user's tokens.

        Args:
            username (str): username
        """
        async for row in self.db.tokens.find({'username': username}, {'_id': False}):
            self.write(row)
            return
        raise tornado.web.HTTPError(404, 'not found')

    @authorization(roles=['admin', 'system'])
    async def put(self, username):
        """
        Set a user's tokens.

        Body args:
            access_token (str): access token
            refresh_token (str): refresh token

        Args:
            username (str): username
        """
        access_token = self.get_json_body_argument('access_token', default='', type=str)
        refresh_token = self.get_json_body_argument('refresh_token', default='', type=str)

        data = {'refresh_token': refresh_token}
        if access_token:
            data['access_token']

        await self.db.tokens.update_one(
            {'username': username},
            {'$set': data},
            upsert=True,
        )
