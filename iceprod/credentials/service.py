import asyncio
import json
import logging
import time

from cachetools.func import ttl_cache
import httpx
import jwt
from rest_tools.utils.auth import OpenIDAuth

logger = logging.getLogger('refresh_service')


@ttl_cache(maxsize=256, ttl=3600)
def get_auth(url):
    return OpenIDAuth(url)


def get_expiration(token):
    """
    Find a token's expiration time.

    Args:
        token (str): jwt token
    Returns:
        float: expiration unix time
    """
    return jwt.decode(token, options={"verify_signature": False})['exp']


def is_expired(cred):
    """
    Check if an OAuth credential is expired.

    Will mark credential as expired if the access token has less than 5 seconds left.

    Args:
        cred (dict): credential dict
    Returns:
        bool: True if expired
    """
    if cred['type'] != 'oauth':
        return False
    return cred['expiration'] < (time.time() + 5)


class RefreshService:
    """
    OAuth refresh service

    Args:
        database (:motor.motor_asyncio.AsyncIOMotorClient:): mongo database
        clients (str): json string of {url: [client id, client secret]}
        refresh_window (float): how long after last use to keep refreshing (in hours)
        expire_buffer (float): how long before expiration to refresh (in hours)
        service_run_interval (float): seconds between refresh runs
    """
    def __init__(self, database, clients, refresh_window, expire_buffer, service_run_interval):
        self.db = database
        self.clients = json.loads(clients)
        self.refresh_window = refresh_window * 3600
        self.expire_buffer = expire_buffer * 3600

        self.start_time = time.time()
        self.service_run_interval = service_run_interval
        self.last_run_time = None
        self.last_success_time = None

    async def refresh_cred(self, cred):
        if not cred.get('refresh_token'):
            raise Exception('cred does not have a refresh token')

        openid_url = jwt.decode(cred['refresh_token'], options={"verify_signature": False})['iss']
        if openid_url not in self.clients:
            raise Exception('jwt issuer not registered')
        auth = get_auth(openid_url)

        # try the refresh token
        args = {
            'grant_type': 'refresh_token',
            'refresh_token': cred['refresh_token'],
            'client_id': self.clients[openid_url][0],
        }
        if len(self.clients[openid_url]) > 1:
            args['client_secret'] = self.clients[openid_url][1]

        new_cred = {}
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(auth.token_url, data=args)
            r.raise_for_status()
            req = r.json()
        except httpx.HTTPError as exc:
            logger.debug('%r', exc.response.text)
            try:
                req = exc.response.json()
            except Exception:
                req = {}
            error = req.get('error', '')
            raise Exception(f'Refresh request failed: {error}') from exc
        else:
            logger.debug('OpenID token refreshed')
            new_cred['access_token'] = req['access_token']
            new_cred['refresh_token'] = req['refresh_token']
            new_cred['expiration'] = get_expiration(req['access_token'])
            logger.debug('%r', new_cred)

        return new_cred

    def should_refresh(self, cred):
        now = time.time()
        refresh_exp = now + self.expire_buffer
        last_use_date = now - self.refresh_window

        if cred.get('last_use', 0) < last_use_date:
            return False
        return cred['expiration'] < refresh_exp

    async def run(self):
        """
        Run loop.
        """
        while True:
            await self._run_once()
            await asyncio.sleep(self.service_run_interval)

    async def _run_once(self):
        try:
            self.last_run_time = time.time()
            last_use_check = self.last_run_time - self.refresh_window
            exp_check = self.last_run_time + self.expire_buffer
            filters = {
                'type': 'oauth',
                'last_use': {'$gt': last_use_check},
                'expiration': {'$lt': exp_check},
            }

            user_creds = {}
            async for row in self.db.user_creds.find(filters, {'_id': False}):
                user_creds[row['url']] = row

            for cred in user_creds.values():
                if cred['type'] != 'oauth':
                    continue
                if not cred['refresh_token']:
                    logger.info('skipping non-refresh token for user %s, url %s', cred['username'], cred['url'])
                    continue
                logger.debug('cred: %r', cred)
                try:
                    if self.should_refresh(cred):
                        args = await self.refresh_cred(cred)
                        await self.db.user_creds.update_one({'username': cred['username'], 'url': cred['url']}, {'$set': args})
                        logger.info('refreshed token for user %s, url %s', cred['username'], cred['url'])
                    else:
                        logger.info('not yet time to refresh token for user %s, url %s', cred['username'], cred['url'])
                except Exception:
                    logger.error('error refreshing token for user %s, url: %s', cred['username'], cred['url'], exc_info=True)

            group_creds = {}
            async for row in self.db.group_creds.find(filters, {'_id': False}):
                group_creds[row['url']] = row

            for cred in group_creds.values():
                if cred['type'] != 'oauth':
                    continue
                if not cred['refresh_token']:
                    logger.info('skipping non-refresh token for group %s, url %s', cred['groupname'], cred['url'])
                    continue
                try:
                    if self.should_refresh(cred):
                        args = await self.refresh_cred(cred)
                        await self.db.group_creds.update_one({'groupname': cred['groupname'], 'url': cred['url']}, {'$set': args})
                        logger.info('refreshed token for group %s, url %s', cred['groupname'], cred['url'])
                    else:
                        logger.info('not yet time to refresh token for group %s, url %s', cred['groupname'], cred['url'])
                except Exception:
                    logger.error('error refreshing token for group %s, url: %s', cred['groupname'], cred['url'], exc_info=True)

            self.last_success_time = time.time()
        except Exception:
            logger.error('error running refresh', exc_info=True)
