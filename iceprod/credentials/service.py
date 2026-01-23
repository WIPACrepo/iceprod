import asyncio
import logging
import time

import httpx
import jwt

from iceprod.common.mongo import AsyncDatabase
from .util import ClientCreds, get_expiration

logger = logging.getLogger('refresh_service')


class ExchangeException(Exception):
    pass


class RefreshService:
    """
    OAuth refresh service

    Args:
        database: mongo database
        clients: json string of {url: [client id, client secret]}
        refresh_window: how long after last use to keep refreshing (in hours)
        expire_buffer: how long before expiration to refresh (in minutes)
        service_run_interval: seconds between refresh runs
    """
    def __init__(self, database: AsyncDatabase, clients: str, refresh_window: float, expire_buffer: int, service_run_interval: float):
        self.db = database
        self.clients = ClientCreds(clients)
        self.clients.validate()
        self.refresh_window = refresh_window * 3600.
        self.expire_buffer = expire_buffer

        self.start_time = time.time()
        self.service_run_interval = service_run_interval
        self.last_run_time = None
        self.last_success_time = None

    async def refresh_cred(self, cred):
        if not cred.get('refresh_token'):
            raise Exception('cred does not have a refresh token')

        openid_url = jwt.decode(cred['refresh_token'], options={"verify_signature": False})['iss']
        try:
            client = self.clients.get_client(openid_url)
        except KeyError:
            raise Exception('jwt issuer not registered')

        # try the refresh token
        args = {
            'grant_type': 'refresh_token',
            'refresh_token': cred['refresh_token'],
            'client_id': client.client_id,
        }
        if client.client_secret:
            args['client_secret'] = client.client_secret
        if cred.get('scope', None) is not None:
            args['scope'] = cred['scope']

        logging.warning('refreshing on %s with args %r', client.auth.token_url, args)

        new_cred = {}
        try:
            async with httpx.AsyncClient() as http_client:
                r = await http_client.post(client.auth.token_url, data=args)
            r.raise_for_status()
            req = r.json()
        except httpx.HTTPStatusError as exc:
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
            new_cred['scope'] = req.get('scope', cred.get('scope', ''))
            logger.debug('%r', new_cred)

        return new_cred

    async def exchange_cred(self, cred, *, client_id: str, new_scope: str | None = None):
        """Exchange a refresh token for one on another client, and return that."""
        if not cred.get('refresh_token'):
            raise Exception('cred does not have a refresh token')
        if not cred.get('access_token'):
            raise Exception('cred does not have an access token')

        openid_url = jwt.decode(cred['refresh_token'], options={"verify_signature": False})['iss']
        try:
            client = self.clients.get_client(openid_url)
        except KeyError:
            raise Exception('jwt issuer not registered')

        # try the refresh token
        args = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': client.client_id,
            'audience': client_id,  # target client
            'scope': new_scope if new_scope else cred['scope'],
            'subject_token': cred['access_token'],
            'subject_token_type': 'urn:ietf:params:oauth:token-type:access_token',
        }
        if client.client_secret:
            args['client_secret'] = client.client_secret

        logging.warning('exchanging on %s with args %r', client.auth.token_url, args)

        new_cred = cred.copy()
        try:
            async with httpx.AsyncClient() as http_client:
                r = await http_client.post(client.auth.token_url, data=args)
            r.raise_for_status()
            req = r.json()
        except httpx.HTTPStatusError as exc:
            logger.debug('%r', exc.response.text)
            try:
                req = exc.response.json()
            except Exception:
                req = {}
            error = req.get('error', '')
            desc = req.get('error_description', '')
            raise ExchangeException(f'Exchange request failed: {error} - {desc}') from exc
        else:
            logger.debug('OpenID token exchanged')
            new_cred['access_token'] = req['access_token']
            new_cred['refresh_token'] = req['refresh_token']
            new_cred['expiration'] = get_expiration(req['access_token'])
            new_cred['scope'] = req.get('scope', args.get('scope', ''))
            logger.debug('%r', new_cred)

        return new_cred

    async def create_cred(self, *, url: str, transfer_prefix: str, username: str, scope: str):
        """Do an impersonation token exchange workflow to generate a cred for a user."""
        try:
            client = self.clients.get_client(url)
        except KeyError:
            raise Exception('url not registered')
        if transfer_prefix not in client.transfer_prefix:
            raise Exception('client transfer prefix does not match')

        # try the refresh token
        args = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': client.client_id,
            'scope': scope,
            'requested_subject': username
        }
        if client.client_secret:
            args['client_secret'] = client.client_secret

        logging.warning('exchanging on %s with args %r', client.auth.token_url, args)

        new_cred = {
            'url': url,
            'type': 'oauth',
            'transfer_prefix': transfer_prefix,
            'scope': scope,
        }
        try:
            async with httpx.AsyncClient() as http_client:
                r = await http_client.post(client.auth.token_url, data=args)
            r.raise_for_status()
            req = r.json()
        except httpx.HTTPStatusError as exc:
            logger.debug('%r', exc.response.text)
            try:
                req = exc.response.json()
            except Exception:
                req = {}
            error = req.get('error', '')
            desc = req.get('error_description', '')
            raise Exception(f'Exchange request failed: {error} - {desc}') from exc
        else:
            logger.debug('OpenID token exchanged')
            new_cred['access_token'] = req['access_token']
            new_cred['refresh_token'] = req['refresh_token']
            new_cred['expiration'] = get_expiration(req['access_token'])
            if 'scope' in req and req['scope'] != scope:
                raise Exception('scopes do not match!')
            logger.debug('%r', new_cred)

        return new_cred

    def should_refresh(self, cred):
        logger.info('should_refresh for cred %r', cred)
        now = time.time()
        # refresh_exp = now + self.expire_buffer
        last_use_date = now - self.refresh_window

        if cred.get('last_use', 0) < last_use_date:
            return False
        return True

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
                'scope': {'$exists': True},
            }

            await self._run_once_user(filters)
            await self._run_once_group(filters)
            await self._run_once_dataset(filters)

            self.last_success_time = time.time()
        except Exception:
            logger.error('error running refresh', exc_info=True)

    async def _run_once_user(self, filters):
        user_creds = {}
        async for row in self.db.user_creds.find(filters, {'_id': False}):
            user_creds[row['url']] = row

        for cred in user_creds.values():
            if cred['type'] != 'oauth':
                continue
            if not cred['refresh_token']:
                logger.info('skipping non-refresh token for user %s, url %s', cred['username'], cred['url'])
                continue
            try:
                if self.should_refresh(cred):
                    args = await self.refresh_cred(cred)
                    await self.db.user_creds.update_one({'username': cred['username'], 'url': cred['url'], 'scope': cred['scope']}, {'$set': args})
                    logger.info('refreshed token for user %s, url %s, scope %s', cred['username'], cred['url'], cred['scope'])
                else:
                    logger.info('not yet time to refresh token for user %s, url %s, scope %s', cred['username'], cred['url'], cred['scope'])
            except Exception:
                logger.error('error refreshing token for user %s, url: %s, scope %s', cred['username'], cred['url'], cred['scope'], exc_info=True)

    async def _run_once_group(self, filters):
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

    async def _run_once_dataset(self, filters):
        dataset_creds = []
        async for row in self.db.dataset_creds.find(filters, {'_id': False}):
            dataset_creds.append(row)

        for cred in dataset_creds:
            if cred['type'] != 'oauth':
                continue
            if not cred['refresh_token']:
                logger.info('skipping non-refresh token for dataset %s, task, %s, url %s', cred['dataset_id'], cred.get('task_name',''), cred['url'])
                continue
            try:
                if self.should_refresh(cred):
                    args = await self.refresh_cred(cred)
                    cred_filter = {'dataset_id': cred['dataset_id'], 'scope': cred['scope'], 'url': cred['url']}
                    if task_name := cred.get('task_name', ''):
                        cred_filter['task_name'] = task_name
                    await self.db.dataset_creds.update_one(cred_filter, {'$set': args})
                    logger.info('refreshed token for dataset %s, task, %s, url %s', cred['dataset_id'], cred.get('task_name',''), cred['url'])
                else:
                    logger.info('not yet time to refresh token for dataset %s, task, %s, url %s', cred['dataset_id'], cred.get('task_name',''), cred['url'])
            except Exception:
                logger.error('error refreshing token for dataset %s, task, %s, url %s', cred['dataset_id'], cred.get('task_name',''), cred['url'], exc_info=True)
