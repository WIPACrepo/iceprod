from collections import defaultdict
import logging
import os
import re
from typing import Any, Self
from urllib.parse import urlencode

import tornado.web
from rest_tools.server import RestHandler, OpenIDLoginHandler, catch_error

from iceprod.core.config import Config as DatasetConfig
from iceprod.core.jsonUtil import json_encode, json_decode
from iceprod.core.parser import ExpParser
from iceprod.credentials.util import Client as CredClient
from .base import authenticated, PublicHandler

logger = logging.getLogger('website-submit')


class Config(PublicHandler):
    """Handle /config urls"""
    @authenticated
    async def get(self):
        assert self.rest_client
        dataset_id = self.get_argument('dataset_id',default=None)
        if not dataset_id:
            self.write_error(400,message='must provide dataset_id')
            return
        dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
        edit = self.get_argument('edit',default=False)
        if edit:
            passkey = self.auth_access_token
        else:
            passkey = None
        config = await self.rest_client.request('GET','/config/{}'.format(dataset_id))
        render_args = {
            'edit':edit,
            'passkey':passkey,
            'dataset': dataset.get('dataset',''),
            'dataset_id':dataset_id,
            'config':config,
            'description':dataset.get('description',''),
            'error': '',
        }
        self.render('submit.html',**render_args)


class TokenClients(RestHandler):
    def initialize(self, *args: Any, token_clients: dict[str, CredClient], **kwargs: Any):  # type: ignore[override]
        self.token_clients = token_clients
        return super().initialize(*args, **kwargs)


class Submit(TokenClients, PublicHandler):  # type: ignore[misc]
    """Handle /submit urls"""
    DEFAULT_CONFIG = {
        "categories": [],
        "dataset": 0,
        "description": "",
        "difplus": None,
        "options": {},
        "parent_id": 0,
        "steering": None,
        "tasks": [],
        "version": 3
    }

    def check_xsrf_cookie(self):
        logger.info('cookies: %r', self.request.cookies)
        super().check_xsrf_cookie()

    @authenticated
    async def get(self):
        config = self.DEFAULT_CONFIG.copy()
        if self.session and self.session.get('config'):
            try:
                config = json_decode(self.session['config'])
            except Exception:
                logger.info('cannot load config from session', exc_info=True)

        error = ''
        if e := self.get_argument('error', None):
            error = e
        elif self.session and self.session.get('error'):
            error = self.session['error']

        render_args = {
            'passkey': '',
            'edit': False,
            'dataset': '',
            'dataset_id': '',
            'config': config,
            'groups': self.auth_groups,
            'description': '',
            'error': error,
        }
        self.render('submit.html', **render_args)

    SCOPE_RE = re.compile(r'(.*?\$|.*?\d{4,}\-\d{4,}|.*?\/IceCube\/20\d\d\/filtered\/.*?\/[01]\d{3})')

    @staticmethod
    def get_scope(path: str, movement: str) -> str:
        """
        Auto-determines scope based on the path.

        Special cases to trim:
        * subdirectories: example: 000000-000999
        * run months: example: 0123
        * unexpanded variables: anything wth $
        """
        prefix = 'storage.read' if movement == 'input' else 'storage.modify'
        try:
            if match := Submit.SCOPE_RE.match(path):
                path = match.group(0)
            path = os.path.dirname(path)
            if not path:
                path = '/'
        except Exception:
            logger.info('error getting scope', exc_info=True)
            path = '/'
        return f'{prefix}:{path}'

    @authenticated
    async def post(self):
        logger.info('new dataset submission!')

        if not self.session:
            raise tornado.web.HTTPError(500, 'session is missing')

        config_str = self.get_body_argument('submit_box')
        logger.info('config_str: %r', config_str)
        description = self.get_body_argument('description')

        config = self.DEFAULT_CONFIG.copy()
        try:
            # validate config
            dc = DatasetConfig(json_decode(config_str))
            dc.config['version'] = 3.2  # force 3.2 config validation
            dc.fill_defaults()
            dc.validate()
            config = dc.config

            parser = ExpParser()

            # get token requests
            token_requests = []
            for task in config['tasks']:
                task_token_scopes = defaultdict(set)
                for data in task['data']:
                    if data['type'] != 'permanent':
                        continue
                    remote = parser.parse(data['remote'], job=config)
                    for prefix in self.token_clients:
                        if remote.startswith(prefix):
                            if scope := self.get_scope(remote[len(prefix):], data['movement']):
                                logger.info('adding scope %s for remote %s', scope, remote)
                                task_token_scopes[prefix].add(scope)
                # add in manual scopes
                for prefix,scope_str in task['token_scopes'].items():
                    for scope in scope_str.split():
                        if scope:
                            task_token_scopes[prefix].add(scope)
                # set token_requests
                for prefix,scopes in task_token_scopes.items():
                    sorted_scope_str = ' '.join(sorted(scopes))
                    task['token_scopes'][prefix] = sorted_scope_str
                    _id = self.token_clients[prefix].id
                    token_requests.append({
                        'task_name': task['name'],
                        'client_id': _id,
                        'prefix': prefix,
                        'scope': sorted_scope_str,
                    })

            njobs = self.get_body_argument('number_jobs')
            group = self.get_body_argument('group')

        except Exception as e:
            logger.warning('failed submit', exc_info=True)
            render_args = {
                'passkey': '',
                'edit': False,
                'dataset': '',
                'dataset_id': '',
                'config': config,
                'groups': self.auth_groups,
                'description': '',
                'error': str(e),
            }
            self.set_status(400)
            self.render('submit.html', **render_args)
            return

        self.session['submit_config'] = json_encode(config)
        self.session['submit_description'] = description
        self.session['submit_tokens'] = json_encode([])
        self.session['submit_jobs'] = njobs
        self.session['submit_group'] = group

        if token_requests:
            # start oauth2 redirect dance
            logger.info('token requests: %r', token_requests)
            first_request = token_requests.pop(0)
            self.session['token_requests'] = json_encode(token_requests)
            _id = first_request.pop('client_id')
            self.redirect(f'/submit/tokens/{_id}?{urlencode(first_request)}')
        else:
            # just process submission
            self.redirect('/submit/complete')


class SubmitDataset(TokenClients, PublicHandler):  # type: ignore[misc]
    @authenticated
    async def get(self):
        if not self.session:
            raise tornado.web.HTTPError(500, 'no session data')
        if not self.rest_client:
            raise tornado.web.HTTPError(500, 'no rest client')

        config_str = self.session['submit_config']
        config = json_decode(config_str)
        tokens = json_decode(self.session['submit_tokens'])

        description = self.session['submit_description']
        njobs = int(self.session['submit_jobs'])
        tasks_per_job = len(config['tasks'])
        ntasks = njobs * tasks_per_job
        group = self.session['submit_group']

        args = {
            'description': description,
            'jobs_submitted': njobs,
            'tasks_submitted': ntasks,
            'tasks_per_job': tasks_per_job,
            'group': group,
        }

        ret = await self.rest_client.request('POST', '/datasets', args)
        dataset_id = ret['result'].split('/')[2]
        await self.rest_client.request('PUT', f'/config/{dataset_id}', config)

        logger.info('token_clients: %r', self.token_clients)
        for token in tokens:
            client = self.token_clients[token['prefix']]
            args = {
                'url': client.url,
                'transfer_prefix': token['prefix'],
                'type': 'oauth',
                'access_token': token['access_token'],
                'refresh_token': token['refresh_token'],
            }
            task_name = token['task_name']
            await self.cred_rest_client.request('POST', f'/datasets/{dataset_id}/tasks/{task_name}/credentials', args)

        self.redirect(f'/dataset/{dataset_id}')


class TokenLogin(TokenClients, OpenIDLoginHandler, PublicHandler):  # type: ignore[misc]
    def initialize(self, *args, login_url: str, oauth_url: str, token_client: CredClient, **kwargs):  # type: ignore[override]
        self.login_url = login_url
        self.oauth_url = oauth_url
        self.token_client = token_client
        super().initialize(*args, **kwargs)

    def oauth_setup(self):
        # this is separate so it can be mocked out in testing
        auth = self.token_client.auth
        self._OAUTH_AUTHORIZE_URL = auth.provider_info['authorization_endpoint']
        self._OAUTH_ACCESS_TOKEN_URL = auth.provider_info['token_endpoint']
        # self._OAUTH_LOGOUT_URL = auth.provider_info['end_session_endpoint']
        self._OAUTH_USERINFO_URL = auth.provider_info['userinfo_endpoint']

    def validate_new_token(self, token) -> dict[str, Any]:
        return self.token_client.auth.validate(token)

    def get_login_url(self) -> str:
        return self.login_url

    @catch_error
    async def get(self: Self):
        username = await self.get_current_user_async()
        if not username:
            logger.info('user not logged in!')
            args = {}
            if scope := self.get_argument('scope', None):
                args['scope'] = scope
            if scope := self.get_argument('task_name', None):
                args['task_name'] = scope
            if scope := self.get_argument('prefix', None):
                args['prefix'] = scope
            next_url = tornado.httputil.url_concat(self.get_login_url(), args)
            url = tornado.httputil.url_concat('/login', {'next': next_url})
            self.redirect(url)
            return

        if self.get_argument('error', False):
            err = self.get_argument('error_description', None)
            if not err:
                err = 'unknown oauth2 error'
            raise tornado.web.HTTPError(400, reason=f'Error while getting tokens: {err}')
        elif self.get_argument('code', False):
            data = self._decode_state(self.get_argument('state'))
            task_name = data['task_name']
            prefix = data['prefix']
            scope = data['scope']
            try:
                tokens = await self.get_authenticated_user(
                    redirect_uri=self.get_login_url(),
                    code=self.get_argument('code'),
                    state=data,
                )
            except tornado.httpclient.HTTPClientError as e:
                try:
                    assert e.response
                    body = json_decode(e.response.body.decode('utf-8'))
                    logger.info('error gettting tokens for %s: %r', username, body)
                    if body['error'] == 'invalid_scope':
                        raise tornado.web.HTTPError(400, reason=f'Invalid permissions for task {task_name} tokens: '+scope)
                    else:
                        err = body.get('error_description', body['error'])
                        raise tornado.web.HTTPError(400, reason=f'Error while getting task {task_name} tokens: ' + err)
                except tornado.web.HTTPError:
                    raise
                except Exception:
                    logger.warning('error gettting tokens for %s', username, exc_info=e)
                    raise tornado.web.HTTPError(400, reason=f'Unknown error while getting task {task_name} tokens!')

            # check token scope
            try:
                token_data = self.token_client.auth.validate(tokens['access_token'])
            except Exception:
                logger.warning('invalid token', exc_info=True)
                raise tornado.web.HTTPError(400, reason=f'Error while getting task {task_name} tokens: access token is invalid')
            else:
                if set(scope.split()) != set(token_data['scope'].split()):
                    logger.warning('scope mismatch: %r != %r', scope, token_data['scope'])
                    raise tornado.web.HTTPError(400, reason=f'Error while getting task {task_name} tokens: scopes do not match')

            assert self.session
            prev_tokens: list = json_decode(self.session.get('submit_tokens', '[]'))
            prev_tokens.append({
                'task_name': task_name,
                'prefix': prefix,
                'client_id': self.token_client.id,
                'access_token': tokens['access_token'],
                'refresh_token': tokens.get('refresh_token')
            })
            self.session['submit_tokens'] = json_encode(prev_tokens)

            # now figure out where to go next
            token_requests: list = json_decode(self.session['token_requests'])

            if token_requests:
                # start oauth2 redirect dance
                first_request = token_requests.pop(0)
                self.session['token_requests'] = json_encode(token_requests)
                _id = first_request.pop('client_id')
                self.redirect(f'/submit/tokens/{_id}?{urlencode(first_request)}')
            else:
                # just process submission
                self.redirect('/submit/complete')

        else:
            self.oauth_client_scope = []
            if scope := self.get_argument('scope', None):
                self.oauth_client_scope.extend(scope.split())
            state = {
                'task_name': self.get_argument('task_name'),
                'prefix': self.get_argument('prefix'),
                'scope': scope,
            }
            extra_params = {"state": self._encode_state(state)}

            self.authorize_redirect(
                redirect_uri=self.get_login_url(),
                client_id=self.oauth_client_id,
                scope=self.oauth_client_scope,
                extra_params=extra_params,
                response_type='code'
            )
