from collections import defaultdict
import logging
import os
from typing import Any

import tornado.web
from rest_tools.server import RestHandler, OpenIDLoginHandler

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

    def get_client_by_id(self, _id: str) -> CredClient:
        for client in self.token_clients.values():
            if client.id == _id:
                return client
        raise KeyError()


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

    @staticmethod
    def get_scope(path: str, movement: str) -> str:
        prefix = 'storage.read' if movement == 'input' else 'storage.modify'
        try:
            if '$' in path:
                i = path.index('$')
                path = path[:i]
                logger.debug('get_scope: $ index = %d, path = %s', i, path)
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
        description = self.get_body_argument('description')

        config = self.DEFAULT_CONFIG.copy()
        try:
            # validate config
            config = json_decode(config_str)
            dc = DatasetConfig(config)
            dc.validate()

            parser = ExpParser()

            # get token requests
            token_requests = []
            for task in config['tasks']:
                task_token_scopes = defaultdict(set)
                for data in task['data']:
                    remote = parser.parse(data['remote'], job=config)
                    for prefix in self.token_clients:
                        if remote.startswith(prefix):
                            if scope := self.get_scope(remote[len(prefix):], data['movement']):
                                logger.info('adding scope %s for remote %s', scope, remote)
                                task_token_scopes[self.token_clients[prefix].id].add(scope)
                for _id,scopes in task_token_scopes.items():
                    token_requests.append((task['name'], _id, ' '.join(scopes)))

            njobs = self.get_body_argument('number_jobs')
            group = self.get_body_argument('group')

        except Exception as e:
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

        self.session['submit_config'] = config_str
        self.session['submit_description'] = description
        self.session['submit_tokens'] = json_encode([])
        self.session['submit_jobs'] = njobs
        self.session['submit_group'] = group

        if token_requests:
            # start oauth2 redirect dance
            first_request = token_requests.pop(0)
            self.session['token_requests'] = json_encode(token_requests)
            self.redirect(f'/submit/tokens/{first_request[1]}?task_name={first_request[0]}&scope={first_request[2]}')
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
        for task_name, _id, access, refresh in tokens:
            client = self.get_client_by_id(_id)
            args = {
                'url': client.url,
                'type': 'oauth',
                'access_token': access,
                'refresh_token': refresh,
            }
            await self.cred_rest_client.request('POST', f'/datasets/{dataset_id}/tasks/{task_name}/credentials', args)

        self.redirect(f'/dataset/{dataset_id}')


class TokenLogin(TokenClients, OpenIDLoginHandler, PublicHandler):
    def initialize(self, *args, login_url, oauth_url, token_client_id, **kwargs):
        super().initialize(*args, **kwargs)
        self.login_url = login_url
        self.oauth_url = oauth_url
        self.token_client_id = token_client_id

    def get_login_url(self):
        return self.login_url

    @authenticated
    async def get(self):
        if self.get_argument('error', False):
            err = self.get_argument('error_description', None)
            if not err:
                err = 'unknown oauth2 error'
            raise tornado.web.HTTPError(400, reason=err)
        elif self.get_argument('code', False):
            data = self._decode_state(self.get_argument('state'))
            task_name = data['task_name']
            tokens = await self.get_authenticated_user(
                redirect_uri=self.get_login_url(),
                code=self.get_argument('code'),
                state=data,
            )

            assert self.session
            prev_tokens: list = json_decode(self.session.get('submit_tokens', '[]'))
            prev_tokens.append((task_name, self.token_client_id, tokens['access_token'], tokens.get('refresh_token')))
            self.session['submit_tokens'] = json_encode(prev_tokens)

            # now figure out where to go next
            token_requests: list = json_decode(self.session['token_requests'])

            if token_requests:
                # start oauth2 redirect dance
                first_request = token_requests.pop(0)
                self.session['token_requests'] = json_encode(token_requests)
                self.redirect(f'/submit/tokens/{first_request[1]}?task_name={first_request[0]}&scope={first_request[2]}')
            else:
                # just process submission
                self.redirect('/submit/complete')

        else:
            self.oauth_client_scope = []
            if scope := self.get_argument('scope', None):
                self.oauth_client_scope.extend(scope.split())
            state = {
                'task_name': self.get_argument('task_name'),
            }
            self.start_oauth_authorization(state)
