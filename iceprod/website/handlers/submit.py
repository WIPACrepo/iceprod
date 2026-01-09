from collections import defaultdict
import logging
import os
import re
from typing import Any, Self
from urllib.parse import urlencode

import requests
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


DEFAULT_CONFIG = {
    "categories": [],
    "dataset": 0,
    "description": "",
    "difplus": None,
    "options": {},
    "parent_id": 0,
    "steering": None,
    "tasks": [],
    "version": 3.2
}


class Submit(PublicHandler):
    """Handle /submit urls"""

    def check_xsrf_cookie(self):
        logger.info('cookies: %r', self.request.cookies)
        super().check_xsrf_cookie()

    @authenticated
    async def get(self):
        config = DEFAULT_CONFIG.copy()

        error = ''
        if e := self.get_argument('error', None):
            error = e

        render_args = {
            'passkey': '',
            'edit': False,
            'dataset': '',
            'dataset_id': '',
            'config': config,
            'groups': self.auth_groups,
            'group': '',
            'description': '',
            'jobs_submitted': 1,
            'error': error,
        }
        self.render('submit.html', **render_args)

    @authenticated
    async def post(self):
        assert self.rest_client
        logger.info('new dataset submission!')

        if not self.session:
            raise tornado.web.HTTPError(500, 'session is missing')

        config = DEFAULT_CONFIG.copy()
        description = ''
        njobs = 1
        group = ''
        try:
            config_str = self.get_body_argument('submit_box')
            config = json_decode(config_str)
            description = self.get_body_argument('description')
            njobs = self.get_body_argument('number_jobs')
            group = self.get_body_argument('group')

            args = {
                'config': config_str,
                'description': description,
                'jobs_submitted': njobs,
                'username': self.current_user,
                'group': group,
            }
            ret = await self.rest_client.request('POST', '/actions/submit', args)
            id_ = ret['result']
        except requests.exceptions.HTTPError as e:
            logger.warning('failed submit', exc_info=True)
            try:
                error = e.response.json()['error']
            except Exception:
                error = e.response.text
            render_args = {
                'passkey': '',
                'edit': False,
                'dataset': '',
                'dataset_id': '',
                'config': config,
                'groups': self.auth_groups,
                'group': group,
                'description': description,
                'jobs_submitted': njobs,
                'error': error,
            }
            self.set_status(400)
            self.render('submit.html', **render_args)
        except Exception as e:
            logger.warning('failed submit', exc_info=True)
            render_args = {
                'passkey': '',
                'edit': False,
                'dataset': '',
                'dataset_id': '',
                'config': config,
                'groups': self.auth_groups,
                'group': group,
                'description': description,
                'jobs_submitted': njobs,
                'error': str(e),
            }
            self.set_status(400)
            self.render('submit.html', **render_args)
        else:
            render_args = {
                'message_id': id_,
                'status': 'queued',
                'message': ''
            }

            self.set_status(202)
            self.set_header('Location', f'/submit/status/{id_}')
            self.render('submit_status.html', **render_args)


class SubmitStatus(PublicHandler):
    @authenticated
    async def get(self, id_):
        assert self.rest_client

        status = 'unknown'
        error = ''
        config = DEFAULT_CONFIG.copy()
        description = ''
        jobs_submitted = 1
        group = ''
        dataset_id = None
        try:
            ret = await self.rest_client.request('GET', f'/actions/submit/{id_}')
            status = ret['status']
            error = ret.get('error_message', '')
            config = json_decode(ret['payload']['config'])
            description = ret['payload']['description']
            jobs_submitted = ret['payload']['jobs_submitted']
            group = ret['payload']['group']
            dataset_id = ret['payload'].get('dataset_id', '')
        except Exception as e:
            error = str(e)

        if status == 'error':
            render_args = {
                'passkey': '',
                'edit': False,
                'dataset': '',
                'dataset_id': '',
                'config': config,
                'groups': self.auth_groups,
                'group': group,
                'description': description,
                'jobs_submitted': jobs_submitted,
                'error': error,
            }
            self.render('submit.html', **render_args)
            return
        elif status == 'complete' and dataset_id:
            self.set_status(201)
            self.set_header('Location', f'/dataset/{dataset_id}')
            error = f'Dataset created at /dataset/{dataset_id}'

        render_args = {
            'message_id': id_,
            'status': status,
            'message': error,
        }

        self.render('submit_status.html', **render_args)

