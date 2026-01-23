import logging

import requests

from iceprod.core.jsonUtil import json_decode, json_encode
from .base import authenticated, PublicHandler

logger = logging.getLogger('website-submit')


class Config(PublicHandler):
    """Handle /config urls"""
    @authenticated
    async def get(self):
        assert self.rest_client
        dataset_id = self.get_argument('dataset_id', default=None)
        if not dataset_id:
            self.write_error(400,message='must provide dataset_id')
            return
        edit = int(self.get_query_argument('edit', default='0'))
        dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
        config = await self.rest_client.request('GET','/config/{}'.format(dataset_id))
        render_args = {
            'edit': edit,
            'dataset': dataset.get('dataset',''),
            'dataset_id': dataset_id,
            'config': json_encode(config, indent=2),
            'description': dataset.get('description',''),
            'error': '',
        }
        self.render('submit.html', **render_args)

    @authenticated
    async def post(self):
        assert self.rest_client
        dataset_id = self.get_body_argument('dataset_id', default=None)
        if not dataset_id:
            self.write_error(400,message='must provide dataset_id')
            return
        edit = int(self.get_body_argument('edit', default='0'))
        if not edit:
            self.write_error(400, message='cannot edit')
            return
        dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
        if not dataset:
            self.write_error(400, message='invalid dataset_id')
            return

        config_str = ''
        description = dataset.get('description', '')
        try:
            config_str = self.get_body_argument('submit_box')
            description = self.get_body_argument('description')
            config = json_decode(config_str)
            # if we decoded successfully, resort and reindent config
            config_str = json_encode(config, indent=2)
            config_str_compact = json_encode(config)

            args = {
                'dataset_id': dataset_id,
                'config': config_str_compact,
                'description': description,
            }
            ret = await self.rest_client.request('POST', '/actions/edit_config', args)
            id_ = ret['result']
        except requests.exceptions.HTTPError as e:
            logger.warning('failed edit', exc_info=True)
            try:
                error = e.response.json()['error']
            except Exception:
                error = e.response.text
            render_args = {
                'edit': edit,
                'dataset': dataset.get('dataset',''),
                'dataset_id': dataset_id,
                'config': config_str,
                'description': description,
                'error': error,
            }
            self.set_status(400)
            self.render('submit.html', **render_args)
        except Exception as e:
            logger.warning('failed submit', exc_info=True)
            render_args = {
                'edit': edit,
                'dataset': dataset.get('dataset',''),
                'dataset_id': dataset_id,
                'config': config_str,
                'description': description,
                'error': str(e),
            }
            self.set_status(400)
            self.render('submit.html', **render_args)
        else:
            render_args = {
                'message_id': id_,
                'status': 'queued',
                'message': '',
                'submit': False,
            }

            self.set_status(303)
            self.set_header('Location', f'/config/status/{id_}')
            self.render('submit_status.html', **render_args)


class ConfigStatus(PublicHandler):
    @authenticated
    async def get(self, id_):
        assert self.rest_client

        status = 'unknown'
        error = ''
        config_str = ''
        description = ''
        dataset = {}
        dataset_id = ''
        try:
            ret = await self.rest_client.request('GET', f'/actions/submit/{id_}')
            status = ret['status']
            error = ret.get('error_message', '')
            config_str = ret['payload']['config']
            description = ret['payload']['description']
            dataset_id = ret['payload']['dataset_id']

            dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
            if not dataset:
                raise Exception('invalid dataset_id')
        except Exception as e:
            error = str(e)

        if status == 'error':
            # fix json indenting
            try:
                config = json_decode(config_str)
                config_str = json_encode(config, indent=2)
            except Exception:
                pass
            render_args = {
                'edit': '1',
                'dataset': dataset.get('dataset',''),
                'dataset_id': dataset_id,
                'config': config_str,
                'description': description,
                'error': error,
            }
            self.set_status(400)
            self.render('submit.html', **render_args)
            return
        elif status == 'complete' and dataset_id:
            self.set_status(303)
            self.set_header('Location', f'/dataset/{dataset_id}')
            error = f'Dataset updated at /dataset/{dataset_id}'

        render_args = {
            'message_id': id_,
            'status': status,
            'message': error,
            'submit': False,
        }

        self.render('submit_status.html', **render_args)


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
    @authenticated
    async def get(self):
        config = json_encode(DEFAULT_CONFIG.copy(), indent=2)

        error = ''
        if e := self.get_argument('error', None):
            error = e

        render_args = {
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

        config_str = ''
        description = ''
        njobs = 1
        group = ''
        try:
            config_str = self.get_body_argument('submit_box')
            description = self.get_body_argument('description')
            group = self.get_body_argument('group')
            njobs = int(self.get_body_argument('number_jobs'))
            config = json_decode(config_str)
            # if we decoded successfully, resort and reindent config
            config_str = json_encode(config, indent=2)
            config_str_compact = json_encode(config)

            args = {
                'config': config_str_compact,
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
                'config': config_str,
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
                'config': config_str,
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
                'message': '',
                'submit': True,
            }

            self.set_status(303)
            self.set_header('Location', f'/submit/status/{id_}')
            self.render('submit_status.html', **render_args)


class SubmitStatus(PublicHandler):
    @authenticated
    async def get(self, id_):
        assert self.rest_client

        status = 'unknown'
        error = ''
        config = ''
        description = ''
        jobs_submitted = 1
        group = ''
        dataset_id = None
        try:
            ret = await self.rest_client.request('GET', f'/actions/submit/{id_}')
            status = ret['status']
            error = ret.get('error_message', '')
            config = ret['payload']['config']
            description = ret['payload']['description']
            jobs_submitted = int(ret['payload']['jobs_submitted'])
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
            self.set_status(303)
            self.set_header('Location', f'/dataset/{dataset_id}')
            error = f'Dataset created at /dataset/{dataset_id}'

        render_args = {
            'message_id': id_,
            'status': status,
            'message': error,
            'submit': True,
        }

        self.render('submit_status.html', **render_args)
