import asyncio
from collections import defaultdict
from dataclasses import asdict, dataclass
from functools import cached_property
import logging
import os
import re
from typing import Any

from rest_tools.client import RestClient
from tornado.web import HTTPError

from iceprod.core.jsonUtil import json_decode, json_encode
from iceprod.core.parser import ExpParser
from iceprod.common.mongo_queue import Message
from iceprod.core.config import Config as DatasetConfig, ValidationError
from iceprod.services.base import AuthData, BaseAction


logger = logging.getLogger('submit')


TOKEN_PREFIXES = {
    'osdf:///icecube/wipac': 'https://token-issuer.icecube.aq',
    'pelican://osg-htc.org/icecube/wipac': 'https://token-issuer.icecube.aq',
}


SCOPE_RE = re.compile(r'(.*?\$|.*?\d{4,}\-\d{4,}|.*?\/IceCube\/20\d\d\/filtered\/.*?\/[01]\d{3})')


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
        if match := SCOPE_RE.match(path):
            logger.info('matched scope from RE: %s', match.group(0))
            logger.info('original path: %s', path)
            path = match.group(0)
        path = os.path.dirname(path)
        if not path:
            path = '/'
        path = path.replace('//', '/')
    except Exception:
        logger.warning('error getting scope', exc_info=True)
        raise
    return f'{prefix}:{path}'


type TokenSubmitterTokens = list[Any]


class TokenSubmitter:
    def __init__(self, logger: logging.Logger, cred_client: RestClient, jobs_submitted: int, config: dict[str, Any], username: str, group: str):
        self._logger = logger
        self._cred_client = cred_client
        self._jobs_submitted = jobs_submitted
        self._config = config
        self._username = username
        self._group = group

    @cached_property
    def token_scopes(self) -> dict[str, set[str]]:
        config = self._config.copy()
        parser = ExpParser()

        # get token scopes
        token_scopes = defaultdict(set)
        options = {'jobs_submitted': self._jobs_submitted}
        options.update(config['options'])
        config['options'] = options
        for task in config['tasks']:
            config['options']['task'] = task['name']
            task_token_scopes = defaultdict(set)
            task_data = task['data'].copy()
            for tray in task.get('trays', []):
                task_data.extend(tray.get('data', []).copy())
                for module in tray.get('modules', []):
                    task_data.extend(module.get('data', []).copy())
            for data in task_data:
                self._logger.info('data: %r', data)
                if data['type'] != 'permanent':
                    continue
                remote = parser.parse(data['remote'], job=config)
                for prefix in TOKEN_PREFIXES:
                    if remote.startswith(prefix):
                        if scope := get_scope(remote[len(prefix):], data['movement']):
                            self._logger.info('adding scope %s for remote %s', scope, remote)
                            task_token_scopes[prefix].add(scope)
            # add in manual scopes
            for prefix,scope_str in task['token_scopes'].items():
                for scope in scope_str.split():
                    if scope and '$' not in scope:
                        task_token_scopes[prefix].add(scope)
            # set scopes per task
            for prefix,scopes in task_token_scopes.items():
                sorted_scope_str = ' '.join(sorted(scopes))
                if '$' in sorted_scope_str:
                    raise Exception('bad scope!')
                task['token_scopes'][prefix] = sorted_scope_str
                token_scopes[prefix].update(scopes)

        return token_scopes

    async def get_tokens(self) -> TokenSubmitterTokens:
        token_scopes = self.token_scopes

        if self._group == 'simprod':
            username = 'ice3simusr'
        elif self._group == 'filtering':
            username = 'i3filter'
        else:
            username = self._username

        # request the tokens
        self._logger.info('token requests: %r', dict(token_scopes))
        tokens = []
        for prefix,scopes in token_scopes.items():
            sorted_scope_str = ' '.join(sorted(scopes))
            args = {
                'username': username,
                'scope': sorted_scope_str,
                'url': TOKEN_PREFIXES[prefix],
                'transfer_prefix': prefix,
            }
            try:
                ret = await self._cred_client.request('POST', '/create', args)
            except Exception as e:
                if 'invalid_scope' in str(e):
                    raise Exception(f'Invalid scopes for {prefix}: {sorted_scope_str}')
                raise e
            tokens.append(ret)

        return tokens

    async def submit_tokens(self, dataset_id: str, tokens: TokenSubmitterTokens):
        async with asyncio.TaskGroup() as tg:
            for token in tokens:
                tg.create_task(self._cred_client.request('POST', f'/datasets/{dataset_id}/credentials', token))

    async def tokens_exist(self, dataset_id: str) -> bool:
        try:
            ret = await self._cred_client.request('GET', f'/datasets/{dataset_id}/credentials')
        except Exception:
            self._logger.info('cannot get existing tokens')
            ret = []
        existing_token_scopes = defaultdict(set)
        for tok in ret:
            if tok['transfer_prefix'] in TOKEN_PREFIXES:
                existing_token_scopes[tok['transfer_prefix']].update(tok['scope'].split())

        if self.token_scopes != existing_token_scopes:
            self._logger.warning('existing tokens do not match requested tokens\nexisting: %r\n requested: %r', dict(existing_token_scopes), dict(self.token_scopes))
            return False
        self._logger.info('tokens already exist')
        return True

    async def resubmit_tokens(self, dataset_id: str):
        """
        Resubmit tokens for an existing dataset.
        """
        tokens = await self.get_tokens()
        try:
            await self._cred_client.request('DELETE', f'/datasets/{dataset_id}/credentials')
        except Exception:
            pass
        await self.submit_tokens(dataset_id=dataset_id, tokens=tokens)


@dataclass
class Fields:
    config: str
    description: str
    jobs_submitted: int
    username: str
    group: str = 'users'
    extra_submit_fields: str | None = None
    dataset_id: str = ''


class Action(BaseAction):
    PRIORITY = 10

    async def create(self, args: dict[str, Any], *, auth_data: AuthData) -> str:
        # check auth
        if 'username' not in args:
            args['username'] = auth_data.username
        elif args['username'] != auth_data.username and 'admin' not in auth_data.roles:
            raise HTTPError(400, reason='cannot submit as a different user')

        try:
            data = Fields(**args)
            if isinstance(data.jobs_submitted, str):
                data.jobs_submitted = int(data.jobs_submitted)
        except Exception as e:
            raise HTTPError(400, reason=str(e))
        if data.group not in auth_data.groups:
            raise HTTPError(400, reason='not a member of selected group')

        # validate config
        try:
            dc = DatasetConfig(json_decode(data.config))
            dc.config['version'] = 3.2  # force 3.2 config validation
            dc.fill_defaults()
            dc.validate()
            data.config = json_encode(dc.config)
        except ValidationError as e:
            raise HTTPError(400, reason=str(e))
        except Exception:
            raise HTTPError(400, reason='config is not valid')

        return await self._push(payload=asdict(data), priority=self.PRIORITY)

    async def run(self, message: Message) -> None:
        assert self._api_client and self._cred_client
        data = message.payload

        submit_data = Fields(**data)
        config = json_decode(submit_data.config)

        # generate the tokens and verify permissions
        ts = TokenSubmitter(
            logger=self._logger,
            cred_client=self._cred_client,
            jobs_submitted=submit_data.jobs_submitted,
            config=config,
            username=submit_data.username,
            group=submit_data.group
        )
        tokens = await ts.get_tokens()

        # now submit the dataset
        tasks_per_job = len(config['tasks'])
        ntasks = submit_data.jobs_submitted * tasks_per_job
        args2 = {
            'description': submit_data.description,
            'jobs_submitted': submit_data.jobs_submitted,
            'tasks_submitted': ntasks,
            'tasks_per_job': tasks_per_job,
            'username': submit_data.username,
            'group': submit_data.group,
        }
        if submit_data.extra_submit_fields:
            extra_fields = json_decode(submit_data.extra_submit_fields)
            for name,value in extra_fields.items():
                if name not in args2:
                    args2[name] = value

        ret = await self._api_client.request('POST', '/datasets', args2)
        dataset_id = ret['result']
        await self._api_client.request('PUT', f'/config/{dataset_id}', config)

        # now submit creds
        await ts.submit_tokens(dataset_id=dataset_id, tokens=tokens)

        self._logger.info("submit complete!")

        await self._queue.update_payload(message.uuid, {
            'dataset_id': dataset_id
        })
