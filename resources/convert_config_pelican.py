"""
IceProd Config Pelican Migration:
- data: all gsiftp:// moves to osdf:///
"""

import argparse
import asyncio
from collections import defaultdict
from getpass import getpass
import json
import os
import jsonschema
import re
import logging
from pathlib import Path
from pprint import pprint
import urllib.parse

import requests
from pymongo import MongoClient
from rest_tools.client import ClientCredentialsAuth, RestClient
from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.core.config import Config
from iceprod.core.parser import ExpParser
from iceprod.credentials.util import ClientCreds, Client as CredClient


notbool = lambda x: not x
strcommalist = lambda x: ','.join(x)

def gsiftp_replacement(value):
    if value.startswith('gsiftp://gridftp.icecube.wisc.edu'):
        new_value = 'osdf:///icecube/wipac' + value.split('gsiftp://gridftp.icecube.wisc.edu',1)[1]
        logging.debug('new value: %s', new_value)
        return new_value
    elif value.startswith('gsiftp://gridftp-scratch.icecube.wisc.edu'):
        #new_value = 'osdf:///icecube/wipac' + value.split('gsiftp://gridftp.icecube.wisc.edu',1)[1]
        logging.debug('old value: %s', value)
        raise Exception('scratch!')
        #return new_remote
    else:
        return value


SCOPE_RE = re.compile(r'(.*?(?:\d{4,}\-\d{4,}|\$|(?:IceCube\/20\d\d\/filtered\/.*?\/\d{4})))')

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
            path = match.group(0)
        path = os.path.dirname(path)
        if not path:
            path = '/'
    except Exception:
        logging.warning('error getting scope', exc_info=True)
        raise
    return f'{prefix}:{path}'


async def cred_token(dataset_id: str, username: str, task_name: str, prefix: str, scope: str, token_client: CredClient, cred_client: RestClient | None = None):
    logging.info('creating credential token for %s.%s prefix=%s, scope=%s', dataset_id, task_name, prefix, scope)
    if username == 'rsnihur':
        username = 'i3filter'
    else:
        username = 'ice3simusr'
    if cred_client:
        # not in dry-run
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'requested_subject': username,
            'requested_token_type': 'urn:ietf:params:oauth:token-type:refresh_token',
            'scope': scope,
        }
        req = requests.post(token_client.url+'/token', data=data, auth=(token_client.client_id, token_client.client_secret))
        try:
            req.raise_for_status()
        except Exception:
            logging.error('error response: %s', req.text)
            logging.error('request headers: %r', req.request.headers)
            logging.error('request body: %r', req.request.body)
            raise
        req_data = req.json()

        args = {
            'url': token_client.url,
            'transfer_prefix': prefix,
            'type': 'oauth',
            'refresh_token': req_data['refresh_token'],
            'scope': scope,
        }
        try:
            await cred_client.request('POST', f'/datasets/{dataset_id}/tasks/{task_name}/credentials', args)
        except Exception:
            await cred_client.request('PATCH', f'/datasets/{dataset_id}/tasks/{task_name}/credentials', args)


async def get_tokens(config, username, task_files: list[dict], token_clients: dict[str, CredClient], cred_client: RestClient | None = None):
    dataset_id = config['dataset_id']
    parser = ExpParser()

    # get token requests
    for task in config['tasks']:
        logging.info('get_tokens() for dataset %s task %s', dataset_id, task['name'])
        task_token_scopes = defaultdict(set)
        task_data = task['data'].copy()
        for tray in task.get('trays', []):
            task_data.extend(tray['data'].copy())
            for module in tray.get('modules', []):
                task_data.extend(module['data'].copy())
        if task['task_files']:
            logging.info('adding task_files')
            task_data.extend(task_files)
        for data in task_data:
            if data['type'] != 'permanent':
                continue
            remote = parser.parse(data['remote'], job=config)
            remote = gsiftp_replacement(remote)
            for prefix in token_clients:
                if remote.startswith(prefix):
                    if scope := get_scope(remote[len(prefix):], data['movement']):
                        logging.debug('adding scope %s for remote %s', scope, remote)
                        task_token_scopes[prefix].add(scope)
        # add in manual scopes
        for prefix,scope_str in task['token_scopes'].items():
            for scope in scope_str.split():
                if scope:
                    task_token_scopes[prefix].add(scope)
        # set token_requests
        logging.info('scopes: %r', task_token_scopes)
        for prefix,scopes in task_token_scopes.items():
            sorted_scope_str = ' '.join(sorted(scopes))
            task['token_scopes'][prefix] = sorted_scope_str

            await cred_token(dataset_id, username, task['name'], prefix, sorted_scope_str, token_clients[prefix], cred_client)


def convert(config):
    logging.info('Now converting config')

    if (steering := config.get('steering', {})) and steering.get('system', None):
        del config['steering']['system']

    for param in config.get('steering', {}).get('parameters',{}):
        value = config['steering']['parameters'][param]
        if isinstance(value, str):
            config['steering']['parameters'][param] = gsiftp_replacement(value)

    def data_cleaner(obj):
        if 'data' in obj:
            if (data := obj['data']) and isinstance(data, list):
                for d in data:
                    if remote := d.get('remote',None):
                        d['remote'] = gsiftp_replacement(remote)

    for i,task in enumerate(config.get('tasks', [])):
        if 'name' not in task:
            task['name'] = f'Task{i}'
        data_cleaner(task)
        for tray in task.get('trays', []):
            data_cleaner(tray)
            for module in tray.get('modules', []):
                data_cleaner(module)

    return config


def conversion(config, output=None):
    logging.info('Converting config')
    new_config = convert(config)
    logging.info('Validating config')
    c = Config(new_config)
    c.fill_defaults()
    c.validate()
    logging.info('Config validated!')

    if output == '-':
        print(json.dumps(c.config, indent=2, sort_keys=True))
    elif output:
        with open(output, 'w') as f:
            json.dump(c.config, f, indent=2, sort_keys=True)
    return c.config


def do_mongo(server, *, dataset_id=None, output=None, token_clients: dict[str, CredClient], cred_client=None, dryrun=False):

    client = MongoClient(server)
    db = client.config

    datasets = {}
    search = {}

    if dataset_id:
        search = {'dataset_id': dataset_id}

    for d in client.datasets.datasets.find(search, projection={"_id": False, "dataset_id": True, "dataset": True, "status": True, "username": True}):
        if d['dataset'] > 22000 or dataset_id:
            datasets[d['dataset_id']] = d

    for config in db.config.find(search, projection={'_id': False}):
        if config['dataset_id'] not in datasets:
            continue
        dataset = datasets[config['dataset_id']]
        try:
            logging.warning('Processing %r', dataset['dataset'])
            new_config = conversion(config, output=output)
        except jsonschema.ValidationError:
            if dataset['status'] != 'processing' and not config['tasks'][0].get('trays'):
                logging.info('skipping dataset %r due to errors', dataset['dataset'])
                continue
            print(json.dumps(config, indent=2, sort_keys=True))
            raise
        except Exception:
            print(json.dumps(config, indent=2, sort_keys=True))
            raise
        assert config['dataset_id'] == new_config['dataset_id']

        logging.warning('Dataset %r config okay, now getting tokens', dataset['dataset'])
        task_files = client.tasks.dataset_files.find(search).to_list(1000)
        asyncio.run(get_tokens(new_config, dataset['username'], task_files, token_clients=token_clients, cred_client=cred_client))

        if output == '-':
            print(json.dumps(new_config, indent=2, sort_keys=True))
        elif output:
            with open(output, 'w') as f:
                json.dump(new_config, f, indent=2, sort_keys=True)

        if not dryrun:
            db.config.find_one_and_replace({'dataset_id': config['dataset_id']}, new_config)
            if task_files:
                client.tasks.dataset_files.update_many(search, {
                    '$set': {
                        'remote': {
                            '$replaceOne': {
                                'input': '$remote',
                                'find': 'gsiftp://gridftp.icecube.wisc.edu',
                                'replacement': 'osdf:///icecube/wipac'
                            }
                        }
                    }
                })
        logging.warning('Completed %r', dataset['dataset'])


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--config', help='config json file')
    group.add_argument('--mongo-server', default=None, help='mongodb server address')
    parser.add_argument('-o', '--output', default=None, help='output config json to file (or "-" for stdout)')
    parser.add_argument('--dataset-id', default=None, help='dataset id (for mongo)')
    parser.add_argument('--token-clients', required=True, help='json of token client info')
    add_auth_to_argparse(parser)
    parser.add_argument('--dry-run', action='store_true', default=False, help='do a dry run (for mongo)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    token_clients = ClientCreds(args.token_clients).get_clients_by_prefix()

    if args.dry_run:
        cred_client = None
    else:
        cred_client = create_rest_client(args)

    if args.config:
        logging.info('Opening config at %s', args.config)
        with open(args.config) as f:
            config = json.load(f)
        conversion(config, output=args.output)
    elif args.mongo_server:
        do_mongo(args.mongo_server, dataset_id=args.dataset_id, output=args.output, token_clients=token_clients, cred_client=cred_client, dryrun=args.dry_run)

if __name__ == '__main__':
    main()