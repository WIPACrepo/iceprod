"""
IceProd Config Pelican Migration:
- data: all gsiftp:// moves to osdf:///
"""

import argparse
import asyncio
from collections import defaultdict
import json
import jsonschema
import logging

from pymongo import MongoClient
from rest_tools.client import RestClient
from iceprod.client_auth import add_auth_to_argparse, create_rest_client
from iceprod.core.config import Config
from iceprod.core.parser import ExpParser
from iceprod.services.actions.submit import TOKEN_PREFIXES, get_scope


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


async def cred_token(dataset_id: str, username: str, prefix: str, scope: str, token_url: str, cred_client: RestClient | None = None):
    logging.info('creating credential token for %s prefix=%s, scope=%s', dataset_id, prefix, scope)
    if username == 'rsnihur':
        username = 'i3filter'
    else:
        username = 'ice3simusr'
    if cred_client:
        # not in dry-run

        args = {
            'url': token_url,
            'transfer_prefix': prefix,
            'type': 'oauth',
        }
        ret = await cred_client.request('GET', f'/datasets/{dataset_id}/credentials', args)
        if ret and len(ret) == 1 and ret[0]['scope'] == scope:
            logging.warning('cred tokens arlready present')
        else:
            logging.warning('need to add cred tokens')
            args = {
                'username': username,
                'scope': scope,
                'url': token_url,
                'transfer_prefix': prefix,
            }
            ret = await cred_client.request('POST', '/create', args)
            await cred_client.request('DELETE', f'/datasets/{dataset_id}/credentials')
            await cred_client.request('POST', f'/datasets/{dataset_id}/credentials', ret)


async def get_tokens(config, dataset, task_files: list[dict], cred_client: RestClient | None = None):
    dataset_id = dataset['dataset_id']
    parser = ExpParser()
    username = dataset['username']
    config = config.copy()
    config['options']['jobs_submitted'] = dataset['jobs_submitted']

    # get token requests
    token_scopes = defaultdict(set)
    for task in config['tasks']:
        logging.info('get_tokens() for dataset %s task %s', dataset_id, task['name'])

        config['options']['task'] = task['name']
        task_token_scopes = defaultdict(set)
        task_data = task['data'].copy()
        for tray in task.get('trays', []):
            task_data.extend(tray.get('data',[]).copy())
            for module in tray.get('modules', []):
                task_data.extend(module.get('data',[]).copy())
        if task['task_files']:
            logging.info('adding task_files')
            task_data.extend(task_files)
        for data in task_data:
            if data['remote'] == '':
                continue
            remote = parser.parse(data['remote'], job=config)
            remote = gsiftp_replacement(remote)
            for prefix in TOKEN_PREFIXES:
                if remote.startswith(prefix):
                    if scope := get_scope(remote[len(prefix):], data['movement']):
                        logging.debug('adding scope %s for remote %s', scope, remote)
                        task_token_scopes[prefix].add(scope)
        # add in manual scopes
        for prefix,scope_str in task['token_scopes'].items():
            for scope in scope_str.split():
                if scope and '$' not in scope:
                    task_token_scopes[prefix].add(scope)
        # set token_requests
        logging.warning('scopes for task %s: %r', task['name'], dict(task_token_scopes))
        for prefix,scopes in task_token_scopes.items():
            sorted_scope_str = ' '.join(sorted(scopes))
            if '$' in sorted_scope_str:
                raise Exception('bad scope!')
            task['token_scopes'][prefix] = sorted_scope_str
            token_scopes[prefix].update(scopes)
    
    logging.warning('scopes for dataset: %r', dict(token_scopes))
    for prefix,scopes in token_scopes.items():
        sorted_scope_str = ' '.join(sorted(scopes))
        if dataset['status'] == 'processing':
            await cred_token(dataset_id, username, prefix, sorted_scope_str, TOKEN_PREFIXES[prefix], cred_client)


def convert(config):
    logging.info('Now converting config')

    if config['version'] < 3.1:
        config['version'] = 3.1

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
                        if d['type'] != 'permanent':
                            d['type'] = 'permanent'

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


def do_mongo(server, *, dataset_id=None, min_dataset_num, max_dataset_num, output=None, cred_client=None, ignore_task_files=False, dryrun=False):

    client = MongoClient(server)
    db = client.config

    datasets = {}
    search = {'dataset': {'$gte': min_dataset_num}}
    projection = {"_id": False, "dataset_id": True, "dataset": True, "status": True, "username": True, "jobs_submitted": True}

    if dataset_id:
        search = {'dataset_id': dataset_id}

    for d in client.datasets.datasets.find(search, projection=projection):
        if (d['dataset'] >= min_dataset_num and d['dataset'] <= max_dataset_num) or dataset_id:
            datasets[d['dataset_id']] = d

    for dataset in sorted(datasets.values(), key=lambda v: v['dataset']):
        config = db.config.find_one({"dataset_id": dataset['dataset_id']}, projection={'_id': False})
        if not config:
            logging.warning('no config for dataset %s', dataset['dataset'])
            continue
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
        search = {"dataset_id": config['dataset_id'], 'remote': {'$not': { '$type': 'object' }}}

        # exclude bad datasets in task_files
        # if config['dataset_id'] in [
        #     "4e402bd823c911eda570141877284d92",
        #     "3aaa2dfc223a11eda2f2141877284d92",
        #     "1157fac62b9d11ed8a6d141877284d92",
        #     "03f187c22a4d11ed8a6d141877284d92",
        #     "cf90ba78d77411eca198141877284d92",
        #     "02a47d3a2dfd11eda570141877284d92",
        #     "97860d2c2ba211edb99a141877284d92",
        #     "bab161fc2c0111eda570141877284d92",
        #     "aaea07461e7911edb75b141877284d92",
        #     "97cbb1ba2a6211edaa4c141877284d92",
        #     "9465e234223d11ed8a6d141877284d92",
        #     "74738da0230a11edaa4c141877284d92",
        #     "cd4b93522b9b11edb75b141877284d92",
        #     "0fa86c32204511ed8a6d141877284d92",
        #     "4123dd342b9211eda570141877284d92",
        #     "0b5387cc2d8111edb75b141877284d92",
        #     "d700e330223f11edb75b141877284d92",
        #     "701b96101f6a11edb99a141877284d92",
        #     "3aaa2dfc223a11eda2f2141877284d92",
        #     "1157fac62b9d11ed8a6d141877284d92",
        #     "4e402bd823c911eda570141877284d92",
        #     "921661582d7711eda2f2141877284d92",
        #     "6edac3440a5611ed8006141877284d92",
        #     "cf90ba78d77411eca198141877284d92",
        #     "8a5b238c2b9b11eda570141877284d92",
        #     "10b73e422bf211ed8a6d141877284d92",
        #     "03f187c22a4d11ed8a6d141877284d92",
        #     "d3a0081a230c11eda570141877284d92",
        #     "b424423a1e8411ed8a6d141877284d92",
        #     "f85e716e231611eda2f2141877284d92",
        #     "d33fcb500a5511edb80b141877284d92",
        #     "4fd54f6c242011eda570141877284d92",
        #     "5787250824c211edb99a141877284d92",
        #     "c3f29700230911edaa4c141877284d92",
        #     "ff6f79922b4c11edb75b141877284d92",
        #     "ff2a91842d9e11ed8a6d141877284d92",
        #     "3c2646aa2da411eda570141877284d92",
        #     "9d9085d81eb111eda2f2141877284d92",
        #     "51d9be4a231411edb75b141877284d92",
        #     "efb1b3fec0b211f0be81fe2e65be79b2"
        # ]:
        #     task_files = []
        # else:
        if ignore_task_files:
            task_files = []
        else:
            task_files = client.tasks.dataset_files.find(search).to_list(1000)
        asyncio.run(get_tokens(new_config, dataset, task_files, cred_client=cred_client))

        if output == '-':
            print(json.dumps(new_config, indent=2, sort_keys=True))
        elif output:
            with open(output, 'w') as f:
                json.dump(new_config, f, indent=2, sort_keys=True)

        if not dryrun:
            db.config.find_one_and_replace({'dataset_id': config['dataset_id']}, new_config)
            if task_files:
                logging.warning('updating task_files (this may take a while)')
                client.tasks.dataset_files.update_many(search, [{
                    '$set': {
                        'remote': {
                            '$replaceOne': {
                                'input': '$remote',
                                'find': 'gsiftp://gridftp.icecube.wisc.edu',
                                'replacement': 'osdf:///icecube/wipac'
                            }
                        }
                    }
                }])
        logging.warning('Completed %r', dataset['dataset'])


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--config', help='config json file')
    group.add_argument('--mongo-server', default=None, help='mongodb server address')
    parser.add_argument('-o', '--output', default=None, help='output config json to file (or "-" for stdout)')
    parser.add_argument('--dataset-id', default=None, help='dataset id (for mongo)')
    parser.add_argument('--min-dataset-num', default=22001, type=int, help='min dataset num')
    parser.add_argument('--max-dataset-num', default=25000, type=int, help='max dataset num')
    parser.add_argument('--ignore-task-files', default=False, action='store_true', help='ignore task_files')
    parser.add_argument('--log-level', default='WARNING')
    add_auth_to_argparse(parser)
    parser.add_argument('--dry-run', action='store_true', default=False, help='do a dry run (for mongo)')
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    if args.dry_run:
        cred_client = None
    else:
        cred_client = create_rest_client(args, retries=0, timeout=5)

    if args.config:
        logging.info('Opening config at %s', args.config)
        with open(args.config) as f:
            config = json.load(f)
        conversion(config, output=args.output)
    elif args.mongo_server:
        do_mongo(
            args.mongo_server,
            dataset_id=args.dataset_id,
            min_dataset_num=args.min_dataset_num,
            max_dataset_num=args.max_dataset_num,
            output=args.output,
            cred_client=cred_client,
            ignore_task_files=args.ignore_task_files,
            dryrun=args.dry_run
        )

if __name__ == '__main__':
    main()