"""
IceProd Config Migration:
- remove all "resources"
- remove all "classes"
- remove all "running_class"
  - use env shell to track down source?
- data compression removed
- remove all "steering/system"

IceProd Dataset Migration:
- dataset status truncated moved to attrinute
  - status -> suspended
- update all tasks ("waiting", "queued", "processing", "reset") -> "idle"
"""

import argparse
from getpass import getpass
import json
import jsonschema
import logging
from pathlib import Path
from pprint import pprint

from pymongo import MongoClient
from iceprod.core.config import Dataset


notbool = lambda x: not x
strcommalist = lambda x: ','.join(x)


def convert(config):
    logging.info('Now converting config')

    if (steering := config.get('steering', {})) and steering.get('system', None):
        del config['steering']['system']

    def data_cleaner(obj):
        ret = []
        if 'batchsys' in obj:
            if not obj['batchsys']:
                obj['batchsys'] = {}
        if 'resources' in obj:
            if obj['resources']:
                for r in obj['resources']:
                    r['type'] = 'permanent'
                    r['movement'] = 'input'
                    if 'compression' in r:
                        del r['compression']
                    ret.append(r)
            del obj['resources']
        if 'data' in obj:
            if (data := obj['data']) and isinstance(data, list):
                mydata = []
                for d in data:
                    if d.get('type', 'permanent') not in ('permanent', 'job_temp', 'dataset_temp', 'site_temp'):
                        logging.warning('%r', d)
                        raise Exception('unknown data type')
                    if 'compression' in d:
                        del d['compression']
                    if d.get('transfer') == 'exists':
                        d['transfer'] = 'maybe'
                    add = True
                    if d.get('type') == 'job_temp' and d.get('movement') == 'input':
                        for dd in list(mydata):
                            if dd.get('type') == 'job_temp' and dd.get('movement') == 'output' and d.get('local') == dd.get('local'):
                                mydata.remove(dd)
                                add = False
                                break
                    if add:
                        mydata.append(d)
                obj['data'] = mydata
                ret.extend(mydata)
            #del obj['data']
        if 'classes' in obj:
            for cl in obj['classes']:
                if cl.get('libs'):
                    raise Exception('classes with libs not allowed')
                ret.append({
                    'type': 'permanent',
                    'movement': 'input',
                    'remote': cl['src'],
                })
            del obj['classes']
        if 'system' in obj:
            del obj['system']
        return ret

    if steering := config.get('steering', {}):
        base_data = data_cleaner(steering)
    else:
        base_data = []
        config['steering'] = {}

    for i,task in enumerate(config.get('tasks', [])):
        if 'name' not in task:
            task['name'] = f'Task{i}'
        data = base_data.copy()
        data.extend(data_cleaner(task))
        for tray in task.get('trays', []):
            data_cleaner(tray)
            for module in tray.get('modules', []):
                data_cleaner(module)
        task['data'] = data

    config['version'] = 3.1
    return config


def conversion(config, output=None):
    logging.info('Converting config')
    new_config = convert(config)
    logging.info('Validating config')
    d = Dataset('', 1, 1, 1, 1, 'processing', 1., 'group', 'user', False, new_config)
    d.fill_defaults()
    d.validate()
    logging.info('Config validated!')

    if output == '-':
        print(json.dumps(d.config, indent=2, sort_keys=True))
    elif output:
        with open(output, 'w') as f:
            json.dump(d.config, f, indent=2, sort_keys=True)
    return d.config


def do_mongo(server, dataset_id=None, output=None, dryrun=False):
    username = input('MongoDB Username:')
    password = getpass('MongoDB Password:')

    port = 27017
    if ':' in server:
        server,port = server.split(':')
        port = int(port)
    if server.startswith('mongodb://'):
        server = server[10:]
    client = MongoClient(server, port, username=username, password=password)
    db = client.config

    datasets = {}
    search = {}
    if dataset_id:
        datasets[dataset_id] = {'dataset': 'specifed'}
        search = {'dataset_id': dataset_id}
    else:
        for d in client.datasets.datasets.find({}, projection={"_id": False, "dataset_id": True, "dataset": True, "status": True}):
            if d['dataset'] > 22000 or d['status'] == 'processing':
                datasets[d['dataset_id']] = d

    for config in db.config.find(search, projection={'_id': False}):
        if config['dataset_id'] not in datasets:
            continue
        dataset = datasets[config['dataset_id']]
        try:
            logging.warning('Processing %r', dataset['dataset'])
            new_config = conversion(config, output=output)
        except jsonschema.exceptions.ValidationError:
            if dataset['status'] != 'processing' and not config['tasks'][0].get('trays'):
                logging.info('skipping dataset %r due to errors', dataset['dataset'])
                continue
            print(json.dumps(config, indent=2, sort_keys=True))
            raise
        except Exception:
            print(json.dumps(config, indent=2, sort_keys=True))
            raise
        assert config['dataset_id'] == new_config['dataset_id']
        if not dryrun:
            db.config.find_one_and_replace({'dataset_id': config['dataset_id']}, new_config)
        logging.warning('Completed %r', dataset['dataset'])


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-c', '--config', help='config json file')
    group.add_argument('--mongo-server', default=None, help='mongodb server address')
    parser.add_argument('-o', '--output', default=None, help='output config json to file (or "-" for stdout)')
    parser.add_argument('--dataset-id', default=None, help='dataset id (for mongo)')
    parser.add_argument('--dry-run', action='store_true', default=False, help='do a dry run (for mongo)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.config:
        logging.info('Opening config at %s', args.config)
        with open(args.config) as f:
            config = json.load(f)
        conversion(config, output=args.output)
    elif args.mongo_server:
        do_mongo(args.mongo_server, dataset_id=args.dataset_id, output=args.output, dryrun=args.dry_run)

if __name__ == '__main__':
    main()