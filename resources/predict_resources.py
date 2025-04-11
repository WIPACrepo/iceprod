import asyncio
from argparse import ArgumentParser
from collections import defaultdict
from functools import partial
import csv
import json
import logging

from iceprod.client_auth import add_auth_to_argparse, create_rest_client


logger = logging.getLogger()


def task_type(name, ord):
    name = name.lower()
    if any(x in name for x in ('generat', 'corsika', 'background', 'topsimulator', 'nugen', 'genie', 'muongun', 'poly')):
        return 'generate'
    elif any(x in name for x in ('prop', 'clsim', 'ppc', 'hits', 'photo', 'server', 'p0')):
        return 'propagate'
    elif any(x in name for x in ('det', 'ic86', 'trigger')):
        return 'detector'
    elif any(x in name for x in ('baseproc', 'offline', 'filter', 'pffilt', 'level2', 'base_proc', 'l1', 'rehyd', 'classifier')):
        return 'offline'
    else:
        logging.info('unknown task: %s, %d', name, ord)
        return 'other'

def determine_group(simprod, iceprod):
    if not simprod:
        simprod = 'Other'
    if simprod == 'ESTES':
        simprod = 'Neutrino Sources'
    elif simprod == 'SnowStorm':
        simprod = 'Low Energy Astro'
    elif simprod == 'General':
        simprod = 'GENERAL'
    if simprod == 'GENERAL' or simprod == 'Other':
        # can we do better?
        if iceprod['group'] == 'filtering':
            return 'i3filter'
        desc = iceprod['description'].lower()
        if 'icetop' in desc or desc.startswith('it '):
            return 'Cosmic Rays'
        elif 'diffuse' in desc or 'cascade' in desc or 'corsika' in desc or 'muongun' in desc or 'dataset 20904' in desc:
            return 'Diffuse'
        elif 'gen2' in desc:
            return 'Gen2'
        elif 'upgrade' in desc or 'pingu' in desc:
            return 'Upgrade'
        elif 'genie' in desc or 'deepcore' in desc or 'low energy' in desc or 'leptoninjector' in desc:
            return 'Low Energy Astro'
        elif 'nugen' in desc or 'neutrino-generator' in desc or 'nutau' in desc:
            return 'Neutrino Sources'

        user = iceprod['username']
        if user == 'mlarson':
            return 'Upgrade'

        logger.info('failed to do better for %s:%s', user, desc)

    return simprod

async def run(rpc, simprod_data, outfile):
    datasets = {}
    iceprod_dataset_data = await rpc.request('GET', f'/datasets', {'keys': 'dataset_id|dataset|group|username|description|start_date'})

    for data in reversed(simprod_data):
        dataset_id = data['did']
        number = data['number']
        logging.info('simprod - processing %d %s', number, dataset_id)
        logging.debug('iceprod data: %r', iceprod_dataset_data[dataset_id])
        ret = await rpc.request('GET', f'/datasets/{dataset_id}/task_stats')
        tasks = {}
        for i,name in enumerate(ret):
            task = ret[name]
            type_ = task_type(name, i)
            if type_ in tasks:
                tasks[type_]['gpu'] |= task['gpu'] > 0
                tasks[type_]['goodput'] += task['total_hrs']
                tasks[type_]['badput'] += task['total_err_hrs']
            else:
                tasks[type_] = {
                    'gpu': task['gpu'] > 0,
                    'goodput': task['total_hrs'],
                    'badput': task['total_err_hrs'],
                }
        datasets[number] = {
            'dataset_id': dataset_id,
            'tasks': tasks,
            'group': determine_group(simprod=data['group'], iceprod=iceprod_dataset_data[dataset_id]),
            'year': iceprod_dataset_data[dataset_id]['start_date'].split('-',1)[0],
        }
    
    # now go through things not in simprod
    for dataset in iceprod_dataset_data.values():
        dataset_id = dataset['dataset_id']
        number = dataset.get('dataset', -1)
        logging.info('iceprod - processing %d %s', number, dataset_id)
        if number == -1:
            raise Exception()
        if number in datasets:
            continue
        ret = await rpc.request('GET', f'/datasets/{dataset_id}/task_stats')
        tasks = {}
        for i,name in enumerate(ret):
            task = ret[name]
            type_ = task_type(name, i)
            if type_ in tasks:
                tasks[type_]['gpu'] |= task['gpu'] > 0
                tasks[type_]['goodput'] += task['total_hrs']
                tasks[type_]['badput'] += task['total_err_hrs']
            else:
                tasks[type_] = {
                    'gpu': task['gpu'] > 0,
                    'goodput': task['total_hrs'],
                    'badput': task['total_err_hrs'],
                }
        datasets[number] = {
            'dataset_id': dataset_id,
            'tasks': tasks,
            'group': determine_group(simprod='', iceprod=dataset),
            'year': iceprod_dataset_data[dataset_id]['start_date'].split('-',1)[0],
        }

    group_data = defaultdict(partial(defaultdict, dict))
    for dataset in datasets.values():
        for name in dataset['tasks']:
            task = dataset['tasks'][name]
            is_gpu = task['gpu']
            d = group_data[dataset['group']][dataset['year']]
            if name not in d:
                d[name] = {
                    'cpu': 0.,
                    'gpu': 0.,
                }
            d[name]['gpu' if is_gpu else 'cpu'] += task['goodput'] + task['badput']

    with open(outfile, 'w', newline='') as csvfile:
        fieldnames = ['group', 'year', 'task', 'gpu', 'cpu']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for group in group_data:
            for year in group_data[group]:
                for task in group_data[group][year]:
                    row = {
                        'group': group,
                        'year': year,
                        'task': task,
                        'gpu': int(group_data[group][year][task]['gpu']),
                        'cpu': int(group_data[group][year][task]['cpu']),
                    }
                    writer.writerow(row)


def main():
    parser = ArgumentParser()
    parser.add_argument('infile')
    parser.add_argument('outfile')
    parser.add_argument('--log-level', default='INFO')
    add_auth_to_argparse(parser)
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    rpc = create_rest_client(args)

    with open(args.infile, 'r') as f:
        simprod_data = json.load(f)

    asyncio.run(run(rpc, simprod_data, args.outfile))


if __name__ == '__main__':
    main()
