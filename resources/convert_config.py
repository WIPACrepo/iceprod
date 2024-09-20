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
import json
import jsonschema
import logging
from pathlib import Path
from pprint import pprint

from iceprod.core.config import Dataset


notbool = lambda x: not x
strcommalist = lambda x: ','.join(x)


def convert(config):
    logging.info('Now converting config')

    if 'system' in config.get('steering', {}):
        del config['steering']['system']

    def data_cleaner(obj):
        if 'batchsys' in obj:
            if not obj['batchsys']:
                obj['batchsys'] = {}
        ret = []
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
            if obj['data']:
                for d in obj['data']:
                    if d.get('type', 'permanent') not in ('permanent', 'job_temp', 'dataset_temp', 'site_temp'):
                        logging.warning('%r', d)
                        raise Exception('unknown data type')
                    if 'compression' in d:
                        del d['compression']
                    if d.get('transfer') == 'exists':
                        d['transfer'] = 'maybe'
                    ret.append(d)
            del obj['data']
        if 'classes' in obj:
            if obj['classes']:
                raise Exception('classes not allowed')
            del obj['classes']
        return ret

    base_data = data_cleaner(config.get('steering', {}))

    for task in config.get('tasks', []):
        data = base_data.copy()
        data.extend(data_cleaner(task))
        for tray in task.get('trays', []):
            data.extend(data_cleaner(tray))
            for module in tray.get('modules', []):
                data.extend(data_cleaner(module))
        task['data'] = data

    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output', default='-', help='output config json file (default stdout)')
    parser.add_argument('config', help='config json file')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    logging.info('Opening config at %s', args.config)
    with open(args.config) as f:
        config = json.load(f)

    new_config = convert(config)

    logging.info('Validating config')
    d = Dataset('', 1, 1, 1, 1, 'processing', 1., 'group', 'user', False, new_config)
    d.fill_defaults()
    d.validate()

    if args.output == '-':
        pprint(new_config)
    else:
        with open(args.output, 'w') as f:
            json.dump(d.config, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()