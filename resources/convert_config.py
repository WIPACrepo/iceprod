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

"""
for each module:
  src: the new src script
  args: a mapping from existing args to new args
"""
CLASS_TO_SRC = {
    'icecube.simprod.modules.IceCube': {
        'src': 'simprod-scripts/resources/scripts/detector.py',
        'args': {
            'execute': {'type': 'args', 'name': '--no-execute', 'use': notbool},
            'gcdfile': {'type': 'kwargs', 'name': 'gcdfile', 'mapper': str},
            'inputfile': {'type': 'kwargs', 'name': 'inputfile', 'mapper': str},
            'outputfile': {'type': 'kwargs', 'name': 'outputfile', 'mapper': str},
            'seed': {'type': 'kwargs', 'name': 'seed', 'mapper': str},
            'procnum': {'type': 'kwargs', 'name': 'procnum', 'mapper': str},
            'nproc': {'type': 'kwargs', 'name': 'nproc', 'mapper': str},
            'summaryfile': {'type': 'kwargs', 'name': 'SummaryFile', 'mapper': str},
            'histogramfilename': {'type': 'kwargs', 'name': 'HistogramFilename', 'mapper': str},
            'enablehistogram': {'type': 'args', 'name': '--EnableHistogram', 'use': bool},

            'mctype': {'type': 'kwargs', 'name': 'MCType', 'mapper': str},
            'uselineartree': {'type': 'args', 'name': '--MCType', 'use': bool},
            'mcprescale': {'type': 'kwargs', 'name': 'MCPrescale', 'mapper': str},
            'icetop': {'type': 'args', 'name': '--IceTop', 'use': bool}, # only add if value is True
            'genie': {'type': 'args', 'name': '--Genie', 'use': bool}, # only add if value is True
            'filtertrigger': {'type': 'args', 'name': '--no-FilterTrigger', 'use': notbool}, # only add if value is False
            'trigger': {'type': 'args', 'name': '--no-Trigger', 'use': notbool}, # only add if value is False
            'lowmem': {'type': 'args', 'name': '--LowMem', 'use': bool},
            'beaconlaunches': {'type': 'args', 'name': '--no-BeaconLaunches', 'use': notbool}, # only add if value is False
            'timeshiftskipkeys': {'type': 'kwargs', 'name': 'TimeShiftSkipKeys', 'mapper': strcommalist},
            'sampleefficiency': {'type': 'kwargs', 'name': 'SampleEfficiency', 'mapper': str},
            'generatedefficiency': {'type': 'kwargs', 'name': 'GeneratedEfficiency', 'mapper': str},
            'runid': {'type': 'kwargs', 'name': 'RunID', 'mapper': str},
            'mcpeseriesname': {'type': 'kwargs', 'name': 'MCPESeriesName', 'mapper': str},
            'detectorname': {'type': 'kwargs', 'name': 'DetectorName', 'mapper': str},
            'skipkeys': {'type': 'kwargs', 'name': 'SkipKeys', 'mapper': strcommalist},
            'usegslrng': {'type': 'args', 'name': '--UseGSLRNG', 'use': bool},
        }
    },
}


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
                if 'running_class' in module:
                    rc = module['running_class']
                    del module['running_class']
                    if rc:
                        logging.warning('converting running_class %s', rc)
                        if rc not in CLASS_TO_SRC:
                            continue
                            raise Exception('unknown running_class')
                        converter = CLASS_TO_SRC[rc]

                        env_shell = module.get('env_shell', '')
                        if not env_shell:
                            raise Exception('no env_shell to base running_class off of')
                        env, meta = env_shell.split(' ')
                        if meta.startswith('/'):
                            base = Path(meta)
                        else:
                            base = Path(env).parent / 'metaprojects' / meta
                        src = base / converter['src']
                        logging.info('new src: %s', src)
                        module['src'] = str(src)

                        if isinstance(oldargs := module.get('args', None), dict):
                            args = {'args': [], 'kwargs': {}}
                            for k,v in oldargs.items():
                                k = k.lower()
                                if k not in converter['args']:
                                    raise Exception(f'unknown arg: {k}')
                                c = converter['args'][k]
                                if (exe := c.get('use')) and not exe(v):
                                    continue
                                if c['type'] == 'args':
                                    args['args'].append(c['name'])
                                elif c['type'] == 'kwargs':
                                    args['kwargs'][c['name']] = c.get('mapper', lambda x: x)(v)
                                else:
                                    raise Exception('unknown type in converter')
                            module['args'] = args

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