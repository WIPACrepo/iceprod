"""
The core execution functions for running on a node.

The fundamental design of the core is to run a task composed of trays and
modules.  The general heirarchy looks like::

    task
    |
    |- tray1
       |
       |- module1
       |
       |- module2
    |
    |- tray2
       |
       |- module3
       |
       |- module4

Parameters can be defined at every level, and each level is treated as a
scope (such that inner scopes inherit from outer scopes).  This is
accomplished via an internal evironment for each scope.

Data movement should be defined at the task level.
"""

from contextlib import contextmanager
import copy
from dataclasses import dataclass
from enum import StrEnum
import logging
import os
from pathlib import Path
from typing import Any, Iterator, Optional


from iceprod.core import config
from iceprod.core.defaults import add_default_options
from iceprod.core import util
from iceprod.core import functions
import iceprod.core.parser
from iceprod.core.jsonUtil import json_encode,json_decode


class ConfigError(Exception):
    pass


class ConfigParser:
    """
    Parse things using a config and the tray/task/module environment.

    Note: dataset config must be valid!

    Args:
        dataset: a dataset object with config
        logger: a logger object, for localized logging
    """
    def __init__(self, dataset: config.Dataset, logger: Optional[logging.Logger] = None):
        dataset.validate()
        self.config = dataset.config
        self.logger = logger if logger else logging.getLogger()
        self.parser = iceprod.core.parser.ExpParser()

    def parseValue(self, value: Any, env: dict = {}) -> Any:
        """
        Parse a value from the available env and global config.

        If the value is a string:
        1. Use the :class:`Meta Parser <iceprod.core.parser>` to parse the string.
        2. Expand any env variables in the result.

        If the value is not a string, pass through the value.

        Args:
            value: the value to parse
            env: tray/task/module env

        Returns:
            the parsed value
        """
        if isinstance(value, str):
            self.logger.debug('parse before:%r| env=%r| options=%r', value, env, self.config.get('options'))
            while value != (ret := self.parser.parse(value, self.config, env)):
                value = ret
            if isinstance(value, str):
                value = os.path.expandvars(value)
            self.logger.debug('parse after:%r', value)
        return value

    def parseObject(self, obj: Any, env: dict) -> Any:
        """
        Recursively parse a dict or list.

        Do not modify original object.

        Args:
            obj: object to parse
            env: tray/task/module env

        Returns:
            the parsed object
        """
        if isinstance(obj, str):
            return self.parseValue(obj, env)
        elif isinstance(obj, (list, tuple)):
            return [self.parseObject(v, env) for v in obj]
        elif isinstance(obj, dict):
            ret = copy.copy(obj)  # use copy.copy in case it's a subclass of dict
            for k in obj:
                ret[k] = self.parseObject(obj[k], env)
            return ret
        else:
            return obj


Env = dict[str, dict[str, Any]]


@contextmanager
def scope_env(cfg: ConfigParser, obj: dict, upperenv: Optional[Env] = None, logger: Optional[logging.Logger] = None) -> Iterator[Env]:
    """
    A context manager for parsing scoped config, such as parameters.

    The returned environment is a dictionary composed of several objects:

    * parameters
        Parameters are defined directly as an object, or as a string pointing
        to another object.  They can use the IceProd meta-language to be
        defined in relation to other parameters specified in inherited
        scopes, or as eval or sprinf functions.

    * input_files
        A set of Data objects (urls and local paths), for files to download before
        the task starts.

    * output_files
        A set of Data objects (urls and local paths), for files to upload after the
        task successfully completes.

    `input_files` and `output_files` are global, while `parameters` is inherited
    at each scope level.

    Args:
        cfg: ConfigParser object
        obj: A partial dataset config section to operate on. The local scope.
        upperenv: previous scope's env output
        logger: a logger object, for localized logging
    """
    env: Env = {
        'parameters': {},
        'input_files': set(),
        'output_files': set(),
        'environment': {
            'OS_ARCH': '$OS_ARCH',
        }
    }
    if upperenv:
        env['parameters'].update(upperenv['parameters'])
        env['input_files'] = upperenv['input_files']
        env['output_files'] = upperenv['output_files']

    logger = logger if logger else logging.getLogger()

    try:
        # copy parameters
        if 'parameters' in obj:
            # copy new parameters to env first so local referrals work
            env['parameters'].update(obj['parameters'])
            # parse parameter values and update if necessary
            for p in obj['parameters']:
                newval = cfg.parseValue(obj['parameters'][p], env)
                if newval != obj['parameters'][p]:
                    env['parameters'][p] = newval

        if 'data' in obj:
            # download data
            for data in obj['data']:
                d = cfg.parseObject(data, env)
                if d['movement'] in ('input','both'):
                    ret = downloadData(d, cfg=cfg, logger=logger)
                    if ret:
                        env['input_files'].add(ret)
                if d['movement'] in ('output','both'):
                    ret = uploadData(d, cfg=cfg, logger=logger)
                    if ret:
                        env['output_files'].add(ret)

    except util.NoncriticalError:
        logger.warning('Noncritical error when setting up environment', exc_info=True)
    except Exception:
        logger.critical('Serious error when setting up environment', exc_info=True)
        raise

    yield env


class Transfer(StrEnum):
    TRUE = 'true'
    MAYBE = 'maybe'
    FALSE = 'false'


@dataclass(frozen=True, slots=True)
class Data:
    """
    IceProd Data instance

    Args:
        url: url location
        local: local filename
        transfer: whether to transfer file (true | maybe | false)
    """
    url: str
    local: str
    transfer: Transfer


def storage_location(data: dict, parser: ConfigParser) -> str:
    """
    Get data storage location from the config.

    Args:
        data: data config object
        parser: config parser

    Returns:
        storage location
    """
    config = parser.config
    type_ = data['type'].lower()
    if type_ not in ['permanent', 'job_temp', 'dataset_temp', 'site_temp']:
        raise ConfigError('data movement "type" is unknown')
    if 'options' in config and type_ in config['options']:
        return parser.parseValue(config['options'][type_])
    elif type_ == 'permanent':
        if 'options' in config and 'data_url' in config['options']:
            return parser.parseValue(config['options']['data_url'])
        else:
            raise ConfigError('"data_url" not defined in config["options"]')
    else:
        raise ConfigError(f'{type_} not defined in config["options"]')


def do_transfer(data: dict) -> Transfer:
    """
    Test if we should actually transfer the file.

    Args:
        data: data config object
    """
    ret = Transfer.TRUE
    if isinstance(data['transfer'], bool):
        ret = Transfer.TRUE if data['transfer'] else Transfer.FALSE
    elif isinstance(data['transfer'], str):
        t = data['transfer'].lower()
        if t in ('n', 'no', 'not', 'f', 'false'):
            ret = Transfer.FALSE
        elif t in ('y', 'yes', 't', 'true'):
            ret = Transfer.TRUE
        else:
            ret = Transfer(t)
    elif isinstance(data['transfer'], (int, float)):
        ret = Transfer.FALSE if data['transfer'] == 0 else Transfer.TRUE
    return ret


def downloadData(data: dict, cfg: ConfigParser, logger=None) -> Optional[Data]:
    """
    Parse download url and local filename.

    Args:
        data: data config object
        cfg: config parser

    Returns:
        either None or a Data object
    """
    if not logger:
        logger = logging
    remote_base = storage_location(data, cfg)
    logger.debug('downloadData(): remote_base: %r', remote_base)
    remote = str(data['remote']) if data['remote'] is not None else ''
    local = str(data['local']) if data['local'] is not None else ''

    if not remote and not local:
        raise ConfigError('need either "remote" or "local" defined for data')
    if not remote:
        url = os.path.join(remote_base, local)
    elif functions.isurl(remote):
        url = remote
    else:
        url = os.path.join(remote_base, remote)

    transfer = do_transfer(data)
    if transfer == Transfer.FALSE:
        logger.info('not transferring file %s', url)
        return

    if not local:
        local = os.path.basename(remote)

    return Data(url, local, transfer)


def uploadData(data: dict, cfg: ConfigParser, logger=None) -> Optional[Data]:
    """
    Parse download url and local filename.

    Args:
        data: data config object
        cfg: config parser

    Returns:
        either None or a Data object
    """
    if not logger:
        logger = logging
    remote_base = storage_location(data, cfg)
    logger.debug('uploadData(): remote_base: %r', remote_base)
    remote = str(data['remote']) if data['remote'] is not None else ''
    local = str(data['local']) if data['local'] is not None else ''

    if not remote and not local:
        raise ConfigError('need either "remote" or "local" defined for data')
    if not remote:
        url = os.path.join(remote_base, local)
    elif not functions.isurl(remote):
        url = os.path.join(remote_base, remote)
    else:
        url = remote

    if not local:
        local = os.path.basename(remote)

    transfer = do_transfer(data)
    if transfer == Transfer.FALSE:
        logger.info('not transferring file %s', local)
        return

    return Data(url, local, transfer)


# Run Functions #

class WriteToScript:
    """
    Write a task to a Bash script, to execute manually.

    Args:
        task: a task object, with dataset config
        workdir: a directory to write the task and any related files
        options: extra dataset config options
        logger: a logger object, for localized logging
    """
    def __init__(self, task: config.Task, workdir: Path, options: Optional[dict] = None, logger: Optional[logging.Logger] = None):
        self.task = task
        self.workdir = workdir
        self.logger = logger if logger else logging.getLogger()

        # default config setup
        self.options = self.task.dataset.config['options']
        self._fill_options()
        if options:
            self.options.update(options)
        self.cfgparser = ConfigParser(self.task.dataset, logger=self.logger)

        # set up script
        self.infiles: set[Data] = set()
        self.outfiles: set[Data] = set()

    def _fill_options(self):
        self.options['dataset_id'] = self.task.dataset.dataset_id
        self.options['dataset'] = self.task.dataset.dataset_num
        self.options['job'] = self.task.job.job_index
        self.options['jobs_submitted'] = self.task.dataset.jobs_submitted
        self.options['task_id'] = self.task.task_id
        self.options['task'] = self.task.name
        self.options['debug'] = self.task.dataset.debug

    async def convert(self):
        scriptname = self.workdir / 'task_runner.sh'
        with open(scriptname, 'w') as f:
            print('#!/bin/sh', file=f)
            print('set -e', file=f)
            add_default_options(self.options)
            print('# Options:', file=f)
            for field in self.options:
                print(f'# {field}={self.options[field]}', file=f)
            print('', file=f)
            print('# set some env vars for expansion', file=f)
            print('OS_ARCH=$(/cvmfs/icecube.opensciencegrid.org/py3-v4.3.0/os_arch.sh)', file=f)
            print('', file=f)
            with scope_env(self.cfgparser, self.task.dataset.config['steering'], logger=self.logger) as globalenv:
                task = self.task.get_task_config()
                if self.task.task_files:
                    task['data'].extend(self.task.task_files)
                self.logger.debug('converting task %s', self.task.name)
                with scope_env(self.cfgparser, task, globalenv, logger=self.logger) as taskenv:
                    for i, tray in enumerate(task['trays']):
                        trayname = tray['name'] if tray.get('name', '') else i
                        for iteration in range(tray['iterations']):
                            self.options['iter'] = iteration
                            self.logger.debug('converting tray %r iter %d', trayname, iteration)
                            print(f'# running tray {trayname}, iter {iteration}', file=f)
                            with scope_env(self.cfgparser, tray, taskenv, logger=self.logger) as trayenv:
                                for j, module in enumerate(tray['modules']):
                                    modulename = module['name'] if module.get('name', '') else j
                                    self.logger.debug('converting module %r', modulename)
                                    print(f'# running module {modulename}', file=f)
                                    with scope_env(self.cfgparser, module, trayenv, logger=self.logger) as moduleenv:
                                        await self._write_module(module, moduleenv, file=f)
                                    print('', file=f)

                self.infiles = globalenv['input_files']
                self.outfiles = globalenv['output_files']

        scriptname.chmod(scriptname.stat().st_mode | 0o700)
        return scriptname

    async def _write_module(self, module, env, file):
        module = module.copy()
        if module['src']:
            module_src = self.cfgparser.parseValue(module['src'], env)
            if functions.isurl(module_src):
                path = os.path.basename(module_src).split('?', 0)[0].split('#', 0)[0]
                env['input_files'].add(Data(
                    url=module_src,
                    local=path,
                    transfer=Transfer.TRUE,
                ))
                module_src = path
            self.logger.info('running module %r with src %s', module['name'], module_src)
        elif module['running_class']:
            module_src = None
            module_class = self.cfgparser.parseValue(module['running_class'], env)
            self.logger.info('running module %r with class %s', module['name'], module_class)
        else:
            self.logger.error('module is missing src')
            raise ConfigError('error running module - need "src"')

        if module['args']:
            module['args'] = self.cfgparser.parseObject(module['args'], env)
        if module['env_shell']:
            module['env_shell'] = self.cfgparser.parseValue(module['env_shell'], env)
        if module['configs']:
            # parse twice to make sure it's parsed, even if it starts as a string
            module['configs'] = self.cfgparser.parseObject(module['configs'], env)
            module['configs'] = self.cfgparser.parseObject(module['configs'], env)

        # set up env_shell
        env_shell = []
        if module['env_shell']:
            env_shell = module['env_shell'].split()
            if functions.isurl(env_shell[0]):
                path = os.path.basename(env_shell[0]).split('?', 0)[0].split('#', 0)[0]
                env['input_files'].add(Data(
                    url=env_shell[0],
                    local=path,
                    transfer=Transfer.TRUE,
                ))
                env_shell[0] = f'./{path}'

        # set up the args
        args = module['args']
        if module_src:
            if args is not None and args != '':
                self.logger.warning('args=%s', args)
                if args and isinstance(args, str) and args[0] in ('{', '['):
                    args = json_decode(args)
                if args and isinstance(args, dict) and set(args) == {'args', 'kwargs'}:
                    args = self.cfgparser.parseObject(args, env)
                elif isinstance(args, str):
                    args = {"args": [self.cfgparser.parseValue(x, env) for x in args.split()], "kwargs": {}}
                elif isinstance(args, list):
                    args = {"args": [self.cfgparser.parseValue(x, env) for x in args], "kwargs": {}}
                elif isinstance(args, dict):
                    args = {"args": [], "kwargs": self.cfgparser.parseObject(args, env)}
                else:
                    args = {"args": [str(args)], "kwargs": {}}

                # convert to cmdline args
                def splitter(a,b):
                    ret = ('-%s' if len(str(a)) <= 1 else '--%s')%str(a)
                    if b is None:
                        return ret
                    else:
                        return ret+'='+str(b)
                args = args['args'] + [splitter(a, args['kwargs'][a]) for a in args['kwargs']]

                # force args to string
                def toStr(a):
                    if isinstance(a,(bytes,str)):
                        return a
                    else:
                        return str(a)
                args = [toStr(a) for a in args]
            else:
                args = []
        else:
            # construct a python file to call the class
            parsed_args = self.cfgparser.parseObject(args, env)
            pymodule, class_ = module_class.rsplit('.', 1)
            args = f"""import json
from {pymodule} import {class_}
args = json.loads('''{json_encode(parsed_args)}''')
obj = {class_}()
for k,v in args.items():
    obj.SetParameter(k, v)
obj.Execute({{}})"""

        # set up the environment
        cmd = []
        if env_shell:
            cmd.extend(env_shell)

        # set up configs
        if module['configs']:
            for filename in module['configs']:
                self.logger.info('creating config %r', filename)
                with open(self.workdir / filename, 'w') as f:
                    f.write(json_encode(module['configs'][filename]))
                env['input_files'].add(Data(
                    url=str(self.workdir / filename),
                    local=filename,
                    transfer=Transfer.TRUE,
                ))

        # run the module
        if (not module_src):
            cmd.extend(['python', '-', "<<____HERE\n" + args + '\n____HERE\n'])
        elif module_src[-3:] == '.py':
            # call as python script
            cmd.extend(['python', module_src] + args)
        elif module_src[-3:] == '.sh':
            # call as shell script
            cmd.extend(['/bin/sh', module_src] + args)
        else:
            # call as regular executable
            if module_src[0] != '/':
                module_src = f'./{module_src}'
            cmd.extend([module_src] + args)

        if module['env_clear']:
            # must be on cvmfs-like environ for this to apply
            envstr = 'env -i PYTHONNOUSERSITE=1 '
            for k in ('OPENCL_VENDOR_PATH', 'http_proxy', 'TMP', 'TMPDIR', '_CONDOR_SCRATCH_DIR', 'CUDA_VISIBLE_DEVICES', 'COMPUTE', 'GPU_DEVICE_ORDINAL'):
                envstr += f'{k}=${k} '
            cmd = envstr.split()+cmd

        self.logger.info('cmd=%r',cmd)
        print(' '.join(cmd), file=file)
