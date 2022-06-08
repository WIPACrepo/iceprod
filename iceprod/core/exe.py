"""
The core execution functions for running on a node.
These are all called from :any:`iceprod.core.i3exec`.

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
"""

from __future__ import absolute_import, division, print_function

import sys
import os
import stat
import time
import imp
import glob
import copy
import filecmp
import tempfile
import shutil
import inspect
from datetime import datetime
from functools import partial
from collections import Container
from contextlib import contextmanager
import subprocess
import asyncio

try:
    import cPickle as pickle
except Exception:
    import pickle

import logging


from iceprod.core import to_log,constants
from iceprod.core import util
from iceprod.core import dataclasses
from iceprod.core import util
from iceprod.core import functions
from iceprod.core.resources import Resources
import iceprod.core.parser
from iceprod.core.jsonUtil import json_encode,json_decode


class Config:
    """Contain the configuration and related methods"""
    def __init__(self, config=None, parser=None, rpc=None, logger=None):
        self.config = config if config else dataclasses.Job()
        self.parser = parser if parser else iceprod.core.parser.ExpParser()
        self.rpc = rpc
        self.logger = logger if logger else logging

    def parseValue(self, value, env={}):
        """
        Parse a value from the available env and global config.

        Uses the :class:`Meta Parser <iceprod.core.parser>` on any string value.
        Pass-through for any other object.

        :param value: The value to parse
        :param env: The environment to use, optional
        :returns: The parsed value
        """
        if isinstance(value,dataclasses.String):
            self.logger.debug('parse before:%r| env=%r',value,env)
            value = self.parser.parse(value,self.config,env)
            if isinstance(value,dataclasses.String):
                value = os.path.expandvars(value)
            self.logger.debug('parse after:%r',value)
        return value

    def parseObject(self,obj,env):
        """Recursively parse a dict or list"""
        if isinstance(obj,dataclasses.String):
            return self.parseValue(obj,env)
        elif isinstance(obj,(list,tuple)):
            return [self.parseObject(v,env) for v in obj]
        elif isinstance(obj,dict):
            ret = copy.copy(obj) # in case it's a subclass of dict, like dataclasses
            for k in obj:
                ret[k] = self.parseObject(obj[k],env)
            return ret
        else:
            return obj

class SetupEnv:
    """
    The internal environment (env) is a dictionary composed of several objects:

    parameters
        Parameters are defined directly as an object, or as a string pointing
        to another object.  They can use the IceProd meta-language to be
        defined in relation to other parameters specified in inherited
        scopes, or as eval or sprinf functions.

    resources
        \ 

    data
        Resources and data are similar in that they handle extra files that
        modules may create or use.  The difference is that resources are only
        for reading, such as pre-built lookup tables, while data can be input
        and/or output. Compression can be automatically handled by IceProd.
        Both resources and data are defined in the environment as strings to
        their file location.
        
    classes
        This is where external software gets added.  The software can be an
        already downloaded resource or just a url to download.  All python
        files get added to the python path and binary libraries get symlinked
        into a directory on the LD_LIBRARY_PATH.  Note that if there is more
        than one copy of the same shared library file, only the most recent
        one is in scope.  Classes are defined in the environment as strings
        to their file location.

    deletions
        These are files that should be deleted when the scope ends.

    uploads
        These are files that should be uploaded when the scope ends.
        Mostly Data objects that are used as output.
    
    shell environment
        An environment to reset to when exiting the context manager.

    To keep the scope correct a new dictionary is created for every level, then
    the inheritable objects are shallow copied (to 1 level) into the new env.
    The deletions are not inheritable (start empty for each scope), and the shell
    environment is set at whatever the previous scope currently has.

    Args:
        cfg (:py:class:`Config`): Config object
        obj (dict): A dict-like object from :py:mod:`iceprod.core.dataclasses`
                    such as :py:class:`iceprod.core.dataclasses.Steering`.
        oldenv (dict): (optional) env that we are running inside
    """
    def __init__(self, cfg, obj, oldenv={}, logger=None):
        self.cfg = cfg
        self.obj = obj
        self.oldenv = oldenv
        self.env = {}
        self.logger = logger if logger else logging
        
        # validation of input
        if not self.obj:
            raise util.NoncriticalError('object to load environment from is empty')
        if isinstance(self.obj, dataclasses.Steering) and not self.obj.valid():
            raise Exception('object is not valid Steering')

    async def __aenter__(self):
        try:
            # attempt to do depth=2 copying
            for key in self.oldenv:
                if key not in ('deletions','uploads','environment','pythonpath','stats'):
                    self.env[key] = copy.copy(self.oldenv[key])

            # make sure things for this env are clear (don't inherit)
            self.env['deletions'] = []
            self.env['uploads'] = []

            # get clear environment variables
            self.env['environment'] = os.environ.copy()
            self.env['pythonpath'] = copy.copy(sys.path)

            # inherit statistics
            if 'stats' in self.oldenv:
                self.env['stats'] = self.oldenv['stats']
            else:
                self.env['stats'] = {'upload':[], 'download':[], 'tasks':[]}

            # copy parameters
            if 'parameters' not in self.env:
                self.env['parameters'] = {}
            if 'parameters' in self.obj:
                # copy new parameters to env first so local referrals work
                self.env['parameters'].update(self.obj['parameters'])
                # parse parameter values and update if necessary
                for p in self.obj['parameters']:
                    newval = self.cfg.parseValue(self.obj['parameters'][p], self.env)
                    if newval != self.obj['parameters'][p]:
                        self.env['parameters'][p] = newval

            if 'resources' not in self.env:
                self.env['resources'] = {}
            if 'resources' in self.obj:
                # download resources
                for resource in self.obj['resources']:
                    await downloadResource(self.env, self.cfg.parseObject(resource, self.env), logger=self.logger)

            if 'data' not in self.env:
                self.env['data'] = {}
            input_files = self.cfg.config['options']['input'].split() if 'input' in self.cfg.config['options'] else []
            output_files = self.cfg.config['options']['output'].split() if 'output' in self.cfg.config['options'] else []
            if 'data' in self.obj:
                # download data
                for data in self.obj['data']:
                    d = self.cfg.parseObject(data, self.env)
                    if d['movement'] in ('input','both'):
                        await downloadData(self.env, d, logger=self.logger)
                        if 'local' in d and d['local']:
                            input_files.append(d['local'])
                        elif 'remote' in d and d['remote']:
                            input_files.append(os.path.basename(d['remote']))
                    if d['movement'] in ('output','both'):
                        self.env['uploads'].append(d)
                        if 'local' in d and d['local']:
                            output_files.append(d['local'])
                        elif 'remote' in d and d['remote']:
                            output_files.append(os.path.basename(d['remote']))
            # add input and output to parseable options
            self.cfg.config['options']['input'] = ' '.join(input_files)
            self.cfg.config['options']['output'] = ' '.join(output_files)
            logging.info('input: %r', self.cfg.config['options']['input'])
            logging.info('output: %r', self.cfg.config['options']['output'])

            if 'classes' not in self.env:
                self.env['classes'] = {}
            if 'classes' in self.obj:
                # set up classes
                for c in self.obj['classes']:
                    await setupClass(self.env, self.cfg.parseObject(c, self.env), logger=self.logger)

        except util.NoncriticalError as e:
            self.logger.warning('Noncritical error when setting up environment', exc_info=True)
        except Exception as e:
            self.logger.critical('Serious error when setting up environment', exc_info=True)
            raise

        return self.env

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if not exc_type:
                # upload data if there was no exception
                if 'uploads' in self.env and ('offline' not in self.cfg.config['options']
                        or (not self.cfg.config['options']['offline'])
                        or (self.cfg.config['options']['offline']
                            and 'offline_transfer' in self.cfg.config['options']
                            and self.cfg.config['options']['offline_transfer'])):
                    for d in self.env['uploads']:
                        await uploadData(self.env, d, logger=self.logger)
        finally:
            # delete any files
            if 'deletions' in self.env and len(self.env['deletions']) > 0:
                for f in reversed(self.env['deletions']):
                    try:
                        os.remove(f)
                        base = os.path.basename(f)
                    except OSError as e:
                        self.logger.error('failed to delete file %s - %s',(str(f),str(e)))
                        if ('options' in self.env and
                            'debug' in self.env['options'] and
                            self.env['options']['debug']):
                            raise

            # reset environment
            if 'environment' in self.env:
                for e in list(os.environ.keys()):
                    if e not in self.env['environment']:
                        del os.environ[e]
                for e in self.env['environment'].keys():
                    os.environ[e] = self.env['environment'][e]


async def downloadResource(env, resource, remote_base=None,
                           local_base=None, checksum=None, logger=None):
    if not logger:
        logger = logging
    """Download a resource and put location in the env"""
    if not remote_base:
        remote_base = env['options']['resource_url']
    if not resource['remote'] and not resource['local']:
        raise Exception('need to specify either local or remote')
    if not resource['remote']:
        url = os.path.join(remote_base, resource['local'])
    elif functions.isurl(resource['remote']):
        url = resource['remote']
    else:
        url = os.path.join(remote_base,resource['remote'])

    execute = resource.do_transfer()
    if execute is False:
        logger.info('not transferring file %s', url)
        return
        
    if not local_base:
        if 'subprocess_dir' in env['options']:
            local_base = env['options']['subprocess_dir']
        else:
            local_base = os.getcwd()
    if not resource['local']:
        resource['local'] = os.path.basename(resource['remote'])
    local = os.path.join(local_base,resource['local'])
    if 'files' not in env:
        env['files'] = {}
    if not os.path.exists(os.path.dirname(local)):
        os.makedirs(os.path.dirname(local))

    # get resource
    if resource['local'] in env['files']:
        logger.info('resource %s already exists in env, so skip download and compression',resource['local'])
        return
    elif os.path.exists(local):
        logger.info('resource %s already exists as file, so skip download',resource['local'])
    else:
        # download resource
        download_options = {}
        if 'options' in env and 'username' in env['options']:
            download_options['username'] = env['options']['username']
        if 'options' in env and 'password' in env['options']:
            download_options['password'] = env['options']['password']
        if 'options' in env and 'ssl' in env['options'] and env['options']['ssl']:
            download_options.update(env['options']['ssl'])
        failed = False
        try:
            start_time = time.time()
            await functions.download(url, local, options=download_options)
            if not os.path.exists(local):
                raise Exception('file does not exist')
            if checksum:
                # check the checksum
                cksm = functions.sha512sum(local)
                if cksm != checksum:
                    raise Exception('checksum validation failed')
        except Exception:
            if execute is False or execute == 'maybe':
                logger.info('not transferring file %s', url)
                return
            failed = True
            logger.critical('failed to download %s to %s', url, local, exc_info=True)
            raise Exception('failed to download {} to {}'.format(url, local))
        finally:
            stats = {
                'name': url,
                'error': failed,
                'now': datetime.utcnow().isoformat(),
                'duration': time.time()-start_time,
            }
            if (not failed) and os.path.exists(local):
                stats['size'] = os.path.getsize(local)
                stats['rate_MBps'] = stats['size']/1000/1000/stats['duration']

            if 'stats' in env and (execute is True or (execute == 'maybe' and not failed)):
                if 'download' not in env['stats']:
                    env['stats']['download'] = []
                env['stats']['download'].append(stats)

            if (not failed) and 'data_movement_stats' in env['options'] and env['options']['data_movement_stats']:
                print(f'{stats["now"]} Data movement stats: input {stats["duration"]:.3f} {stats["size"]:.0f} {stats["name"]}')

    # check compression
    if (resource['compression'] and
        (functions.iscompressed(url) or functions.istarred(url))):
        # uncompress file
        files = functions.uncompress(local)
        # add uncompressed file(s) to env
        env['files'][resource['local']] = files
    else:
        # add file to env
        env['files'][resource['local']] = local
    logger.warning('resource %s added to env',resource['local'])

async def downloadData(env, data, logger=None):
    """Download data and put location in the env"""
    if not logger:
        logger = logging
    remote_base = data.storage_location(env)
    if 'options' in env and 'subprocess_dir' in env['options']:
        local_base = env['options']['subprocess_dir']
    else:
        local_base = os.getcwd()
    
    execute = data.do_transfer()
    checksum = None
    if execute is not False:
        try:
            filecatalog = data.filecatalog(env)
            path, checksum = filecatalog.get(data['local'])
        except Exception:
            # no filecatalog available
            pass
    await downloadResource(env, data, remote_base, local_base,
                           checksum=checksum, logger=logger)

async def uploadData(env, data, logger=None):
    """Upload data"""
    if not logger:
        logger = logging
    remote_base = data.storage_location(env)
    if 'options' in env and 'subprocess_dir' in env['options']:
        local_base = env['options']['subprocess_dir']
    else:
        local_base = os.getcwd()
    if (not data['remote']) and not data['local']:
        raise Exception('need either remote or local defined')
    if not data['remote']:
        url = os.path.join(remote_base, data['local'])
    elif not functions.isurl(data['remote']):
        url = os.path.join(remote_base, data['remote'])
    else:
        url = data['remote']

    if not data['local']:
        data['local'] = os.path.basename(data['remote'])
    local = os.path.join(local_base, data['local'])

    execute = data.do_transfer()
    exists = os.path.exists(local)
    if execute is False or (execute == 'maybe' and not exists):
        logger.info('not transferring file %s', local)
        return
    elif not exists:
        raise Exception('file {} does not exist'.format(local))

    # check compression
    if data['compression']:
        # get compression type, if specified
        if ((functions.iscompressed(url) or functions.istarred(url)) and
            not (functions.iscompressed(local) or functions.istarred(local))):
            # url has compression on it, so use that
            if '.tar.' in url:
                c = '.'.join(url.rsplit('.',2)[-2:])
            else:
                c = url.rsplit('.',1)[-1]
            try:
                local = functions.compress(local,c)
            except Exception:
                logger.warning('cannot compress file %s to %s', local, c)
                raise

    # upload file
    upload_options = {}
    if 'options' in env and 'username' in env['options']:
        upload_options['username'] = env['options']['username']
    if 'options' in env and 'password' in env['options']:
        upload_options['password'] = env['options']['password']
    if 'options' in env and 'ssl' in env['options'] and env['options']['ssl']:
        upload_options.update(env['options']['ssl'])
    do_checksum = True
    if 'options' in env and 'upload_checksum' in env['options']:
        do_checksum = env['options']['upload_checksum']
    failed = False
    try:
        start_time = time.time()
        await functions.upload(local, url, checksum=do_checksum, options=upload_options)
    except Exception:
        failed = True
        logger.critical('failed to upload %s to %s', local, url, exc_info=True)
        raise Exception('failed to upload {} to {}'.format(local, url))
    finally:
        stats = {
            'name': url,
            'error': failed,
            'now': datetime.utcnow().isoformat(),
            'duration': time.time()-start_time,
        }
        if not failed:
            stats['size'] = os.path.getsize(local)
            stats['rate_MBps'] = stats['size']/1000/1000/stats['duration']
        if 'stats' in env:
            env['stats']['upload'].append(stats)

        if (not failed) and 'data_movement_stats' in env['options'] and env['options']['data_movement_stats']:
            print(f'{stats["now"]} Data movement stats: output {stats["duration"]:.3f} {stats["size"]:.0f} {stats["name"]}')

    # if successful, add to filecatalog
    try:
        filecatalog = data.filecatalog(env)
    except Exception:
        pass # no filecatalog available
    else:
        try:
            cksm = functions.sha512sum(local)
            metadata = {
                'file_size': stats['size'],
                'create_date': stats['now'],
                'modify_date': stats['now'],
                'data_type': 'simulation',
                'transfer_duration': stats['duration'],
                'transfer_MBps': stats['rate_MBps'],
            }
            options = ('dataset','dataset_id','task_id','task','job','debug')
            metadata.update({env['options'][k] for k in options if k in env['options']})
            filecatalog.add(data['local'], url, cksm, metadata)
        except Exception:
            logger.warning('failed to add %r to filecatalog', url, exc_info=True)

async def setupClass(env, class_obj, logger=None):
    """Set up a class for use in modules, and put it in the env"""
    if not logger:
        logger = logging
    if not 'classes' in env:
        env['classes'] = {}
    if not class_obj:
        raise Exception('Class is not defined')
    loaded = False
    if class_obj['name'] in env['classes']:
        # class already loaded, so leave it alone
        logger.info('class %s already loaded',class_obj['name'])
    elif class_obj['resource_name']:
        # class is downloaded as a resource
        if 'files' not in env or class_obj['resource_name'] not in env['files']:
            logger.error('resource %s for class %s does not exist',
                         class_obj['resource_name'],class_obj['name'])
        else:
            local = env['files'][class_obj['resource_name']]
            if not isinstance(local,dataclasses.String):
                local = local[0]
            if (class_obj['src'] and
                os.path.exists(os.path.join(local,class_obj['src']))):
                # treat src as a path inside the resource
                local = os.path.join(local,class_obj['src'])
            loaded = True
    else:
        # get url of class
        i = 0
        while True:
            url = class_obj['src']
            if url and functions.isurl(url):
                i = 10 # skip repeat download attempts
            else:
                if i == 0:
                    # first, look in resources
                    if 'options' in env and 'resource_url' in env['options']:
                        url = os.path.join(env['options']['resource_url'],class_obj['src'])
                    else:
                        url = os.path.join('http://prod-exe.icecube.wisc.edu/',class_obj['src'])
                elif i == 1:
                    # then, look in regular svn
                    if 'options' in env and 'svn_repository' in env['options']:
                        url = os.path.join(env['options']['svn_repository'],class_obj['src'])
                    else:
                        url = os.path.join('http://code.icecube.wisc.edu/svn/projects/',class_obj['src'])
                else:
                    raise util.NoncriticalError('Cannot find class %s because of bad src url'%class_obj['name'])

            if 'options' in env and 'local_temp' in env['options']:
                local_temp = env['options']['local_temp']
            else:
                local_temp = os.path.join(os.getcwd(),'classes')
                env['options']['local_temp'] = local_temp
            if not os.path.exists(local_temp):
                os.makedirs(local_temp)
            if 'PYTHONPATH' in os.environ and local_temp not in os.environ['PYTHONPATH']:
                os.environ['PYTHONPATH'] += ':'+local_temp
            elif 'PYTHONPATH' not in os.environ:
                os.environ['PYTHONPATH'] = local_temp

            local = os.path.join(local_temp,class_obj['name'].replace(' ','_'))

            download_options = {}
            if 'options' in env and 'username' in env['options']:
                download_options['username'] = env['options']['username']
            if 'options' in env and 'password' in env['options']:
                download_options['password'] = env['options']['password']
            if 'options' in env and 'ssl' in env['options'] and env['options']['ssl']:
                download_options.update(env['options']['ssl'])

            # download class
            logger.warning('attempting to download class %s to %s',url,local_temp)
            try:
                download_local = await functions.download(url, local_temp,
                        options=download_options)
            except Exception:
                logger.info('download failed, {} attempts left'.format(i), exc_info=True)
                if i < 10:
                    i += 1
                    continue # retry with different url
                raise
            if not os.path.exists(download_local):
                raise Exception('failed to download {} to {}'.format(url, local))
            if functions.iscompressed(download_local) or functions.istarred(download_local):
                files = functions.uncompress(download_local, out_dir=local_temp)
                # check if we extracted a tarfile
                if isinstance(files,dataclasses.String):
                    local = files
                elif isinstance(files,list):
                    dirname = os.path.commonprefix(files)
                    if dirname:
                        dirname = os.path.join(local_temp, dirname.split(os.path.sep)[0])
                    else:
                        dirname = local_temp
                    logger.info('looking up tarball at %r', dirname)
                    if os.path.isdir(dirname):
                        logger.info('rename %r to %r', local, dirname)
                        local = dirname
                else:
                    logger.warning('files is strange datatype: %r',
                                type(files))
            elif local != download_local:
                logger.info('rename %r to %r', download_local, local)
                os.rename(download_local, local)
            loaded = True
            break

    if loaded:
        # add to env
        env['classes'][class_obj['name']] = local
        logger.warning('class %s loaded at %r',class_obj['name'],local)

        # add binary libraries to the LD_LIBRARY_PATH
        def ldpath(root,f=None):
            root = os.path.abspath(root)
            def islib(f):
                return f[-3:] == '.so' or '.so.' in f or f[-2:] == '.a' or '.a.' in f
            if (f and islib(f)) or any(islib(f) for f in os.listdir(root)):
                logger.info('adding to LD_LIBRARY_PATH: %s',root)
                if 'LD_LIBRARY_PATH' in os.environ:
                    if root in os.environ['LD_LIBRARY_PATH'].split(':'):
                        return # already present
                    os.environ['LD_LIBRARY_PATH'] = root+':'+os.environ['LD_LIBRARY_PATH']
                else:
                    os.environ['LD_LIBRARY_PATH'] = root
            else:
                logger.debug('no libs in %s',root)
        def addToPythonPath(root):
            if glob.glob(os.path.join(root,'*.py')):
                logger.info('adding to PYTHONPATH: %s',root)
                if 'PYTHONPATH' in os.environ:
                    if root in os.environ['PYTHONPATH'].split(':'):
                        return # already present
                    os.environ['PYTHONPATH'] = root+':'+os.environ['PYTHONPATH']
                else:
                    os.environ['PYTHONPATH'] = root
            else:
                logger.debug('no python files: %s',root)
        if os.path.isdir(local):
            # build search list
            search_list = [local]
            search_list.extend(glob.glob(os.path.join(local,'lib*')))
            search_list.extend(glob.glob(os.path.join(local,'lib*/python*/*-packages')))
            if class_obj['libs'] is not None:
                search_list.extend(os.path.join(local,x) for x in class_obj['libs'].split(':'))
            for s in search_list:
                if not os.path.isdir(s):
                    continue
                addToPythonPath(s)
                ldpath(s)
        elif os.path.exists(local):
            root, f = os.path.split(local)
            if f.endswith('.py'):
                if root not in sys.path:
                    addToPythonPath(root)
            else:
                # check for binary library
                ldpath(root,f)
        # modify environment variables
        logger.info('env_vars = %s',class_obj['env_vars'])
        if class_obj['env_vars']:
            for e in class_obj['env_vars'].split(';'):
                try:
                    k,v = e.split('=')
                except ValueError as e:
                    logger.warning('bad env variable: %s',e)
                    continue
                v = v.replace('$CLASS',local)
                logger.info('setting envvar: %s = %s',k,v)
                if k in os.environ:
                    os.environ[k] = v+':'+os.environ[k]
                else:
                    os.environ[k] = v

### Run Functions ###

async def runtask(cfg, globalenv, task, logger=None):
    """Run the specified task"""
    if not task:
        raise Exception('No task provided')
    if not logger:
        logger = logging

    # set up task_temp
    if not os.path.exists('task_temp'):
        os.mkdir('task_temp')
    globalenv['task_temp'] = os.path.join(os.getcwd(),'task_temp')

    # set up stats
    stats = {}

    # check if we have any files in the task_files API
    if task['task_files'] and ((not cfg.config['options']['offline']) or cfg.config['options']['offline_transfer']):
        files = await cfg.rpc.task_files(cfg.config['options']['dataset_id'],
                                         cfg.config['options']['task_id'])
        task['data'].extend(files)

    try:
        # set up local env
        async with SetupEnv(cfg, task, globalenv, logger=logger) as env:
            # run trays
            for tray in task['trays']:
                tmpstat = {}
                async for proc in runtray(cfg, env, tray, stats=tmpstat, logger=logger):
                    yield proc
                if len(tmpstat) > 1:
                    stats[tray['name']] = tmpstat
                elif len(tmpstat) == 1:
                    stats[tray['name']] = tmpstat[list(tmpstat.keys())[0]]
    finally:
        # destroy task temp
        try:
            functions.removedirs('task_temp')
        except Exception as e:
            logger.warning('error removing task_temp directory: %r',
                           e, exc_info=True)

    globalenv['stats']['tasks'].append(stats)

async def runtray(cfg, globalenv,tray,stats={}, logger=None):
    """Run the specified tray"""
    if not tray:
        raise Exception('No tray provided')
    if not logger:
        logger = logging

    # set up tray_temp
    if not os.path.exists('tray_temp'):
        os.mkdir('tray_temp')
    globalenv['tray_temp'] = os.path.join(os.getcwd(),'tray_temp')

    # run iterations
    try:
        tmpenv = globalenv.copy()
        for i in range(tray['iterations']):
            # set up local env
            cfg.config['options']['iter'] = i
            tmpstat = {}
            async with SetupEnv(cfg, tray, tmpenv, logger=logger) as env:
                # run modules
                for module in tray['modules']:
                    async for proc in runmodule(cfg, env, module, stats=tmpstat, logger=logger):
                        yield proc
            stats[i] = tmpstat

    finally:
        # destroy tray temp
        try:
            functions.removedirs('tray_temp')
        except Exception as e:
            logger.warning('error removing tray_temp directory: %s',
                           str(e), exc_info=True)

async def runmodule(cfg, globalenv, module, stats={}, logger=None):
    """Run the specified module"""
    if not module:
        raise Exception('No module provided')
    if not logger:
        logger = logging

    # set up local env
    module = module.copy()
    async with SetupEnv(cfg, module, globalenv, logger=logger) as env:
        if module['running_class']:
            module['running_class'] = cfg.parseValue(module['running_class'],env)
        if module['args']:
            module['args'] = cfg.parseObject(module['args'],env)
        if module['src']:
            module['src'] = cfg.parseValue(module['src'],env)
        if module['env_shell']:
            module['env_shell'] = cfg.parseValue(module['env_shell'],env)
        if module['configs']:
            # parse twice to make sure it's parsed, even if it starts as a string
            module['configs'] = cfg.parseObject(module['configs'],env)
            module['configs'] = cfg.parseObject(module['configs'],env)

        # make subprocess to run the module
        async with ForkModule(cfg, env, module, logger=logger, stats=stats) as process:
            # yield process back to pilot or driver, so it can be killed
            yield process

class ForkModule:
    """
    Modules are run in a forked process to prevent segfaults from killing IceProd.
    Their stdout and stderr is dumped into the log file with prefixes on each
    line to designate its source.  Any error or the return value is returned to
    the main process via a Queue.

    If a module defines a src, that is assumed to be a Class which should be
    added to the env.  The running_class is where the exact script or binary
    is chosen.  It can match several things:

    * A fully defined python module.class import (also takes module.function)
    * A python class defined in the src provided
    * A regular python script
    * An executable of some type (this is run in a subprocess with shell
      execution disabled)
    """
    def __init__(self, cfg, env, module, logger=None, stats=None):
        self.cfg = cfg
        self.env = env
        self.module = module
        if not logger:
            logger = logging
        self.logger = logger
        self.stats = stats if stats else {}
        self.proc = None
        
        self.error_filename = constants['task_exception']
        self.stats_filename = constants['stats']
        if 'subprocess_dir' in cfg.config['options'] and cfg.config['options']['subprocess_dir']:
            subdir = cfg.config['options']['subprocess_dir']
            self.error_filename = os.path.join(subdir, self.error_filename)
            self.stats_filename = os.path.join(subdir, self.stats_filename)

        if os.path.exists(self.error_filename):
            os.remove(self.error_filename)

    async def __aenter__(self):
        module_src = None
        if self.module['src']:
            if not functions.isurl(self.module['src']):
                module_src = self.module['src']
            else:
                # get script to run
                c = dataclasses.Class()
                c['src'] = self.module['src']
                c['name'] = os.path.basename(c['src'])
                if '?' in c['name']:
                    c['name'] = c['name'][:c['name'].find('?')]
                elif '#' in c['name']:
                    c['name'] = c['name'][:c['name'].find('#')]
                await setupClass(self.env,c,logger=self.logger)
                if c['name'] not in self.env['classes']:
                    raise Exception('Failed to install class %s'%c['name'])
                module_src = self.env['classes'][c['name']]

        # set up env_shell
        env_shell = None
        if self.module['env_shell']:
            env_shell = self.module['env_shell'].split()
            self.logger.info('searching for env_shell at %r', env_shell[0])
            if not os.path.exists(env_shell[0]):
                env_class = env_shell[0].split('/')[0]
                self.logger.info('searching for env_shell as %r class', env_class)
                if env_class in self.env['classes']:
                    env_tmp = env_shell[0].split('/')
                    env_tmp[0] = self.env['classes'][env_class]
                    env_shell[0] = '/'.join(env_tmp)
                else:
                    self.logger.info('attempting to download env_shell')
                    c = dataclasses.Class()
                    c['src'] = env_shell[0]
                    c['name'] = os.path.basename(c['src'])
                    await setupClass(self.env,c,logger=self.logger)
                    if c['name'] not in self.env['classes']:
                        raise Exception('Failed to install class %s'%c['name'])
                    env_shell[0] = self.env['classes'][c['name']]

        if module_src:
            self.logger.warning('running module \'%s\' with src %s',
                    self.module['name'], module_src)
        else:
            self.logger.warning('running module \'%s\' with class %s',
                    self.module['name'], self.module['running_class'])

        # set up the args
        args = self.module['args']
        if args:
            self.logger.warning('args=%s',args)
            if args and isinstance(args,dataclasses.String) and args[0] in ('{','['):
                args = json_decode(args)
            if args and isinstance(args, dict) and set(args) == {'args','kwargs'}:
                args = self.cfg.parseObject(args, self.env)
            elif isinstance(args,dataclasses.String):
                args = {"args":[self.cfg.parseValue(x,self.env) for x in args.split()],"kwargs":{}}
            elif isinstance(args,list):
                args = {"args":[self.cfg.parseValue(x,self.env) for x in args],"kwargs":{}}
            elif isinstance(args,dict):
                args = {"args":[],"kwargs":self.cfg.parseObject(args,self.env)}
            else:
                raise Exception('args is unknown type')

        # set up the environment
        cmd = []
        if env_shell:
            cmd.extend(env_shell)

        kwargs = {'close_fds': True}
        if 'subprocess_dir' in self.cfg.config['options'] and self.cfg.config['options']['subprocess_dir']:
            subdir = self.cfg.config['options']['subprocess_dir']
            if not os.path.exists(subdir):
                os.makedirs(subdir)
            kwargs['cwd'] = subdir
        else:
            kwargs['cwd'] = os.getcwd()
        self.stdout = open(os.path.join(kwargs['cwd'], constants['stdout']), 'ab')
        self.stderr = open(os.path.join(kwargs['cwd'], constants['stderr']), 'ab')
        kwargs['stdout'] = self.stdout
        kwargs['stderr'] = self.stderr

        # set up configs
        if self.module['configs']:
            for filename in self.module['configs']:
                self.logger.info('creating config %r', filename)
                with open(os.path.join(kwargs['cwd'], filename),'w') as f:
                    f.write(json_encode(self.module['configs'][filename]))

        # run the module
        if self.module['running_class']:
            self.logger.info('run as a class using the helper script')
            exe_helper = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      'exe_helper.py')
            cmd.extend(['python', exe_helper, '--classname',
                        self.module['running_class']])
            if self.env['options']['debug']:
                cmd.append('--debug')
            if module_src:
                cmd.extend(['--filename', module_src])
            if args:
                args_filename = constants['args']
                if 'cwd' in kwargs:
                    args_filename = os.path.join(kwargs['cwd'], args_filename)
                with open(args_filename,'w') as f:
                    f.write(json_encode(args))
                cmd.append('--args')
        elif module_src:
            self.logger.info('run as a script directly')
            if args:
                def splitter(a,b):
                    ret = ('-%s' if len(str(a)) <= 1 else '--%s')%str(a)
                    if b is None:
                        return ret
                    else:
                        return ret+'='+str(b)
                args = args['args']+[splitter(a,args['kwargs'][a]) for a in args['kwargs']]
                # force args to string
                def toStr(a):
                    if isinstance(a,(bytes,str)):
                        return a
                    else:
                        return str(a)
                args = [toStr(a) for a in args]
            else:
                args = []

            shebang = False
            if os.path.exists(module_src):
                try:
                    with open(module_src) as f:
                        if f.read(10).startswith('#!'):
                            # shebang found
                            mode = os.stat(module_src).st_mode
                            if not (mode & stat.S_IXUSR):
                                os.chmod(module_src, mode | stat.S_IXUSR)
                            shebang = True
                except Exception:
                    self.logger.warning('cannot get shebang for %s', module_src,
                                   exc_info=True)

            if (not shebang) and module_src[-3:] == '.py':
                # call as python script
                cmd.extend(['python', module_src]+args)
            elif (not shebang) and module_src[-3:] == '.sh':
                # call as shell script
                cmd.extend(['/bin/sh', module_src]+args)
            else:
                # call as regular executable
                cmd.extend([module_src]+args)
        else:
            self.logger.error('module is missing class and src')
            raise Exception('error running module')

        self.logger.warning('subprocess cmd=%r',cmd)
        if self.module['env_clear']:
            # must be on cvmfs-like environ for this to apply
            env = {'PYTHONNOUSERSITE':'1'}
            if 'SROOT' in os.environ:
                prefix = os.environ['SROOT']
            elif 'ICEPRODROOT' in os.environ:
                prefix = os.environ['ICEPRODROOT']
            else:
                prefix = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            for k in os.environ:
                if k in ('OPENCL_VENDOR_PATH','http_proxy','TMP','TMPDIR','_CONDOR_SCRATCH_DIR'):
                    # pass through unchanged
                    env[k] = os.environ[k]
                elif ('sroot' in k.lower() or 'iceprod' in k.lower() or
                      k in ('CUDA_VISIBLE_DEVICES','COMPUTE','GPU_DEVICE_ORDINAL')):
                    # don't pass these at all
                    pass
                else:
                    # filter SROOT out of environ
                    ret = [x for x in os.environ[k].split(':') if x.strip() and (not x.startswith(prefix)) and not 'iceprod' in x.lower()]
                    if ret:
                        env[k] = ':'.join(ret)
            # handle resource environment
            if 'resources' in self.cfg.config['options']:
                Resources.set_env(self.cfg.config['options']['resources'], env)
            self.logger.warning('env = %r', env)
            kwargs['env'] = env

        self.proc = await asyncio.create_subprocess_exec(*cmd, **kwargs)
        return self.proc

    async def __aexit__(self, exc_type, exc, tb):
        try:
            self.stdout.close()
            self.stderr.close()
        except Exception:
            pass
        if not exc_type:
            # now clean up after process
            if self.proc and self.proc.returncode:
                self.logger.warning('return code: {}'.format(self.proc.returncode))
                try:
                    with open(self.error_filename, 'rb') as f:
                        e = pickle.load(f)
                except Exception:
                    self.logger.warning('cannot load exception info from failed module', )
                    raise Exception('module failed')
                else:
                    if isinstance(e, Exception):
                        raise e
                    else:
                        raise Exception(str(e))

            # get stats, if available
            if os.path.exists(self.stats_filename):
                try:
                    new_stats = pickle.load(open(self.stats_filename, 'rb'))
                    if self.module['name']:
                        self.stats[module['name']] = new_stats
                    else:
                        self.stats.update(new_stats)
                except Exception:
                    self.logger.warning('cannot load stats info from module')
