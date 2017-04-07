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

try:
    import cPickle as pickle
except:
    import pickle

import logging
logger = logging.getLogger('exe')

# make sure we have subprocess with timeout support
if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

from iceprod.core import to_log,constants
from iceprod.core import util
from iceprod.core import dataclasses
from iceprod.core import util
from iceprod.core import functions
from iceprod.core.exe_json import stillrunning
import iceprod.core.parser
from iceprod.core.jsonUtil import json_encode,json_decode


class Config:
    """Contain the configuration and related methods"""
    def __init__(self, config=None, parser=None):
        self.config = config if config else dataclasses.Job()
        self.parser = parser if parser else iceprod.core.parser.ExpParser()

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
            logger.debug('parse before:%r| env=%r',value,env)
            value = self.parser.parse(value,self.config,env)
            if isinstance(value,dataclasses.String):
                value = os.path.expandvars(value)
            logger.debug('parse after:%r',value)
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

@contextmanager
def setupenv(cfg, obj, oldenv={}):
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
    try:
        # start with empty env
        env = {}
        # attempt to do depth=2 copying
        for key in oldenv:
            if key not in ('deletions','uploads','environment','pythonpath','stats'):
                env[key] = copy.copy(oldenv[key])

        if not obj:
            raise util.NoncriticalError('object to load environment from is empty')
        if isinstance(obj,dataclasses.Steering) and not obj.valid():
            raise Exception('object is not valid Steering')

        # make sure things for this env are clear (don't inherit)
        env['deletions'] = []
        env['uploads'] = []

        # get clear environment variables
        env['environment'] = os.environ.copy()
        env['pythonpath'] = copy.copy(sys.path)

        # inherit statistics
        if 'stats' in oldenv:
            env['stats'] = oldenv['stats']
        else:
            env['stats'] = {'upload':[], 'download':[], 'tasks':[]}

        # copy parameters
        if 'parameters' not in env:
            env['parameters'] = {}
        if 'parameters' in obj:
            # copy new parameters to env first so local referrals work
            env['parameters'].update(obj['parameters'])
            # parse parameter values and update if necessary
            for p in obj['parameters']:
                newval = cfg.parseValue(obj['parameters'][p],env)
                if newval != obj['parameters'][p]:
                    env['parameters'][p] = newval

        if 'resources' not in env:
            env['resources'] = {}
        if 'resources' in obj:
            # download resources
            for resource in obj['resources']:
                downloadResource(env, cfg.parseObject(resource,env))

        if 'data' not in env:
            env['data'] = {}
        if 'data' in obj:
            # download data
            for data in obj['data']:
                d = cfg.parseObject(data,env)
                if d['movement'] in ('input','both'):
                    downloadData(env,d)
                if d['movement'] in ('output','both'):
                    env['uploads'].append(d)

        if 'classes' not in env:
            env['classes'] = {}
        if 'classes' in obj:
            # set up classes
            for c in obj['classes']:
                setupClass(env, cfg.parseObject(c,env))

    except util.NoncriticalError as e:
        logger.warning('Noncritical error when setting up environment',exc_info=True)
    except Exception as e:
        logger.critical('Serious error when setting up environment',exc_info=True)
        raise

    try:
        yield env
        
        # upload data
        if 'uploads' in env:
            for d in env['uploads']:
                try:
                    uploadData(env, d)
                except util.NoncriticalError, e:
                    logger.error('failed when uploading file %s - %s' % (str(d),str(d)))
                    if 'options' in env and 'debug' in env['options'] and env['options']['debug']:
                        raise
    finally:
        # delete any files
        if 'deletions' in env and len(env['deletions']) > 0:
            for f in reversed(env['deletions']):
                try:
                    os.remove(f)
                    base = os.path.basename(f)
                except OSError, e:
                    logger.error('failed to delete file %s - %s',(str(f),str(e)))
                    if 'options' in env and 'debug' in env['options'] and env['options']['debug']:
                        raise

        # reset environment
        if 'environment' in env:
            for e in os.environ.keys():
                if e not in env['environment']:
                    del os.environ[e]
            for e in env['environment'].keys():
                os.environ[e] = env['environment'][e]

def downloadResource(env, resource, remote_base=None,
                     local_base=None):
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
    if not local_base:
        if 'resource_directory' in env['options']:
            local_base = env['options']['resource_directory']
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
            functions.download(url, local, options=download_options)
            if not os.path.exists(local):
                raise Exception('file does not exist')
        except Exception:
            failed = True
            logger.critical('failed to download %s to %s', url, local, exc_info=True)
            raise Exception('failed to download {} to {}'.format(url, local))
        finally:
            if 'stats' in env:
                stats = {
                    'name': url,
                    'error': failed,
                    'now': datetime.utcnow().isoformat(),
                    'duration': time.time()-start_time,
                }
                if not failed:
                    stats['size'] = os.path.getsize(local)
                    stats['rate_MBps'] = stats['size']/1000/1000/stats['duration']
                env['stats']['download'].append(stats)

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
    logger.warn('resource %s added to env',resource['local'])

def downloadData(env, data):
    """Download data and put location in the env"""
    remote_base = data.storage_location(env)
    if 'options' in env and 'data_directory' in env['options']:
        local_base = env['options']['data_directory']
    else:
        local_base = os.getcwd()
    downloadResource(env,data,remote_base,local_base)

def uploadData(env, data):
    """Upload data"""
    remote_base = data.storage_location(env)
    if 'options' in env and 'data_directory' in env['options']:
        local_base = env['options']['data_directory']
    else:
        local_base = os.getcwd()
    if not data['remote']:
        url = os.path.join(remote_base, data['local'])
    elif not functions.isurl(data['remote']):
        url = os.path.join(remote_base, data['remote'])
    else:
        url = data['remote']
    local = os.path.join(local_base,data['local'])
    if not os.path.exists(local):
        raise util.NoncriticalError('file %s does not exist'%local)

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
    failed = False
    try:
        start_time = time.time()
        functions.upload(local, url, options=upload_options)
    except:
        failed = True
        logger.critical('failed to upload %s to %s', local, url, exc_info=True)
        raise Exception('failed to upload {} to {}'.format(local, url))
    finally:
        if 'stats' in env:
            stats = {
                'name': url,
                'error': failed,
                'now': datetime.utcnow().isoformat(),
                'duration': time.time()-start_time,
            }
            if not failed:
                stats['size'] = os.path.getsize(local)
                stats['rate_MBps'] = stats['size']/1000/1000/stats['duration']
            env['stats']['upload'].append(stats)

def setupClass(env, class_obj):
    """Set up a class for use in modules, and put it in the env"""
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
            if local_temp not in os.environ['PYTHONPATH']:
                os.environ['PYTHONPATH'] += ':'+local_temp

            local = os.path.join(local_temp,class_obj['name'].replace(' ','_'))

            download_options = {}
            if 'options' in env and 'username' in env['options']:
                download_options['username'] = env['options']['username']
            if 'options' in env and 'password' in env['options']:
                download_options['password'] = env['options']['password']
            if 'options' in env and 'ssl' in env['options'] and env['options']['ssl']:
                download_options.update(env['options']['ssl'])

            # download class
            logger.warn('attempting to download class %s to %s',url,local_temp)
            try:
                download_local = functions.download(url, local_temp,
                                                    options=download_options)
            except Exception:
                logger.info('failed to download', exc_info=True)
                if i < 10:
                    i += 1
                    continue # retry with different url
                raise
            if not os.path.exists(download_local):
                raise Exception('download failed')
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
                    logger.warn('files is strange datatype: %r',
                                type(files))
            elif local != download_local:
                logger.info('rename %r to %r', download_local, local)
                os.rename(download_local, local)
            loaded = True
            break

    if loaded:
        # add to env
        env['classes'][class_obj['name']] = local
        logger.warn('class %s loaded at %r',class_obj['name'],local)

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
                except ValueError,e:
                    logger.warning('bad env variable: %s',e)
                    continue
                v = v.replace('$CLASS',local)
                logger.info('setting envvar: %s = %s',k,v)
                if k in os.environ:
                    os.environ[k] = v+':'+os.environ[k]
                else:
                    os.environ[k] = v

### Run Functions ###

def runtask(cfg, globalenv, task):
    """Run the specified task"""
    if not task:
        raise Exception('No task provided')

    # set up task_temp
    if not os.path.exists('task_temp'):
        os.mkdir('task_temp')
    globalenv['task_temp'] = os.path.join(os.getcwd(),'task_temp')

    # set up stats
    stats = {}

    try:
        # set up local env
        with setupenv(cfg, task, globalenv) as env:
            # run trays
            for tray in task['trays']:
                tmpstat = {}
                runtray(cfg, env, tray, stats=tmpstat)
                if len(tmpstat) > 1:
                    stats[tray['name']] = tmpstat
                elif len(tmpstat) == 1:
                    stats[tray['name']] = tmpstat[tmpstat.keys()[0]]
    finally:
        # destroy task temp
        try:
            functions.removedirs('task_temp')
        except Exception as e:
            logger.warning('error removing task_temp directory: %r',
                           e, exc_info=True)

    globalenv['stats']['tasks'].append(stats)

def runtray(cfg, globalenv,tray,stats={}):
    """Run the specified tray"""
    if not tray:
        raise Exception('No tray provided')

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
            with setupenv(cfg, tray, tmpenv) as env:
                # run modules
                for module in tray['modules']:
                    runmodule(cfg, env, module, stats=tmpstat)
            stats[i] = tmpstat

    finally:
        # destroy tray temp
        try:
            functions.removedirs('tray_temp')
        except Exception as e:
            logger.warning('error removing tray_temp directory: %s',
                           str(e), exc_info=True)

def runmodule(cfg, globalenv, module, stats={}):
    """Run the specified module"""
    if not module:
        raise Exception('No module provided')

    # set up local env
    with setupenv(cfg, module, globalenv) as env:
        if module['running_class']:
            module['running_class'] = cfg.parseValue(module['running_class'],env)
        if module['args']:
            module['args'] = cfg.parseValue(module['args'],env)
        if module['src']:
            module['src'] = cfg.parseValue(module['src'],env)
        if module['env_shell']:
            module['env_shell'] = cfg.parseValue(module['env_shell'],env)

        # make subprocess to run the module
        if os.path.exists(constants['task_exception']):
            os.remove(constants['task_exception'])
        process = fork_module(cfg, env, module)
        try:
            interval = float(cfg.config['options']['stillrunninginterval'])
        except:
            interval = 0
        if interval < 60:
            interval = 60
        while process.poll() is None:
            if ('offline' in cfg.config['options'] and
                not cfg.config['options']['offline']):
                # check for DB kill
                try:
                    stillrunning(cfg)
                except:
                    if process.poll() is None:
                        process.kill()
                        time.sleep(1)
                    logger.critical('DB kill')
                    raise
            try:
                process.wait(interval)
            except subprocess.TimeoutExpired:
                pass
        if process.returncode:
            try:
                with open(constants['task_exception']) as f:
                    e = pickle.load(f)
                    if isinstance(e, Exception):
                        raise e
                    else:
                        raise Exception(str(e))
            except:
                logger.warn('cannot load exception info from failed module')
                raise Exception('module failed')

        # get stats, if available
        if os.path.exists(constants['stats']):
            new_stats = pickle.load(open(constants['stats']))
            if module['name']:
                stats[module['name']] = new_stats
            else:
                stats.update(new_stats)

def fork_module(cfg, env, module):
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
    module_src = None
    if module['src']:
        # get script to run
        c = dataclasses.Class()
        c['src'] = module['src']
        c['name'] = os.path.basename(c['src'])
        if '?' in c['name']:
            c['name'] = c['name'][:c['name'].find('?')]
        elif '#' in c['name']:
            c['name'] = c['name'][:c['name'].find('#')]
        setupClass(env,c)
        if c['name'] not in env['classes']:
            raise Exception('Failed to install class %s'%c['name'])
        module_src = env['classes'][c['name']]

    # set up env_shell
    env_shell = None
    if module['env_shell']:
        env_shell = module['env_shell'].split()
        logger.info('searching for env_shell at %r', env_shell[0])
        if not os.path.exists(env_shell[0]):
            env_class = env_shell[0].split('/')[0]
            logger.info('searching for env_shell as %r class', env_class)
            if env_class in env['classes']:
                env_tmp = env_shell[0].split('/')
                env_tmp[0] = env['classes'][env_class]
                env_shell[0] = '/'.join(env_tmp)
            else:
                logger.info('attempting to download env_shell')
                c = dataclasses.Class()
                c['src'] = env_shell[0]
                c['name'] = os.path.basename(c['src'])
                setupClass(env,c)
                if c['name'] not in env['classes']:
                    raise Exception('Failed to install class %s'%c['name'])
                env_shell[0] = env['classes'][c['name']]

    logger.warn('running module \'%s\' with class %s',module['name'],
                module['running_class'])

    # set up the args
    args = module['args']
    if args:
        logger.warn('args=%s',args)
        if (args and isinstance(args,dataclasses.String) and
            args[0] in ('{','[')):
            args = json_decode(args)
        if isinstance(args,dataclasses.String):
            args = {"args":[cfg.parseValue(x,env) for x in args.split()],"kwargs":{}}
        elif isinstance(args,list):
            args = {"args":[cfg.parseValue(x,env) for x in args],"kwargs":{}}
        elif isinstance(args,dict):
            args = {"args":[],"kwargs":cfg.parseObject(args,env)}
        else:
            raise Exception('args is unknown type')

    # set up the environment
    cmd = []
    if env_shell:
        cmd.extend(env_shell)

    # run the module
    if module['running_class']:
        logger.info('run as a class using the helper script')
        cmd.extend(['python', '-m', 'iceprod.core.exe_helper', '--classname',
                    module['running_class']])
        if env['options']['debug']:
            cmd.append('--debug')
        if module_src:
            cmd.extend(['--filename', module_src])
        if args:
            with open(constants['args'],'w') as f:
                f.write(json_encode(args))
            cmd.append('--args')
    elif module_src:
        logger.info('run as a script directly')
        if args:
            def splitter(a,b):
                ret = ('-%s' if len(str(a)) <= 1 else '--%s')%str(a)
                if b is None:
                    return ret
                else:
                    return ret+'='+str(b)
            args = args['args']+[splitter(a,args['kwargs'][a]) for a in args['kwargs']]
        else:
            args = []

        if module_src[-3:] == '.py':
            # call as python script
            cmd.extend(['python', module_src]+args)
        elif module_src[-3:] == '.sh':
            # call as shell script
            cmd.extend(['/bin/sh', module_src]+args)
        else:
            # call as regular executable
            cmd.extend([module_src]+args)
    else:
        logger.error('module is missing class and src')
        raise Exception('error running module')

    logger.warn('subprocess cmd=%r',cmd)
    if module['env_clear']:
        iceprod_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        local_path = env['options']['local_temp'] if 'local_temp' in env['options'] else None
        env = {'PYTHONPATH':iceprod_path+(':'+local_path if local_path else '')+':'+os.getcwd()}
        for k in ('CUDA_VISIBLE_DEVICES','COMPUTE','GPU_DEVICE_ORDINAL','http_proxy'):
            if k in os.environ:
                env[k] = os.environ[k]
        logger.warn('env = %r', env)
        return subprocess.Popen(cmd, env=env)
    else:
        return subprocess.Popen(cmd)
