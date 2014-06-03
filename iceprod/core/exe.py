"""
The core execution functions for running on a node.

These are all called from `i3exec`.
"""

from __future__ import absolute_import, division, print_function

import sys
import os
import time
import imp
import subprocess
import multiprocessing
import glob
import copy
import filecmp
import tempfile
import shutil
import inspect
from functools import partial
from collections import Container

try:
    import cPickle as pickle
except:
    import pickle

import logging
logger = logging.getLogger('exe')

from iceprod.core import to_log,constants
from iceprod.core import util
from iceprod.core import dataclasses
from iceprod.core import functions
from iceprod.core import parser
from iceprod.core.jsonRPCclient import JSONRPC
from iceprod.core.jsonUtil import json_compressor,json_decode
from iceprod.modules.ipmodule import IPBaseClass


### Environment Functions ###

config = dataclasses.Job()
parseObj = parser.ExpParser()
def parseValue(value,env={}):
    """
    Parse a value from the available env and global config.
    
    Uses the :class:`Meta Parser <iceprod.core.parser>` on any string value.
    Pass-through for any other object.
    
    :param value: The value to parse
    :param env: The environment to use, optional
    :returns: The parsed value
    """
    if isinstance(value,str):
        return os.path.expandvars(parseObj.parse(value,config,env))
    else:
        return value

def parseObject(obj,env):
    """Run :func:`parseValue` on all attributes of an object"""
    ret = copy.copy(obj)
    for attr in obj.keys():
        tmp = obj[attr]
        if isinstance(tmp,str):
            ret[attr] = parseValue(tmp,env)
    return ret

def setupenv(obj,oldenv={}):
    """Set up an environment to run things in"""
    try:
        # start with empty env
        env = {}
        # attempt to do depth=2 copying
        for key in oldenv:
            env[key] = copy.copy(oldenv[key])
        
        if not obj:
            raise dataclasses.NoncriticalError('object to load environment from is empty')
        
        # make sure deletions for this env are clear (don't inherit)
        env['deletions'] = []
        
        # get clear environment variables
        env['environment'] = os.environ.copy()
        env['pythonpath'] = copy.copy(sys.path)
        
        # copy parameters
        if 'parameters' not in env:
            env['parameters'] = {}
        if 'parameters' in obj:
            # copy new parameters to env first so local referrals work
            env['parameters'].update(obj['parameters'])
            # parse parameter values and update if necessary
            for p in obj['parameters']:
                newval = parseValue(obj['parameters'][p],env)
                if newval != obj['parameters'][p]:
                    env['parameters'][p] = newval
        
        if 'resources' not in env:
            env['resources'] = {}
        if 'resources' in obj:
            # download resources
            for resource in obj['resources']:
                downloadResource(env,parseObject(resource,env))
        
        if 'data' not in env:
            env['data'] = {}
        if 'data' in obj:
            # download data
            for data in obj['data']:
                d = parseObject(data,env)
                if d['movement'] in ('input','both'):
                    downloadData(env,d)
                if d['movement'] in ('output','both'):
                    if 'uploads' not in env:
                        env['uploads'] = []
                    env['uploads'].append(d)
            
        if 'classes' not in env:
            env['classes'] = {}
        if 'classes' in obj:
            # set up classes
            for c in obj['classes']:
                setupClass(env,parseObject(c,env))
        
        if 'projects' not in env:
            env['projects'] = {}
        if 'projects' in obj:
            # set up projects
            for project in obj.projects:
                setupProject(env,parseObject(project,env))
        
    except dataclasses.NoncriticalError as e:
        logger.warning('Noncritical error when setting up environment',exc_info=True)
    except Exception as e:
        logger.critical('Serious error when setting up environment',exc_info=True)
        raise
    return env
    
def destroyenv(env):
    """Do cleanup on env destruction"""
    # upload data
    if 'uploads' in env:
        for d in env['uploads']:
            try:
                uploadData(env,d)
            except dataclasses.NoncriticalError, e:
                logger.error('failed when uploading file %s - %s' % (str(d),str(d)))
                if 'parameters' in env and 'debug' in env['parameters'] and env['parameters']['debug']:
                    raise
    
    # delete any files
    if 'deletions' in env and len(env['deletions']) > 0:
        for f in reversed(env['deletions']):
            try:
                os.remove(f)
                base = os.path.basename(f)
                if 'ld_local_path' in env and base in env['ld_local_path']:
                    env['ld_local_path'][base].pop() # remove current deletion from list
                    if len(env['ld_local_path'][base]) > 0:
                        (src,dest) = env['ld_local_path'][base][-1] # ressurect last link
                        os.symlink(src,dest)
                    else:
                        del env['ld_local_path'][base] # remove file from dict
            except OSError, e:
                logger.error('failed to delete file %s - %s',(str(f),str(e)))
                if 'parameters' in env and 'debug' in env['parameters'] and env['parameters']['debug']:
                    raise
    
    # reset environment
    if 'environment' in env:
        for e in os.environ.keys():
            if e not in env['environment']:
                del os.environ[e]
        for e in env['environment'].keys():
            os.environ[e] = env['environment'][e]
    if 'pythonpath' in env:
        sys.path = env['pythonpath']

def downloadResource(env,resource,remote_base=None,local_base=None):
    """Download a resource and put location in the env"""
    if not remote_base:
        if 'resource_url' in env['parameters']:
            remote_base = env['parameters']['resource_url']
        else:
            remote_base = 'http://x2100.icecube.wisc.edu'
    if functions.isurl(resource['remote']):
        url = resource['remote']
    else:
        url = os.path.join(remote_base,resource['remote'])
    if not local_base:
        if 'resource_directory' in env['parameters']:
            local_base = env['parameters']['resource_directory']
        else:
            local_base = 'resource'
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
        if 'parameters' in env and 'http_username' in env['parameters']:
            download_options['http_username'] = env['parameters']['http_username']
        if 'parameters' in env and 'http_password' in env['parameters']:
            download_options['http_password'] = env['parameters']['http_password']
        
        if not functions.download(url,local,options=download_options):
            raise dataclasses.NoncriticalError('Failed to download %s'%url)
    
    # check compression
    if resource['compression']:
        # uncompress file
        try:
            files = functions.uncompress(local)
        except dataclasses.NoncriticalError:
            pass
        else:
            # add uncompressed file(s) to env
            env['files'][resource.local] = files
            return
    
    # add file to env
    env['files'][resource['local']] = local
    logger.warn('resource %s added to env',resource['local'])

def downloadData(env,data):
    """Download data and put location in the env"""
    remote_base = data['storage_location'](env)
    if 'data_directory' in env['parameters']:
        local_base = env['parameters']['data_directory']
    else:
        local_base = 'data'
    downloadResource(env,data,remote_base,local_base)

def uploadData(env,data):
    """Upload data"""
    parameters = env['parameters']
    remote_base = data['storage_location'](env)
    if 'data_directory' in parameters:
        local_base = parameters['data_directory']
    else:
        local_base = 'data'
    url = os.path.join(remote_base,data['remote'])
    local = os.path.join(local_base,data['local'])
    if not os.path.exists(local):
        raise dataclasses.NoncriticalError('file %s does not exist'%local)
    
    # remove tar or compress file extensions to get at the real file
    suffixes = ('.tar',)+functions.compress_suffixes
    local2 = reduce(lambda a,b:a.replace(b,''),suffixes,local)
    if os.path.isdir(local) or functions.istarred(url):
        # make a tar file
        try:
            local2 = functions.tar(local2+'.tar',local2,workdir=local_base)
        except dataclasses.NoncriticalError:
            pass
        if not '.tar' in url:
            newlocal = tempfile.mkstemp(dir=local_base)[1]
            shutil.move(local2,newlocal)
            local2 = newlocal
    
    # check compression
    if data['compression']:
        # get compression type, if specified
        c = functions.iscompressed(url)
        if c:
            # url has compression on it, so use that
            try:
                local2 = functions.compress(local2,c)
            except dataclasses.NoncriticalError as e:
                logger.warning('cannot compress file %s'%local)
                pass
    
    # upload file
    proxy = False
    upload_options = {}
    if 'parameters' in env and 'http_username' in parameters:
        upload_options['http_username'] = parameters['http_username']
    if 'parameters' in env and 'http_password' in parameters:
        upload_options['http_password'] = parameters['http_password']
    if 'parameters' in env and 'proxy' in parameters:
        proxy = parameters['proxy']
    try:
        ret = functions.upload(local,url,proxy=proxy,options=upload_options)
        if not ret:
            raise dataclasses.NoncriticalError('upload returned false')
    except dataclasses.NoncriticalError as e:
        logger.critical('cannot upload file %s'%(str(e)))
        raise

def setupClass(env,class_obj):
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
            if not isinstance(local,str):
                local = local[0]
            if (class_obj.src and 
                os.path.exists(os.path.join(local,class_obj['src']))):
                # treat src as a path inside the resource
                local = os.path.join(local,class_obj['src'])
            loaded = True
    else:
        # get url of class
        i = 0
        while True:
            url = class_obj['src']
            if functions.isurl(url):
                i = 10 # skip repeat download attempts
            else:
                if i == 0:
                    # first, look in resources
                    if 'parameters' in env and 'resource_url' in env['parameters']:
                        url = os.path.join(env['parameters']['resource_url'],class_obj['src'])
                    else:
                        url = os.path.join('http://x2100.icecube.wisc.edu/downloads/',class_obj['src'])
                elif i == 1:
                    # then, look in regular svn
                    if 'parameters' in env and 'svn_repository' in env['parameters']:
                        url = os.path.join(env['parameters']['svn_repository'],class_obj['src'])
                    else:
                        url = os.path.join('http://code.icecube.wisc.edu/svn/projects/',class_obj['src'])
                else:
                    raise dataclasses.NoncriticalError('Cannot find class %s because of bad src url'%class_obj['name'])
            
            if 'parameters' in env and 'local_temp' in env['parameters']:
                local_temp = env['parameters']['local_temp']
            else:
                local_temp = os.path.join(os.getcwd(),'classes')
            if not os.path.exists(local_temp):
                os.makedirs(local_temp)
                
            local = os.path.join(local_temp,class_obj['name'].replace(' ','_'))
            
            download_options = {}
            if 'parameters' in env and 'http_username' in env['parameters']:
                download_options['http_username'] = env['parameters']['http_username']
            if 'parameters' in env and 'http_password' in env['parameters']:
                download_options['http_password'] = env['parameters']['http_password']
            
            # download class
            logger.warn('attempting to download class %s',url)
            try:
                if not os.path.exists(local) and not functions.download(url,local,options=download_options):
                    if i < 10:
                        i += 1
                        continue # retry with different url
                    raise dataclasses.NoncriticalError('Failed to download %s'%url)
            except:
                if i < 10:
                    i += 1
                    continue # retry with different url
                raise
            try:
                files = functions.uncompress(local)
            except dataclasses.NoncriticalError:
                pass
            else:
                # check if we extracted a tarfile
                if isinstance(files,str):
                    local = files
                elif isinstance(files,list):
                    dirname = os.path.join(local_temp,os.path.commonprefix(files))
                    if os.path.isdir(dirname):
                        local = dirname
            loaded = True
            break
        
    if loaded:
        # add to env
        env['classes'][class_obj['name']] = local
        logger.warn('class %s loaded at %r',class_obj['name'],local)
        
        # add binary libraries to the LD_LIBRARY_PATH
        local_lib = os.path.join(os.getcwd(),'resource_libs')  # must be the same as specified in loader.sh
        if not os.path.exists(local_lib):
            os.makedirs(local_lib)
        if 'deletions' not in env:
            env['deletions'] = []
        if 'ld_local_path' not in env:
            env['ld_local_path'] = {}
        def ldpath(root,f):
            src = os.path.join(root,f)
            if src[0] != '/':
                src = os.path.join(os.getcwd(),src)
            if f[-3:] == '.so' or '.so.' in f or f[-2:] == '.a' or '.a.' in f:
                dest = os.path.join(local_lib,f)
                if os.path.exists(dest):
                    if filecmp.cmp(src,os.readlink(dest),False):
                        logger.warning('library has same name but different contents: %s',str(src))
                        os.remove(dest)
                    else: # if files are exactly the same, skip
                        return
                os.symlink(src,dest)
                env['deletions'].append(dest)
                if f not in env['ld_local_path']:
                    env['ld_local_path'][f] = [(src,dest)]
                else:
                    env['ld_local_path'][f].append((src,dest))
            else:
                logger.debug('not a binary library file: %s',str(src))
        def addToPythonPath(root):
            # only add to PYTHONPATH if not there
            if 'lib' in root.split(os.sep) or 'lib64' in root.split(os.sep):
                for p in sys.path:
                    d = os.path.commonprefix([root,p])
                    if not d or d in ('',os.sep):
                        continue
                    elif d in sys.path and ('lib' in d.split(os.sep) 
                        or 'lib64' in d.split(os.sep)):
                        logger.debug('already in PYTHONPATH: %s as %s',root,d)
                        return
            # add to PYTHONPATH
            logger.info('adding to PYTHONPATH: %s',root)
            sys.path.append(root)
            os.environ['PYTHONPATH'] += ':'+root
        if os.path.isdir(local):
            # build search list
            search_list = []
            if class_obj['libs'] is not None:
                search_list.extend(class_obj['libs'].split(':'))
                if 'lib64' not in search_list:
                    search_list.append('lib64')
                if 'lib' not in search_list:
                    search_list.append('lib')
            else:
                search_list = ['lib64','lib']
            # do general search
            for root, dirs, files in os.walk(local):
                dirs = filter(lambda a:a not in search_list,dirs)
                for f in files:
                    if f.endswith('.py'):
                        addToPythonPath(root)
                    else:
                        # check for binary library
                        ldpath(root,f)
            # now search by list
            if search_list is not None and len(search_list) > 0:    
                for s in reversed(search_list):
                    s_dir = os.path.join(local,s)
                    if not os.path.isdir(s_dir):
                        continue
                    # add to the python path so imports work
                    addToPythonPath(s_dir)
                    # add to LD_LIBRARY_PATH
                    for root,dirs,files in os.walk(s_dir):
                        for f in files:
                            ldpath(root,f)
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
        if class_obj['env_vars'] is not None:
            for e in class_obj['env_vars'].split(';'):
                try:
                    k,v = e.split('=')
                except ValueError,e:
                    logger.warning('bad env variable: %s',e)
                    continue
                v = v.replace('$CLASS',local)
                logger.info('setting envvar: %s = %s',k,v)
                if k == 'PYTHONPATH':
                    sys.path.append(v)
                    os.environ['PYTHONPATH'] += ':'+v
                elif k in os.environ:
                    os.environ[k] = v+':'+os.environ[k]
                else:
                    os.environ[k] = v

def setupProject(env,project):
    """Set up a project for use in modules, and put it in the env"""
    if not 'projects' in env:
        env['projects'] = {}
    if not project:
        raise Exception('Project is not defined')
    if project['name'] in env['projects']:
        # project already loaded, so leave it alone
        logger.info('project %s already loaded'%project['name'] )
    else:
        logger.info('project %s being loaded from %s',project['name'] ,project['class_name'])
        # import project
        try:
            x = __import__(project['class_name'],globals(),locals(),[project['class_name']])
        except ImportError as e:
            # try as iceprod.modules
            try:
                x = __import__('iceprod.modules.'+project['class_name'],globals(),locals(),[project['class_name']])
            except ImportError as e:
                logger.error('cannot import project %s: %s',str(project['class_name']),str(e))
                pass
            else:
                # add to env
                env['projects'][project['name'] ] = x
        else:
            # add to env
            env['projects'][project['name'] ] = x

### Run Functions ###

def runtask(globalenv,task):
    """Run the specified task"""
    if not task:
        raise Exception('No task provided')
    ret = []

    # set up task_temp
    if not os.path.exists('task_temp'):
        os.mkdir('task_temp')
    globalenv['task_temp'] = os.path.join(os.getcwd(),'task_temp')
    
    # set up stats
    stats = {}
    
    try:
        # set up local env
        env = setupenv(task,globalenv)
        
        try:
            # run trays
            for tray in task['trays']:
                tmpstat = {}
                ret.append(runtray(env, task['trays'][tray], stats=tmpstat))
                if len(tmpstat) > 1:
                    stats[task['trays'][tray]['name']] = tmpstat
                elif len(tmpstat) == 1:
                    stats[task['trays'][tray]['name']] = tmpstat[tmpstat.keys()[0]]
        
        finally:
            # destroy env
            destroyenv(env)
            del env
        
        # finish task
        if ('offline' in config['options'] and 
            not config['options']['offline']):
            try:
                finishtask(stats)
            except Exception as e:
                logger.error('cannot finish task: %r',e,exc_info=True)
    
    finally:
        # destroy task temp
        try:
            functions.removedirs('task_temp')
        except Exception as e:
            logger.warning('error removing task_temp directory: %r',
                           e, exc_info=True)
    
    if len(ret) == 1:
        return ret[0]
    else:
        return ret

def runtray(globalenv,tray,stats={}):
    """Run the specified tray"""
    if not tray:
        raise Exception('No tray provided')
    ret = []

    # set up tray_temp
    if not os.path.exists('tray_temp'):
        os.mkdir('tray_temp')
    globalenv['tray_temp'] = os.path.join(os.getcwd(),'tray_temp')
    
    # run iterations
    try:
        tmpenv = globalenv.copy()
        for i in xrange(tray['iterations']):
            # set up local env
            tmpenv['parameters']['tray_iteration'] = i
            env = setupenv(tray,tmpenv)
            tmpret = []
            tmpstat = {}
            
            try:
                # run modules
                for module in tray['modules']:
                    tmpret.append(runmodule(env, tray['modules'][module],
                                            stats=tmpstat))
                
            finally:
                stats[i] = tmpstat
                # destroy env
                ret.append(tmpret)
                destroyenv(env)
                del env
    
    finally:
        # destroy tray temp
        try:
            functions.removedirs('tray_temp')
        except Exception as e:
            logger.warning('error removing tray_temp directory: %s',
                           str(e), exc_info=True)
    
    if len(ret) == 1:
        return ret[0]
    else:
        return ret

def runmodule(globalenv,module,stats={}):
    """Run the specified module"""
    if not module:
        raise Exception('No module provided')
    ret = None
    
    # set up local env
    env = setupenv(module,globalenv)
    if module['running_class']:
        module['running_class'] = parseValue(module['running_class'],env)
    if module['args']:
        module['args'] = parseValue(module['args'],env)
    if module['src']:
        module['src'] = parseValue(module['src'],env)
    
    try:
        # launch multiprocessing to handle actual module
        queue = multiprocessing.Queue()
        process = multiprocessing.Process(target=run_module,
                                          args=[env,module,queue])
        process.start()
        try:
            interval = float(config['options']['stillrunninginterval'])
        except:
            interval = 0
        if interval < 60:
            interval = 60
        while process.is_alive():
            if ('offline' in config['options'] and 
                not config['options']['offline']):
                # check for DB kill
                try:
                    stillrunning()
                except:
                    if process.is_alive():
                        process.terminate()
                        time.sleep(1)
                    logger.critical('DB kill')
                    raise
            process.join(interval)
        try:
            data = queue.get(False)
        except:
            pass
        else:
            if data:
                try:
                    # return value of module
                    ret = pickle.loads(data)
                except:
                    logger.error('error decoding data from process')
                    raise
                else:
                    if isinstance(ret,Exception):
                        # there was an exception during the module
                        logger.warn('exception in module - %r',ret)
                        raise ret
            else:
                logger.error('run_module did not return data')
                raise NoncriticalError('run_module did not return data')
    finally:
        # destroy env
        destroyenv(env)
        del env
    
    # split out stats, if any
    if isinstance(ret,tuple):
        if module['name']:
            stats[module['name']] = ret[1]
        else:
            stats.update(ret[1])
        ret = ret[0]
    return ret

def run_module(env,module,queue):
    """Helper to runmodule.  Runs in a separate process to contain
       any badness that happens here.
    """
    # make empty stats dict so the module can fill it
    env['stats'] = {}
    try:
        if module['src']:
            # get script to run
            c = dataclasses.Class()
            c['src'] = module['src']
            c['name'] = os.path.basename(c['src'])
            setupClass(env,c)
            if c['name'] not in env['classes']:
                raise dataclasses.NoncriticalError('Failed to install class %s'%c['name'])
            logger.info('class stored at %s',env['classes'][c['name']])
        
        # run the module script
        logger.warn('running module \'%s\' with class %s',module['name'],
                    module['running_class'])
        ret = None
        if not module['running_class']:
            script = True
        else:
            script = False
            mods = module['running_class'].rsplit('.',1)
            if len(mods) < 2:
                cl = module['running_class']
                if module['src']:
                    mod = os.path.splitext(c['name'])[0]
                else:
                    # check if it is in a project
                    for p in env['projects']:
                        if cl in {x for x in dir(env['projects'][p]) if x[0] != '_'}:
                            mod = env['projects'][p]
                    else:
                        raise dataclasses.NoncriticalError('Must specify full python path to class to run in module.running_class')
            else:
                mod,cl = mods
                if mod.startswith('iceprod.modules'):
                    mod = mod.split('.',2)[2]
            try:
                logger.warn('attempt to import module %s with class %s',mod,cl)
                if not isinstance(mod,str):
                    logger.info('mod already loaded')
                    x = mod
                elif (mod in env['projects'] and cl in 
                      {x for x in dir(env['projects'][mod]) if x[0] != '_'}):
                    logger.info('from a project')
                    x = env['projects'][mod]
                elif module.src:
                    logger.info('from src')
                    if module.src[-3:] == '.py':
                        filepath = env['classes'][c['name']]
                        if '.' in mod:
                            mod = mod.rsplit('.',1)[1]
                        x = imp.load_source(mod,filepath)
                    else:
                        script = True
                else:
                    logger.info('raw import')
                    x = __import__(mod,globals(),locals(),[cl])
            except Exception as e:
                if str(e) == 'No module named %s' % mod:
                    # can't find module
                    logger.warning('failed to find the module to import, try as a script')
                    script = True
                else:
                    # it's something other than a missing module, probably a real error
                    logger.error('failed to import module - error in the module: %s',str(e))
                    raise
        
        # run the module
        if not script and hasattr(x,cl):
            # the class actually exists
            clas = getattr(x,cl)
            if inspect.isclass(clas) and issubclass(clas,IPBaseClass):
                # old style iceprod modules
                logger.info('old style class')
                mod_cl = clas()
                if 'parameters' in env:
                    for p in env['parameters']:
                        p_value = env['parameters'][p]
                        try:
                            mod_cl.SetParameter(p,p_value)
                        except:
                            try:
                                logger.warn('failed to add parameter %s with value %r',p,p_value)
                            except:
                                logger.warn('failed to add parameter %r',p)
                            continue
                        else:
                            logger.warn('added parameter %s with value %r',p,p_value)
                ret = mod_cl.Execute(env['stats'])
            #elif inspect.isclass(clas) and issubclass(clas,IPModule):
                # new style iceprod modules
            #    logger.info('new style class')
            #    mod_cl = clas(env)
            else:
                # unknown callable, just call it and hope that's all it needs
                args = module['args']
                logger.warn('unknown callable, args=%s',args)
                if args and args[0] in ('{','['):
                    # args is json
                    args = json_decode(args)
                if not args:
                    ret = clas()
                elif isinstance(args,basestring):
                    ret = clas(args)
                elif isinstance(args,list):
                    ret = clas(*args)
                elif isinstance(args,dict):
                    ret = clas(**args)
                else:
                    raise Exception('args is unknown type')
                if (ret is not None and 
                    not isinstance(ret,(bool,int,float,complex,Container))):
                    ret = None
        elif module['src']:
            # the class isn't actually present, so try running it
            # as a script as a last resort
            args = module['args']
            logger.warn('call as script, args=%s',args)
            if not args:
                args = []
            else:
                if args[0] in ('{','['):
                    # args is json
                    args = json_decode(args)
                if isinstance(args,basestring):
                    args = args.split(' ')
                elif isinstance(args,list):
                    pass
                elif isinstance(args,dict):
                    def splitter(a,b):
                        ret = '-%s=%s' if len(str(a)) <= 1 else '--%s=%s'
                        return ret%(str(a),str(b))
                    args = [splitter(a,args[a]) for a in args]
                else:
                    raise Exception('args is unknown type')
            try:
                mod_name = env['classes'][c['name']].replace(';`','').strip()
                if not mod_name or mod_name == '':
                    raise Exception('mod_name is blank')
                ret = None
                # all: shell disabled for protection
                if mod_name[-3:] == '.py':
                    # call as python script
                    proc = subprocess.Popen([sys.executable,mod_name]+args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                elif mod_name[-3:] == '.sh':
                    # call as shell script
                    proc = subprocess.Popen(['/bin/sh',mod_name]+args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                else:
                    # call as regular executable
                    proc = subprocess.Popen([mod_name]+args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                for l in proc.communicate():
                    sys.stdout.write("%s\n"%l)
                ret = proc.returncode
            except:
                logger.critical('cannot run module \'%s\' with script %s',module['name'],mod_name)
                raise
            else:
                if ret:
                    # something went wrong 
                    logger.error('error running module \'%s\' with script %s',module['name'],mod_name)
                    raise dataclasses.NoncriticalError('error running module \'%s\' with script %s'%(module['name'],mod_name))
        else:
            logger.warn('module is a script or class is missing, and src is not defined')
            raise dataclasses.NoncriticalError('error running module')
    except Exception as e:
        # log the traceback
        import traceback
        logger.error(traceback.format_exc(100))
        # pickle the error and send it to the other threadq
        queue.put(pickle.dumps(e,pickle.HIGHEST_PROTOCOL))
    else:
        if env['stats']:
            ret = (ret,env['stats'])
        # pickle the return value and send it to the other thread
        queue.put(pickle.dumps(ret,pickle.HIGHEST_PROTOCOL))


### Functions for JSONRPC ###

def setupjsonRPC(url,passkey):
    """Setup the JSONRPC communications"""
    JSONRPC.start(address=url,passkey=passkey) # TODO: add ssl options
    try:
        ret = JSONRPC.echo('e')
    except Exception as e:
        logger.error('error: %r',e)
        raise Exception('JSONRPC communcation did not start.  '
                        'url=%s and passkey=%s'%(url,passkey))
    else:
        if ret != 'e':
            raise Exception('JSONRPC communication error when starting - '
                            'echo failed (%r).  url=%s and passkey=%s'
                            %(ret,url,passkey))

def downloadtask(gridspec):
    """Download a new task from the server"""
    try:
        platform = os.environ['PLATFORM']
    except:
        platform = functions.platform()
    hostname = functions.gethostname()
    ifaces = functions.getInterfaces()
    python_unicode = 'ucs4' if sys.maxunicode == 1114111 else 'ucs2'
    # TODO: add resources like GPUs, high memory, etc
    task = JSONRPC.new_task(gridspec=gridspec, platform=platform, 
                            hostname=hostname, ifaces=ifaces,
                            python_unicode=python_unicode)
    if isinstance(task,Exception):
        # an error occurred
        raise task
    return task

def processing():
    """Tell the server that we are processing this task"""
    if 'task_id' not in config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot update status')
    ret = JSONRPC.set_processing(config['options']['task_id'])
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def finishtask(stats={}):
    """Finish a task"""
    if 'task_id' not in config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot finish task')
    if 'DBkill' in config['options'] and config['options']['DBkill']:
        return # don't finish task on a DB kill
    outstats = stats
    if 'stats' in config['options']:
        # filter stats
        stat_keys = set(json_decode(config['options']['stats']))
        outstats = {k:stats[k] for k in stats if k in stat_keys}
    ret = JSONRPC.finish_task(config['options']['task_id'],stats=outstats)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def stillrunning():
    """Check if the task should still be running according to the DB"""
    if 'task_id' not in config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot finish task')
    ret = JSONRPC.stillrunning(config['options']['task_id'])
    if isinstance(ret,Exception):
        # an error occurred
        raise ret
    if not ret:
        config['options']['DBkill'] = True
        raise Exception('task should be stopped')

def taskerror():
    """Tell the server about the error experienced"""
    if 'task_id' not in config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send error')
    if 'DBkill' in config['options'] and config['options']['DBkill']:
        return # don't change status on a DB kill
    ret = JSONRPC.task_error(config['options']['task_id'])
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def _upload_logfile(task_id,name,file):
    """Upload a log file"""
    if 'DBkill' in config['options'] and config['options']['DBkill']:
        return # don't upload logs on a DB kill
    data = json_compressor.compress(open(file).read())
    ret = JSONRPC.upload_logfile(task_id,name,data)
    if isinstance(ret,Exception):
        # an error occurred
        raise ret

def uploadLogging():
    """Upload all logging files"""
    uploadLog()
    uploadErr()
    uploadOut()
    
def uploadLog():
    """Upload log files"""
    if 'task_id' not in config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send log')
    _upload_logfile(config['options']['task_id'],'stdlog',constants['stdlog'])
    
def uploadErr():
    """Upload error files"""
    if 'task_id' not in config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send error')
    _upload_logfile(config['options']['task_id'],'stderr',constants['stderr'])
    
def uploadOut():
    """Upload out files"""
    if 'task_id' not in config['options']:
        raise Exception('config["options"][task_id] not specified, '
                        'so cannot send output')
    _upload_logfile(config['options']['task_id'],'stdout',constants['stdout'])
