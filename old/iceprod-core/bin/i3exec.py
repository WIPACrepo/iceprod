#!/usr/bin/env python
"""
  The task runner
  
  copyright (c) 2012 the icecube collaboration
"""

import os
import sys
import logging
import logging.config    
import signal
from functools import partial

from iceprod.core import to_file, constants
import iceprod.core.dataclasses
import iceprod.core.xml
import iceprod.core.exe

import iceprod.core.logger
logging.basicConfig()
logger = None

def handler1(signum, frame):
   logger.warn('Signal handler called with signal %s' % signum)
   logger.warn('Exiting...')
   os._exit(0)

def main(cfgfile=None, validate=True, url=None, debug=False,
         passkey='', offline=False, gridspec=None):
    """Main task runner for iceprod"""
    global logger
    # set up stdout and stderr
    stdout = partial(to_file,sys.stdout,constants['stdout'])
    stderr = partial(to_file,sys.stderr,constants['stderr'])
    with stdout(), stderr():
        # set up logger
        if debug:
            logl = 'INFO'
        else:
            logl = 'WARNING'
        logf = os.path.abspath(os.path.expandvars(constants['stdlog']))
        iceprod.core.logger.setlogger('i3exec',
                                      None,
                                      loglevel=logl,
                                      logfile=logf,
                                      logsize=67108864,
                                      lognum=1)
        iceprod.core.logger.removestdout()
        logger = logging.getLogger('i3exec')
        logger.warn('starting...%s ' % logger.name)
        
        signal.signal(signal.SIGQUIT, handler1)
        signal.signal(signal.SIGINT, handler1)
        
        if offline is True:
            # run in offline mode
            runner(cfgfile,validate,url,debug,offline)
            return
        
        # setup jsonRPC
        iceprod.core.exe.setupjsonRPC(url+'/jsonrpc',passkey)
        
        if cfgfile is not None:
            # default configuration - a single task
            runner(cfgfile,validate,url,debug)
        elif gridspec is None:
            logger.critical('There is no cfgfile and no gridspec')
        else:
            # get many tasks from server
            errors = 0
            while errors < 5:
                try:
                    cfgfile = iceprod.core.exe.downloadtask(gridspec)
                except Exception as e:
                    errors += 1
                    logger.error('cannot download task. current error count is %d',
                                 errors,exc_info=True)
                    continue
                
                if cfgfile is None:
                    break # assuming server wants client to exit
                else:
                    try:
                        runner(cfgfile,validate,url,debug)
                    except Exception as e:
                        logger.error('%r',e)
                        errors += 1
                        logger.error('task encountered an error. current error count is %d',errors)
            if errors >= 5:
                logger.error('too many errors when running tasks')
        logger.warn('finished running normally; exiting...')
    
def runner(cfgfile,validate,url,debug=False,offline=False):
    # load xml
    config = None
    if isinstance(cfgfile,str):
        try:
            config = iceprod.core.xml.loadXML(cfgfile,validate)
            if not config or config == False:
                raise Exception('Config not found')
        except Exception as e:
            logger.critical('Error loading configuration: %s' % str(e))
            raise
    elif isinstance(cfgfile,iceprod.core.dataclasses.Job):
        config = cfgfile
    else:
        logger.warn('cfgfile: %r',cfgfile)
        raise Exception('cfgfile is not a str or a Job')
    # main options are in config options section now

    # set logging verbosity
    if 'debug' in config.options and 'loglevel' not in config.options:
        config.options['loglevel'] = iceprod.core.dataclasses.Parameter('loglevel','INFO')
    if ('loglevel' in config.options and 
        config.options['loglevel'].value.upper() in iceprod.core.logger.setlevel):
        try:
            logging.getLogger().setLevel(iceprod.core.logger.setlevel[config.options['loglevel'].value.upper()])
        except Exception as e:
            logger.warn('failed to set a new log level: %r',e)
    
    # check that validate, resource_url, debug are in options
    if 'validate' not in config.options:
        o = iceprod.core.dataclasses.Parameter('validate',validate)
        config.options['validate'] = o
    if 'resource_url' not in config.options:
        o = iceprod.core.dataclasses.Parameter('resource_url',str(url)+'/download')
        config.options['resource_url'] = o
    if 'debug' not in config.options:
        o = iceprod.core.dataclasses.Parameter('debug',bool(offline))
        config.options['debug'] = o
    
    # make sure some basic options are set
    if 'offline' not in config.options:
        o = iceprod.core.dataclasses.Parameter('offline',offline)
        config.options['offline'] = o
    if 'data_url' not in config.options:
        o = iceprod.core.dataclasses.Parameter('data_url','gsiftp://gridftp-rr.icecube.wisc.edu/')
        config.options['data_url'] = o
    if 'svn_repository' not in config.options:
        o = iceprod.core.dataclasses.Parameter('svn_repository','http://code.icecube.wisc.edu/svn/')
        config.options['svn_repository'] = o
    if 'job_temp' not in config.options:
        o = iceprod.core.dataclasses.Parameter('job_temp',os.path.join(os.getcwd(),'job_temp'))
        config.options['job_temp'] = o
    if 'local_temp' not in config.options:
        o = iceprod.core.dataclasses.Parameter('local_temp',os.path.join(os.getcwd(),'local_temp'))
        config.options['local_temp'] = o
    if 'job' not in config.options:
        o = iceprod.core.dataclasses.Parameter('job','0')
        config.options['job'] = o
    if 'stillrunninginterval' not in config.options:
        o = iceprod.core.dataclasses.Parameter('stillrunninginterval',60)
        config.options['stillrunninginterval'] = o
    
    if not config.steering:
        # make sure steering exists in the config
        config.steering = iceprod.core.dataclasses.Steering()
    
    # set up global config value
    iceprod.core.exe.config = config
    
    # set up global env, based on config.options and config.steering
    env = iceprod.core.exe.setupenv(config.steering,{'parameters':config.options})
    
    print_opts = {}
    for k in config.options:
        v = config.options[k]
        if isinstance(v,iceprod.core.dataclasses.Parameter):
            v = {v.name:v.value}
        print_opts[k] = v
    logger.warn("config options: %r",print_opts)
    
    if not offline:
        # tell the server that we are processing this task
        try:
            iceprod.core.exe.processing()
        except Exception as e:
            logging.error(e)
    
    # find tasks to run
    try:
        if 'task' in config.options:
            logger.warn('task specified: %r',config.options['task'].value)
            # run only this task name or number
            if config.options['task'].value in config.tasks:
                # run this task
                iceprod.core.exe.runtask(env,config.tasks[config.options['task'].value])
            else:
                try:
                    task = int(config.options['task'].value)
                    if len(config.tasks) > task:
                        # run task specified by task
                        i = 0
                        for value in config.tasks.values():
                            if i == task:
                                iceprod.core.exe.runtask(env,value)
                                break
                            i += 1
                    else:
                        logger.critical('task specified in options is \'%s\', but no task found' % config.options['task'].value)
                        logger.critical('tasks available are: %s',str(config.tasks.keys()))
                        raise Exception('cannot find specified task')
                except:
                    logger.critical('task failed to run')
                    raise
        else:
            # run all tasks in order
            for task in config.tasks.values():
                iceprod.core.exe.runtask(env,task)
    except Exception as e:
        logger.error('task failed, exiting without running completion steps. (%r)',e)
        # set task status on server
        if not offline:
            try:
                iceprod.core.exe.taskerror()
            except Exception as e:
                logging.error(e)
        raise
    else:
        # destroy env
        iceprod.core.exe.destroyenv(env)
        del env
    finally:
        # upload log files to server
        try:
            if ('upload' in config.options and 
                ((not config.options['upload'].type) or
                 (config.options['upload'].type == 'string'))):
                upload = config.options['upload'].value.lower()    
                for up in upload.split('|'):
                    if up.startswith('logging'):
                        # upload err,log,out files
                        iceprod.core.exe.uploadLogging()
                        break
                    elif up.startswith('log'):
                        # upload log files
                        iceprod.core.exe.uploadLog()
                    elif up.startswith('err'):
                        # upload err files
                        iceprod.core.exe.uploadErr()
                    elif up.startswith('out'):
                        # upload out files
                        iceprod.core.exe.uploadOut()
        except Exception as e:
            logger.error('failed when uploading logging info - %s' % str(e))
    

if __name__ == '__main__':
    # get arguments
    import argparse
    parser = argparse.ArgumentParser(description='IceProd Core')
    parser.add_argument('-f','--cfgfile', type=str,
                        help='Specify config file')
    parser.add_argument('-v','--validate', type=str, default='True',
                        help='Specify if the config file should be validated')
    parser.add_argument('-u','--url', type=str,
                        help='URL of the iceprod server')
    parser.add_argument('-p','--passkey', type=str,
                        help='passkey for communcation with iceprod server')
    parser.add_argument('-d','--debug', action='store_true', default=False,
                        help='Enable debug actions and logging')
    parser.add_argument('--offline', action='store_true', default=False,
                        help='Enable offline mode (don\'t talk with server)')
    parser.add_argument('--gridspec', type=str,
                        help='specify gridspec for pilot jobs')

    args = vars(parser.parse_args())
    print args
    
    # check cfgfile
    if args['cfgfile'] is not None and not os.path.isfile(args['cfgfile']):
        if os.path.isfile(os.path.join(os.getcwd(),args['cfgfile'])):
            args['cfgfile'] = os.path.join(os.getcwd(),args['cfgfile'])
        else:
            args['cfgfile'] = None
    # check validate
    if args['validate'] in ('True','true','1'):
        args['validate'] = True
    else:
        args['validate'] = False
    
    # start iceprod
    main(**args)
    