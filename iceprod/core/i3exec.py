"""
The task runner.

Run it with `python -m iceprod.core.i3exec`.

optional arguments:
  -h, --help            show this help message and exit
  -f CFGFILE, --cfgfile CFGFILE
                        Specify config file
  -u URL, --url URL     URL of the iceprod server
  -p PASSKEY, --passkey PASSKEY
                        passkey for communcation with iceprod server
  --pilot_id PILOTID    ID of the pilot (if this is a pilot)
  -d, --debug           Enable debug actions and logging
  --offline             Enable offline mode (don't talk with server)
  --offline_transfer True/False
                        Enable offline file transfer
  --logfile LOGFILE     Specify the logfile to use
  --job JOB             Index of the job to run
  --task TASK           Name of the task to run
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import logging
import logging.config
import time
import signal
from functools import partial
import tempfile
import shutil
import threading
import asyncio

from tornado.ioloop import IOLoop

from iceprod.core import to_file, constants
import iceprod.core.dataclasses
import iceprod.core.serialization
import iceprod.core.exe
from iceprod.core.exe_json import ServerComms
import iceprod.core.pilot
import iceprod.core.resources

import iceprod.core.logger

def load_config(cfgfile):
    """Load a config from file, serialized string, dictionary, etc"""
    logger = logging.getLogger('i3exec')
    config = None
    if isinstance(cfgfile,str):
        try:
            if os.path.exists(cfgfile):
                config = iceprod.core.serialization.serialize_json.load(cfgfile)
            else:
                config = iceprod.core.serialization.serialize_json.loads(cfgfile)
            if not config:
                raise Exception('Config not found')
        except Exception as e:
            logger.critical('Error loading configuration: %s' % str(e))
            raise
    elif isinstance(cfgfile,iceprod.core.dataclasses.Job):
        config = cfgfile
    elif isinstance(cfgfile,dict):
        config = iceprod.core.serialization.dict_to_dataclasses(cfgfile)
    else:
        logger.warning('cfgfile: %r',cfgfile)
        raise Exception('cfgfile is not a str or a Job')
    return config

def main(cfgfile=None, logfile=None, url=None, debug=False,
         passkey='', pilot_id=None, offline=False, offline_transfer=False):
    """Main task runner for iceprod"""
    # set up logger
    if debug:
        logl = 'INFO'
    else:
        logl = 'WARNING'
    if logfile:
        logf = os.path.abspath(os.path.expandvars(logfile))
    else:
        logf = os.path.abspath(os.path.expandvars(constants['stdlog']))
    iceprod.core.logger.set_logger(loglevel=logl,
                                   logfile=logf,
                                   logsize=67108864,
                                   lognum=1)
    logging.warning('starting IceProd core')

    if cfgfile is None:
        logging.critical('There is no cfgfile')
        raise Exception('missing cfgfile')
    elif isinstance(cfgfile, str):
        config = load_config(cfgfile)
    else:
        config = cfgfile
    logging.info('config: %r',config)

    if not offline:
        # if we are not in offline mode, we need a url
        if not url:
            logging.critical('url missing')
            raise Exception('url missing')

        # setup jsonRPC
        kwargs = {}
        if 'username' in config['options']:
            kwargs['username'] = config['options']['username']
        if 'password' in config['options']:
            kwargs['password'] = config['options']['password']
        if 'ssl' in config['options'] and config['options']['ssl']:
            kwargs.update(config['options']['ssl'])
        rpc = ServerComms(url+'/jsonrpc', passkey, None, **kwargs)

    async def run():
        if offline:
            logging.info('offline mode')
            async for proc in runner(config, url, debug=debug,
                    offline=offline, offline_transfer=offline_transfer):
                await proc.wait()
        elif 'tasks' in config and config['tasks']:
            logging.info('online mode - single task')
            # tell the server that we are processing this task
            if 'task_id' not in config['options']:
                raise Exception('config["options"]["task_id"] not specified')
            try:
                await rpc.processing(config['options']['task_id'])
            except Exception:
                logging.error('json error', exc_info=True)

            # set up stdout and stderr
            stdout = partial(to_file,sys.stdout,constants['stdout'])
            stderr = partial(to_file,sys.stderr,constants['stderr'])
            with stdout(), stderr():
                async for proc in runner(config, url, rpc=rpc, debug=debug):
                    await proc.wait()
        else:
            logging.info('pilot mode - get many tasks from server')
            if 'gridspec' not in config['options']:
                logging.critical('gridspec missing')
                raise Exception('gridspec missing')
            if not pilot_id:
                logging.critical('pilot_id missing')
                raise Exception('pilot_id missing')
            pilot_kwargs = {}
            if 'run_timeout' in config['options']:
                pilot_kwargs['run_timeout'] = config['options']['run_timeout']
            async with iceprod.core.pilot.Pilot(config, rpc=rpc, debug=debug,
                                     runner=partial(runner, rpc=rpc, url=url, debug=debug),
                                     pilot_id=pilot_id, **pilot_kwargs) as p:
                await p.run()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())

    logging.warning('finished running normally; exiting...')

async def runner(config, url, rpc=None, debug=False, offline=False, offline_transfer=False):
    """Run a config.

    #. Set some default options if not set in configuration.
    #. Set up global env based on the configuration.
    #. Run tasks
       * If a task is specified in the configuration options:

         If the task is specified by name or number, run only that task.
         If there is a problem finding the task specified, raise a
         critical error.

       * Otherwise, run all tasks in the configuration in the order
         they were written.

    #. Destroy the global env, uploading and deleting files as needed.
    #. Upload the log, error, and output files if specified in options.

    Args:
        config (`iceprod.core.dataclasses.Job`): Dataset configuration
        url (str): URL to server
        rpc (:py:class:`iceprod.core.exe_json.ServerComms`): RPC object
        debug (bool): (optional) turn on debug logging
        offline (bool): (optional) enable offline mode
        offline_transfer (bool): (optional) enable/disable offline data transfers
    """
    # set logging
    if offline:
        logger = logging.getLogger('task')
    else:
        if 'task_id' not in config['options']:
            raise Exception('task_id not set in config options')
        logger = logging.getLogger(config['options']['task_id'])
    if 'debug' not in config['options']:
        config['options']['debug'] = debug
    if ('debug' in config['options'] and config['options']['debug'] and
        'loglevel' not in config['options']):
        config['options']['loglevel'] = 'INFO'
    if ('loglevel' in config['options'] and
        config['options']['loglevel'].upper() in iceprod.core.logger.setlevel):
        try:
            logger.set_log_level(config['options']['loglevel'])
        except Exception:
            logger.warning('failed to set a new log level', exc_info=True)

    # make sure some basic options are set
    if 'job' not in config['options']:
        config['options']['job'] = 0
    if 'jobs_submitted' not in config['options']:
        config['options']['jobs_submitted'] = 1
    if 'resource_url' not in config['options']:
        config['options']['resource_url'] = str(url)+'/download'
    if 'offline' not in config['options']:
        config['options']['offline'] = offline
    if 'offline_transfer' not in config['options']:
        config['options']['offline_transfer'] = offline_transfer
    if 'data_url' not in config['options']:
        config['options']['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
    if 'svn_repository' not in config['options']:
        config['options']['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
    if 'site_temp' not in config['options']:
        config['options']['site_temp'] = 'gsiftp://gridftp-scratch.icecube.wisc.edu/local/simprod/'
    if 'dataset_temp' not in config['options']:
        config['options']['dataset_temp'] = os.path.join(config['options']['site_temp'],'$(dataset)')
    if 'job_temp' not in config['options']:
        config['options']['job_temp'] = os.path.join(config['options']['dataset_temp'],'$(job)')
    if 'subprocess_dir' not in config['options']:
        config['options']['subprocess_dir'] = os.getcwd()
    if 'task_temp' not in config['options']:
        config['options']['task_temp'] = 'file:'+os.path.join(config['options']['subprocess_dir'],'task_temp')
    if 'tray_temp' not in config['options']:
        config['options']['tray_temp'] = 'file:'+os.path.join(config['options']['subprocess_dir'],'tray_temp')
    if 'local_temp' not in config['options']:
        config['options']['local_temp'] = os.path.join(config['options']['subprocess_dir'],'local_temp')
    if 'stillrunninginterval' not in config['options']:
        config['options']['stillrunninginterval'] = 60
    if 'upload' not in config['options']:
        config['options']['upload'] = 'logging'

    if not config['steering']:
        # make sure steering exists in the config
        config['steering'] = iceprod.core.dataclasses.Steering()

    if offline:
        try:
            import psutil
        except ImportError:
            resources = None
        else:
            # track resource usage in separate thread
            resource_stop = False
            resources = iceprod.core.resources.Resources(debug=debug)
            resources.claim('a')
            resources.register_process('a',psutil.Process(), os.getcwd())
            def track():
                while not resource_stop:
                    resources.check_claims()
                    time.sleep(1)
            resource_thread = threading.Thread(target=track)
            resource_thread.start()

    # make exe Config
    cfg = iceprod.core.exe.Config(config=config, rpc=rpc, logger=logger)

    # set up global env, based on config['options'] and config.steering
    env_opts = cfg.parseObject(config['options'], {})
    stats = {}
    try:
        try:
            # keep track of the start time
            start_time = time.time()

            async with iceprod.core.exe.SetupEnv(cfg, config['steering'], {'options':env_opts}, logger=logger) as env:
                logger.warning("config options: %r",config['options'])

                # find tasks to run
                if 'task' in config['options']:
                    logger.warning('task specified: %r',config['options']['task'])
                    # run only this task name or number
                    name = config['options']['task']
                    if isinstance(name, iceprod.core.dataclasses.String) and name.isdigit():
                        name = int(name)
                    if isinstance(name, iceprod.core.dataclasses.String):
                        # find task by name
                        for task in config['tasks']:
                            if task['name'] == name:
                                async for proc in iceprod.core.exe.runtask(cfg, env, task, logger=logger):
                                    yield proc
                                break
                        else:
                            logger.critical('cannot find task named %r', name)
                            raise Exception('cannot find specified task')
                    elif isinstance(name, int):
                        # find task by index
                        if (name >= 0 and
                            name < len(config['tasks'])):
                            async for proc in iceprod.core.exe.runtask(cfg, env, config['tasks'][name], logger=logger):
                                yield proc
                        else:
                            logger.critical('cannot find task index %d', name)
                            raise Exception('cannot find specified task')

                    else:
                        logger.critical('task specified in options is %r, but no task found',
                                        name)
                        raise Exception('cannot find specified task')
                    # finish task
                    if not offline:
                        await rpc.finish_task(config['options']['task_id'],
                                dataset_id=config['options']['dataset_id'],
                                stats=env['stats'], start_time=start_time)
                elif offline:
                    # run all tasks in order
                    for task in config['tasks']:
                        async for proc in iceprod.core.exe.runtask(cfg, env, task):
                            yield proc
                else:
                    raise Exception('task to run not specified')

        except Exception as e:
            logger.error('task failed, exiting without running completion steps.',
                         exc_info=True)
            # set task status on server
            if not offline:
                try:
                    await rpc.task_error(config['options']['task_id'],
                            dataset_id=config['options']['dataset_id'],
                            stats=env['stats'], start_time=start_time,
                            reason=str(e))
                except Exception as e:
                    logger.error(e)
                # forcibly turn on logging, so we can see the error
                config['options']['upload'] = 'logging'
            raise

    finally:
        # check resources
        if offline and resources:
            resource_stop = True
            resource_thread.join()
            print('Resources:')
            r = resources.get_final('a')
            if not r:
                print('  None')
            else:
                for k in r:
                    print('  {}: {:.2f}'.format(k,r[k]))
        # upload log files to server
        try:
            if (not offline) and 'upload' in config['options']:
                if isinstance(config['options']['upload'],
                              iceprod.core.dataclasses.String):
                    upload = config['options']['upload'].lower().split('|')
                elif isinstance(config['options']['upload'],(tuple,list)):
                    upload = [x.lower() for x in config['options']['upload']]
                else:
                    raise Exception('upload config is not a valid type')
                for up in upload:
                    if up.startswith('logging'):
                        # upload err,log,out files
                        await rpc.uploadLog()
                        await rpc.uploadErr()
                        await rpc.uploadOut()
                        break
                    elif up.startswith('log'):
                        # upload log files
                        await rpc.uploadLog()
                    elif up.startswith('err'):
                        # upload err files
                        await rpc.uploadErr()
                    elif up.startswith('out'):
                        # upload out files
                        await rpc.uploadOut()
        except Exception as e:
            logger.error('failed when uploading logging info',exc_info=True)

    logger.warning('finished without error')


if __name__ == '__main__':
    # get arguments
    import argparse
    parser = argparse.ArgumentParser(description='IceProd Core')
    parser.add_argument('-f','--cfgfile', type=str,
                        help='Specify config file')
    parser.add_argument('-u','--url', type=str,
                        help='URL of the iceprod server')
    parser.add_argument('-p','--passkey', type=str,
                        help='passkey for communcation with iceprod server')
    parser.add_argument('--pilot_id', type=str, default=None,
                        help='ID of the pilot (if this is a pilot)')
    parser.add_argument('-d','--debug', action='store_true', default=False,
                        help='Enable debug actions and logging')
    parser.add_argument('--offline', action='store_true', default=False,
                        help='Enable offline mode (don\'t talk with server)')
    parser.add_argument('--offline_transfer', type=bool, default=False,
                        help='Enable/disable file transfer during offline mode')
    parser.add_argument('--logfile', type=str, default=None,
                        help='Specify the logfile to use')
    parser.add_argument('--job', type=int, default=None,
                        help='Index of the job to run')
    parser.add_argument('--jobs_submitted', type=int, default=None,
                        help='Total number of jobs in this dataset')
    parser.add_argument('--task', type=str, default=None,
                        help='Name of the task to run')

    args = vars(parser.parse_args())
    print(args)

    # check cfgfile
    if args['cfgfile'] is not None and not os.path.isfile(args['cfgfile']):
        if os.path.isfile(os.path.join(os.getcwd(),args['cfgfile'])):
            args['cfgfile'] = os.path.join(os.getcwd(),args['cfgfile'])
        else:
            args['cfgfile'] = None

    options = {k: args.pop(k) for k in ('job','jobs_submitted','task')}
    if not options['jobs_submitted'] and options['job']:
        options['jobs_submitted'] = options['job']+1
    options['debug'] = args['debug']
    if args['cfgfile']:
        cfgfile = load_config(args['cfgfile'])
        for k in options:
            if options[k] is not None and k not in cfgfile['options']:
                cfgfile['options'][k] = options[k]
        args['cfgfile'] = cfgfile

    # start iceprod
    main(**args)
