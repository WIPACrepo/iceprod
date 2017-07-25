"""Pilot functionality"""

from __future__ import absolute_import, division, print_function

import os
import sys
import time
import logging
import tempfile
import shutil
from functools import partial
from multiprocessing import Process, active_children
from multiprocessing.queues import SimpleQueue
from collections import namedtuple
from datetime import timedelta
from glob import glob
import signal
import traceback

from iceprod.core.functions import gethostname
from iceprod.core import to_file, constants
from iceprod.core import exe_json
from iceprod.core.exe import Config
from iceprod.core.resources import Resources
from iceprod.core.dataclasses import Number, String
import iceprod.core.logger

logger = logging.getLogger('pilot')

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Condition

try:
    import psutil
except ImportError:
    psutil = None

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(name):
        pass


def process_wrapper(func, title, pilot_id='', hostname='', resources={}):
    """
    Set process title. Set log file, stdout, stderr. Then go on to call func.

    Args:
        func (callable): The function that we really want to run
        title (str): The new process title
        pilot_id (str): The pilot id
        hostname (str): The hostname
        resources (dict): The resources of this process
    """
    try:
        setproctitle(title)
    except Exception:
        pass

    Resources.set_env(resources)

    iceprod.core.logger.new_file(constants['stdlog'])
    logger.warn('pilot_id: %s', pilot_id)
    logger.warn('hostname: %s', hostname)
    env_str = '\n'.join('    '+k+' = '+os.environ[k] for k in os.environ)
    logger.warn('environment: \n%s', env_str)

    stdout = partial(to_file,sys.stdout,constants['stdout'])
    stderr = partial(to_file,sys.stderr,constants['stderr'])
    with stdout(), stderr():
        func()

class Pilot(object):
    """
    A pilot task runner.

    The pilot allows multiple tasks to run in sequence or parallel.
    It keeps track of resource usage, killing anything that goes over
    requested amounts.

    Args:
        config (dict): the configuration dictionary
        runner (callable): the task/config runner
    """
    def __init__(self, config, runner, pilot_id, debug=False, run_timeout=60):
        self.config = config
        self.runner = runner
        self.pilot_id = pilot_id
        self.hostname = gethostname()
        self.debug = debug
        self.run_timeout = timedelta(seconds=run_timeout)
        self.message_queue = SimpleQueue()

        if self.debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        try:
            setproctitle('iceprod2_pilot({})'.format(pilot_id))
        except Exception:
            pass

        self.running = True
        self.lock = Condition()

        logger.warn('pilot_id: %s', self.pilot_id)
        logger.warn('hostname: %s', self.hostname)

        # hint at resources for pilot
        # don't pass them as raw, because that overrides condor
        if 'resources' in config['options']:
            for k in config['options']['resources']:
                v = config['options']['resources'][k]
                name = 'NUM_'+k.upper()
                if k in ('cpu','gpu'):
                    name += 'S'
                os.environ[name] = str(v)
        self.resources = Resources(debug=self.debug)

        self.start_time = time.time()

        self.ioloop = IOLoop.current()

        # set up jsonrpc forwarder
        self.ioloop.add_callback(self.message_queue_monitor)

        # set up resource monitor
        self.tasks = {}
        if psutil:
            self.ioloop.add_callback(self.resource_monitor)
        else:
            logger.warn('no psutil. not checking resource usage')

        # set up signal handler
        def handler(signum, frame):
            logger.critical('termination signal received')
            self.running = False
            self.ioloop.add_callback_from_signal(self.term_handler)
        signal.signal(signal.SIGTERM, handler)

        # run the loop
        self.ioloop.run_sync(self.run)

        # make sure any child processes are dead
        self.hard_kill()

        if self.debug:
            # append out, err, log
            for dirs in glob('tmp*'):
                for filename in (constants['stdout'], constants['stderr'],
                                 constants['stdlog']):
                    if os.path.exists(os.path.join(dirs,filename)):
                        with open(filename,'a') as f:
                            print('', file=f)
                            print('----',dirs,'----', file=f)
                            with open(os.path.join(dirs,filename)) as f2:
                                print(f2.read(), file=f)

    def term_handler(self):
        """Handle a SIGTERM gracefully"""
        logger.info('checking resources after SIGTERM')
        start_time = time.time()
        for task_id in list(self.tasks):
            if task_id in overages:
                reason = overages[task_id]
            else:
                reason = 'pilot SIGTERM'

            # clean up task
            used_resources = self.resources.get_final(task_id)
            self.clean_task(task_id)
            message = reason
            message += '\n\npilot SIGTERM\npilot_id: {}'.format(self.pilot_id)
            message += '\nhostname: {}'.format(self.hostname)
            exe_json.task_kill(task_id, resources=used_resources,
                               reason=reason, message=message)

        # stop the pilot
        exe_json.update_pilot(self.pilot_id, tasks='',
                              resources_available=self.resources.get_available(),
                              resources_claimed=self.resources.get_claimed())
        self.ioloop.stop()

    def hard_kill(self):
        """Forcefully kill any child processes"""
        if psutil:
            # kill children correctly
            processes = psutil.Process().children(recursive=True)
            processes.reverse()
            for p in processes:
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    pass
                except Exception:
                    logger.warn('error killing process',
                                exc_info=True)
        processes = active_children()
        for p in processes:
            p.terminate()

    def message_queue_monitor(self):
        """Forward JSONRPC messages from tasks"""
        sleep_time = 0.1
        try:
            if not self.message_queue.empty():
                task_id,func_name,cfg,args,kwargs = self.message_queue.get()
                logger.info('forwarding message for %s to %s', task_id, func_name)
                cfg = Config(cfg)
                if func_name in ('finishtask','taskerror'):
                    kwargs['resources'] = self.resources.get_final(task_id)
                try:
                    ret = getattr(exe_json, func_name)(cfg,*args,**kwargs)
                except Exception as e:
                    logger.info('exception returned from forward', exc_info=True)
                    ret = e
                if task_id not in self.tasks:
                    logger.warn('cannot forward return value')
                else:
                    self.tasks[task_id]['recv_queue'].put(ret)
        except Exception:
            logger.info('error forwarding message', exc_info=True)
        self.ioloop.call_later(sleep_time, self.message_queue_monitor)

    @gen.coroutine
    def resource_monitor(self):
        """Monitor the tasks, killing any that go over resource limits"""
        try:
            sleep_time = 0.5 # check every X seconds
            while self.running or self.tasks:
                logger.debug('pilot monitor - checking resource usage')
                start_time = time.time()
                
                overages = self.resources.check_claims()
                for task_id in overages:
                    used_resources = self.resources.get_peak(task_id)
                    logger.warn('kill %r for going over resources: %r',
                                task_id, used_resources)
                    self.clean_task(task_id)
                    message = overages[task_id]
                    message += '\n\npilot_id: {}'.format(self.pilot_id)
                    message += '\nhostname: {}'.format(self.hostname)
                    exe_json.task_kill(task_id, resources=used_resources,
                                       reason=overages[task_id], message=message)
                    # try to queue another task
                    logger.info('killed, so notify')
                    self.lock.notify()

                duration = time.time()-start_time
                logger.debug('sleep_time %.2f, duration %.2f',sleep_time,duration)
                if duration < sleep_time:
                    yield self.lock.wait(timeout=timedelta(seconds=sleep_time-duration))
        except Exception:
            logger.error('pilot monitor died', exc_info=True)
            raise
        logger.warn('pilot monitor exiting')

    @gen.coroutine
    def run(self):
        """Run the pilot"""
        errors = int(self.resources.total['cpu'])*10
        tasks_running = 0
        while self.running:
            while self.running:
                try:
                    task_configs = exe_json.downloadtask(self.config['options']['gridspec'],
                                                         resources=self.resources.get_available())
                except Exception:
                    errors -= 1
                    if errors < 1:
                        self.running = False
                        logger.warn('errors over limit, draining')
                    logger.error('cannot download task. current error count is %d',
                                 errors, exc_info=True)
                    continue
                logger.info('task configs: %r', task_configs)

                if not task_configs:
                    logger.info('no task available')
                    if not self.tasks:
                        self.running = False
                        logger.warn('no task available, draining')
                    break
                else:
                    for task_config in task_configs:
                        try:
                            task_id = task_config['options']['task_id']
                        except Exception:
                            errors -= 1
                            if errors < 1:
                                self.running = False
                                logger.warn('errors over limit, draining')
                            logger.error('error getting task_id from config')
                            break
                        try:
                            if 'resources' not in task_config['options']:
                                task_config['options']['resources'] = None
                            task_resources = self.resources.claim(task_id, task_config['options']['resources'])
                            task_config['options']['resources'] = task_resources
                        except Exception:
                            errors -= 1
                            if errors < 1:
                                self.running = False
                                logger.warn('errors over limit, draining')
                            logger.warn('error claiming resources %s', task_id,
                                        exc_info=True)
                            message = 'pilot_id: {}\nhostname: {}\n\n'.format(self.pilot_id, self.hostname)
                            message += traceback.format_exc()
                            exe_json.task_kill(task_id, reason='failed to claim resources',
                                               message=message)
                            break
                        try:
                            self.create_task(task_config)
                        except Exception:
                            errors -= 1
                            if errors < 1:
                                self.running = False
                                logger.warn('errors over limit, draining')
                            logger.warn('error creating task %s', task_id,
                                        exc_info=True)
                            message = 'pilot_id: {}\nhostname: {}\n\n'.format(self.pilot_id, self.hostname)
                            message += traceback.format_exc()
                            exe_json.task_kill(task_id, reason='failed to create task',
                                               message=message)
                            self.clean_task(task_id)
                            break
                    else:
                        tasks_running += len(task_configs)
                        exe_json.update_pilot(self.pilot_id, tasks=','.join(self.tasks),
                                              resources_available=self.resources.get_available(),
                                              resources_claimed=self.resources.get_claimed())

                if (self.resources.available['cpu'] < 1
                    or self.resources.available['memory'] < 1):
                    logger.info('no resources left, so wait for tasks to finish')
                    break

            # wait until we can queue more tasks
            while self.running or self.tasks:
                logger.info('wait while tasks are running. timeout=%r',self.run_timeout)
                ret = yield self.lock.wait(timeout=self.run_timeout)
                logger.debug('yield returned %r',ret)
                # check if any processes have died
                for task_id in list(self.tasks):
                    if not self.tasks[task_id]['p'].is_alive():
                        if self.tasks[task_id]['p'].exitcode != 0:
                            logger.info('task %s exited with bad code: %r',
                                        task_id, self.tasks[task_id]['p'].exitcode)
                            errors -= 1
                            if errors < 1:
                                self.running = False
                                logger.warn('errors over limit, draining')
                        self.clean_task(task_id)
                if len(self.tasks) < tasks_running:
                    logger.info('%d tasks removed', tasks_running-len(self.tasks))
                    tasks_running = len(self.tasks)
                    exe_json.update_pilot(self.pilot_id, tasks=','.join(self.tasks),
                                          resources_available=self.resources.get_available(),
                                          resources_claimed=self.resources.get_claimed())
                    if self.running:
                        break

        # last update for pilot state
        exe_json.update_pilot(self.pilot_id, tasks='',
                              resources_available=self.resources.get_available(),
                              resources_claimed=self.resources.get_claimed())

        if errors < 1:
            logger.critical('too many errors when running tasks')
        else:
            logger.warn('cleanly stopping pilot')

    def create_task(self, config):
        """
        Create a new Task and start running it

        Args:
            config (dict): The task config
        """
        task_id = config['options']['task_id']
                    
        # add grid-specific config
        for k in self.config['options']:
            if k == 'resources':
                pass
            elif k not in config['options']:
                config['options'][k] = self.config['options'][k]

        # add message queue
        send_queue = SimpleQueue()
        config['options']['message_queue'] = [self.message_queue, send_queue]

        # run task in tmp dir
        main_dir = os.getcwd()
        try:
            tmpdir = tempfile.mkdtemp(dir=main_dir)

            # symlink important files
            if 'ssl' in config['options']:
                for f in config['options']['ssl']:
                    os.symlink(os.path.join(main_dir,config['options']['ssl'][f]),
                               os.path.join(tmpdir,config['options']['ssl'][f]))

            # start the task
            os.chdir(tmpdir)
            r = config['options']['resources']
            p = Process(target=partial(process_wrapper, partial(self.runner, config),
                                       'iceprod_task_{}'.format(task_id),
                                       hostname=self.hostname,
                                       pilot_id=self.pilot_id, resources=r))
            p.start()
            if psutil:
                ps = psutil.Process(p.pid)
                ps.nice(psutil.Process().nice()+1)
            else:
                ps = None
            ur = {k:0 for k in r}
            self.tasks[task_id] = {'p':p, 'process':ps, 'tmpdir':tmpdir,
                                   'recv_queue':send_queue}
            self.resources.register_process(task_id, ps, tmpdir)
        except Exception:
            logger.error('error creating task', exc_info=True)
        finally:
            os.chdir(main_dir)

    def clean_task(self, task_id):
        """Clean up a Task.

        Delete remaining processes and the task temp dir. Release resources
        back to the pilot.

        Args:
            task_id (str): the task_id
        """
        logger.info('cleaning task %s', task_id)
        if task_id in self.tasks:
            task = self.tasks[task_id]
            del self.tasks[task_id]

            # kill process if still running
            try:
                if psutil:
                    # kill children correctly
                    processes = task['process'].children(recursive=True)
                    processes.reverse()
                    processes.append(task['process'])
                    for p in processes:
                        try:
                            p.terminate()
                        except psutil.NoSuchProcess:
                            pass
                        except Exception:
                            logger.warn('error terminating process',
                                        exc_info=True)

                    def on_terminate(proc):
                        logger.info("process %r terminated with exit code %r",
                                    proc, proc.returncode)
                    try:
                        gone, alive = psutil.wait_procs(processes, timeout=0.1,
                                                        callback=on_terminate)
                        for p in alive:
                            try:
                                p.kill()
                            except psutil.NoSuchProcess:
                                pass
                            except Exception:
                                logger.warn('error killing process',
                                            exc_info=True)
                    except Exception:
                        logger.warn('failed to kill processes',
                                    exc_info=True)
                task['p'].terminate()
            except Exception:
                logger.warn('error deleting process', exc_info=True)

            # clean tmpdir
            try:
                if not self.debug:
                    shutil.rmtree(task['tmpdir'])
            except Exception:
                logger.warn('error deleting tmpdir', exc_info=True)

        # return resources to pilot
        try:
            self.resources.release(task_id)
        except Exception:
            logger.warn('error releasing resources', exc_info=True)
