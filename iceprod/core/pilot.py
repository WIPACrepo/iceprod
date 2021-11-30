"""Pilot functionality"""

from __future__ import absolute_import, division, print_function

import os
import sys
import math
import time
import logging
import tempfile
import shutil
import random
from functools import partial
from collections import namedtuple
from datetime import datetime, timedelta
from glob import glob
import signal
import traceback
import asyncio
import concurrent.futures

import iceprod
from iceprod.core.functions import gethostname
from iceprod.core import to_file, constants
from iceprod.core import exe_json
from iceprod.core.exe import Config
from iceprod.core.resources import Resources
from iceprod.core.dataclasses import Number, String
import iceprod.core.logger

logger = logging.getLogger('pilot')

try:
    import psutil
except ImportError:
    psutil = None

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(name):
        pass

class Pilot:
    """
    A pilot task runner.

    The pilot allows multiple tasks to run in sequence or parallel.
    It keeps track of resource usage, killing anything that goes over
    requested amounts.

    Use as an async context manager::

        async with Pilot(*args) as p:
            await p.run()

    Args:
        config (dict): the configuration dictionary
        runner (callable): the task/config runner
        pilot_id (str): the pilot id
        rpc (:py:class:`iceprod.core.exe_json.ServerComms`): RPC to server
        debug (bool): debug mode (default False)
        run_timeout (int): how often to check if a task is running
        backoff_delay (int): what constant delay to use for backoff
        backoff_factor (int): what starting delay to use for exponential backoff
        download_delay (int): min delay between each task download attempt
        resource_interval (float): seconds between resouce measurements
        restrict_site (bool): restrict running tasks to explicitly requiring this site
    """
    def __init__(self, config, runner, pilot_id, rpc=None, debug=False,
                 run_timeout=180, backoff_delay=60, backoff_factor=1,
                 download_delay=60, resource_interval=1.0, restrict_site=False):
        self.config = config
        self.runner = runner
        self.pilot_id = pilot_id
        self.hostname = gethostname()
        self.rpc = rpc
        self.debug = debug
        self.run_timeout = run_timeout
        self.backoff_delay = backoff_delay
        self.backoff_factor = backoff_factor
        self.download_delay = download_delay
        self.resource_interval = resource_interval
        self.query_params = {}
        self.last_download = None

        self.running = True
        self.tasks = {}

        try:
            setproctitle('iceprod2_pilot({})'.format(pilot_id))
        except Exception:
            pass

        logger.warning('pilot_id: %s', self.pilot_id)
        logger.warning('hostname: %s', self.hostname)

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
        if restrict_site:
            if not self.resources.site:
                logger.error('cannot restrict site, as the site is unknown')
            else:
                self.query_params['requirements.site'] = self.resources.site

        self.start_time = time.time()

    async def __aenter__(self):
        # update pilot status
        await self.rpc.update_pilot(self.pilot_id, tasks=[],
                host=self.hostname, version=iceprod.__version__,
                site=self.resources.site,
                start_date=datetime.utcnow().isoformat(),
                resources_available=self.resources.get_available(),
                resources_claimed=self.resources.get_claimed())

        loop = asyncio.get_event_loop()
        # set up resource monitor
        if psutil:
            loop.create_task(self.resource_monitor())
        else:
            logger.warning('no psutil. not checking resource usage')

        # set up signal handler
        def handler(signum, frame):
            logger.critical('termination signal received')
            self.running = False
            self.term_handler()
        self.prev_signal = signal.signal(signal.SIGTERM, handler)

        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
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

            await self.rpc.delete_pilot(self.pilot_id)
        except Exception:
            logger.error('error in aexit', exc_info=True)

        # restore previous signal handler
        signal.signal(signal.SIGTERM, self.prev_signal)

    def term_handler(self):
        """Handle a SIGTERM gracefully"""
        logger.info('checking resources after SIGTERM')
        start_time = time.time()
        overages = self.resources.check_claims()
        for task_id in list(self.tasks):
            task = self.tasks[task_id]
            try:
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
                kwargs = {
                    'resources': used_resources,
                    'reason': reason,
                    'message': message,
                }
                if 'dataset_id' in task['config']['options']:
                    kwargs['dataset_id'] = task['config']['options']['dataset_id']
                self.rpc.task_kill_sync(task_id, **kwargs)
            except Exception:
                pass

        # stop the pilot
        try:
            self.rpc.delete_pilot_sync(self.pilot_id)
        except Exception:
            pass
        sys.exit(1)

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
                    logger.warning('error killing process',
                                   exc_info=True)
        for task in self.tasks.values():
            try:
                task['p'].kill()
            except ProcessLookupError:
                logger.warning('error killing process',
                               exc_info=True)

    async def resource_monitor(self):
        """Monitor the tasks, killing any that go over resource limits"""
        try:
            sleep_time = self.resource_interval # check every X seconds
            while self.running or self.tasks:
                logger.debug('pilot monitor - checking resource usage')
                start_time = time.time()

                try:
                    overages = self.resources.check_claims()
                    for task_id in overages:
                        used_resources = self.resources.get_peak(task_id)
                        logger.warning('kill %r for going over resources: %r',
                                    task_id, used_resources)
                        message = overages[task_id]
                        message += '\n\npilot_id: {}'.format(self.pilot_id)
                        message += '\nhostname: {}'.format(self.hostname)
                        kwargs = {
                            'resources': used_resources,
                            'reason': overages[task_id],
                            'message': message,
                        }
                        if 'dataset_id' in self.tasks[task_id]['config']['options']:
                            kwargs['dataset_id'] = self.tasks[task_id]['config']['options']['dataset_id']

                        self.clean_task(task_id)
                        await self.rpc.task_kill(task_id, **kwargs)
                except Exception:
                    logger.error('error in resource_monitor', exc_info=True)

                duration = time.time()-start_time
                logger.debug('sleep_time %.2f, duration %.2f',sleep_time,duration)
                if duration < sleep_time:
                    await asyncio.sleep(sleep_time-duration)
        except Exception:
            logger.error('pilot monitor died', exc_info=True)
        logger.warning('pilot monitor exiting')

    async def run(self):
        """Run the pilot"""
        download_errors = max_download_errors = 5
        iceprod_errors = max_iceprod_errors = 10
        task_errors = max_task_errors = int(10**math.log10(10+self.resources.total['cpu']))
        logger.info('max_errors: %d, %d', max_download_errors, max_task_errors)
        tasks_running = 0
        async def backoff():
            """Backoff for rate limiting"""
            delay = self.backoff_delay+self.backoff_factor*(1+random.random())
            logger.info('backoff %d', delay)
            await asyncio.sleep(delay)
            self.backoff_factor *= 2
        while self.running or self.tasks:
            while self.running:
                # retrieve new task(s)
                if self.last_download and time.time()-self.last_download < self.download_delay:
                    logger.warning('last download attempt too recent, backing off')
                    await asyncio.sleep(time.time()-self.last_download+self.download_delay)
                    break
                self.last_download = time.time()
                #if self.resources.total['gpu'] and not self.resources.available['gpu']:
                #    logger.info('gpu pilot with no gpus left - not queueing')
                #    break
                try:
                    task_configs = await self.rpc.download_task(
                            self.config['options']['gridspec'],
                            resources=self.resources.get_available(),
                            site=self.resources.site,
                            query_params=self.query_params)
                except Exception:
                    download_errors -= 1
                    if download_errors < 1:
                        self.running = False
                        logger.warning('errors over limit, draining')
                    logger.error('cannot download task. current error count is %d',
                                 max_download_errors-download_errors, exc_info=True)
                    await backoff()
                    continue
                logger.info('task configs: %r', task_configs)

                if not task_configs:
                    logger.info('no task available')
                    if not self.tasks:
                        self.running = False
                        logger.warning('no task available, draining')
                    break
                else:
                    # start up new task(s)
                    for task_config in task_configs:
                        try:
                            task_id = task_config['options']['task_id']
                        except Exception:
                            iceprod_errors -= 1
                            if iceprod_errors < 1:
                                self.running = False
                                logger.warning('errors over limit, draining')
                            logger.error('error getting task_id from config')
                            break
                        try:
                            if 'resources' not in task_config['options']:
                                task_config['options']['resources'] = None
                            task_resources = self.resources.claim(task_id, task_config['options']['resources'])
                            task_config['options']['resources'] = task_resources
                        except Exception:
                            iceprod_errors -= 1
                            if iceprod_errors < 1:
                                self.running = False
                                logger.warning('errors over limit, draining')
                            logger.warning('error claiming resources %s', task_id,
                                        exc_info=True)
                            message = 'pilot_id: {}\nhostname: {}\n\n'.format(self.pilot_id, self.hostname)
                            message += traceback.format_exc()
                            kwargs = {
                                'reason': 'failed to claim resources',
                                'message': message,
                            }
                            if 'dataset_id' in task_config['options']:
                                kwargs['dataset_id'] = task_config['options']['dataset_id']
                            await self.rpc.task_kill(task_id, **kwargs)
                            break
                        try:
                            f = self.create_task(task_config)
                            task = await f.__anext__()
                            task['iter'] = f
                            self.tasks[task_id] = task
                        except Exception:
                            iceprod_errors -= 1
                            if iceprod_errors < 1:
                                self.running = False
                                logger.warning('errors over limit, draining')
                            logger.warning('error creating task %s', task_id,
                                        exc_info=True)
                            message = 'pilot_id: {}\nhostname: {}\n\n'.format(self.pilot_id, self.hostname)
                            message += traceback.format_exc()
                            kwargs = {
                                'reason': 'failed to create task',
                                'message': message,
                            }
                            if 'dataset_id' in task_config['options']:
                                kwargs['dataset_id'] = task_config['options']['dataset_id']
                            await self.rpc.task_kill(task_id, **kwargs)
                            self.clean_task(task_id)
                            break

                    # update pilot status
                    await self.rpc.update_pilot(self.pilot_id, tasks=list(self.tasks),
                                          resources_available=self.resources.get_available(),
                                          resources_claimed=self.resources.get_claimed())

                if (self.resources.available['cpu'] < 1
                    or self.resources.available['memory'] < 1
                    or (self.resources.total['gpu'] and not self.resources.available['gpu'])):
                    logger.info('no resources left, so wait for tasks to finish')
                    break
                # otherwise, backoff
                await backoff()

            # wait until we can queue more tasks
            while self.running or self.tasks:
                logger.info('wait while tasks are running. timeout=%r',self.run_timeout)
                start_time = time.time()
                while self.tasks and time.time()-self.run_timeout < start_time:
                    done,pending = await asyncio.wait([task['p'].wait() for task in self.tasks.values()],
                                                      timeout=self.resource_interval,
                                                      return_when=concurrent.futures.FIRST_COMPLETED)
                    if done:
                        break

                tasks_running = len(self.tasks)
                for task_id in list(self.tasks):
                    # check if any processes have died
                    proc = self.tasks[task_id]['p']
                    clean = False
                    if proc.returncode is not None:
                        f = self.tasks[task_id]['iter']
                        try:
                            task = await f.__anext__()
                        except StopAsyncIteration:
                            logger.warning('task %s finished', task_id)
                        except Exception:
                            logger.warning('task %s failed', task_id,
                                           exc_info=True)
                            task_errors -= 1
                        else:
                            logger.warning('task %s yielded again', task_id)
                            task['iter'] = f
                            self.tasks[task_id] = task
                            continue

                        # if we got here, the task is done
                        clean = True

                        # make sure the task is not running anymore
                        try:
                            await self.rpc.still_running(task_id)
                        except Exception:
                            pass
                        else:
                            logger.warning('task %s is still running, so killing it', task_id)
                            kwargs = {
                                'reason': 'task exited with return code {}'.format(proc.returncode),
                                'message': 'task exited with return code {}'.format(proc.returncode),
                                'resources': self.resources.get_final(task_id),
                            }
                            if 'dataset_id' in self.tasks[task_id]['config']['options']:
                                kwargs['dataset_id'] = self.tasks[task_id]['config']['options']['dataset_id']
                            await self.rpc.task_kill(task_id, **kwargs)
                    else:
                        # check if the DB has killed a task
                        try:
                            await self.rpc.still_running(task_id)
                        except Exception:
                            logger.warning('task %s killed by db', task_id)
                            kwargs = {
                                'reason': 'server kill',
                                'message': 'The server has marked the task as no longer running',
                            }
                            if 'dataset_id' in self.tasks[task_id]['config']['options']:
                                kwargs['dataset_id'] = self.tasks[task_id]['config']['options']['dataset_id']
                            await self.rpc.task_kill(task_id, **kwargs)
                            clean = True
                    if clean:
                        self.clean_task(task_id)
                if task_errors < 1:
                    self.running = False
                    logger.warning('errors over limit, draining')

                # update pilot status
                if (not self.tasks) or len(self.tasks) < tasks_running:
                    logger.info('%d tasks removed', tasks_running-len(self.tasks))
                    tasks_running = len(self.tasks)
                    await self.rpc.update_pilot(self.pilot_id, tasks=list(self.tasks),
                                          resources_available=self.resources.get_available(),
                                          resources_claimed=self.resources.get_claimed())
                    if self.running:
                        break
                elif (self.running and self.resources.available['cpu'] > 1
                      and self.resources.available['memory'] > 1
                      and (self.resources.available['gpu'] or not self.resources.total['gpu'])):
                    logger.info('resources available, so request a task')
                    break

        # last update for pilot state
        await self.rpc.update_pilot(self.pilot_id, tasks=[],
                              resources_available=self.resources.get_available(),
                              resources_claimed=self.resources.get_claimed())

        if task_errors < 1:
            logger.critical('too many errors when running tasks')
            raise RuntimeError('too many errors')
        else:
            logger.warning('cleanly stopping pilot')

    async def create_task(self, config):
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

        tmpdir = tempfile.mkdtemp(suffix='.{}'.format(task_id), dir=os.getcwd())
        config['options']['subprocess_dir'] = tmpdir

        # start the task
        r = config['options']['resources']
        async for proc in self.runner(config, resources=self.resources):
            ps = psutil.Process(proc.pid) if psutil else None
            self.resources.register_process(task_id, ps, tmpdir)
            data = {
                'p': proc,
                'process': ps,
                'tmpdir': tmpdir,
                'config': config,
            }
            yield data

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
                    try:
                        processes = task['process'].children(recursive=True)
                    except psutil.NoSuchProcess:
                        pass # process already died
                    else:
                        processes.reverse()
                        processes.append(task['process'])
                        for p in processes:
                            try:
                                p.terminate()
                            except psutil.NoSuchProcess:
                                pass
                            except Exception:
                                logger.warning('error terminating process',
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
                                    logger.warning('error killing process',
                                                exc_info=True)
                        except Exception:
                            logger.warning('failed to kill processes',
                                        exc_info=True)
                task['p'].kill()
            except ProcessLookupError:
                pass # process already died
            except Exception:
                logger.warning('error deleting process', exc_info=True)

            # copy stdout/stderr
            try:
                os.rename(os.path.join(task['tmpdir'],constants['stderr']), constants['stderr'])
                os.rename(os.path.join(task['tmpdir'],constants['stdout']), constants['stdout'])
            except Exception:
                logger.warning('error copying std[out,err] files', exc_info=True)

            # clean tmpdir
            try:
                if not self.debug:
                    shutil.rmtree(task['tmpdir'])
            except Exception:
                logger.warning('error deleting tmpdir', exc_info=True)

        # return resources to pilot
        try:
            self.resources.release(task_id)
        except Exception:
            logger.warning('error releasing resources', exc_info=True)
