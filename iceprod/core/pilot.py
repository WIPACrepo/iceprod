"""Pilot functionality"""

from __future__ import absolute_import, division, print_function

import os
import time
import logging
import threading
import tempfile
import shutil
from functools import partial
from multiprocessing import Process
from collections import namedtuple

from iceprod.core import exe_json
from iceprod.core.util import Task_Resources, get_task_resources
from iceprod.core.dataclasses import Number, String

logger = logging.getLogger('pilot')

try:
    import psutil
except ImportError:
    psutil = None

Task = namedtuple('Task', ['p','process','resources','tmpdir'])

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
    def __init__(self, config, runner, pilot_id, run_timeout=60):
        self.config = config
        self.runner = runner
        self.pilot_id = pilot_id
        self.run_timeout = run_timeout

        # set up resources for pilot
        self.lock = threading.Condition()
        self.resources = get_task_resources()
        if 'resources' in config['options']:
            self.resources.update(config['options']['resources'])

        self.start_time = time.time()

        # set up monitor
        self.tasks = {}
        if psutil:
            self.monitor_running = True
            self.monitor_thread = threading.Thread(target=self.monitor)
            self.monitor_thread.start()
        else:
            logger.warn('no psutil. not checking resource usage')
        
        self.run()
        if psutil:
            self.monitor_running = False
            self.monitor_thread.join()

    def monitor(self):
        """Monitor the tasks, killing any that go over resource limits"""
        try:
            sleep_time = 0.1 # check every X seconds
            disk_sleep_time = 180
            disk_start_time = time.time()
            while self.monitor_running:
                start_time = time.time()
                with self.lock:
                    logger.info('pilot monitor - checking resource usage')
                    for task_id in list(self.tasks):
                        task = self.tasks[task_id]
                        used_resources = {r:0 for r in task.resources}
                        processes = [task.process]+task.process.children()
                        for p in processes:
                            used_resources['cpu'] += p.cpu_percent()/100.0
                            used_resources['memory'] += p.memory_info().rss/1000000000.0
                        used_resources['time'] = (start_time - task.process.create_time())/3600.0
                        if start_time - disk_start_time > disk_sleep_time:
                            disk_start_time = start_time
                            used_resources['disk'] = du(task.tmpdir)/1000000000.0
                        logger.debug('task %r used %r',task_id,used_resources)

                        kill = False
                        reason = ''
                        for r in used_resources:
                            if used_resources[r] > task.resources[r]:
                                kill = True
                                reason = 'Resource overuse for {}: {}'.format(r,used_resources[r])
                                break
                        if kill:
                            logger.warn('kill %r for going over resources %r',
                                        task_id, used_resources)
                            processes.reverse() # kill children first
                            for p in processes:
                                p.terminate()
                            
                            def on_terminate(proc):
                                logger.info("process %r terminated with exit code %r",
                                            proc, proc.returncode)
                            gone, alive = psutil.wait_procs(processes, timeout=0.1,
                                                            callback=on_terminate)
                            for p in alive:
                                p.kill()
                            self.clean_task(task)
                            del self.tasks[task_id]
                            exe_json.task_kill(task_id, resources=used_resources,
                                               reason=reason)
                            # try to queue another task
                            self.lock.notify()

                duration = time.time()-start_time
                if duration < sleep_time:
                    time.sleep(sleep_time - duration)
        except Exception:
            logger.error('pilot monitor died', exc_info=True)

    def run(self):
        """Run the pilot"""
        with self.lock:
            errors = 0
            tasks_running = 0
            running = True
            while running:
                while running:
                    try:
                        task_config = exe_json.downloadtask(self.config['options']['gridspec'],
                                                            resources=self.resources)
                    except Exception:
                        errors += 1
                        if errors > 5:
                            running = False
                        logger.error('cannot download task. current error count is %d',
                                     errors, exc_info=True)
                        continue
                    logger.info('task config: %r', task_config)

                    if task_config is None:
                        logger.info('no task available')
                        if not self.tasks:
                            running = False
                        break
                    else:
                        task_id = None
                        try:
                            task_id = task_config['options']['task_id']
                            self.create_task(task_config)
                        except Exception:
                            errors += 1
                            if errors > 5:
                                running = False
                            logger.warn('error creating task %s', task_id,
                                        exc_info=True)
                        else:
                            tasks_running += 1
                            for r in self.resources:
                                if r in ('cpu','memory','disk', 'gpu'):
                                    self.resources[r] -= task_config['options']['resources'][r]
                                elif isinstance(self.resources[r], String):
                                    self.resources[r] = self.resources[r].split(',')
                            exe_json.update_pilot(self.pilot_id, tasks=','.join(self.tasks))

                # wait until we can queue more tasks
                while running or self.tasks:
                    logger.info('wait while tasks are running')
                    self.lock.wait(timeout=self.run_timeout)
                    # check if any processes have died
                    for task_id in list(self.tasks):
                        if not self.tasks[task_id].p.is_alive():
                            self.clean_task(self.tasks[task_id])
                            del self.tasks[task_id]
                    if len(self.tasks) < tasks_running:
                        logger.info('%d tasks removed', tasks_running-len(self.tasks))
                        tasks_running = len(self.tasks)
                        exe_json.update_pilot(self.pilot_id, tasks=','.join(self.tasks))
                        if running:
                            break

            if errors >= 5:
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

        # add resources
        if 'resources' not in config['options']:
            config['options']['resources'] = {}
        for r in self.resources:
            if r in config['options']['resources']:
                if config['options']['resources'][r] > self.resources[r]:
                    raise Exception('requested {} of {}, but pilot has {}'.format(
                        config['options']['resources'][r], r, self.resources[r]))
            else:
                config['options']['resources'][r] = self.resources[r]

        # run task in tmp dir
        main_dir = os.getcwd()
        try:
            tmpdir = tempfile.mkdtemp(dir=main_dir)
            os.chdir(tmpdir)
            for f in os.listdir(main_dir):
                os.symlink(os.path.join(main_dir, f),
                           os.path.join(tmpdir, f))

            # start the task
            p = Process(target=partial(self.runner, config))
            p.start()
            if psutil:
                ps = psutil.Process(p.pid)
                ps.nice(psutil.Process().nice()+1)
            else:
                ps = None
            r = config['options']['resources']
            self.tasks[task_id] = Task(p=p, process=ps, resources=r,
                                       tmpdir=tmpdir)
        finally:
            os.chdir(main_dir)

    def clean_task(self, task):
        """
        Clean up a Task

        Args:
            task (Task): the Task
        """
        logger.info('cleaning task at %s', task.tmpdir)
        try:
            # return resources to pilot
            for r in task.resources:
                if r in ('cpu','memory','disk', 'gpu'):
                    self.resources[r] += task.resources[r]
            # clean tmpdir
            shutil.rmtree(task.tmpdir)
        except Exception:
            logger.warn('error cleaning up task', exc_info=True)

def du(path):
    """
    Perform a "du" on a path, getting the disk usage.

    Args:
        path (str): The path to analyze

    Returns:
        int: bytes used
    """
    total = 0
    for root,dirs,files in os.walk(path):
        for d in list(dirs):
            p = os.path.join(root,d)
            if os.path.islink(p):
                dirs.remove(d)
        for f in files:
            p = os.path.join(root,f)
            if not os.path.islink(p):
                total += os.path.getsize(p)
    return total
