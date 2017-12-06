"""
The schedule module is a basic cron for IceProd, executing tasks
at various intervals or fixed times.
"""

from __future__ import absolute_import, division, print_function

import time
from threading import Thread,Event,Condition
import logging
from functools import partial
from contextlib import contextmanager

import tornado.gen

import iceprod.server
from iceprod.server import module
from iceprod.server.schedule import Scheduler
from iceprod.server.globus import SiteGlobusProxy

logger = logging.getLogger('modules_schedule')

class schedule(module.module):
    """
    Run the schedule module, which handles periodic tasks.
    """

    def __init__(self,*args,**kwargs):
        # run default init
        super(schedule,self).__init__(*args,**kwargs)
        self.scheduler = None

    def start(self):
        """Start schedule"""
        super(schedule,self).start()
        if not self.scheduler:
            # make Scheduler
            self.scheduler = Scheduler(self.io_loop)
            def cb():
                try:
                    self._make_schedule()
                except Exception:
                    logger.error('error making schedule', exc_info=True)
            self.io_loop.add_callback(cb)

        self.scheduler.start()

    def stop(self):
        """Stop schedule"""
        if self.scheduler:
            self.scheduler = None
        super(schedule,self).stop()

    def kill(self):
        """Kill thread"""
        if self.scheduler:
            self.scheduler = None
        super(schedule,self).kill()

    def _make_schedule(self):
        """Make the default schedule"""

        config = self.cfg['schedule']

        # mark datasets complete
        self.scheduler.schedule(config['dataset_completion'],
                self.modules['db']['cron_dataset_completion'])

        # collate node resources
        self.scheduler.schedule(config['collate_resources'],
                partial(self.modules['db']['node_collate_resources'],
                        site_id=self.cfg['site_id']))

        self.scheduler.schedule(config['remove_old_passkeys'],
                self.modules['db']['cron_remove_old_passkeys'])

        self.scheduler.schedule(config['generate_web_graphs'],
                self.modules['db']['cron_generate_web_graphs'])

        self.scheduler.schedule(config['dataset_status_monitoring'],
                self.modules['db']['cron_dataset_status_monitoring'])

        if self.cfg['modules']['queue']:
            self._queue_schedule()

        if 'status' in self.cfg['master'] and self.cfg['master']['status']:
            self._master_schedule()
        elif 'url' in self.cfg['master'] and self.cfg['master']['url']:
            self._client_schedule()
        else:
            self._single_schedule()

    def _queue_schedule(self):
        """For active grids"""
        self.scheduler.schedule(config['pilot_monitoring'],
                self.modules['db']['cron_pilot_monitoring'])

        self.scheduler.schedule(config['suspend_overusage_tasks'],
                self.modules['db']['cron_suspend_overusage_tasks'])

        self.scheduler.schedule(config['check_active_pilots_tasks'],
                self.modules['db']['cron_check_active_pilots_tasks'])

    def _single_schedule(self):
        """For single-node systems, with no client-master relationship"""
        config = self.cfg['schedule']
        self.scheduler.schedule(config['job_completion'],
                self.modules['db']['cron_job_completion'],
                delete_jobs=False)

        self.scheduler.schedule(config['buffer_jobs_tasks'],
                self.modules['db']['queue_buffer_jobs_tasks'])

    def _client_schedule(self):
        """For clients attached to masters"""
        config = self.cfg['schedule']
        self.scheduler.schedule(config['dataset_update'],
                self.modules['db']['cron_dataset_update'])

        self.scheduler.schedule(config['job_completion'],
                self.modules['db']['cron_job_completion'],
                delete_jobs=True)

    def _master_schedule(self):
        """For masters with clients"""
        # fake a grid, so we can do grid-like things
        from iceprod.server.grid import BaseGrid
        args = [None, self.cfg['queue']['*'], self.cfg, self.modules,
                self.io_loop, self.executor, self.statsd]
        master_grid = BaseGrid(*args)

        config = self.cfg['schedule']

        self.scheduler.schedule(config['job_completion'],
                self.modules['db']['cron_job_completion'])

        self.scheduler.schedule(config['buffer_jobs_tasks'],
                self.modules['db']['queue_buffer_jobs_tasks'])

        self.scheduler.schedule(config['task_stat_monitoring'],
                self.modules['db']['cron_task_stat_monitoring'])

        self.scheduler.schedule(config['check_iceprod'], master_grid.check_iceprod)

        try:
            # make sure the gridftp proxy is set up
            proxy_kwargs = {}
            if 'gridftp_cfgfile' in self.cfg['queue']:
                proxy_kwargs['cfgfile'] = self.cfg['queue']['gridftp_cfgfile']
            proxy = SiteGlobusProxy(**proxy_kwargs)
            proxy.update_proxy()
            self.scheduler.schedule(config['clean_completed_jobs'], self.modules['db']['cron_clean_completed_jobs'])
        except Exception:
            logger.warning('Failed to schedule clean_completed_jobs')