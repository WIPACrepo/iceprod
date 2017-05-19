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
from itertools import izip

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

        # mark datasets complete
        self.scheduler.schedule('every 1 hours',
                self.modules['db']['cron_dataset_completion'])

        # mark jobs complete
        self.scheduler.schedule('every 31 minutes',
                self.modules['db']['cron_job_completion'])

        # collate node resources
        self.scheduler.schedule('every 1 hours',
                partial(self.modules['db']['node_collate_resources'],
                        site_id=self.cfg['site_id']))

        self.scheduler.schedule('every 6 hours',
                self.modules['db']['cron_remove_old_passkeys'])

        self.scheduler.schedule('every 10 minutes',
                self.modules['db']['cron_generate_web_graphs'])

        if ('master' in self.cfg and 'status' in self.cfg['master'] and
            self.cfg['master']['status']):
            self._master_schedule()

    def _master_schedule(self):
        # fake a grid, so we can do grid-like things
        from iceprod.server.grid import BaseGrid
        args = [None, self.cfg['queue']['*'], self.cfg, self.modules,
                self.io_loop, self.executor, self.statsd]
        master_grid = BaseGrid(*args)

        # make sure the gridftp proxy is set up
        proxy_kwargs = {}
        if 'gridftp_cfgfile' in self.cfg['queue']:
            proxy_kwargs['cfgfile'] = self.cfg['queue']['gridftp_cfgfile']
        proxy = SiteGlobusProxy(**proxy_kwargs)
        proxy.update_proxy()

        self.scheduler.schedule('every 10 minutes',
                self.modules['db']['queue_buffer_jobs_tasks'])

        self.scheduler.schedule('every 5 minutes', master_grid.check_iceprod)

        self.scheduler.schedule('every 1 days',
                self.modules['db']['cron_clean_completed_jobs'])

