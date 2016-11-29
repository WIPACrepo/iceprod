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

        # mark dataset complete
        self.scheduler.schedule('every 1 hours',
                self.modules['db']['cron_dataset_completion'])

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
        from iceprod.server.grid import grid
        args = [None, self.cfg['queue']['*'], self.cfg, self.modules,
                self.io_loop, self.executor]
        master_grid = grid(*args)

        self.scheduler.schedule('every 1 minutes', master_grid.check_iceprod)

