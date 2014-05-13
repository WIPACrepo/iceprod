"""
  schedule module
"""

import time
from threading import Thread,Event,Condition
import logging
from contextlib import contextmanager
from itertools import izip

import iceprod.server
from iceprod.server import module
from iceprod.server.schedule import Scheduler

class schedule(module.module):
    """
    Run the schedule module, which handles periodic tasks
    """
    
    def __init__(self,cfg):
        # run default init
        super(schedule,self).__init__(cfg)
        
        self.scheduler = None
        
        # start Scheduler
        self.start()
    
    def start(self):
        """Start schedule"""
        if not self.scheduler:
            # make Scheduler
            self.scheduler = Scheduler()
            self._make_schedule()
        
        # start things
        self.scheduler.start()
        super(schedule,self).start()
    
    def stop(self):
        """Stop schedule"""
        self.scheduler.finish()
        self.scheduler.join(10) # wait up to 10 seconds for scheduler to finish
        if self.scheduler.isAlive():
            module.logger.warn('scheduler still running after 10 seconds')
        self.scheduler = None
        super(schedule,self).stop()
    
    def kill(self):
        """Kill thread"""
        self.scheduler.finish()
        self.scheduler.join(0.01) # wait only a very short amount of time
        self.scheduler = None
        super(schedule,self).kill()
    
    def _make_schedule(self):
        """Make the default schedule"""
        
        # mark dataset complete
        self.scheduler.schedule('every 1 hours',self._dataset_complete)
    
    def _dataset_complete(self):
        """Check for newly completed datasets and mark them as such"""
        module.logger.warn('running dataset completion check')
        try:
            ret = self.messaging.db.cron_dataset_completion(async=False)
        except Exception as e:
            ret = e
        if isinstance(ret,Exception):
            module.logger.warn('error running dataset completion check',
                               exc_info=True)
    