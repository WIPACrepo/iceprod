"""
The master_updater module queues and sends updates to the master.
It uses a disk-based store to make sure updates survive restarting
the local instance.
"""

import os
import logging
try:
    import cPickle as pickle
except ImportError:
    import pickle
from collections import deque

import tornado.gen
from tornado.concurrent import run_on_executor

import iceprod.server
from iceprod.server import module
from iceprod.server.master_communication import send_master

logger = logging.getLogger('modules_master_updater')

class master_updater(module.module):
    """
    Run the master_updater module, which handles updating the master.
    """

    def __init__(self,*args,**kwargs):
        # run default init
        super(master_updater,self).__init__(*args,**kwargs)
        self.service['add'] = self.add

        self.filename = '.master_updater_queue'
        self.buffer = deque()
        self.send_in_progress = False

    def start(self):
        """Start master updater"""
        super(master_updater,self).start()
        if ('master_updater' in self.cfg and
            'filename' in self.cfg['master_updater']):
            self.filename = os.path.expandvars(os.path.expanduser(
                            self.cfg['master_updater']['filename']))
        if os.path.exists(self.filename):
            self._load()
        self.io_loop.add_callback(self._send)

    def stop(self):
        """Stop master updater"""
        super(master_updater,self).stop()

    def kill(self):
        """Kill master updater"""
        super(master_updater,self).kill()

    def _load(self):
        """Load from cache file"""
        self.buffer = pickle.load(open(self.filename))

    @run_on_executor
    def _save(self):
        """Save to cache file"""
        pickle.dump(self.buffer, open(self.filename+'_new', 'wb'), -1)
        os.rename(self.filename+'_new', self.filename)

    @tornado.gen.coroutine
    def add(self, obj):
        """Add obj to queue"""
        try:
            self.buffer.append(obj)
            yield self._save()
            if not self.send_in_progress:
                self.io_loop.add_callback(self._send)
        except Exception:
            logger.error('failed to add %r to buffer', obj, exc_info=True)
            raise

    @tornado.gen.coroutine
    def _send(self):
        """Send an update to the master"""
        if self.buffer:
            self.send_in_progress = True
            data = self.buffer[0]
            params = {'updates':[data]}
            try:
                yield send_master(self.cfg, 'master_update', **params)
            except:
                logger.warn('error sending to master', exc_info=True)
                # If the problem is server side, give it a minute.
                # This should stop a DDOS from happening.
                self.io_loop.call_later(60, self._send)
            else:
                # remove data we just successfully sent
                self.buffer.popleft()
                yield self._save()
                self.io_loop.add_callback(self._send)
        else:
            self.send_in_progress = False

