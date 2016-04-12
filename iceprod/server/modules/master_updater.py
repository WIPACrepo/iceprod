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

import tornado.ioloop

import iceprod.server
from iceprod.server import module
from iceprod.server.RPCinternal import RPCService
from iceprod.server.master_communication import send_master

logger = logging.getLogger('modules_master_updater')

class master_updater(module.module):
    """
    Run the master_updater module, which handles updating the master.
    """

    def __init__(self,*args,**kwargs):
        # run default init
        super(master_updater,self).__init__(*args,**kwargs)
        self.service_class = UpdateService(self)

        self.filename = '.master_updater_queue'
        self.buffer = deque()
        self.send_in_progress = False
        self.start()

    def start(self,*args,**kwargs):
        """Start the messaging service"""
        kwargs['callback'] = self._start
        super(master_updater,self).start(*args,**kwargs)

    def stop(self):
        """Stop schedule"""
        super(master_updater,self).stop()

    def kill(self):
        """Kill thread"""
        super(master_updater,self).kill()

    def _start(self):
        if ('master_updater' in self.cfg and
            'filename' in self.cfg['master_updater']):
            self.filename = os.path.expandvars(os.path.expanduser(
                            self.cfg['master_updater']['filename']))
        self.buffer = pickle.load(open(self.filename))
        self.send_in_progress = False
        if self.buffer:
            self._send()

    def update_cfg(self,cfg):
        self.cfg = cfg
        if ('master_updater' in self.cfg and
            'filename' in self.cfg['master_updater']):
            filename = os.path.expandvars(os.path.expanduser(
                       self.cfg['master_updater']['filename']))
            if filename != self.filename:
                os.rename(self.filename,filename)
                self.filename = filename

    def add(self,obj):
        """Add to queue"""
        try:
            self.buffer.append(obj)
            pickle.dump(self.buffer,open(self.filename+'_new','w'),-1)
            os.rename(self.filename+'_new',self.filename)
            if not self.send_in_progress:
                self._send()
        except Exception:
            logger.error('failed to add %r to buffer'%obj,exc_info=True)
            raise

    def _send(self):
        """Send an update to the master"""
        if self.buffer:
            self.send_in_progress = True
            data = self.buffer[0]
            def cb(ret=None):
                if isinstance(ret,Exception):
                    logger.warn('error sending: %r',ret)
                    # If the problem is server side, give it a minute.
                    # This should stop a DDOS from happening.
                    tornado.ioloop.IOLoop.current().call_later(60, self._send)
                else:
                    self.buffer.popleft()
                    pickle.dump(self.buffer,open(self.filename+'_new','w'),-1)
                    os.rename(self.filename+'_new',self.filename)
                    self._send()
            params = {'updates':[data]}
            send_master(self.cfg,'master_update',callback=cb,**params)
        else:
            self.send_in_progress = False

class UpdateService(module.Service):
    """
    Override the basic :class:`Service` handler to handle more messages.
    """
    def reload(self,cfg,callback=None):
        logger.warn('reload()')
        self.mod.update_cfg(cfg)
        if callback:
            callback()

    def add(self,arg,callback=None):
        try:
            self.mod.add(arg)
        except Exception:
            if callback:
                callback(False)
        else:
            if callback:
                callback(True)

