"""
Interface for configuring modules
"""

from __future__ import print_function
import multiprocessing
import logging
import logging.config
import signal
import os
from functools import partial

try:
    from setproctitle import setproctitle
except ImportError:
    setproctitle = None
    print('Could not import setproctitle module')
import iceprod.core.logger

from iceprod.server.RPCinternal import RPCService

def handler(signum, frame):
   logging.warn('Signal handler called with signal %s' % signum)
   logging.warn('This is a child process. Signal will be ignored')

class module(object):
    """
    This is an abstract class representing a server module.
    
    At the end of every subclass's __init__(), self.start() should be called.
    
    :param basic_config: A :ref:`BasicConfig` object.
    """
    def __init__(self,basic_cfg):
        # set some variables
        self.basic_config = basic_cfg
        self.messaging_url = basic_cfg.messaging_url
        self.messaging = None
        self.cfg = {}
        
        # Provide a default listener for basic start,stop events.
        # Feel free to override.
        self.service_name = self.__class__.__name__
        self.service_class = Service(self)
        
        # ignore signals (these should only be handled in the main process)
        signal.signal(signal.SIGQUIT, handler)
        signal.signal(signal.SIGINT, handler)
        
        # start logging
        module_name = 'iceprod_server.'+self.__class__.__name__
        logging.basicConfig()
        self.logger = logging.getLogger(module_name)
        if self.logger.name not in self.basic_config.logging:
            self.basic_config.logging[self.logger.name] = self.__class__.__name__+'.log'
        iceprod.core.logger.setlogger(self.logger.name,self.basic_config)
        
        # remove stdout logging handler
        iceprod.core.logger.removestdout()
        
        # Change name of process for ps
        if setproctitle:
            try:
                setproctitle(self.logger.name)
            except Exception:
                self.logger.warn('could not set proctitle')
    
    def start(self,blocking=True,callback=None):
        """Start the messaging service"""
        kwargs = {'address':self.messaging_url,
                  'block':blocking,
                 }
        if self.service_name and self.service_class:
            kwargs['service_name'] = self.service_name
            kwargs['service_class'] = self.service_class
        self.messaging = RPCService(**kwargs)
        def cb(ret=None):
            if ret and isinstance(ret,dict):
                self.cfg = ret
            else:
                self.logger.error('failed to get cfg settings: %r',ret)
            if callback:
                callback()
        self.messaging.config.get(callback=cb)
        self.messaging.start()
        # if blocking is True, messaging will block the thread until closed
    
    def stop(self):
        """Stop the messaging service"""
        if self.messaging:
            self.messaging.stop()
    
    def kill(self):
        """Stop the messaging service"""
        if self.messaging:
            self.messaging.stop()

class Service():
    """
    A basic service class, so we can respond to the basic messages.
    Subclass this to add more messages.
    """
    def __init__(self,mod):
        self.mod = mod
    def start(self,callback=None):
        self.mod.start()
        if callback:
            callback()
    def stop(self,callback=None):
        self.mod.stop()
        if callback:
            callback()
    def kill(self,callback=None):
        self.mod.kill()
        if callback:
            callback()
    def restart(self,callback=None):
        self.mod.stop()
        self.mod.start()
        if callback:
            callback()
    def reload(self,cfg,callback=None):
        self.mod.stop()
        self.mod.cfg = cfg
        self.mod.start()
        if callback:
            callback()
