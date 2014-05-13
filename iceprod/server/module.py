"""
  Interface for configuring modules

  copyright (c) 2012 the icecube collaboration
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

from iceprod.server.RPCInternal import RPCService

logger = None

def handler(signum, frame):
   logger.warn('Signal handler called with signal %s' % signum)
   logger.warn('This is a child process. Signal will be ignored')

class module(object):
    """
    This is an abstract class representing a server module.
    
    At the end of every subclass's __init__(), self.start() should be called.
    
    :param cfg: Initial config file data, loaded from daemon.
    """
    def __init__(self,cfg):
        # set some variables
        self.cfg = cfg
        self.messaging = None
        
        # Provide a default listener for basic start,stop events.
        # Feel free to override.
        self.service_name = self.__class__.__name__
        self.service_class = Service(self)
        
        # ignore signals (these should only be handled in the main process)
        signal.signal(signal.SIGQUIT, handler)
        signal.signal(signal.SIGINT, handler)
        
        # start logging
        logging.basicConfig()
        global logger
        logger = logging.getLogger('iceprod_server.'+self.__class__.__name__)
        if ('logging' in self.cfg and 
            logger.name not in self.cfg['logging']):
            self.cfg['logging'][logger.name] = self.__class__.__name__+'.log'
        iceprod.core.logger.setlogger(logger.name,self.cfg)
        
        # remove stdout logging handler
        iceprod.core.logger.removestdout()
        
        # Change name of process for ps
        if setproctitle:
            try:
                setproctitle(logger.name)
            except Exception:
                logger.warn('could not set proctitle')
    
    def start(self,blocking=True):
        """Start the messaging service"""
        kwargs = {'address':self.cfg['messaging']['address'],
                  'blocking':blocking,
                 }
        if self.service_name and self.service_class:
            kwargs['service_name'] = self.service_name
            kwargs['service_class'] = self.service_class
        self.messaging = RPCService(**kwargs)
        self.messaging.start()
        # if blocking is True, messaging will block the thread until closed
    
    def stop(self):
        """Stop the messaging service"""
        self.messaging.stop()
    
    def kill(self):
        """Stop the messaging service"""
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
