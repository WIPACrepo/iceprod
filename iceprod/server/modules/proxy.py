"""
The proxy module is a convenience module for starting and stopping a 
proxy server with IceProd. It currently uses 
`Squid <http://www.squid-cache.org/>`_, which requires installation before
use. The module will auto-configure `Squid`, so a default install suffices.

Note that large grids should probably configure and use their own proxy
infrastructure instead of running this module.
"""

from __future__ import absolute_import, division, print_function

import time
from threading import Thread,Event,Condition
import logging

import iceprod.server
from iceprod.server import module
from iceprod.server.squid import Squid

class proxy(module.module):
    """
    Run the proxy module, which runs a squid proxy
    """
    
    def __init__(self,*args,**kwargs):
        super(proxy,self).__init__(*args,**kwargs)
        self.squid = None
        self.service_class = ProxyService(self)
        self.start()
    
    def _getargs(self):
        if 'proxy' in self.cfg:
            kwargs = self.cfg['proxy'].copy()
            if 'http_username' in self.cfg:
                kwargs['username'] = self.cfg['http_username']
            if 'http_password' in self.cfg:
                kwargs['password'] = self.cfg['http_password']
            return kwargs
        else:
            return {}
    
    def start(self):
        """Start proxy if not already running"""
        super(proxy,self).start(callback=self._start)
    def _start(self):
        # after module is started, start squid
        self.squid = Squid(**self._getargs())
        self.squid.start()
        
    def stop(self):
        """Stop proxy"""
        if self.squid:
            self.squid.stop()
        super(proxy,self).stop()
    
    def kill(self):
        """Kill proxy"""
        if self.squid:
            self.squid.kill()
        super(proxy,self).stop()
        
    def update_cfg(self,newcfg):
        """Update the cfg, making any necessary changes"""
        self.cfg = newcfg
        if self.squid:
            self.squid.update(**self._getargs())
            self.squid.restart()

class ProxyService(module.Service):
    """
    Override the basic :class:`Service` handler to provide a more
    effecient reload method.
    """
    def reload(self,cfg,callback=None):
        self.mod.update_cfg(cfg)
        if callback:
            callback()
