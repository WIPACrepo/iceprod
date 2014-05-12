"""
proxy module
"""

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
    
    def __init__(self,cfg):
        super(proxy,self).__init__(cfg)
        self.squid = Squid(**self._getargs())
        self.service_class = ProxyService(self)
        self.start()
    
    def _getargs(self):
        kwargs = self.cfg['proxy'].copy()
        kwargs['username'] = self.cfg['http_username']
        kwargs['password'] = self.cfg['http_password']
        return kwargs
    
    def start(self):
        """Start proxy if not already running"""
        self.squid.start()
        super(proxy,self).start()
        
    def stop(self):
        """Stop proxy"""
        self.squid.stop()
        super(proxy,self).stop()
    
    def kill(self):
        """Kill proxy"""
        self.squid.kill()
        super(proxy,self).stop()
        
    def update_cfg(self,newcfg):
        """Update the cfg, making any necessary changes"""
        self.cfg = newcfg
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
