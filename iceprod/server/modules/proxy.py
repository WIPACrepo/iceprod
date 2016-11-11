"""
The proxy module is a convenience module for starting and stopping a 
proxy server with IceProd. It currently uses 
`Squid <http://www.squid-cache.org/>`_, which requires installation before
use. The module will auto-configure `Squid`, so a default install suffices.

Note that large grids should probably configure and use their own proxy
infrastructure instead of running this module.
"""

from __future__ import absolute_import, division, print_function

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
        super(proxy,self).start()
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
