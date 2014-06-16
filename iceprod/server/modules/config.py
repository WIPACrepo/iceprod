"""
config module
"""

import logging

import iceprod.server
from iceprod.server import module
from iceprod.server.config import IceProdConfig

class config(module.module):
    """
    Run the config module, which handles iceprod configuration settings.
    """
    
    def __init__(self,cfg):
        # run default init
        super(config,self).__init__(cfg)
        self.service_class = ConfigService(self)
        
        self.config = IceProdConfig()
        self.start()
    
    def start(self):
        """Start schedule"""
        self.config.load()
        super(config,self).start()
    
    def stop(self):
        """Stop schedule"""
        super(config,self).stop()
        self.config.save()
    
    def kill(self):
        """Kill thread"""
        super(config,self).kill()
        self.config.save()

class ConfigService(module.Service):
    """
    Override the basic :class:`Service` handler to handle config messages.
    """
    def reload(self,cfg,callback=None):
        self.mod.update_cfg(cfg)
        if callback:
            callback()
    
    def get(self,key=None,callback=None):
        if callback:
            if key:
                if key in self.mod.config:
                    ret = self.mod.config[key]
                else:
                    ret = KeyError()
            else:
                ret = dict(self.mod.config)
            callback(ret)
    
    def set(self,key,value,callback=None):
        self.mod.config[key] = value
        if callback:
            callback()
    
    def delete(self,key,callback=None):
        del self.mod.config[key]
        if callback:
            callback()
