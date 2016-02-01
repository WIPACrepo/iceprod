"""
The config module stores and shares configuration settings with the rest of
iceprod server. Other modules may request an updated config or make a change
to the current config. If a change is made, the new config is BROADCAST to
all modules.
"""

import logging

import iceprod.server
from iceprod.server import module
from iceprod.server.config import IceProdConfig
from iceprod.server.RPCinternal import RPCService

class config(module.module):
    """
    Run the config module, which handles iceprod configuration settings.

    This is a module wrapper around
    :class:`iceprod.server.config.IceProdConfig`.

    :param filename: filename for config file (optional)
    """

    def __init__(self,*args,**kwargs):
        # pull out config filename, if available
        filename = None
        if 'filename' in kwargs:
            filename = kwargs.pop('filename')
        # run default init
        super(config,self).__init__(*args,**kwargs)
        self.service_class = ConfigService(self)

        self.config = IceProdConfig(filename=filename)
        self.start()

    def start(self,blocking=True):
        """Start the messaging service"""
        self.config = IceProdConfig()
        self.config.load()
        kwargs = {'address':self.messaging_url,
                  'block':blocking,
                 }
        if self.service_name and self.service_class:
            kwargs['service_name'] = self.service_name
            kwargs['service_class'] = self.service_class
        self.messaging = RPCService(**kwargs)
        self.messaging.start()
        # if blocking is True, messaging will block the thread until closed

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
    def get_config_string(self, callback=None):
        if callback:
            callback(self.mod.config.save_to_string())
    def set_config_string(self, config_text, callback=None):
        self.mod.config.load(text = config_text)
        if callback:
            callback(True)
        self.mod.messaging.BROADCAST.reload(cfg=dict(self.mod.config))


    def reload(self,cfg,callback=None):
        # do nothing - we are the config source
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

    def set(self,key,value,update=True,callback=None):
        self.mod.config[key] = value
        if callback:
            callback()
        if update:
            self.mod.messaging.BROADCAST.reload(cfg=dict(self.mod.config))

    def delete(self,key,callback=None):
        del self.mod.config[key]
        if callback:
            callback()
        self.mod.messaging.BROADCAST.reload(cfg=dict(self.mod.config))
