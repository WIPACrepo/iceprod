"""
The messaging module runs an :ref:`RPCinternl.Server` instance, acting as the
messaging server for all ZMQ messages.
"""

import logging

import iceprod.server
from iceprod.server import module
from iceprod.server.RPCinternal import Server

class messaging(module.module):
    """
    Run the messaging module, which handles zmq messaging.
    """
    
    def __init__(self,*args,**kwargs):
        # run default init
        super(messaging,self).__init__(*args,**kwargs)
        self.start()
    
    def start(self):
        """Start messaging"""
        self.messaging = Server(address=self.messaging_url)
        self.messaging.start()
