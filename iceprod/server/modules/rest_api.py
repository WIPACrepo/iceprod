"""
The REST API module runs any endpoints in the REST API,
as specified in the configuration.
"""
import logging

import iceprod.server
from iceprod.server.tornado import setup_rest, startup
from iceprod.server import module

logger = logging.getLogger('modules_rest_api')

class rest_api(module.module):
    """
    Run the REST API module, which handles REST API endpoints.

    Endpoints live in individual modules in `iceprod.server.rest_api.*`.
    """
    def __init__(self,*args,**kwargs):
        super(rest_api,self).__init__(*args,**kwargs)

        # set up the REST API
        app = setup_rest(self.cfg)

        kwargs = {}
        if 'rest_api' in self.cfg:
            if 'address' in self.cfg['rest_api']:
                kwargs['address'] = self.cfg['rest_api']['address']
            if 'port' in self.cfg['rest_api']:
                kwargs['port'] = self.cfg['rest_api']['port']
        startup(app, **kwargs)
