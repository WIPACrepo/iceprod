"""
The REST API module runs any endpoints in the REST API,
as specified in the configuration.
"""
import logging
import importlib
import os

from rest_tools.server import RestServer

import iceprod.server
from iceprod.server import module
from iceprod.server import get_pkgdata_filename

logger = logging.getLogger('modules_rest_api')

class rest_api(module.module):
    """
    Run the REST API module, which handles REST API endpoints.

    Endpoints live in individual modules in `iceprod.server.rest_api.*`.
    """
    def __init__(self,*args,**kwargs):
        super(rest_api,self).__init__(*args,**kwargs)

        # set up the REST API
        routes, args = setup_rest(self.cfg, module=self)
        self.server = RestServer(**args)
        for r in routes:
            self.server.add_route(*r)

        kwargs = {}
        if 'rest_api' in self.cfg:
            if 'address' in self.cfg['rest_api']:
                kwargs['address'] = self.cfg['rest_api']['address']
            if 'port' in self.cfg['rest_api']:
                kwargs['port'] = self.cfg['rest_api']['port']
        self.server.startup(**kwargs)


def setup_rest(config, module=None):
    """
    Setup a REST Tornado server according to the config.

    The config should have defined the REST apis to use::

        {"rest": {"name1":{...}, "name2":{...} } }

    Args:
        config (:py:class:`iceprod.server.config.Config`): An IceProd config

    Returns:
        tuple: (routes, application args)
    """
    rest = config.get('rest',{})
    routes = []
    for key in rest:
        logger.warning('setting up tornado for REST.%s', key)
        mod = importlib.import_module('iceprod.server.rest.'+key)
        routes.extend(mod.setup(config, module=module))
    logger.info('REST routes being served:')
    for r in routes:
        logger.info('  %r', r)

    # get package data
    static_path = get_pkgdata_filename('iceprod.server','data/www')
    if static_path is None or not os.path.exists(static_path):
        logger.info('static path: %r',static_path)
        raise Exception('bad static path')
    template_path = get_pkgdata_filename('iceprod.server','data/www_templates')
    if template_path is None or not os.path.exists(template_path):
        logger.info('template path: %r',template_path)
        raise Exception('bad template path')

    kwargs = {
        'static_path': static_path,
        'template_path': template_path,
        'max_body_size': 10**9, # 1GB
    }
    return (routes, kwargs)