"""
Helpers for setting up `Tornado <http://tornado.readthedocs.io>`_ servers.
"""

import os
import socket
import logging
import importlib
import tornado.web

from iceprod.server import get_pkgdata_filename

logger = logging.getLogger('tornado')


def tornado_logger(handler):
    """Log tornado messages to our logger"""
    if handler.get_status() < 400:
        log_method = logger.debug
    elif handler.get_status() < 500:
        log_method = logger.warning
    else:
        log_method = logger.error
    request_time = 1000.0 * handler.request.request_time()
    log_method("%d %s %.2fms", handler.get_status(),
            handler._request_summary(), request_time)

def setup_rest(config):
    """
    Setup a REST Tornado server according to the config.

    The config should have defined the REST apis to use::

        {"rest": {"name1":{...}, "name2":{...} } }

    Args:
        config (:py:class:`iceprod.server.config.Config`): An IceProd config

    Returns:
        :py:class:`tornado.web.Application`
    """
    rest = config.get('rest',{})
    routes = []
    for key in rest:
        logger.warn('setting up tornado for REST.%s', key)
        mod = importlib.import_module('iceprod.server.rest.'+key)
        routes.extend(mod.setup(config))

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
        'log_function': tornado_logger,
    }
    return tornado.web.Application(routes, **kwargs)

def startup(app, address='localhost', port=8080):
    """
    Start up a Tornado server.

    Note that after calling this method you still need to call
    `IOLoop.current().start()` to start the server.

    Args:
        app (:py:class:`tornado.web.Application`): Tornado application
        address (str): bind address
        port (int): bind port
    """
    logger.warning('tornado bound to %s:%d', address, port)
    
    http_server = tornado.httpserver.HTTPServer(
            app, xheaders=True)
    http_server.bind(port, address=address, family=socket.AF_INET)
    http_server.start()
