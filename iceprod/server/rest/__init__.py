import logging

import tornado.web

import iceprod
from iceprod.server.auth import Auth

logger = logging.getLogger('rest.auth')

def RESTHandlerSetup(config):
    """
    Default RESTHandler setup.

    Args:
        config (dict): config dict

    Returns:
        dict: handler config
    """
    auth = None
    if 'auth' in config:
        kwargs = {
            'secret': config['auth'].get('secret')
        }
        if 'expiration' in config['auth']:
            kwargs['expiration'] = config['auth']['expiration']
        if 'expiration_temp' in config['auth']:
            kwargs['expiration_temp'] = config['auth']['expiration_temp']
        auth = Auth(**kwargs)
    return {'auth': auth}

class RESTHandler(tornado.web.RequestHandler):
    """Default REST handler"""
    def initialize(self, auth=None, **kwargs):
        super(RESTHandler, self).initialize(**kwargs)
        self.auth = auth

    def set_default_headers(self):
        self._headers['Server'] = 'IceProd/' + iceprod.__version__
        
    def write_error(self,status_code=500,**kwargs):
        """Write out custom error json."""
        data = {
            'code': status_code,
            'error': self._reason,
        }
        self.write(data)
        self.finish()


def catch_error(method):
    """Decorator to catch and handle errors on handlers"""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            self.statsd.incr(self.__class__.__name__+'.error')
            logger.warning('Error in website handler', exc_info=True)
            message = 'Error generating page for '+self.__class__.__name__
            if self.debug:
                message = message + '\n' + str(e)
            self.send_error(500, message=message)
    return wrapper