import logging
import json
from functools import wraps, update_wrapper, partialmethod

import tornado.web
import tornado.gen
import tornado.httpclient
from tornado.platform.asyncio import to_asyncio_future

import iceprod
from iceprod.server.auth import Auth

logger = logging.getLogger('rest')

def RESTHandlerSetup(config):
    """
    Default RESTHandler setup.

    Args:
        config (dict): config dict

    Returns:
        dict: handler config
    """
    debug = True if 'debug' in config and config['debug'] else False
    auth = None
    auth_url = ''
    if 'auth' in config:
        kwargs = {
            'secret': config['auth'].get('secret')
        }
        if 'expiration' in config['auth']:
            kwargs['expiration'] = config['auth']['expiration']
        if 'expiration_temp' in config['auth']:
            kwargs['expiration_temp'] = config['auth']['expiration_temp']
        auth = Auth(**kwargs)
        if 'url' in config['auth']:
            auth_url = config['auth']['url']
    return {
        'debug': debug,
        'auth': auth,
        'auth_url': auth_url
    }

class RESTHandler(tornado.web.RequestHandler):
    """Default REST handler"""
    def initialize(self, debug=False, auth=None, auth_url=None, **kwargs):
        super(RESTHandler, self).initialize(**kwargs)
        self.debug = debug
        self.auth = auth
        self.auth_url = auth_url
        self.auth_data = {}
        self.auth_key = None

    def set_default_headers(self):
        self._headers['Server'] = 'IceProd/' + iceprod.__version__

    def get_template_namespace(self):
        namespace = super(RESTHandler,self).get_template_namespace()
        namespace['version'] = iceprod.__version__
        return namespace

    def get_current_user(self):
        try:
            type,token = self.request.headers['Authorization'].split(' ', 1)
            if type.lower() != 'bearer':
                raise Exception('bad header type')
            logger.debug('token: %r', token)
            data = self.auth.validate(token)
            self.auth_data = data
            self.auth_key = token
            return data['sub']
        except Exception:
            logger.info('failed auth', exc_info=True)
        return None

    def write_error(self,status_code=500,**kwargs):
        """Write out custom error json."""
        data = {
            'code': status_code,
            'error': self._reason,
        }
        self.write(data)
        self.finish()

def authenticated(method):
    """
    Decorate methods with this to require that the Authorization
    header is filled with a valid token.

    On failure, raises a 403 error.

    Raises:
        :py:class:`tornado.web.HTTPError`
    """
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        if not self.current_user:
            raise tornado.web.HTTPError(403, reason="authentication failed")
        return await method(self, *args, **kwargs)
    return wrapper

def catch_error(method):
    """
    Decorator to catch and handle errors on handlers.

    All failures caught here 
    """
    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        try:
            return await method(self, *args, **kwargs)
        except tornado.web.HTTPError:
            raise # tornado can handle this
        except Exception as e:
            logger.warning('Error in website handler', exc_info=True)
            try:
                self.statsd.incr(self.__class__.__name__+'.error')
            except Exception:
                pass
            message = 'Error in '+self.__class__.__name__
            if self.debug:
                message = message + '\n' + str(e)
            self.send_error(500, reason=message)
    return wrapper

def authorization(**_auth):
    """
    Handle authorization.

    Like :py:func:`iceprod.server.rest.authenticated`, this requires the Authorization header
    to be filled with a valid token.  Note that calling both decorators
    is not necessary, as this decorator will perform authentication
    checking as well.

    Args:
        roles (list): The roles to match
        attrs (list): The attributes to match

    Raises:
        :py:class:`tornado.web.HTTPError`
    """
    def make_wrapper(method):
        @catch_error
        @wraps(method)
        async def wrapper(self, *args, **kwargs):
            if not self.current_user:
                raise tornado.web.HTTPError(403, reason="authentication failed")

            roles = _auth.get('roles', [])
            attrs = _auth.get('attrs', {})
            logger.debug('roles: %r    attrs: %r', roles, attrs)

            authorized = False

            auth_role = self.auth_data.get('role',[])
            logger.debug('token_roles: %r', auth_role)
            if roles and auth_role in roles:
                authorized = True

            if (not authorized) and ('dataset_id:read' in attrs or 'dataset_id:write' in attrs):
                # we need to ask /auths about this
                dataset_id = kwargs.get('dataset_id', None)
                if (not dataset_id) or not isinstance(dataset_id,str):
                    raise tornado.web.HTTPError(403, reason="authorization failed")
                url = self.auth_url+'/auths/'+dataset_id+'/actions/'
                http_client = tornado.httpclient.AsyncHTTPClient()
                if 'dataset_id:read' in attrs:
                    await to_asyncio_future(http_client.fetch(url+'read',
                            headers={'Authorization': b'bearer '+self.auth_key}))
                if 'dataset_id:write' in attrs:
                    await to_asyncio_future(http_client.fetch(url+'write',
                            headers={'Authorization': b'bearer '+self.auth_key}))

            if authorized:
                return await method(self, *args, **kwargs)
            else:
                raise tornado.web.HTTPError(403, reason="authorization failed")
        return wrapper
    return make_wrapper
