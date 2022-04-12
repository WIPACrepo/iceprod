import logging
import json
from functools import wraps, update_wrapper, partialmethod

import tornado.web
import tornado.httpclient
from tornado.platform.asyncio import to_asyncio_future

from rest_tools.client import RestClient
from rest_tools.utils import Auth
from rest_tools.server import RestHandlerSetup, authenticated, catch_error
from rest_tools.server import RestHandler as BaseRestHandler

import iceprod

logger = logging.getLogger('rest')

def RESTHandlerSetup(config, module=None):
    ret = RestHandlerSetup(config)
    ret['module'] = module
    return ret

class RESTHandler(BaseRestHandler):
    """Default REST handler"""
    def initialize(self, module=None, **kwargs):
        super(RESTHandler, self).initialize(**kwargs)
        self.module = module

    def prepare(self):
        if self.module and self.module.statsd:
            self.module.statsd.incr('prepare.{}.{}'.format(self.__class__.__name__, self.request.method))
            # for individual dataset/job/task info
            #self.module.statsd.incr('prepare.{}.{}'.format(self.request.path.replace('.','_'), self.request.method))

    def on_finish(self):
        if self.module and self.module.statsd:
            self.module.statsd.incr('finish.{}.{}.{}'.format(self.__class__.__name__,
                             self.request.method, self.get_status()))

    def set_default_headers(self):
        self._headers['Server'] = 'IceProd/' + iceprod.__version__

    def get_template_namespace(self):
        namespace = super(RESTHandler,self).get_template_namespace()
        namespace['version'] = iceprod.__version__
        return namespace



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
        @authenticated
        @catch_error
        @wraps(method)
        async def wrapper(self, *args, **kwargs):
            roles = _auth.get('roles', [])
            attrs = _auth.get('attrs', {})

            authorized = False

            auth_role = self.auth_data.get('role',None)
            if roles and auth_role in roles:
                authorized = True
            else:
                logger.info('roles: %r    attrs: %r', roles, attrs)
                logger.info('token_role: %r', auth_role)
                logger.info('role mismatch')

            if (not authorized) and ('dataset_id:read' in attrs or 'dataset_id:write' in attrs):
                # we need to ask /auths about this
                try:
                    dataset_id = kwargs.get('dataset_id', None)
                    if (not dataset_id) or not isinstance(dataset_id,str):
                        raise tornado.web.HTTPError(403, reason="authorization failed")
                    url = '/auths/'+dataset_id+'/actions/'
                    http_client = RestClient(self.auth_url, token=self.auth_key)
                    if 'dataset_id:read' in attrs:
                        await http_client.request('GET', url+'read')
                        authorized = True
                    elif 'dataset_id:write' in attrs:
                        await http_client.request('GET', url+'write')
                        authorized = True
                except Exception:
                    logger.info('/auths failure', exc_info=True)
                    raise tornado.web.HTTPError(403, reason="authorization failed")

            if authorized:
                return await method(self, *args, **kwargs)
            else:
                raise tornado.web.HTTPError(403, reason="authorization failed")
        return wrapper
    return make_wrapper
