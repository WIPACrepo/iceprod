import logging

from rest_tools.server import RestHandlerSetup, RestHandler

import iceprod
from .auth import AttrAuthMixin

logger = logging.getLogger('rest')


def IceProdRestConfig(config=None, statsd=None, database=None, auth_database=None, s3conn=None):
    config['server_header'] = 'IceProd/' + iceprod.__version__
    ret = RestHandlerSetup(config)
    ret['statsd'] = statsd
    ret['database'] = database
    ret['auth_database'] = auth_database
    ret['s3'] = s3conn
    return ret


class APIBase(AttrAuthMixin, RestHandler):
    """Default REST handler"""
    def initialize(self, database=None, auth_database=None, statsd=None, s3=None, **kwargs):
        super().initialize(**kwargs)
        self.db = database
        self.auth_db = auth_database
        self.statsd = statsd
        self.s3 = s3

    def prepare(self):
        super().prepare()
        if self.statsd:
            self.statsd.incr(f'prepare.{self.__class__.__name__}.{self.request.method}')

    def on_finish(self):
        super().on_finish()
        if self.statsd:
            self.statsd.incr(f'finish.{self.__class__.__name__}.{self.request.method}.{self.get_status()}')

    def get_template_namespace(self):
        namespace = super().get_template_namespace()
        namespace['version'] = iceprod.__version__
        return namespace

    def get_current_user(self):
        """Get keycloak username if available"""
        if not super().get_current_user():
            return None

        if 'iceprod-system' in self.auth_data.get('resource_access', {}).get('iceprod', {}).get('roles', []):
            username = 'iceprod-system'
        else:
            username = self.auth_data.get('preferred_username', None)
            if not username:
                logger.info('could not find auth username')

        return username
