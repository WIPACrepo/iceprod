import logging

from rest_tools.server import RestHandlerSetup, RestHandler

from iceprod import __version__ as version_string
from iceprod.prom_utils import PromRequestMixin
from .auth import AttrAuthMixin

logger = logging.getLogger('rest')


def IceProdRestConfig(config=None, database=None, auth_database=None, s3conn=None):
    config['server_header'] = 'IceProd/' + version_string
    ret = RestHandlerSetup(config)
    ret['database'] = database
    ret['auth_database'] = auth_database
    ret['s3'] = s3conn
    return ret


class APIBase(AttrAuthMixin, PromRequestMixin, RestHandler):
    """Default REST handler"""
    def initialize(self, database=None, auth_database=None, s3=None, **kwargs):
        super().initialize(**kwargs)
        self.db = database
        self.auth_db = auth_database
        self.s3 = s3

    def get_template_namespace(self):
        namespace = super().get_template_namespace()
        namespace['version'] = version_string
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
