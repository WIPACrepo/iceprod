import logging
from typing import Any

import motor.motor_asyncio
from rest_tools.server import RestHandlerSetup, RestHandler
from tornado.escape import json_encode

from iceprod.util import VERSION_STRING
from iceprod.prom_utils import PromRequestMixin
from .auth import AttrAuthMixin

logger = logging.getLogger('rest')


DB = motor.motor_asyncio.AsyncIOMotorDatabase | motor.motor_asyncio.AsyncIOMotorClient


def IceProdRestConfig(config: dict[str, Any], database: DB, auth_database=None, s3conn=None):
    if config:
        config['server_header'] = 'IceProd/' + VERSION_STRING
    ret = RestHandlerSetup(config)
    ret['database'] = database
    ret['auth_database'] = auth_database
    ret['s3'] = s3conn
    return ret


class APIBase(AttrAuthMixin, PromRequestMixin, RestHandler):
    """Default REST handler"""
    def initialize(self, *args, database, auth_database, s3=None, **kwargs):
        super().initialize(*args, **kwargs)
        self.db = database
        self.auth_db = auth_database
        self.s3 = s3

    def get_template_namespace(self):
        namespace = super().get_template_namespace()
        namespace['version'] = VERSION_STRING
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

    def write(self, chunk: str | bytes | dict | list) -> None:  # type: ignore[override]
        """Write dict or list to json"""
        if isinstance(chunk, (dict, list)):
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            super().write(json_encode(chunk))
        else:
            super().write(chunk)
