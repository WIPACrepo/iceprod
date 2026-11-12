import logging
from typing import Any

from rest_tools.server import RestHandler, RestHandlerSetup
from tornado.escape import json_encode
from tornado.web import HTTPError

from iceprod.common.prom_utils import PromRequestMixin
from iceprod.util import VERSION_STRING

from ..common.mongo import AsyncDatabase, AsyncMongoClient
from .auth import AttrAuthMixin

logger = logging.getLogger('rest')

type DB = AsyncMongoClient | AsyncDatabase

#: max number of datasets allowed in a single multi-dataset request
MAX_DATASET_IDS = 100


def IceProdRestConfig(config: dict[str, Any], database: DB | None = None, auth_database: AsyncDatabase | None = None, s3conn=None):
    if config:
        config['server_header'] = 'IceProd/' + VERSION_STRING
    ret = RestHandlerSetup(config)
    ret['database'] = database
    ret['s3'] = s3conn
    return ret


class APIBase(AttrAuthMixin, PromRequestMixin, RestHandler):
    """Default REST handler"""
    def initialize(self, *args, database: DB, db_client: AsyncMongoClient | None = None, s3=None, **kwargs):  # type: ignore[override]
        logger.debug('initialze APIBase: args=%r, kwargs=%r', args, kwargs)
        super().initialize(*args, **kwargs)
        logger.debug('do rest of initialize APIBase')
        self.db = database
        self.db_client = db_client
        self.auth_db: AsyncDatabase | None = db_client['auth'] if db_client else None  # type: ignore
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

    def prepare(self):
        super().prepare()
        # Set the Cache-Control header to "no-store"
        self.set_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        # Also set Pragma and Expires headers for compatibility with older HTTP versions
        self.set_header('Pragma', 'no-cache')
        self.set_header('Expires', '0')
        # Set Content Security Policy to deny all
        self.set_header('Content-Security-Policy', "default-src 'none'; frame-ancestors 'none';")

    async def get_readable_dataset_ids(self) -> list[str]:
        """
        Parse and authorize the `dataset_ids` argument for multi-dataset requests.

        Returns:
            list: dataset ids

        Raises:
            HTTPError 400: missing or too many dataset ids
            HTTPError 403: a dataset is not readable by the requester
        """
        dataset_ids = [d for d in self.get_argument('dataset_ids', '').split('|') if d]
        if not dataset_ids:
            raise HTTPError(400, reason='Missing dataset_ids')
        if len(dataset_ids) > MAX_DATASET_IDS:
            raise HTTPError(400, reason=f'Too many dataset_ids (max {MAX_DATASET_IDS})')
        for d in dataset_ids:
            if not await self.manual_attr_auth('dataset_id', d, 'read'):
                raise HTTPError(403, reason='Unauthorized dataset_id')
        return dataset_ids

    def write(self, chunk: str | bytes | dict | list) -> None:  # type: ignore[override]
        """Write dict or list to json"""
        if isinstance(chunk, (dict, list)):
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            super().write(json_encode(chunk))
        else:
            super().write(chunk)
