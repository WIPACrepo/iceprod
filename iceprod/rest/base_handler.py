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

#: max ids in a single `|`-separated GET list param (same limit as MAX_DATASET_IDS)
MAX_IDS = 100


def IceProdRestConfig(config: dict[str, Any], database: DB | None = None, auth_database: AsyncDatabase | None = None, s3conn=None):
    if config:
        config['server_header'] = 'IceProd/' + VERSION_STRING
    ret = RestHandlerSetup(config)
    ret['database'] = database
    ret['s3'] = s3conn
    return ret


class APIBase(AttrAuthMixin, PromRequestMixin, RestHandler):
    """Default REST handler"""
    def initialize(self, *args, database: DB, db_client: AsyncMongoClient | None = None, s3=None, **kwargs):  # ty: ignore[incompatible-method-override]
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

    def get_int_range_filter(self, name: str) -> int | dict[str, int] | None:
        """
        Parse an integer filter from the `name`, `name_min`, and `name_max` args.

        `name` is an exact match. `name_min` and `name_max` are inclusive bounds,
        and cannot be combined with `name`.

        Args:
            name: base argument name (e.g. "job_index")

        Returns:
            an int (exact match), a Mongo `{'$gte':..., '$lte':...}` dict, or None if unset

        Raises:
            HTTPError(400): on a non-integer value or exact + range conflict
        """
        def _parse(arg: str) -> int | None:
            val = self.get_argument(arg, None)
            if val is None or val == '':
                return None
            try:
                return int(val)
            except ValueError:
                raise HTTPError(400, reason=f'Bad argument "{arg}": must be integer')

        exact = _parse(name)
        range_filter: dict[str, int] = {}
        if (low := _parse(f'{name}_min')) is not None:
            range_filter['$gte'] = low
        if (high := _parse(f'{name}_max')) is not None:
            range_filter['$lte'] = high

        if exact is not None:
            if range_filter:
                raise HTTPError(400, reason=f'Cannot combine "{name}" with "{name}_min"/"{name}_max"')
            return exact
        return range_filter or None

    def get_id_list(self, name: str, max_len: int = MAX_IDS) -> list[str] | None:
        """
        Parse a `|`-separated list of ids from a GET argument.

        Empty entries (from stray `|` characters) are dropped.

        Args:
            name: argument name (e.g. "task_ids")
            max_len: maximum number of ids allowed

        Returns:
            list of ids, or None if the argument is unset or empty

        Raises:
            HTTPError(400): if more than `max_len` ids are given
        """
        val = self.get_argument(name, '')
        ids = [x for x in val.split('|') if x]
        if not ids:
            return None
        if len(ids) > max_len:
            raise HTTPError(400, reason=f'Too many "{name}": max is {max_len}')
        return ids

    def write(self, chunk: str | bytes | dict | list) -> None:  # type: ignore[override]
        """Write dict or list to json"""
        if isinstance(chunk, (dict, list)):
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            super().write(json_encode(chunk))
        else:
            super().write(chunk)
