from dataclasses import dataclass
import importlib.resources
import json
from hashlib import shake_128
import logging
import time

from cachetools.func import ttl_cache
import jsonschema
import jwt
from rest_tools.utils.auth import OpenIDAuth


logger = logging.getLogger('credentials.util')


CONFIG_SCHEMA = json.loads((importlib.resources.files('iceprod.credentials')/'data'/'credentials.schema.json').read_text())


@ttl_cache(maxsize=256, ttl=3600)
def _get_auth(url):
    return OpenIDAuth(url)


def get_expiration(token):
    """
    Find a token's expiration time.

    Args:
        token (str): jwt token
    Returns:
        float: expiration unix time
    """
    return jwt.decode(token, options={"verify_signature": False})['exp']


def is_expired(cred):
    """
    Check if an OAuth credential is expired.

    Will mark credential as expired if the access token has less than 5 seconds left.

    Args:
        cred (dict): credential dict
    Returns:
        bool: True if expired
    """
    if cred['type'] != 'oauth':
        return False
    return cred['expiration'] < (time.time() + 5)


@dataclass
class Client:
    url: str
    client_id: str
    client_secret: str
    transfer_prefix: list[str]

    @property
    def id(self) -> str:
        data = self.url + self.client_id
        return shake_128(data.encode('utf-8')).hexdigest(16)

    @property
    def auth(self) -> OpenIDAuth:
        return _get_auth(self.url)


class ClientCreds:
    """
    Client Credentials

    Args:
        clients: json string - dict of url to :class:Client: entries
    """
    def __init__(self, clients: str):
        self._config = json.loads(clients)

    def fill_defaults(self):
        def _fill_dict(obj, schema):
            for prop in schema.get('properties',{}):
                schema_value = schema.get('properties',{})[prop]
                v = schema_value.get('default', None)
                if prop not in obj and v is not None:
                    obj[prop] = v
            for k in obj:
                schema_value = {}
                if k in schema.get('properties',{}):
                    schema_value = schema.get('properties',{})[k]
                elif ap := schema.get('additionalProperties',{}):
                    if isinstance(ap, dict):
                        schema_value = ap
                logging.debug('filling defaults for %s: %r', k, schema_value)
                try:
                    t = schema_value.get('type', 'str')
                    logging.debug('obj[k] type == %r, schema_value[type] == %r', type(obj[k]), t)
                    if isinstance(obj[k], dict) and t == 'object':
                        _fill_dict(obj[k], schema_value)
                    elif isinstance(obj[k], list) and t == 'array':
                        _fill_list(obj[k], schema_value)
                except KeyError:
                    logging.warning('error processing key %r with schema %r', k, schema_value)
                    raise

        def _fill_list(user, schema):
            for item in user:
                if isinstance(item, dict):
                    _fill_dict(item, schema['items'])

        _fill_dict(self._config, CONFIG_SCHEMA)

    def validate(self):
        """Validate config"""
        self.fill_defaults()
        logger.debug('validating config %r against schema %r', self._config, CONFIG_SCHEMA)
        jsonschema.validate(self._config, CONFIG_SCHEMA)

    def get_auth(self, url: str) -> OpenIDAuth:
        """Get an OpenIDAuth object, to verify tokens."""
        if url not in self._config:
            raise KeyError('unknown url')
        return _get_auth(url)

    def get_client(self, url: str) -> Client:
        logger.info('getting client for %s', url)
        c = self._config[url]
        return Client(
            url=url,
            client_id=c['client_id'],
            client_secret=c['client_secret'],
            transfer_prefix=c['transfer_prefix'],
        )

    def get_clients_by_prefix(self) -> dict[str, Client]:
        ret = {}
        for url in self._config:
            c = self.get_client(url)
            for prefix in c.transfer_prefix:
                ret[prefix] = c
        return ret
