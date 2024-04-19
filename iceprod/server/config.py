"""
Detailed configuration for IceProd
"""

import importlib.resources
import json
import logging
import os
from pathlib import Path

import jsonschema

logger = logging.getLogger('config')


CONFIG_SCHEMA = json.loads((importlib.resources.files('iceprod.server')/'data'/'config.schema.json').read_text())


class IceProdConfig(dict):
    """
    IceProd configuration.

    The main iceprod configuration. Designed to be modified in-program,
    not worrying about hand-editing. Currently uses a json file as backing.

    Use just like a dictionary. Note that load() and save() are called
    automatically, but are available for manual calling.

    Note that this class is not thread-safe.

    Args:
        filename: filename for config file (optional)
        defaults: use default values (optional: default True)
        validate: turn validation on/off (optional: default True)
        override: override list of key=value strings
        save: enable saving to file (optional: default True)
    """
    def __init__(self, filename=None, defaults=True, validate=True, override=None, save=True):
        if filename:
            self.filename = filename
        else:
            self.filename = os.path.expandvars('$I3PROD/etc/iceprod_config.json')

        if save and not Path(self.filename).parent.is_dir():
            raise RuntimeError('$I3PROD/etc does not exist')

        self.validate = validate
        self._save = save
        self.loading = False

        # load user input, apply defaults, and save
        self.load()
        if defaults:
            self.fill_defaults()
        self.do_validate()
        self.save()

        if override:
            self.apply_overrides(override)
            logger.info('after overrides: %s',self)

        self.do_validate()

    def fill_defaults(self):
        def _load_ref(schema_value):
            if '$ref' in list(schema_value.keys()):
                # load from ref
                parts = schema_value['$ref'].split('/')[1:]
                schema_value = CONFIG_SCHEMA
                while parts:
                    schema_value = schema_value.get(parts.pop(0), {})
                logging.debug('loading from ref: %r', schema_value)
            return schema_value

        def _fill_dict(user, schema):
            for prop in schema['properties']:
                schema_value = _load_ref(schema['properties'][prop])
                v = schema_value.get('default', None)
                if prop not in user and v is not None:
                    if isinstance(v, (dict, list)):
                        v = v.copy()
                    user[prop] = v
            for k in user:
                schema_value = _load_ref(schema['properties'].get(k, {}))
                logging.debug('filling defaults for %s: %r', k, schema_value)
                try:
                    t = schema_value.get('type', 'str')
                    logging.debug('user[k] type == %r, schema_value[type] == %r', type(user[k]), t)
                    if isinstance(user[k], dict) and t == 'object':
                        _fill_dict(user[k], schema_value)
                    elif isinstance(user[k], list) and t == 'array':
                        _fill_list(user[k], schema_value)
                except KeyError:
                    logging.warning('error processing key %r with schema %r', k, schema_value)
                    raise

        def _fill_list(user, schema):
            for item in user:
                if isinstance(item, dict):
                    _fill_dict(item, schema['items'])

        _fill_dict(self, CONFIG_SCHEMA)

    def apply_overrides(self, overrides):
        for item in overrides:
            key,val = item.split('=',1)

            # try decoding value
            if val == 'true':
                val = True
            elif val == 'false':
                val = False
            elif val.isdigit():
                val = int(val)
            else:
                try:
                    val = float(val)
                except ValueError:
                    try:
                        val = json.loads(val)
                    except Exception:
                        pass

            # put value at right key
            key = key.split('.')
            logging.debug(f'setting {key}={val}')
            obj = self
            while len(key) > 1:
                if key[0] not in obj:
                    obj[key[0]] = {}
                obj = obj[key[0]]
                key = key[1:]
            obj[key[0]] = val

    def do_validate(self):
        if self.validate:
            jsonschema.validate(self, CONFIG_SCHEMA)
        else:
            logger.warning('skipping validation of config')

    def load(self):
        """Load config from file, overwriting current contents."""
        try:
            self.loading = True
            if os.path.exists(self.filename):
                with open(self.filename) as f:
                    obj = json.load(f)
                    for key in obj:
                        self[key] = obj[key]
        except Exception:
            logger.warning('failed to load from config file %s', self.filename,
                           exc_info=True)
            raise
        finally:
            self.loading = False

    def load_string(self, text):
        """Load a config from a string, saving to file."""
        try:
            self.loading = True
            obj = json.loads(text)
            for key in obj:
                self[key] = obj[key]
            self.do_validate()
        finally:
            self.loading = False
            self.save()

    def save_to_string(self):
        return json.dumps(self, indent=4)

    def save(self):
        """Save config from file."""
        if self._save and not self.loading:
            try:
                # save securely
                with os.fdopen(os.open(self.filename+'.tmp', os.O_WRONLY | os.O_CREAT, 0o600), 'w') as f:
                    json.dump(self, f, indent=4)
                os.rename(self.filename+'.tmp', self.filename)
            except Exception:
                logger.warning('failed to save to config file %s', self.filename,
                               exc_info=True)

    # insert save function into dict methods
    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.save()

    def __delitem__(self, key):
        super().__delitem__(key)
        self.save()
