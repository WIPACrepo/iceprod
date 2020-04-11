"""
Detailed configuration for IceProd
"""

from __future__ import absolute_import, division, print_function

import os
import logging

from iceprod.core.jsonUtil import json_encode, json_decode
from iceprod.server import GlobalID, get_pkgdata_filename

import json
try:
    from jsonschema import validate
    from jsonschema.exceptions import ValidationError
except ImportError:
    validate = None
    ValidationError = Exception

logger = logging.getLogger('config')


def locateconfig(filename):
    """Locate a config file"""
    cfgpaths = [os.path.expandvars('$I3PROD')]
    if os.getcwd() not in cfgpaths:
        cfgpaths.append(os.getcwd())
    cfgpath = get_pkgdata_filename('iceprod.server','data')
    if cfgpath:
        cfgpaths.append(cfgpath)
    for cfgpath in list(cfgpaths):
        # try for an etc directory
        i = cfgpaths.index(cfgpath)
        if os.path.isdir(os.path.join(cfgpath,'etc')):
            cfgpaths.insert(i,os.path.join(cfgpath,'etc'))
            # try for an iceprod directory
            if os.path.isdir(os.path.join(cfgpath,'etc','iceprod')):
                cfgpaths.insert(i,os.path.join(cfgpath,'etc','iceprod'))
    for cfgpath in cfgpaths:
        if os.path.isfile(os.path.join(cfgpath,filename)):
            return os.path.join(cfgpath,filename)
    raise Exception('config {} not found'.format(filename))

class IceProdConfig(dict):
    """
    IceProd configuration.

    The main iceprod configuration. Designed to be modified in-program,
    not worrying about hand-editing. Currently uses a json file as backing.

    Use just like a dictionary. Note that load() and save() are called
    automatically, but are available for manual calling.

    Note that this class is not thread-safe.

    :param filename: filename for config file (optional)
    :param defaults: use default values (optional: default True)
    :param validate: turn validation on/off (optional: default True)
    """
    def __init__(self, filename=None, defaults=True, validate=True, override=None):
        if filename:
            self.filename = filename
        else:
            basename = 'iceprod_config.json'
            try:
                self.filename = locateconfig(basename)
            except Exception:
                logger.warning('config file does not exist, so making a new one')
                if 'I3PROD' in os.environ:
                    prefix = os.path.join(os.environ['I3PROD'], 'etc')
                else:
                    prefix = os.getcwd()
                self.filename = os.path.join(prefix, basename)

        self.validate = validate
        self.loading = False

        # load user input, apply defaults, and save
        self.load()
        if defaults:
            self.defaults()
        self.save()

        if override:
            self.apply_overrides(override)
            logger.info('after overrides: %s',self)

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
                        val = json_decode(val)
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

    def defaults(self):
        """Set default values if unset."""
        try:
            self.loading = True
            filename = get_pkgdata_filename('iceprod.server',
                                            'data/etc/config_defaults.json')
            text = open(filename).read()
            obj = json_decode(text)
            def setter(new_obj,self_obj):
                logger.debug('setter()')
                orig_keys = self_obj.keys()
                for key in new_obj:
                    logger.debug('key = %s',key)
                    if key == '*':
                        for key2 in orig_keys:
                            logger.debug('key2=%s',key2)
                            if isinstance(self_obj[key2],dict):
                                setter(new_obj['*'],self_obj[key2])
                    elif key not in self_obj:
                        logger.debug('setting key')
                        self_obj[key] = new_obj[key]
                    elif isinstance(self_obj[key],dict):
                        setter(new_obj[key],self_obj[key])
                logger.debug('~setter()')
            logger.info('before defaults: %s',self)
            setter(obj,self)
            # special case for site_id
            if 'site_id' not in self:
                self['site_id'] = GlobalID.siteID_gen()
                logger.warning('Generating new site_id: %s',self['site_id'])
            logger.info('with defaults: %s',self)
        except Exception:
            logger.warning('failed to load from default config file %s',
                        filename, exc_info=True)
        finally:
            self.loading = False

    def do_validate(self):
        if validate and self.validate:
            try:
                filename = get_pkgdata_filename('iceprod.server', 'data/etc/iceprod_schema.json')
                schema = json.load(open(filename))
                validate(self, schema)
            except ValidationError as e:
                path = '.'.join(e.path)
                logger.warning('Validation error at "%s": %s' % (path, e.message))
                raise e
        else:
            logger.warning('skipping validation of config')

    def load(self):
        """Load config from file, overwriting current contents."""
        try:
            self.loading = True
            if os.path.exists(self.filename):
                text = open(self.filename).read()
                obj = json_decode(text)
                for key in obj:
                    self[key] = obj[key]
                self.do_validate()
        except ValidationError:
            raise
        except Exception:
            logger.warning('failed to load from config file %s',self.filename,
                        exc_info=True)
        finally:
            self.loading = False

    def load_string(self, text):
        """Load a config from a string, saving to file."""
        try:
            self.loading = True
            obj = json_decode(text)
            for key in obj:
                self[key] = obj[key]
            self.do_validate()
        finally:
            self.loading = False
            self.save()

    def save_to_string(self):
        return json_encode(self, indent = 4)

    def save(self):
        """Save config from file."""
        if not self.loading:
            try:
                text = json_encode(self, indent = 4)
                # save securely
                with os.fdopen(os.open(self.filename+'.tmp', os.O_WRONLY | os.O_CREAT, 0o600),'w') as f:
                    f.write(text)
                os.rename(self.filename+'.tmp',self.filename)
            except Exception:
                logger.warning('failed to save to config file %s',self.filename,
                            exc_info=True)

    # insert save function into dict methods
    def __setitem__(self, key, value):
        super(IceProdConfig,self).__setitem__(key, value)
        self.save()
    def __delitem__(self, key):
        super(IceProdConfig,self).__delitem__(key)
        self.save()
