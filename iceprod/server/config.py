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

class IceProdConfig(dict):
    """
    IceProd configuration.
    
    The main iceprod configuration. Designed to be modified in-program,
    not worrying about hand-editing. Currently uses a json file as backing.
    
    Use just like a dictionary. Note that load() and save() are called
    automatically, but are available for manual calling.
    
    Note that this class is not thread-safe.
    
    :param filename: filename for config file (optional)
    """
    def __init__(self, filename=None):
        if filename:
            self.filename = filename
        else:
            self.filename = os.path.join(os.getcwd(),'iceprod_config.json')
        self.loading = False
        
        # load user input, apply defaults, and save
        self.load()
        self.defaults()
        self.save()
    
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
                logger.warn('Generating new site_id: %s',self['site_id'])
            logger.info('with defaults: %s',self)
        except Exception:
            logger.warn('failed to load from default config file %s',
                        self.filename, exc_info=True)
        finally:
            self.loading = False
    
    def load(self):
        """Load config from file, overwriting current contents."""
        try:
            self.loading = True
            if os.path.exists(self.filename):
                text = open(self.filename).read()
                obj = json_decode(text)
                
                filename = get_pkgdata_filename('iceprod.server',
                                                'data/etc/iceprod_schema.json')
                schema = json.load(open(filename))

                try:
                    if validate:
                        validate(obj, schema)
                    else:
                        logger.warn('skipping validation of config')
                except ValidationError as e:
                    path = '.'.join(e.path)
                    logger.warn('Validation error at "%s": %s' % (path, e.message))
                    raise e
                
                for key in obj:
                    self[key] = obj[key]
        except Exception:
            logger.warn('failed to load from config file %s',self.filename,
                        exc_info=True)
        finally:
            self.loading = False
    
    def save(self):
        """Save config from file."""
        if not self.loading:
            try:
                text = json_encode(self)
                with open(self.filename,'w') as f:
                    f.write(text)
            except Exception:
                logger.warn('failed to save to config file %s',self.filename,
                            exc_info=True)
    
    # insert save function into dict methods
    def __setitem__(self, key, value):
        super(IceProdConfig,self).__setitem__(key, value)
        self.save()
    def __delitem__(self, key):
        super(IceProdConfig,self).__delitem__(key)
        self.save()
