"""
Detailed configuration for IceProd
"""

from __future__ import absolute_import, division, print_function

import os
import logging

from iceprod.core.jsonUtil import json_encode, json_decode

logger = logging.getLogger('config')

class IceProdConfig(dict):
    """
    IceProd configuration.
    
    The main iceprod configuration. Designed to be modified in-program,
    not worrying about hand-editing. Currently uses a json file as backing.
    
    Use just like a dictionary. Note that load() and save() are called
    automatically, but are available for manual calling.
    
    Note that this class is not thread-safe.
    """
    def __init__(self):
        self.filename = os.path.join(os.getcwd(),'iceprod_config.json')
        self.loading = False
        self.load()
    
    def load(self):
        """Load config from file, overwriting current contents."""
        try:
            self.loading = True
            text = open(self.filename).read()
            obj = json_decode(text)
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
