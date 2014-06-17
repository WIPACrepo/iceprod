"""
Basic config file support for IceProd.
"""

from __future__ import absolute_import, division, print_function

import os

# In py3, ConfigParser was renamed to the more-standard configparser
try:
    import configparser
except ImportError:
    import ConfigParser as configparser

def locateconfig():
    """Locate a config file"""
    hostname = os.uname()[1].split('.')[0]
    if 'I3PROD' in os.environ:
        cfgpath = os.path.expandvars('$I3PROD')
    elif 'I3PREFIX' in os.environ:
        cfgpath = os.path.expandvars('$I3PREFIX')
    else:
        cfgpath = os.getcwd()
    # try for an etc directory
    if os.path.isdir(os.path.join(cfgpath,'etc')):
        cfgpath = os.path.join(cfgpath,'etc')
        # try for an iceprod directory
        if os.path.isdir(os.path.join(cfgpath,'iceprod')):
            cfgpath = os.path.join(cfgpath,'iceprod')
    # try common file names
    if os.path.isfile(os.path.join(cfgpath,'iceprod.cfg')):
        cfgpath = os.path.join(cfgpath,'iceprod.cfg')
    elif os.path.isfile(os.path.join(cfgpath,hostname+'.cfg')):
        cfgpath = os.path.join(cfgpath,hostname+'.cfg')
    else:
        raise Exception('cfgpath is not a valid path')
    return cfgpath

class BasicConfig(object):
    """
    IceProd basic configuration.
    
    Settings for basic server daemon startup and connectivity.
    """
    def __init__(self):
        # modules to start
        self.db = True
        self.proxy = False
        self.queue = True
        self.schedule = True
        self.website = True
        self.config = True
        
        # start order
        self.start_order = ['config','db','proxy','website','schedule','queue']
        
        # messaging server url
        self.messaging_url = os.path.join('ipc://',os.getcwd(),'unix_socket.sock')
    
    def read_file(self, filename):
        if not os.path.isfile(filename):
            raise Exception('%s is not a file'%filename)
        cp = configparser.RawConfigParser()
        read_files = cp.read(filename)
        if filename not in read_files:
            raise Exception('failed to read %s'%filename)
        
        for option_spec in self.CONFIG_FILE_OPTIONS:
            self.set_attr_from_config_option(cp, *option_spec)
    
    def set_attr_from_config_option(self, cp, attr, where, type_=''):
        """Set an attribute on self if it exists in the ConfigParser."""
        section, option = where.split(":")
        if cp.has_option(section, option):
            method = getattr(cp, 'get'+type_)
            setattr(self, attr, method(section, option))
    
    CONFIG_FILE_OPTIONS = [
        # [modules]
        ('db', 'modules:db', 'boolean'),
        ('proxy', 'modules:proxy', 'boolean'),
        ('queue', 'modules:queue', 'boolean'),
        ('schedule', 'modules:schedule', 'boolean'),
        ('website', 'modules:website', 'boolean'),
        ('config', 'modules:config', 'boolean'),
        
        # [messaging]
        ('messaging_url', 'messaging:url')
    ]
