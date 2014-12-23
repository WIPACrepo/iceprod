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

from iceprod.server import get_pkgdata_filename

def locateconfig():
    """Locate a config file"""
    hostname = os.uname()[1].split('.')[0]
    if 'I3PROD' in os.environ:
        cfgpath = os.path.expandvars('$I3PROD')
    elif 'I3PREFIX' in os.environ:
        cfgpath = os.path.expandvars('$I3PREFIX')
    else:
        cfgpath = get_pkgdata_filename('iceprod.server','data')
        if not cfgpath:
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
        self.messaging = True
        
        # start order
        self.start_order = ['messaging','config','db','proxy','website',
                            'schedule','queue']
        
        # messaging server url
        # TODO: better directory for this
        self.messaging_url = os.path.join('ipc://',os.getcwd(),'unix_socket.sock')
        
        # logging
        self.logging = {'logfile':'iceprod.log'}
    
    def read_file(self, filename):
        if not os.path.isfile(filename):
            raise Exception('%s is not a file'%filename)
        cp = configparser.RawConfigParser()
        read_files = cp.read(filename)
        if filename not in read_files:
            raise Exception('failed to read %s'%filename)
        
        for option_spec in self.CONFIG_FILE_OPTIONS:
            self.set_attr_from_config_option(cp, *option_spec)
        self.messaging_url = os.path.expandvars(self.messaging_url)
    
    def set_attr_from_config_option(self, cp, attr, where, type_=''):
        """Set an attribute on self if it exists in the ConfigParser."""
        section, option = where.split(":")
        if cp.has_option(section, option):
            method = getattr(cp, 'get'+type_)
            val = method(section, option)
            if '|' in attr:
                parts = [x for x in attr.split('|') if x]
                d = getattr(self,parts.pop(0))
                while len(parts) > 1:
                    d = d[parts.pop(0)]
                d[parts[-1]] = val
            else:
                setattr(self, attr, val)
    
    CONFIG_FILE_OPTIONS = [
        # [modules]
        ('db', 'modules:db', 'boolean'),
        ('proxy', 'modules:proxy', 'boolean'),
        ('queue', 'modules:queue', 'boolean'),
        ('schedule', 'modules:schedule', 'boolean'),
        ('website', 'modules:website', 'boolean'),
        ('config', 'modules:config', 'boolean'),
        ('messaging', 'modules:messaging', 'boolean'),
        
        # [messaging]
        ('messaging_url', 'messaging:messaging_url'),
        
        # [logging]
        ('logging|level', 'logging:level'),
        ('logging|format', 'logging:format'),
        ('logging|size', 'logging:size', 'int'),
        ('logging|num', 'logging:num', 'int'),
        ('logging|logfile', 'logging:logfile'),
    ]
