"""
  gridftp interface with tornado IOLoop hooks

  copyright (c) 2012 the icecube collaboration
"""

import os
from functools import partial
import subprocess
import logging

from tornado.ioloop import IOLoop

from iceprod.core.gridftp import GridFTP
from iceprod.server.config import IceProdConfig

logger = logging.getLogger('gridftp')

class _MetaGridFTPTornado(type):
    """Wrapper around the GridFTP class"""

    _fsfuncs = [x for x in dir(GridFTP) if callable(getattr(GridFTP,x)) and not isinstance(getattr(GridFTP,x),type)]
    _fsconst = [x for x in dir(GridFTP) if x[:2] != '__' and not callable(getattr(GridFTP,x))]

    @classmethod
    def __getattr__(cls,name):
        if name in cls._fsfuncs:
            return partial(cls.f,name)
        elif name in cls._fsconst:
            return getattr(GridFTP,name)
        #elif name == 'FSEvent':
        #    return cls.FSEvent
        else:
            raise Exception('%s is not a GridFTP object'%name)

    @classmethod
    def f(cls,name,*args,**kwargs):
        ret = None
        if 'callback' in kwargs and kwargs['callback'] is not None:
            callback = kwargs.pop('callback')
            def cb(*args,**kwargs):
                IOLoop.instance().add_callback(partial(callback,*args,**kwargs))
            kwargs['callback'] = cb
            if 'streaming_callback' in kwargs and kwargs['streaming_callback'] is not None:
                streaming_callback = kwargs.pop('streaming_callback')
                def scb(*args,**kwargs):
                    IOLoop.instance().add_callback(partial(streaming_callback,*args,**kwargs))
                kwargs['streaming_callback'] = scb
        return getattr(GridFTP,name)(*args,**kwargs)

class GridFTPTornado(object):
    __metaclass__ = _MetaGridFTPTornado
    def __getattr__(self,name):
        return getattr(GridFTPTornado,name)

class SiteGlobusProxy(object):
    """
    Manage site-wide globus proxy

    :param cfgfile: cfgfile location (optional)
    :param duration: proxy duration (optional, default 72 hours)
    """
    def __init__(self,cfgfile=None,duration=None):
        if not cfgfile:
            cfgfile = os.path.join(os.getcwd(),'globus_proxy.json')
        self.cfg = IceProdConfig(filename=cfgfile, defaults=False,
                                 validate=False)
        if duration:
            self.cfg['duration'] = duration
        elif 'duration' not in self.cfg:
            self.cfg['duration'] = 72

    def set_passphrase(self, p):
        """Set the passphrase"""
        self.cfg['passphrase'] = p

    def set_duration(self, d):
        """Set the duration"""
        self.cfg['duration'] = d

    def update_proxy(self):
        """Update the proxy"""
        if 'passphrase' not in self.cfg:
            raise Exception('passphrase missing')
        if 'duration' not in self.cfg:
            raise Exception('duration missing')
        FNULL = open(os.devnull, 'w')
        logger.info('duration: %r',self.cfg['duration'])
        if subprocess.call(['grid-proxy-info','-e',
                            '-valid','%d:0'%self.cfg['duration'],
                           ], stdout=FNULL, stderr=FNULL):
            # proxy needs updating
            p = subprocess.Popen(['grid-proxy-init','-pwstdin',
                                  '-valid','%d:0'%(self.cfg['duration']+1),
                                 ], stdin=subprocess.PIPE)
            p.communicate(input=self.cfg['passphrase']+'\n')
            p.wait()
            if p.returncode > 0:
                raise Exception('grid-proxy-init failed')

    def get_proxy(self):
        """Get the proxy location"""
        FNULL = open(os.devnull, 'w')
        return subprocess.check_output(['grid-proxy-info','-path'],
                                       stderr=FNULL).strip()
