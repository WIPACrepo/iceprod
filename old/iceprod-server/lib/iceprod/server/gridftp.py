"""
  gridftp interface with tornado IOLoop hooks

  copyright (c) 2012 the icecube collaboration
"""

from functools import partial

from tornado.ioloop import IOLoop

from iceprod.core.gridftp import GridFTP

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
