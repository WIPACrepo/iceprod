"""
Classes supporting dataset serialization.

The interface is as follows::

    class Serialization:
        def dump(config, filename, **kwargs):
            return None
        def dumps(config, **kwargs):
            return string
        def load(filename, **kwargs):
            return config
        def loads(string, **kwargs):
            return config

The kwargs for each function are optional keyword arguments to pass
to the underlying serialization library. Each function is a static method
and can be called like::

    Serialization.dump(config,filename)
"""

import os
try:
    from io import StringIO
except ImportError:
    import StringIO
import logging

from iceprod.core import dataclasses

logger = logging.getLogger('serialization')

class SerializationError(Exception):
    """An exception that occurs during serialization."""
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        if self.value:
            return 'SerializationError(%r)'%(self.value)
        else:
            return 'SerializationError()'
    def __reduce__(self):
        return (SerializationError,(self.value,))

def dict_to_dataclasses(input_dict):
    """
    Convert a dictionary to dataclasses objects.
    
    :param input_dict: input dictionary
    :returns: :class:`iceprod.core.dataclasses.Job`
    """
    ret = dataclasses.Job(input_dict)
    ret.convert()
    return ret

import json as _json
class serialize_json(object):
    """
    Serialize a dataset config to json.
    """
    @staticmethod
    def dump(obj, filename, **kwargs):
        return _json.dump(obj, open(filename,'w'), **kwargs)
    @staticmethod
    def dumps(obj, **kwargs):
        return _json.dumps(obj, **kwargs)
    @staticmethod
    def load(filename, **kwargs):
        return dict_to_dataclasses(_json.load(open(filename), **kwargs))
    @staticmethod
    def loads(obj, **kwargs):
        return dict_to_dataclasses(_json.loads(obj, **kwargs))
            