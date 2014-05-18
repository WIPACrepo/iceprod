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
import StringIO
import logging

from iceprod.core import dataclasses

logger = logging.getLogger('serialization')

class SerializationError(Exception):
    """An exception that occurs during serialization."""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return 'SerializationError(%r)'%(self.value)
    def __reduce__(self):
        return (SerializationError,(self.value,))

def dict_to_dataclasses(input):
    """
    Convert a dictionary to dataclasses objects.
    
    :param input: input dictionary
    :returns: :class:`iceprod.core.dataclasses.Job`
    """
    ret = dataclasses.Job(input)
    ret.convert()
    return ret


try:
    import json as _json
except ImportError:
    logger.info('json serializer is unavailable')
else:
    class serialize_json(object):
        """
        Serialize a dataset config to json.
        """
        def dump(self, obj, filename, **kwargs):
            return _json.dump(obj, open(filename,'w'), **kwargs)
        def dumps(self, obj, **kwargs):
            return _json.dumps(obj, **kwargs)
        def load(self, filename, **kwargs):
            return dict_to_dataclasses(_json.load(open(filename), **kwargs))
        def loads(self, str, **kwargs):
            return dict_to_dataclasses(_json.loads(str, **kwargs))
            