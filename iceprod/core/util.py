"""
Utilities for IceProd functionality.
"""

from __future__ import absolute_import, division, print_function


class NoncriticalError(Exception):
    """An exception that can be logged and then ignored."""
    def __init__(self, value=''):
        self.value = value

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        if self.value:
            return 'NoncriticalError(%r)'%(self.value)
        else:
            return 'NoncriticalError()'

    def __reduce__(self):
        return (NoncriticalError,(self.value,))
