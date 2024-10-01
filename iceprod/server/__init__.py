"""
Some general functions used by the iceprod server
"""

import os
import sys
import logging
from pkgutil import get_loader
import importlib
import subprocess


def find_module_recursive(name, path=None):
    """ Recursively search for submodule. Submodules must be separated with a '.' """
    import imp
    res = None
    for x in name.split('.'):
        res = imp.find_module(x, path)
        path = [res[1]]
    return res


def listmodules(package_name=''):
    """List modules in a package or directory"""
    file, pathname, description = find_module_recursive(package_name)
    if file:
        # Not a package
        return []
    ret = []
    for module in os.listdir(pathname):
        if module.endswith('.py') and module != '__init__.py':
            tmp = os.path.splitext(module)[0]
            ret.append(package_name+'.'+tmp)
    return ret


def run_module(name,*args,**kwargs):
    """Import and start the module"""
    class_name = name.rsplit('.',1)[1]
    x = importlib.import_module(name)
    return (getattr(x,class_name))(*args,**kwargs)


def salt(length=2):
    """Returns a string of random letters"""
    import string
    import random
    letters = string.ascii_letters+string.digits
    return ''.join([random.SystemRandom().choice(letters) for _ in range(length)])


class KwargConfig(object):
    """A way to validate kwargs passed in to a class"""
    def __init__(self):
        # defaults
        self._cfg = {}
        self._cfg_types = {}

    def validate(self,kwargs):
        # setup cfg variables
        for s in kwargs.keys():
            v = kwargs[s]
            if not isinstance(s,str):
                raise Exception('parameter name %s is not a string'%(str(s)))
            if s not in self._cfg:
                logging.warning('%s is not a valid arg',s)
                continue
            t = self._cfg_types[s]
            if t in ('str','file','dir'):
                if not isinstance(v,str):
                    raise Exception('%s is not a string'%(str(s)))
                if t in ('file','dir'):
                    v = os.path.expanduser(os.path.expandvars(v))
                    if t == 'file' and not ('_file' in s or '_log' in s):
                        try:
                            os.path.exists(v)
                        except Exception:
                            raise Exception('parameter %s with filepath %s does not exist'%(s,v))
            elif t == 'int':
                if not isinstance(v,int):
                    raise Exception('%s is not an int'%(str(s)))
            elif t == 'float':
                if not isinstance(v,float):
                    raise Exception('%s is not a float'%(str(s)))
            else:
                raise Exception('%s has an unknown type'%(str(s)))
            self._cfg[s] = v

        # make directories
        for c in self._cfg_types:
            if self._cfg_types[c] == 'file':
                d = os.path.dirname(self._cfg[c])
                if not os.path.isdir(d):
                    os.makedirs(d)
            if self._cfg_types[c] == 'dir':
                d = self._cfg[c]
                if not os.path.isdir(d):
                    os.makedirs(d)


def get_pkg_binary(package, binary):
    """Try finding the binary path based on the python package"""
    try:
        loader = get_loader(package)
        f = loader.get_filename()
        while f and 'lib' in f:
            f = os.path.dirname(f)
        filepath = os.path.join(f,'bin',binary)
        if os.path.exists(filepath):
            return filepath
        filepath = os.path.join(f,'sbin',binary)
        if os.path.exists(filepath):
            return filepath
    except Exception:
        pass

    # try going up from sys.argv[0]
    try:
        f = os.path.abspath(sys.argv[0])
        while f and 'iceprod' in f:
            filepath = os.path.join(f,'bin',binary)
            if os.path.exists(filepath):
                return filepath
            f = os.path.dirname(f)
        filepath = os.path.join(f,'bin',binary)
        if os.path.exists(filepath):
            return filepath
    except Exception:
        pass

    # try just asking the shell
    try:
        filepath = subprocess.check_output(["which",binary]).decode('utf-8').strip('\n')
        if os.path.exists(filepath):
            return filepath
    except Exception:
        pass
    return None
