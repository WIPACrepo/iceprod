"""
Some general functions used by the iceprod server
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import logging
from pkgutil import get_loader
import importlib


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


class GlobalID(object):
    """Global ID configuration and generation"""
    import string
    # never change these settings, otherwise all old ids will fail
    CHARS = string.ascii_letters+string.digits
    CHARS_LEN = len(CHARS)
    # define dict to make reverse lookup super fast
    INTS_DICT = {c:i for i,c in enumerate(CHARS)}
    IDLEN = 15
    MAXSITEID = 10**10
    MAXLOCALID = 10**15

    @classmethod
    def int2char(cls,i):
        if not isinstance(i,int) or i < 0: # only deal with positive ints
            logging.warning('bad input to int2char: %r',i)
            raise Exception('bad input to int2char')
        out = ''
        while i >= 0:
            out += cls.CHARS[i%cls.CHARS_LEN]
            i = i//cls.CHARS_LEN - 1
        return out[::-1]

    @classmethod
    def char2int(cls,c):
        if not isinstance(c,str) or len(c) < 1: # only deal with string
            logging.warning('bad input to char2int: %r',c)
            raise Exception('bad input to char2int')
        out = -1
        for i,cc in enumerate(reversed(c)):
            if cc not in cls.CHARS:
                raise Exception('non-char input to chars2int')
            out += (cls.INTS_DICT[cc]+1)*(cls.CHARS_LEN**i)
        return out

    @classmethod
    def siteID_gen(cls):
        """Generate a new site id"""
        import random
        return cls.int2char(random.randint(0,cls.MAXSITEID-1))

    @classmethod
    def globalID_gen(cls,id,site_id):
        """Generate a new global id given a local id and site id"""
        if isinstance(id,str):
            id = cls.char2int(id)
        elif not isinstance(id,int):
            raise Exception('id is not a string, int, or long')
        if isinstance(site_id,str):
            return cls.int2char(cls.char2int(site_id)*cls.MAXLOCALID+id)
        elif isinstance(site_id,int):
            return cls.int2char(site_id*cls.MAXLOCALID+id)
        else:
            raise Exception('Site id is not a string, int, or long')

    @classmethod
    def localID_ret(cls,id,type='str'):
        """Retrieve a local id from a global id"""
        ret = cls.char2int(id) % cls.MAXLOCALID
        if type == 'str':
            ret = cls.int2char(ret)
        return ret

    @classmethod
    def siteID_ret(cls,id,type='str'):
        """Retrieve a site id from a global id"""
        ret = cls.char2int(id) // cls.MAXLOCALID
        if type == 'str':
            ret = cls.int2char(ret)
        return ret

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
            if not s in self._cfg:
                logger.warning('%s is not a valid arg',s)
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


def get_pkgdata_filename(package, resource):
    """Get a filename for a resource bundled within the package"""
    loader = get_loader(package)
    if loader is None or not hasattr(loader, 'get_data'):
        return None
    mod = sys.modules.get(package) or loader.load_module(package)
    if mod is None or not hasattr(mod, '__file__'):
        return None

    # Modify the resource name to be compatible with the loader.get_data
    # signature - an os.path format "filename" starting with the dirname of
    # the package's __file__
    parts = resource.split('/')
    parts.insert(0, os.path.dirname(mod.__file__))
    return os.path.join(*parts)

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
        return subprocess.check_output(["which",binary]).strip('\n')
    except Exception:
        pass
    return None
