"""
Help run class-based modules, including iceprod modules.

This is run in a subprocess to help set up the environment,
as well as contain any crashes.

Note that this file should be backward-compatible with python 2.7+,
as it will be run under the user's environment.
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import imp
import inspect
import logging
import importlib
from collections import Iterable

try:
    import cPickle as pickle
except Exception:
    import pickle

from json import loads as json_decode

# from iceprod.core import constants
constants_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'__init__.py')
try:
    spec = importlib.util.spec_from_file_location('constants', constants_path)
    constants_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(constants_mod)
except AttributeError:
    import imp
    constants_mod = imp.load_source('constants',constants_path)
constants = constants_mod.constants

try:
    String = basestring
except NameError:
    String = str

def get_args():
    """Read json of [args, kwargs] from the std args file"""
    with open(constants['args']) as f:
        data = f.read()
        logging.debug('get_args raw: %r',data)
        return json_decode(data)

def unicode_to_ascii(obj):
    if isinstance(obj,String):
        return str(obj)
    elif isinstance(obj,dict):
        ret = {}
        for k in obj:
            ret[unicode_to_ascii(k)] = unicode_to_ascii(obj[k])
        return ret
    elif isinstance(obj,set):
        return set(unicode_to_ascii(k) for k in obj)
    elif isinstance(obj,Iterable):
        return [unicode_to_ascii(k) for k in obj]
    else:
        return obj

def run(classname, filename=None, args=False, debug=False):
    logging.basicConfig(level=logging.DEBUG if debug else logging.WARN)
    logging.warning('exe_helper(%s)', classname)

    if not classname:
        raise Exception('classname is missing')
    class_args = {'args':[],'kwargs':{}}
    if args:
        class_args = get_args()
        logging.info('args: %r', class_args)
    class_args = unicode_to_ascii(class_args)
    parts = classname.rsplit('.',1)
    if len(parts) == 1:
        p,cl = os.path.basename(filename),parts[0]
    else:
        p,cl = parts
    if filename:
        logging.info('try loading from source: %s', filename)
        mod = imp.load_source(p, filename)
        class_obj = getattr(mod,cl)
    else:
        logging.info('try regular import: %s.%s', p, cl)
        mod = __import__(p,globals(),locals(),[cl])
        class_obj = getattr(mod,cl)

    if (inspect.isclass(class_obj) and
        any(True for c in inspect.getmro(class_obj) if c.__name__ == 'IPBaseClass')):
        logging.info('IceProd v1 class')
        instance = class_obj()
        for k in class_args['kwargs']:
            instance.SetParameter(k,class_args['kwargs'][k])
        stats = {}
        ret = instance.Execute(stats)
        if stats:
            pickle.dump(stats,open(constants['stats'],'wb'))
        if ret:
            raise Exception('Execute() returned %r'%ret)
    else:
        logging.info('regular callable')
        class_obj(*class_args['args'],**class_args['kwargs'])

def main():
    import argparse
    parser = argparse.ArgumentParser(description='IceProd Core')
    parser.add_argument('-c','--classname', type=str, default=None,
                        help='Specify class name')
    parser.add_argument('-f','--filename', type=str, default=None,
                        help='Specify file to find the class in')
    parser.add_argument('-a','--args', action='store_true', default=False,
                        help='Enable arg file detection')
    parser.add_argument('-d','--debug', action='store_true', default=False,
                        help='Enable debug actions and logging')
    args = vars(parser.parse_args())
    try:
        run(**args)
    except Exception as e:
        with open(constants['task_exception'],'wb') as f:
            pickle.dump(e,f)
        raise

if __name__ == '__main__':
    main()
