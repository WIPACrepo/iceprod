"""
Help run class-based modules, including iceprod modules.

This is run in a subprocess to help set up the environment,
as well as contain any crashes.
"""

from __future__ import absolute_import, division, print_function

import os
import imp
import inspect

try:
    import cPickle as pickle
except:
    import pickle

import logging

from iceprod.core import constants
from iceprod.core.jsonUtil import json_decode

def get_args():
    """Read json of [args, kwargs] from the std args file"""
    with open(constants['args']) as f:
        data = f.read()
        logging.debug('get_args raw: %r',data)
        return json_decode(data)

def run(classname, filename=None, args=False, debug=False):
    logging.basicConfig(level=logging.DEBUG if debug else logging.WARNING)
    logging.warn('exe_helper(%s)', classname)

    if not classname:
        raise Exception('classname is missing')
    class_args = {'args':[],'kwargs':{}}
    if args:
        class_args = get_args()
        logging.info('args: %r', class_args)
    if filename:
        logging.info('try loading from source')
        mod = imp.load_source(classname, filename)
        class_obj = getattr(mod,classname)
    else:
        logging.info('try regular import')
        p,cl = classname.rsplit('.',1)
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
            pickle.dump(stats,open(constants['stats'],'w'))
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
    run(**args)

if __name__ == '__main__':
    main()
