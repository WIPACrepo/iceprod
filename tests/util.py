from __future__ import absolute_import, division, print_function

import os
import sys
import logging
import fnmatch
import unittest

from functools import wraps

def printer(input,passed=True,skipped=False):
    numcols = 60
    padding = 3 if skipped else 4
    while len(input) > numcols:
        # wrap longer strings
        pos = input.rfind(' ',0,numcols)
        if pos < 0:
            break
        tmp_str = input[0:pos]
        input = '     '+input[pos+1:]
        print(tmp_str)
    # print string aligned left, and passed or failed
    final_str = input
    for i in xrange(len(input),numcols+padding):
        final_str += ' '

    if skipped:
        logging.error(final_str+'skipped')
        final_str += '\033[36m'+'skipped'+'\033[0m'
    elif passed:
        logging.error(final_str+'passed')
        final_str += '\033[32m'+'passed'+'\033[0m'
    else:
        logging.error(final_str+'failed')
        final_str += '\033[31m'+'failed'+'\033[0m'
    print(final_str)

def unittest_reporter(*args,**kwargs):
    def make_wrapper(obj):
        module = os.path.basename(obj.func_globals['__file__']).split('_test')[0]
        if 'module' in kwargs:
            module = kwargs['module']
        name = '_'.join(obj.func_name.split('_')[2:])+'()'
        if 'name' in kwargs:
            name = kwargs['name']
        if name.startswith(' '):
            test_name = '%s%s'%(module,name)
        else:
            test_name = '%s.%s'%(module,name)
        skip = False
        if 'skip' in kwargs:
            skip = kwargs['skip']
            if callable(skip):
                skip = skip()
            skip = bool(skip)
        @wraps(obj)
        def wrapper(*args, **kwargs):
            if skip:
                printer('Test '+test_name,skipped=True)
                return
            try:
                obj(*args,**kwargs)
            except Exception as e:
                logging.error('Error running %s test',name,
                             exc_info=True)
                printer('Test '+test_name,passed=False)
                raise
            else:
                printer('Test '+test_name)
        return wrapper
    if kwargs:
        return make_wrapper
    else:
        return make_wrapper(*args)

test_glob = '*'
def skipTest(obj, attr):
    if fnmatch.fnmatch(obj.__name__,test_glob):
        return unittest.skip()
    return lambda func: func
def glob_tests(x):
    """glob the tests that were requested"""
    return fnmatch.filter(x,test_glob)

def listmodules(package_name=''):
    """List modules in a package or directory"""
    import os,imp
    package_name_os = package_name.replace('.','/')
    file, pathname, description = imp.find_module(package_name_os)
    if file:
        # Not a package
        return []
    ret = []
    for module in os.listdir(pathname):
        if module.endswith('.py') and module != '__init__.py':
            tmp = os.path.splitext(module)[0]
            ret.append(package_name+'.'+tmp)
    return ret


class messaging_mock(object):
    """
    A fake :class:`iceprod.server.RPCinternal.Service` object.
    Designed to replace module.messaging.
    """
    def __init__(self):
        self.called = []
        self.args = []
        self.ret = None
        self._local_called = []
    def start(self):
        self._local_called.append('start')
    def stop(self):
        self._local_called.append('stop')
    def kill(self):
        self._local_called.append('kill')
    def __request(self, service, method, args, kwargs):
        self.called.append([service,method,args,kwargs])
        logging.info(self.called[-1])
        if 'callback' in kwargs and kwargs['callback']:
            if self.ret and service in self.ret and method in self.ret[service]:
                kwargs['callback'](self.ret[service][method])
            else:
                kwargs['callback']()
        elif 'async' in kwargs and kwargs['async'] is False:
            if self.ret and service in self.ret and method in self.ret[service]:
                return self.ret[service][method]
    def __getattr__(self,name):
        class _Method:
            def __init__(self,send,service,name):
                self.__send = send
                self.__service = service
                self.__name = name
            def __getattr__(self,name):
                return _Method(self.__send,self.__service,
                               "%s.%s"%(self.__name,name))
            def __call__(self,*args,**kwargs):
                return self.__send(self.__service,self.__name,args,kwargs)
        class _Service:
            def __init__(self,send,service):
                self.__send = send
                self.__service = service
            def __getattr__(self,name):
                return _Method(self.__send,self.__service,name)
            def __call__(self,**kwargs):
                raise Exception('Service %s, method name not specified'%(
                                self.__service))
        return _Service(self.__request,name)
