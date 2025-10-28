import os
import sys
import logging
import fnmatch
import unittest
import inspect
from collections import defaultdict
from collections.abc import Iterable
from functools import wraps, partial

from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.testing import gen_test

def printer(in_str,passed=True,skipped=False):
    numcols = 60
    firstpos = 0
    padding = 3 if skipped else 4
    while len(in_str) > numcols:
        # wrap longer strings
        pos = in_str.rfind(' ',firstpos,numcols)
        if pos <= firstpos or pos >= numcols:
            pos = in_str.rfind('/',firstpos,numcols)
            if pos <= firstpos or pos >= numcols:
                break
        tmp_str = in_str[0:pos+1]
        in_str = '     '+in_str[pos+1:]
        firstpos = 4
        print(tmp_str)
    # print string aligned left, and passed or failed
    final_str = in_str
    for i in range(len(in_str),numcols+padding):
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
        filename = obj.__globals__['__file__']
        module = os.path.basename(filename).split('_test')[0]
        prevdir = os.path.basename(os.path.dirname(filename))
        if prevdir not in ('core','server'):
            module = prevdir+'/'+module
        if 'module' in kwargs:
            module = kwargs['module']
        name = '_'.join(obj.__name__.split('_')[2:])+'()'
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
        def wrapper(self, *args, **kwargs):
            from iceprod.core import to_log
            if skip:
                printer('Test '+test_name,skipped=True)
                return
            try:
                with to_log(sys.stdout), to_log(sys.stderr):
                    if inspect.isgeneratorfunction(obj):
                        logging.info('test is async')
                        gen_test(obj)(self, *args,**kwargs)
                    elif inspect.iscoroutinefunction(obj):
                        logging.info('test is async')
                        IOLoop.current().run_sync(partial(obj, self, *args,**kwargs))
                    else:
                        logging.info('test is seq')
                        obj(self, *args,**kwargs)
            except Exception:
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
    import os
    import imp
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

def assertCountEqualRecursive(self, a, b, skip=[]):
    self.assertEqual(type(a), type(b))
    if isinstance(a, dict):
        if set(a).symmetric_difference(b)-set(skip):
            raise AssertionError('different keys')
        for k in a:
            if k not in skip:
                self.assertCountEqualRecursive(a[k], b[k])
    elif isinstance(a, Iterable):
        self.assertCountEqual(a, b)
    else:
        self.assertEqual(a, b)
unittest.TestCase.assertCountEqualRecursive = assertCountEqualRecursive


class services_mock(dict):
    """
    A fake `iceprod.modules.module.modules` object.

    It mocks a two-level dict of function objects,
    recording all calls and allowing different return values.
    """
    def __init__(self):
        self.called = []
        self.ret = defaultdict(dict)
    def __request(self, service, method, *args, **kwargs):
        self.called.append((service, method, args, kwargs))
        logging.info('__request: %r', self.called[-1])
        ret = self.ret[service][method]
        if isinstance(ret, Exception):
            raise ret
        f = Future()
        f.set_result(ret)
        return f
    def __contains__(self, name):
        return name in self.ret
    def __missing__(self, name):
        class Service(dict):
            def __init__(self, name, request):
                self.name = name
                self.request = request
            def __missing__(self, key):
                return partial(self.request,name,key)
        return Service(name,self.__request)

def return_once(*args, **kwargs):
    """Return every argument once, then return end_value repeatedly"""
    end_value = kwargs.pop('end_value', Exception())
    for a in args:
        yield a
    while True:
        yield end_value


def cmp_list(a,b):
    """Compare all items in a with b"""
    for aa,bb in zip(a,b):
        a_list = isinstance(aa,(list,tuple))
        b_list = isinstance(bb,(list,tuple))
        a_dict = isinstance(aa,dict)
        b_dict = isinstance(bb,dict)
        if a_list != b_list or a_dict != b_dict:
            return False
        if a_list:
            if not cmp_list(aa,bb):
                return False
        elif a_dict:
            if not cmp_dict(aa,bb):
                return False
        elif aa != bb:
            return False
    return True

def cmp_dict(a,b):
    """Compare all items in a with b"""
    for k in a:
        if k not in b:
            return False
        a_list = isinstance(a[k],(list,tuple))
        b_list = isinstance(a[k],(list,tuple))
        a_dict = isinstance(a[k],dict)
        b_dict = isinstance(b[k],dict)
        if a_list != b_list or a_dict != b_dict:
            return False
        if a_list:
            if not cmp_list(a[k],b[k]):
                return False
        elif a_dict:
            if not cmp_dict(a[k],b[k]):
                return False
        elif a[k] != b[k]:
            return False
    return True
