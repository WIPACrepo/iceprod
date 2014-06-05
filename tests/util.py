from __future__ import absolute_import, division, print_function

import sys
import logging
import fnmatch

def printer(input,passed=True):
    numcols = 60
    padding = 4
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
    
    if passed:
        logging.error(final_str+'passed')
        final_str += '\033[32m'+'passed'+'\033[0m'
    else:
        logging.error(final_str+'failed')
        final_str += '\033[31m'+'failed'+'\033[0m'
    print(final_str)

test_glob = '*'
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