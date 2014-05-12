#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
  Tester for iceprod core.

  copyright (c) 2011 the icecube collaboration

  @version: $Revision: $
  @date: $Date: $
  @author: David Schultz <david.schultz@icecube.wisc.edu>
  
"""

from __future__ import print_function

# override multiprocessing to get coverage to work
import multiprocessing

class CoverageProcess(multiprocessing.Process):
    def run(self):
        import coverage
        cov = coverage.coverage(data_suffix=True,
                                source=['iceprod.core','core_tester'])
        cov.start()
        super(CoverageProcess,self).run()
        cov.stop()
        cov.save()
multiprocessing.Process = CoverageProcess

import os
import sys
import Queue

import logging
import logging.config
import signal

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import iceprod.core

import iceprod.core.logger
logging.basicConfig()
logger = logging.getLogger('core_tester')

def handler1(signum, frame):
   logger.warn('Signal handler called with signal %s' % signum)
   logger.warn('Exiting...')
   os._exit(0)



def printer(str,passed=True):
    numcols = 60
    padding = 4
    while len(str) > numcols:
        # wrap longer strings
        pos = str.rfind(' ',0,numcols)
        if pos < 0:
            break
        tmp_str = str[0:pos]
        str = '     '+str[pos+1:]
        print(tmp_str)
    # print string aligned left, and passed or failed
    final_str = str
    for i in xrange(len(str),numcols+padding):
        final_str += ' '
    
    if passed:
        logger.error(final_str+'passed')
        final_str += '\033[32m'+'passed'+'\033[0m'
    else:
        logger.error(final_str+'failed')
        final_str += '\033[31m'+'failed'+'\033[0m'
    print(final_str)
    
def glob_tests(x):
    """glob the tests that were requested"""    
    glob_func_str = '*'
    if len(sys.argv) > 2:
        glob_func_str = sys.argv[2]
    import fnmatch
    return fnmatch.filter(x,glob_func_str)

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

if __name__ == "__main__":   
    glob_str = '*'
    if len(sys.argv) > 1:
        glob_str = sys.argv[1]
    
    # set up logger
    rootLogger = logging.getLogger()
    for handler in rootLogger.handlers:
        if isinstance(handler,logging.StreamHandler):
            rootLogger.removeHandler(handler)
    iceprod.core.logger.setlogger(logger.name,
                                  None,
                                  loglevel='DEBUG',
                                  logfile='core_tester.log',
                                  logsize=67108864)
    signal.signal(signal.SIGQUIT, handler1)
    signal.signal(signal.SIGINT, handler1)
    logger.warn('starting...%s ' % logger.name)
    
    # accumulate tests
    loader = unittest.defaultTestLoader
    test_suites = []
    try:
        # python 2.7 features
        test_suites = loader.discover('iceprod.core.tests',glob_str+'.py')
    except:
        import fnmatch
        for m in listmodules('iceprod.core.tests'):
            module_name = m.rsplit('.',1)[1]
            if fnmatch.fnmatch(module_name,glob_str):
                x = __import__(name,globals(),locals(),[module_name])
                test_suites.append(x.load_tests(loader,None,None))

    # run tests
    alltests = unittest.TestSuite(test_suites)
    runner = unittest.TextTestRunner(verbosity=0)
    runner.run(alltests)
    
