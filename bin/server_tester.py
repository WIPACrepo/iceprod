#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
  Tester for iceprod server.

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
        cov = coverage.coverage(data_suffix=True)
        cov.start()
        super(CoverageProcess,self).run()
        cov.stop()
        cov.save()
multiprocessing.Process = CoverageProcess

import os, sys
import Queue
    
import logging
import logging.config    
import signal
        
try:
    import unittest2 as unittest
except ImportError:
    import unittest
    
import iceprod.server
from iceprod.server import listmodules, run_module, basic_config

import iceprod.core.logger
logging.basicConfig()
logger = logging.getLogger('server_tester')
        
def handler1(signum, frame):
   logger.warn('Signal handler called with signal %s' % signum)
   logger.warn('Exiting...')
   os._exit(0)


class server_module():
    """Manage a server module"""
    message_queue = multiprocessing.Queue()      # global message queue
    
    def __init__(self,mod_name,cfg):
        self.queue = multiprocessing.Queue()
        self.pipe, pipe = multiprocessing.Pipe()
        self.mod_name = mod_name
        if cfg is None:
            raise Exception, "cfg is none in server_module.__init__()"
        self.process = multiprocessing.Process(target=run_module,
                args=[self.mod_name,[cfg,
                                     self.queue,
                                     pipe,
                                     server_module.message_queue]])
                
    def start(self):
        self.process.start()
        
    def stop(self):
        self.put_message('stop')
    
    def put_message(self,m):
        """Put a message on the queue for the process to receive"""
        self.queue.put(m)
    
    def get_message(self):
        """Get a message from the queue
           Returns the message or an Empty exception"""
        return self.queue.get(false)
        
    def put_object(self,o):
        """Put an object on the pipe for the process to receive"""
        self.pipe.send(o)

def printer(str,passed=True):
    numcols = 60
    padding = 4
    while len(str) > numcols:
        # wrap longer strings
        pos = str.rfind(' ',0,numcols)
        if pos < 0:
            break
        tmp_str = str[0:pos]
        str = str[pos+1:]
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
    
if __name__ == "__main__":    
    grep_str = '*'
    if len(sys.argv) > 1:
        grep_str = sys.argv[1]
        
    # get config
    cfg = basic_config.BasicConfig()
    cfg.read_file( basic_config.locateconfig() )

    
    # set up logger
    localhost = os.uname()[1].split(".")[0]
    cfg.logging['logfile'] = 'server_tester.log'
    cfg.logging['level'] = 'DEBUG'
    iceprod.core.logger.setlogger(logger.name,cfg)
    signal.signal(signal.SIGQUIT, handler1)
    signal.signal(signal.SIGINT, handler1)
    logger.warn('starting... log files available in log directory')
    
    # remove stdout logging handler
    log = logging.getLogger()
    for handler in log.handlers:
        if isinstance(handler,logging.StreamHandler):
            log.removeHandler(handler)
    logger.info('loggers=%s' % str(log.handlers))
    
    # accumulate tests
    loader = unittest.defaultTestLoader
    test_suites = []

    test_directory = 'tests.server'

    try:
        # python 2.7 features
        test_suites = loader.discover(test_directory,grep_str+'.py')
    except:
        import fnmatch
        for m in listmodules(test_directory):
            module_name = m.rsplit('.',1)[1]
            if fnmatch.fnmatch(module_name,grep_str):
                x = __import__(test_directory+'.'+module_name,globals(),locals(),[module_name])
                test_suites.append(x.load_tests(loader,None,None))

    # run tests
    alltests = unittest.TestSuite(test_suites)
    runner = unittest.TextTestRunner(verbosity=0)
    runner.run(alltests)
    
