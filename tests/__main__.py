from __future__ import absolute_import, division, print_function

# override multiprocessing to get coverage to work properly
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

import os
import sys
import signal
import logging
import logging.handlers
import unittest

logging.basicConfig()
logger = logging.getLogger('tests')

def handler1(signum, frame):
   logger.warn('Signal handler called with signal %s' % signum)
   logger.warn('Exiting...')
   os._exit(0)

glob_str = '*'
if len(sys.argv) > 1:
    glob_str = sys.argv[1]

# set up logger
rootLogger = logging.getLogger()
for handler in rootLogger.handlers:
    if isinstance(handler,logging.StreamHandler):
        rootLogger.removeHandler(handler)
if os.path.exists('tests.log'):
    os.remove('tests.log')
fileHandler = logging.handlers.RotatingFileHandler('tests.log','a',
                                                   67108864,1)
logformat='%(asctime)s %(levelname)s %(name)s : %(message)s'
fileHandler.setFormatter(logging.Formatter(logformat))
rootLogger.addHandler(fileHandler)
rootLogger.setLevel(logging.DEBUG)

# handle any signals
signal.signal(signal.SIGQUIT, handler1)
signal.signal(signal.SIGINT, handler1)

# start testing
logger.warn('starting...%s ' % logger.name)
print('Testing...')
print('(detailed log file available at test.log)')
print('')

# accumulate tests
loader = unittest.defaultTestLoader
test_suites = unittest.TestSuite()
test_suites.addTests(loader.discover('tests.core',glob_str+'_test.py'))
test_suites.addTests(loader.discover('tests.server',glob_str+'_test.py'))

# run tests
runner = unittest.TextTestRunner(verbosity=0)
runner.run(test_suites)
