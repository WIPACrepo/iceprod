from __future__ import absolute_import, division, print_function

# override multiprocessing to get coverage to work properly
import multiprocessing
class CoverageProcess(multiprocessing.Process):
    def run(self):
        import coverage
        import os
        cwd = os.getcwd()
        while os.path.basename(cwd).startswith('tmp'):
            cwd = os.path.dirname(cwd)
        cov = coverage.Coverage(data_suffix=True, branch=True,
                                source=[os.path.join(cwd,'iceprod')])
        cov.start()
        super(CoverageProcess,self).run()
        cov.stop()
        cov.save()
multiprocessing.Process = CoverageProcess

import os
import sys
import time
import signal
import logging
import logging.handlers
import argparse
import unittest

logging.basicConfig()
logger = logging.getLogger('tests')

def handler1(signum, frame):
   logger.warn('Signal handler called with signal %s' % signum)
   logger.warn('Exiting...')
   os._exit(0)

class MyArgumentParser(argparse.ArgumentParser):
    """Override exit to indicate a failure"""
    def exit(self, status=0, message=None):
        if status == 0:
            status = 2
        super(MyArgumentParser, self).exit(status=status, message=message)

parser = MyArgumentParser()
parser.add_argument('--core',action='store_true')
parser.add_argument('--server',action='store_true')
parser.add_argument('file_glob', type=str, nargs='?', default='*')
parser.add_argument('test_glob', type=str, nargs='?', default='*')
try:
    args = parser.parse_args()
except Exception, SystemExit:
    print('exception')
    raise
if not args.core and not args.server:
    # if neither is selected, select all
    args.core = True
    args.server = True

import tests.util
tests.util.test_glob = args.test_glob

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
print('(detailed log file available at tests.log)')
print('')

# accumulate tests
loader = unittest.defaultTestLoader
test_suites = unittest.TestSuite()
if args.core:
    test_suites.addTests(loader.discover('tests.core',args.file_glob+'_test.py'))
if args.server:
    test_suites.addTests(loader.discover('tests.server',args.file_glob+'_test.py'))

# run tests
test_result = unittest.TestResult()
start_time = time.time()
test_suites.run(test_result)
for err in test_result.errors:
    if 'ModuleImportFailure' in str(err[0]):
        print(err[1])
print('-'*70)
print('Ran %d tests in %0.3fs'%(test_suites.countTestCases(),time.time()-start_time))
if not test_result.wasSuccessful():
    print('%d tests failed'%len(test_result.errors))
    sys.exit(1)
