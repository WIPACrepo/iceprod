"""
Logfile setup
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import logging
import logging.handlers

setlevel = {
  'CRITICAL': logging.CRITICAL, # execution cannot continue
  'FATAL': logging.CRITICAL,
  'ERROR': logging.ERROR, # something is wrong, but try to continue
  'WARNING': logging.WARNING, # non-ideal behavior, important event
  'WARN': logging.WARNING,
  'INFO': logging.INFO, # initial debug information
  'DEBUG': logging.DEBUG # the things no one wants to see
  }

host = os.uname()[1].split(".")[0]

def setlogger(loglevel='INFO', logfile='sys.stdout', logsize=2**24, lognum=4):
    """Add an output to the root logger"""
    logformat='%(asctime)s %(levelname)s %(name)s : %(message)s'

    rootLogger = logging.getLogger()
    rootLogger.setLevel(setlevel[loglevel.upper()])

    if logfile.strip() != 'sys.stdout':
        if not logfile.startswith('/'):
            if 'I3PROD' in os.environ:
                logfile = os.path.expanduser(os.path.expandvars(
                            os.path.join('$I3PROD', 'var', 'log', logfile)))
            else:
                logfile = os.path.join(os.getcwd(), 'log', host, logfile)
        if not os.path.exists(os.path.dirname(logfile)):
            os.makedirs(os.path.dirname(logfile))
        fileHandler = logging.handlers.RotatingFileHandler(logfile, 'a',
                                                           logsize, lognum)
        formatter = logging.Formatter(logformat)
        fileHandler.setFormatter(formatter)
        for handler in rootLogger.handlers:
            log.removeHandler(handler)
        rootLogger.addHandler(fileHandler)
    else:
        logging.basicConfig(format=logformat)

    rootLogger.info('loglevel %s, logfile %s, logsize %d, lognum %d',
                    loglevel, logfile, logsize, lognum)

def set_log_level(loglevel='INFO'):
    rootLogger = logging.getLogger('')
    rootLogger.setLevel(setlevel[loglevel.upper()])

def new_file(filename):
    """Write logging to a new file"""
    log = logging.getLogger()
    for handler in log.handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            new_handler = logging.handlers.RotatingFileHandler(filename, 'a',
                                                               handler.maxBytes,
                                                               handler.backupCount)
            new_handler.setFormatter(handler.formatter)
            log.removeHandler(handler)
            log.addHandler(new_handler)
    logging.info('loggers=%s' % str(log.handlers))

def removestdout():
    """Remove the stdout log output from the root logger"""
    log = logging.getLogger()
    for handler in log.handlers:
        if isinstance(handler,logging.StreamHandler):
            log.removeHandler(handler)
    logging.info('loggers=%s' % str(log.handlers))

def rotate():
    """Rotate the file in the root logger"""
    log = logging.getLogger()
    for handler in log.handlers:
        if isinstance(handler,logging.handlers.RotatingFileHandler):
            handler.doRollover()
    logging.info('loggers=%s' % str(log.handlers))
