"""
Logfile setup
"""

from __future__ import absolute_import, division, print_function

import os
import time
import logging
import logging.handlers
import gzip

setlevel = {
    'CRITICAL': logging.CRITICAL,  # execution cannot continue
    'FATAL': logging.CRITICAL,
    'ERROR': logging.ERROR,  # something is wrong, but try to continue
    'WARNING': logging.WARNING,  # non-ideal behavior, important event
    'WARN': logging.WARNING,
    'INFO': logging.INFO,  # initial debug information
    'DEBUG': logging.DEBUG  # the things no one wants to see
}

host = os.uname()[1].split(".")[0]


def set_logger(loglevel='INFO', logfile=None, logsize=2**28, lognum=4):
    """Add an output to the root logger"""
    logformat = '%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'

    rootLogger = logging.getLogger()

    if logfile:
        if not logfile.startswith('/'):
            if 'I3PROD' in os.environ:
                logfile = os.path.expanduser(os.path.expandvars(
                    os.path.join('$I3PROD', 'var', 'log', logfile)))
            else:
                logfile = os.path.join(os.getcwd(), 'log', host, logfile)
        if not os.path.exists(os.path.dirname(logfile)):
            os.makedirs(os.path.dirname(logfile))
        if logfile.endswith('.gz'):
            z_file = gzip.open(logfile, mode='wt', encoding='utf-8')
            fileHandler = logging.StreamHandler(z_file)
        else:
            fileHandler = logging.handlers.RotatingFileHandler(logfile, 'a',
                                                               logsize, lognum)
        formatter = logging.Formatter(logformat)
        formatter.converter = time.gmtime
        fileHandler.setFormatter(formatter)
        rootLogger.addHandler(fileHandler)
        for handler in rootLogger.handlers:
            if handler != fileHandler:
                rootLogger.removeHandler(handler)
        rootLogger.setLevel(setlevel[loglevel.upper()])
        rootLogger.info('fileHandler used')
    else:
        logging.basicConfig(format=logformat, level=setlevel[loglevel.upper()])
        rootLogger.info('basicConfig used')

    rootLogger.info('loglevel %s, logfile %s, logsize %d, lognum %d',
                    loglevel, logfile, logsize, lognum)


def set_log_level(loglevel='INFO'):
    rootLogger = logging.getLogger()
    rootLogger.setLevel(setlevel[loglevel.upper()])


def new_file(filename):
    """Write logging to a new file"""
    log = logging.getLogger()
    handlers = False
    for handler in log.handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            new_handler = logging.handlers.RotatingFileHandler(filename, 'a',
                                                               handler.maxBytes,
                                                               handler.backupCount)
            new_handler.setFormatter(handler.formatter)
            log.addHandler(new_handler)
            log.removeHandler(handler)
            handlers = True
    if not handlers:
        set_logger(logfile=filename)
    logging.info('loggers=%r', log.handlers)


def remove_stdout():
    """Remove the stdout log output from the root logger"""
    log = logging.getLogger()
    logging.info('remove_stdout(): loggers=%s', log.handlers)
    for handler in log.handlers:
        if isinstance(handler,logging.StreamHandler):
            log.removeHandler(handler)
    logging.info('loggers=%s', log.handlers)


def remove_handlers():
    """Remove all handlers from the root logger"""
    log = logging.getLogger()
    logging.info('remove_handlers(): loggers=%s', log.handlers)
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.info('loggers=%s', log.handlers)


def rotate():
    """Rotate the file in the root logger"""
    log = logging.getLogger()
    logging.info('rotate() loggers=%s', log.handlers)
    for handler in log.handlers:
        if isinstance(handler,logging.handlers.RotatingFileHandler):
            handler.doRollover()
