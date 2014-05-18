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

def setlogger(loggername,cfg=None,loglevel='WARN',logfile='sys.stdout',
              logsize=1048576,lognum=4):
    """Add an output to the root logger"""
    logformat='%(asctime)s %(levelname)s %(name)s : %(message)s'
    
    if cfg:
        if 'level' in cfg['logging']:
            loglevel  = cfg['logging']['level']
        if 'format' in cfg['logging']:
            logformat=cfg['logging']['format']
        if 'size' in cfg['logging']:
            logsize  = cfg['logging']['size']
        if 'num' in cfg['logging']:
            lognum  = cfg['logging']['num']
        
        if loggername in cfg['logging']:
            logfile   = os.path.expandvars(cfg['logging'][loggername])
        else:
            logfile   = os.path.expandvars(cfg['logging']['logfile'])

    rootLogger = logging.getLogger('')
    if loglevel not in setlevel:
        loglevel = 'WARN'
    rootLogger.setLevel(setlevel[loglevel])
    
    if logfile.strip() != 'sys.stdout':
        if not logfile.startswith('/'):
            if 'I3PROD' in os.environ:
                logfile = os.path.expanduser(os.path.expandvars(
                            os.path.join('$I3PROD','var','log',logfile)))
            else:
                logfile = os.path.join(os.getcwd(),'log',host,logfile)
        if not os.path.exists(os.path.dirname(logfile)):
            os.makedirs(os.path.dirname(logfile))
        fileHandler = logging.handlers.RotatingFileHandler(logfile,'a',
                                                           logsize,lognum)
        formatter = logging.Formatter(logformat)
        fileHandler.setFormatter(formatter)
        rootLogger.addHandler(fileHandler)
    else:
        logging.basicConfig()

def removestdout():
    """Remove the stdout log output from the root logger"""
    log = logging.getLogger()
    for handler in log.handlers:
        if isinstance(handler,logging.StreamHandler):
            log.removeHandler(handler)
    logging.info('loggers=%s' % str(log.handlers))
