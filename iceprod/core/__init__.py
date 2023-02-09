"""
Some constants and basic functions with wide uses.
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import logging
from contextlib import contextmanager
import tempfile

# some basic constants used for the task runner
constants = {'stdout': 'iceprod_out',
             'stderr': 'iceprod_err',
             'stdlog': 'iceprod_log',
             'stats': 'iceprod_stats',
             'task_exception': 'iceprod_task_exception',
             'args': 'iceprod_args',}


@contextmanager
def to_file(stream=None, file='out'):
    """redirect a stream (like stdout) to regular file

       :param stream: previous file object
       :param file: new file object
    """
    oldstream = os.dup(stream.fileno())
    with open(file,'w') as f:
        os.dup2(f.fileno(), stream.fileno())
        try:
            yield  # go do something useful
        finally:
            os.dup2(oldstream, stream.fileno())


@contextmanager
def to_log(stream=None, prefix='', level='info'):
    """redirect a stream (like stdout) to log file

       :param stream: file object
       :param prefix: logging prefix
       :param level: logging level
    """
    if not prefix:
        if stream == sys.stdout:
            prefix = 'stdout'
        elif stream == sys.stderr:
            prefix = 'stderr'
    logger = logging.getLogger(prefix)

    oldstream = os.dup(stream.fileno())
    with tempfile.TemporaryFile() as f:
        stream.flush()
        os.dup2(f.fileno(), stream.fileno())
        try:
            yield  # go do something useful
        finally:
            f.flush()
            os.dup2(oldstream, stream.fileno())
            f.seek(0)
            for s in f.readlines():
                s = s.strip()
                if not s:
                    continue
                if level == 'debug':
                    logger.debug(s)
                elif level == 'info':
                    logger.info(s)
                elif level in ('warn','warning'):
                    logger.warning(s)
                elif level in ('err','error'):
                    logger.error(s)
                elif level in ('crit','critical'):
                    logger.critical(s)
                else:
                    logger.warning('error printing logging')
