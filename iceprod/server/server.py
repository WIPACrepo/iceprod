"""
Server
======

Run the iceprod server.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, UTC
import logging
import os
import sys

from iceprod.core.logger import set_log_level
from iceprod.server.config import IceProdConfig
from iceprod.server.queue import Queue

logger = logging.getLogger('Server')


class Server(object):
    """
    The actual server.

    """
    def __init__(self, config_params=None, outfile=None, errfile=None):
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.cfg = IceProdConfig(override=config_params)
        self.outfile = outfile
        self.errfile = errfile

        self.rotate_logs_task = None
        self.queue = Queue(self.cfg)

        set_log_level(self.cfg['logging']['level'])

    async def rotate_logs(self):
        current_date = datetime.utcnow()
        while self.outfile and self.errfile:
            if current_date.day != datetime.utcnow().day:
                # rotate files
                current_date = datetime.utcnow()
                if self.outfile:
                    roll_files(sys.stdout, self.outfile)
                if self.errfile:
                    roll_files(sys.stderr, self.errfile)
            await asyncio.sleep(3600)

    async def run(self):
        self.rotate_logs_task = asyncio.create_task(self.rotate_logs())
        try:
            await self.queue.run()
        finally:
            if self.rotate_logs_task:
                self.rotate_logs_task.cancel()
                self.rotate_logs_task = None
            await asyncio.sleep(0)


def roll_files(fd, filename, num_files=5):
    d = datetime.now(UTC)
    ext = (d-timedelta(days=num_files-1)).strftime('%Y-%m-%d')
    newfile = f'{filename}.{ext}'
    if os.path.exists(newfile):  # delete last file
        os.remove(newfile)
    for i in range(num_files-2, 0, -1):
        ext = (d-timedelta(days=i)).strftime('%Y-%m-%d')
        oldfile = f'{filename}.{ext}'
        if os.path.exists(oldfile):
            os.rename(oldfile, newfile)
        newfile = oldfile
    if os.path.exists(filename):
        os.rename(filename, newfile)

    # redirect file descriptor
    newfd = open(filename, fd.mode)
    fd.flush()
    os.dup2(newfd.fileno(), fd.fileno())
    if fd != sys.stdout and fd != sys.stderr:
        fd.close()
    return newfd
