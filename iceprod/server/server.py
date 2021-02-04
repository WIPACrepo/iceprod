"""
Server
======

Run the iceprod server.
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import logging
from functools import partial
import importlib
import subprocess
from datetime import datetime, timedelta
import asyncio

from tornado.ioloop import IOLoop

from concurrent.futures import ThreadPoolExecutor

from iceprod.core.logger import set_log_level
from iceprod.server.config import IceProdConfig


logger = logging.getLogger('Server')

class Server(object):
    """
    The actual server.

    """
    def __init__(self, config_params=None, outfile=None, errfile=None):
        self.io_loop = IOLoop.current()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.cfg = IceProdConfig(override=config_params)
        self.modules = {}
        self.services = {'daemon': {'restart': self.restart,
                                    'reload': self.reload,
                                    'stop': self.stop,
                                    'kill': self.kill,
                                    'get_running_modules': lambda: self.modules.keys(),
                                   },
                        }
        self.outfile = outfile
        self.errfile = errfile

        set_log_level(self.cfg['logging']['level'])

        if 'blocking_threshold' in self.cfg['logging']:
            self.io_loop.set_blocking_log_threshold(self.cfg['logging']['blocking_threshold'])

        for mod_name in self.cfg['modules']:
            if self.cfg['modules'][mod_name]:
                try:
                    m = importlib.import_module('iceprod.server.modules.'+mod_name)
                    mod = getattr(m, mod_name)(cfg=self.cfg,
                                               io_loop=self.io_loop,
                                               executor=self.executor,
                                               modules=self.services)
                    self.modules[mod_name] = mod
                    self.services[mod_name] = mod.service
                    mod.start()
                except Exception:
                    logger.critical('cannot start module', exc_info=True)
                    self.kill()
                    raise

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

    def run(self):
        try:
            self.io_loop.add_callback(self.rotate_logs)
            self.io_loop.start()
        except Exception:
            logger.critical('exception not caught', exc_info=True)
            self.kill()

    def restart(self):
        env = os.environ.copy()
        extra_path = os.path.join(os.environ['I3PROD'],'bin')
        env['PATH'] = extra_path+(':'+env['PATH'] if 'PATH' in env else '')
        subprocess.Popen(['iceprod_server.py','restart'],
                         cwd=os.environ['I3PROD'], env=env)

    def reload(self):
        for m in self.modules.values():
            m.stop()
            m.start()

    def stop(self):
        for m in self.modules.values():
            m.stop()
        self.io_loop.stop()

    def kill(self):
        for m in self.modules.values():
            m.kill()
        self.io_loop.stop()

def roll_files(fd, filename, num_files=5):
    d = datetime.utcnow()
    ext = (d-timedelta(days=num_files-1)).strftime('%Y-%m-%d')
    newfile = f'{filename}.{ext}'
    if os.path.exists(newfile): # delete last file
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
