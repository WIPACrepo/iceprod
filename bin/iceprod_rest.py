#!/usr/bin/env python
"""
Server process for starting IceProd.
"""

import asyncio
import os
import sys
import logging
import signal
import importlib
from functools import partial

# Assuming that the directory structure remains constant, this will
# automatically set the python path to find iceprod
bin_dir = os.path.dirname( os.path.abspath(sys.argv[0]) )
root_path = os.path.dirname( bin_dir )
if not root_path in sys.path:
    sys.path.append(root_path)


import iceprod
import iceprod.core.logger
from iceprod.server.config import IceProdConfig
from iceprod.rest.server import Server

# start server
async def start_server():
    s = Server()
    await s.start()
    try:
        await asyncio.Event().wait()
    finally:
        await s.stop()

def runner(stdout=False, *args, **kwargs):
    # set logger
    if stdout:
        iceprod.core.logger.set_logger()
    else:
        iceprod.core.logger.set_logger(logfile='iceprod_server.log')

    # Change name of process for ps
    try:
        from setproctitle import setproctitle
        setproctitle('iceprod_rest.main')
    except Exception:
        logging.warning("could not rename process")

    # setup env
    cfg = IceProdConfig()
    for key in ['HOST', 'PORT', 'OPENID_URL', 'OPENID_AUDIENCE', 'DB_URL', 'STATSD_ADDRESS', 'STATSD_PREFIX', 'S3_ADDRESS', 'S3_ACCESS_KEY', 'S3_SECRET_KEY']:
        if key in cfg['rest']:
            os.environ[key] = cfg['rest'][key]

    asyncio.run(start_server())
    logging.warning('iceprod exiting')

def setup_I3PROD():
    if 'I3PROD' not in os.environ:
        os.environ['I3PROD'] = os.getcwd()
    for d in ('var/log','var/run','etc'):
        d = os.path.join(os.environ['I3PROD'],d)
        if not os.path.exists(d):
            os.makedirs(d)

def main():
    import argparse

    def key_val(val):
        if '=' not in val:
            msg = f'{val} is not of the form key=val'
            raise argparse.ArgumentTypeError(msg)
        return val

    # Override values with cmdline options
    parser = argparse.ArgumentParser(description='IceProd REST')
    parser.add_argument('-n','--non-daemon',dest='daemon',action='store_false',
                        default=True,help='Do not daemonize')
    parser.add_argument('action',nargs='?',type=str,default='start',
                        choices=['start','stop','kill','hardkill','restart'])
    parser.add_argument('--pidfile',type=str,default='$I3PROD/var/run/iceprod.pid',
                        help='PID lockfile')
    parser.add_argument('--umask',type=int,default=0o077, #octal
                        help='File creation umask')
    args = parser.parse_args()

    setup_I3PROD()

    if args.daemon:
        # now daemonize
        from iceprod.server.daemon import Daemon
        pidfile = os.path.expanduser(os.path.expandvars(args.pidfile))
        chdir = os.path.expanduser(os.path.expandvars('$I3PROD'))
        umask = args.umask
        d = Daemon(pidfile, runner,
                   chdir=chdir,
                   umask=umask,
                   stdout='var/log/out',
                   stderr='var/log/err')
        getattr(d,args.action)()
    else:
        if args.action != 'start':
            raise Exception('only daemon-mode can execute actions')
        else:
            runner(stdout=True)

if __name__ == '__main__':
    main()
