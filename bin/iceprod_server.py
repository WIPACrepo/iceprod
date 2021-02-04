#!/usr/bin/env python
"""
Server process for starting IceProd.
"""

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

from tornado.ioloop import IOLoop

import iceprod
import iceprod.server
import iceprod.core.logger
from iceprod.server.server import Server

data_class_path = iceprod.server.get_pkgdata_filename('iceprod.server','data/www/dataclasses.js')
if not os.path.exists(data_class_path):
    print('Generating data classes')
    import inspect
    import json
    from iceprod.core import dataclasses
    dcs = {}
    names = dataclasses._plurals.copy()
    for name, obj in inspect.getmembers(dataclasses,inspect.isclass):
        if name[0] != '_' and dict in inspect.getmro(obj):
            dcs[name] = obj().output()
            names[name] = obj.plural
    data = {'classes':dcs,'names':names}
    with open(data_class_path,'w') as f:
        f.write('var dataclasses='+json.dumps(data,separators=(',',':'))+';')

def check_module(name, message='', required=False):
    try:
        importlib.import_module(name)
    except ImportError:
        print('Cannot import python module %s. %s' % (name, message))
        if required:
            print('Required module "%s" not found. Exiting...' % name)
            exit(0)

def check_dependencies():
    check_module('setproctitle', 'Will not be able to set process title.')
    check_module('tornado', required=True)
    check_module('jsonschema')
    check_module('rest_tools', required=True)

check_dependencies()

def runner(stdout=False, *args, **kwargs):
    # set logger
    if stdout:
        iceprod.core.logger.set_logger()
    else:
        iceprod.core.logger.set_logger(logfile='iceprod_server.log')

    # Change name of process for ps
    try:
        from setproctitle import setproctitle
        setproctitle('iceprod_server.main')
    except Exception:
        logging.warning("could not rename process")

    s = Server(*args, **kwargs)

    # set signal handlers
    def sig_handle(signum, frame):
        logging.warning('signal handler called for %r', signum)
        if signum == signal.SIGINT:
            IOLoop.current().add_callback_from_signal(s.stop)
        else:
            IOLoop.current().add_callback_from_signal(s.kill)
    signal.signal(signal.SIGINT, sig_handle)
    signal.signal(signal.SIGQUIT, sig_handle)

    s.run()
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
    parser = argparse.ArgumentParser(description='IceProd Server')
    parser.add_argument('-n','--non-daemon',dest='daemon',action='store_false',
                        default=True,help='Do not daemonize')
    parser.add_argument('action',nargs='?',type=str,default='start',
                        choices=['start','stop','kill','hardkill','restart'])
    parser.add_argument('--pidfile',type=str,default='$I3PROD/var/run/iceprod.pid',
                        help='PID lockfile')
    parser.add_argument('--umask',type=int,default=0o077, #octal
                        help='File creation umask')
    parser.add_argument('-c','--config-param',dest='config',action='append',type=key_val,
                        help='extra config params, ex: rest_api.auth_key=XXX')
    args = parser.parse_args()

    setup_I3PROD()

    run = partial(runner, config_params=args.config)

    if args.daemon:
        # now daemonize
        from iceprod.server.daemon import Daemon
        pidfile = os.path.expanduser(os.path.expandvars(args.pidfile))
        chdir = os.path.expanduser(os.path.expandvars('$I3PROD'))
        umask = args.umask
        run = partial(run, outfile=os.path.abspath('var/log/out'), errfile=os.path.abspath('var/log/err'))
        d = Daemon(pidfile, run,
                   chdir=chdir,
                   umask=umask,
                   stdout='var/log/out',
                   stderr='var/log/err')
        getattr(d,args.action)()
    else:
        if args.action != 'start':
            raise Exception('only daemon-mode can execute actions')
        else:
            run(stdout=True)

if __name__ == '__main__':
    main()
