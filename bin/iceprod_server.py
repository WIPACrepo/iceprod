#!/usr/bin/env python
"""
Server process for running remote batch jobs.
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import time
from threading import Event
import multiprocessing
from functools import partial
import logging
import logging.config
import signal
import importlib

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(name):
        pass

# Assuming that the directory structure remains constant, this will automatically set the python path to find iceprod
bin_dir = os.path.dirname( os.path.abspath(sys.argv[0]) )
root_path = os.path.dirname( bin_dir )
if not root_path in sys.path: sys.path.append( root_path )

data_class_path = os.path.join( root_path, 'iceprod/server/data/www/dataclasses.js')
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

import iceprod
import iceprod.server
import iceprod.server.basic_config
import iceprod.server.RPCinternal
import iceprod.core.logger

def check_module(name, message = '', required = False):
    try:
        importlib.import_module(name)
    except ImportError:
        print ('Cannot import python module %s. %s' % (name, message))
        if required:
            print('Required module "%s" not found. Exiting...' % name)
            exit(0)

def check_dependencies():
    check_module('apsw', 'SQLite database will not be available.')
    check_module('setproctitle', 'Will not be able to set process title.')
    check_module('tornado', required = True)
    check_module('zmq', required = True)
    check_module('jsonschema')
    check_module('concurrent.futures', required = True)


check_dependencies()


def load_config(cfgfile):
    """Load the cfg from file or directly"""
    if isinstance(cfgfile,str):
        # Read config file
        logging.warn('loading new cfg from file %s'%cfgfile)
        cfg = iceprod.server.basic_config.BasicConfig()
        cfg.read_file(cfgfile)
        return cfg
    else:
        # assume we were passed cfg data directly
        logging.warn('loading new cfg directly')
        return cfgfile

def set_logger(cfg):
    """Setup the root logger"""
    iceprod.core.logger.setlogger('logfile',cfg)

    # remove stdout logging handler, if present
    iceprod.core.logger.removestdout()


class server_module():
    """Manage a server module"""
    process_class = multiprocessing.Process

    def __init__(self,mod_name,cfg):
        self.mod_name = mod_name
        self.process = server_module.process_class(
                target=iceprod.server.run_module,
                args=[mod_name,cfg])
        self.process.daemon = True

    def start(self):
        self.process.start()

    def kill(self):
        self.process.terminate()

def handle_stop(obj, signum, frame):
    logging.warn('Signal handle_stop called with signal %s' % signum)
    logging.warn('Stopping...')
    obj.stop()
def handle_kill(obj, signum, frame):
    logging.warn('Signal handle_kill called with signal %s' % signum)
    logging.warn('Killing...')
    obj.kill()

def set_signals(obj):
    signal.signal(signal.SIGINT, partial(handle_stop,obj))
    signal.signal(signal.SIGQUIT, partial(handle_kill,obj))

def main(cfgfile,cfgdata=None):

    if cfgdata:
        # use the config data directly
        cfg = load_config(cfgdata)
    else:
        # Read config file
        cfg = load_config(cfgfile)

    # set logger
    set_logger(cfg)
    logger = logging.getLogger('iceprod_server')

    # Change name of process for ps
    try:
        setproctitle('iceprod_server.main')
    except Exception:
        logger.warn("could not rename process")

    def module_start(mod):
        logger.warn('starting %s',mod)
        rmod = server_module(mod,cfg)
        rmod.start()
        return rmod

    # start messaging
    class Respond():
        def __init__(self,running_modules):
            self.running_modules = running_modules
            self.broadcast_ignore = set()

        def start(self,mod=None,callback=None):
            if 'START' in self.broadcast_ignore:
                self.broadcast_ignore.remove('START')
                if callback:
                    callback()
                return
            logger.warn('START %s',mod if mod else '')
            if mod:
                if '.' not in mod:
                    mod = 'iceprod.server.modules.'+mod
                mod_name = mod.rsplit('.',1)[-1]
                if mod_name in self.running_modules:
                    getattr(messaging,mod_name).start(async=True)
                else:
                    self.running_modules[mod_name] = module_start(mod)
            else:
                self.broadcast_ignore.add('START')
                messaging.BROADCAST.start(async=True)
            if callback:
                callback()

        def stop(self,mod=None,callback=None):
            if 'STOP' in self.broadcast_ignore:
                self.broadcast_ignore.remove('STOP')
                if callback:
                    callback()
                return
            logger.warn('STOP %s',mod if mod else '')
            if mod:
                mod_name = mod.rsplit('.',1)[-1]
                getattr(messaging,mod).stop(async=True)
                if mod_name in self.running_modules:
                    del self.running_modules[mod_name]
                if callback:
                    callback()
            else:
                self.broadcast_ignore.add('STOP')
                if callback:
                    callback()
                def cb(*args):
                    logger.debug('joining stopped modules')
                    try:
                        for mod in self.running_modules:
                            if mod == 'messaging':
                                continue
                            self.running_modules[mod].process.join(1)
                            if self.running_modules[mod].process.is_alive():
                                getattr(messaging,mod).kill(async=True)
                            else:
                                del self.running_modules[mod]
                        for mod in self.running_modules:
                            if mod == 'messaging':
                                continue
                            self.running_modules[mod].process.join(.1)
                            del self.running_modules[mod]
                    except Exception:
                        logger.warn('error joining modules',exc_info=True)
                    finally:
                        messaging.SERVER.stop(timeout=1,callback=cb2)
                def cb2(*args):
                    logger.debug('joining messaging module')
                    try:
                        if 'messaging' in self.running_modules:
                            self.running_modules['messaging'].process.join(.5)
                            del self.running_modules['messaging']
                    except Exception:
                        logger.warn('error joining messaging module',exc_info=True)
                    finally:
                        logger.debug('kill messaging')
                        messaging.kill()
                messaging.BROADCAST.stop(timeout=1,callback=cb)

        def restart(self,mod=None,callback=None):
            if 'RESTART' in self.broadcast_ignore:
                self.broadcast_ignore.remove('RESTART')
                if callback:
                    callback()
                return
            logger.warn('RESTART %s',mod if mod else '')
            if mod:
                getattr(messaging,mod).restart(async=True)
            else:
                self.broadcast_ignore.add('RESTART')
                messaging.BROADCAST.restart(async=True)
            if callback:
                callback()

        def kill(self,mod=None,callback=None):
            if 'KILL' in self.broadcast_ignore:
                self.broadcast_ignore.remove('KILL')
                if callback:
                    callback()
                return
            logger.warn('KILL %s',mod if mod else '')
            if mod:
                mod_name = mod.rsplit('.',1)[-1]
                getattr(messaging,mod).kill(async=False,timeout=0.1)
                if mod_name in self.running_modules:
                    self.running_modules[mod_name].process.join(.1)
                    if self.running_modules[mod_name].process.is_alive():
                        self.running_modules[mod_name].kill()
                    del self.running_modules[mod_name]
                if callback:
                    callback()
            else:
                self.broadcast_ignore.add('KILL')
                if callback:
                    callback()
                def cb(*args):
                    logger.debug('joining killed modules')
                    try:
                        for mod in self.running_modules:
                            if mod == 'messaging':
                                continue
                            self.running_modules[mod].process.join(.1)
                            del self.running_modules[mod]
                    except Exception:
                        logger.warn('error joining modules',exc_info=True)
                    finally:
                        messaging.SERVER.kill(timeout=0.1,callback=cb2)
                def cb2(*args):
                    logger.debug('joining messaging module')
                    try:
                        if 'messaging' in self.running_modules:
                            self.running_modules['messaging'].process.join(.05)
                            del self.running_modules['messaging']
                    except Exception:
                        logger.warn('error joining messaging module',exc_info=True)
                    finally:
                        logger.debug('kill messaging')
                        messaging.kill()
                messaging.BROADCAST.kill(timeout=0.1,callback=cb)

    running_modules = {}
    respond_obj = Respond(running_modules)

    # set signal handlers
    set_signals(respond_obj)

    # setup messaging
    kwargs = {'address':cfg.messaging_url,
              'service_name':'daemon',
              'service_class':respond_obj,
              'immediate_setup':False,
             }
    messaging = iceprod.server.RPCinternal.RPCService(**kwargs)

    # get modules
    available_modules = {}
    for mod in iceprod.server.listmodules('iceprod.server.modules'):
        mod_name = mod.rsplit('.',1)[1]
        available_modules[mod_name] = mod
    logger.info('available modules: %s',available_modules.keys())

    # start modules specified in cfg
    for mod in cfg.start_order:
        if mod in available_modules and getattr(cfg,mod) is True:
            running_modules[mod] = module_start(available_modules[mod])
            time.sleep(1) # wait 1 second between starts

    # wait for messages
    messaging.setup()
    messaging.start()

    # terminate all modules
    for mod in running_modules.values():
        mod.kill()

    logger.warn('shutdown')

if __name__ == '__main__':
    import argparse

    # Override values with cmdline options
    parser = argparse.ArgumentParser(description='IceProd Server')
    parser.add_argument('-f','--file',dest='file',type=str,default=None,
                        help='Path to config file')
    parser.add_argument('-d','--daemon',dest='daemon',action='store_true',
                        help='Daemonize?')
    parser.add_argument('action',nargs='?',type=str,default='start',
                        choices=['start','stop','kill','hardkill','restart'])
    parser.add_argument('--pidfile',type=str,default='$I3PROD/var/run/iceprod.pid',
                        help='PID lockfile')
    parser.add_argument('--umask',type=int,default=077,
                        help='File creation umask')
    args = parser.parse_args()

    if args.file:
        cfgfile = os.path.expanduser(os.path.expandvars(args.file))
    else:
        cfgfile = iceprod.server.basic_config.locateconfig()
    cfgdata = None

    # start iceprod
    if args.daemon:
        if args.action in ('start','restart'):
            # try loading cfgfile before daemonizing to catch the bad cfgfile error
            cfgdata = load_config(cfgfile)

        # now daemonize
        from iceprod.server.daemon import Daemon
        pidfile = os.path.expanduser(os.path.expandvars(args.pidfile))
        chdir = os.path.expanduser(os.path.expandvars('$I3PROD'))
        umask = args.umask
        d = Daemon(pidfile,partial(main,cfgfile,cfgdata),
                   chdir=chdir,
                   umask=umask,
                   stdout='var/log/out',
                   stderr='var/log/err')
        getattr(d,args.action)()
    else:
        main(cfgfile)

