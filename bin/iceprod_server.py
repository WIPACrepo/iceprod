#!/usr/bin/env python
"""
Server process for running remote batch jobs.
"""

import os
import sys
import time
from threading import Event
import multiprocessing
from functools import partial
import logging
import logging.config
import signal

import iceprod
import iceprod.server
try:
    import iceprod.procname
except ImportError:
    print('Could not import procname module')
import iceprod.core.logger


def load_config(cfgfile):
    """Load the cfg from file or directly"""
    if isinstance(cfgfile,str):
        # Read config file
        logging.warn('loading new cfg from file %s'%cfgfile)
        return iceprod.server.getconfig(cfgfile)
    else:
        # assume we were passed cfg data directly
        logging.warn('loading new cfg directly'%cfgfile)
        return cfgfile

def set_logger():
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

def handle_stop(msg, signum, frame):
    logging.warn('Signal handle_stop called with signal %s' % signum)
    logging.warn('Stopping...')
    msg.BROADCAST.stop()
    time.sleep(1)
    msg.SERVER.stop()
def handle_kill(msg, signum, frame):
    logging.warn('Signal handle_kill called with signal %s' % signum)
    logging.warn('Killing...')
    msg.BROADCAST.kill()

def set_signals(msg):
    signal.signal(signal.SIGINT, partial(handle_stop,msg))
    signal.signal(signal.SIGQUIT, partial(handle_kill,msg))

def main(cfgfile,cfgdata=None):
    
    if cfgdata:
        # use the config data directly
        cfg = load_config(cfgdata)
    else:
        # Read config file
        cfg = load_config(cfgfile)
    
    # set logger
    set_logger()
    logger = logging.getLogger('iceprod_server')
    
    # Change name of process for ps
    try:
        iceprod.procname.setprocname('iceprod_server.main')
    except:
        logger.warn("Could not import procname module.")
        logger.warn("Will not be able to set process name for daemon")
    
    # start messaging
    class Respond():
        def __init__(self,shutdown_event):
            self.shutdown_event = shutdown_event
        def start(self,mod=None,callback=None):
            logger.warn('START %s',mod if mod else '')
            if mod:
                getattr(messaging,mod).start(asyc=True)
            else:
                messaging.BROADCAST.start(async=True)
            if callback:
                callback()
        def stop(self,mod=None,callback=None):
            logger.warn('STOP %s',mod if mod else '')
            if mod:
                getattr(messaging,mod).stop(asyc=True)
            else:
                messaging.BROADCAST.stop(async=True)
            if callback:
                callback()
        def restart(self,mod=None,callback=None):
            logger.warn('RESTART %s',mod if mod else '')
            if mod:
                getattr(messaging,mod).restart(asyc=True)
            else:
                messaging.BROADCAST.restart(async=True)
            if callback:
                callback()
        def kill(self,mod=None,callback=None):
            logger.warn('KILL %s',mod if mod else '')
            if mod:
                getattr(messaging,mod).stop(asyc=True)
                if callback:
                    callback()
            else:
                messaging.BROADCAST.stop()
                time.sleep(1)
                messaging.SERVER.stop()
                time.sleep(0.1)
                if callback:
                    callback()
                shutdown.set()
        def shutdown(self,callback=None):
            logger.warn('SHUTDOWN')
            messaging.BROADCAST.stop()
            time.sleep(1)
            messaging.SERVER.stop()
            time.sleep(0.1)
            if callback:
                callback()
            shutdown.set()
    shutdown = Event()
    shutdown.clear()
    kwargs = {'address':self.cfg['messaging']['address'],
              'service_name':'daemon',
              'service_class':Respond(shutdown),
             }
    messaging = RPCinternal.RPCService(**kwargs)
    messaging.start()
    
    # set signal handlers
    set_signals(messaging)
    
    # get modules
    available_modules = iceprod.server.listmodules('iceprod.server.modules')
    running_modules = []

    # start modules specified in cfg
    start_order = cfg['server_modules']['start_order']
    def start_order_cmp(a,b):
        try:
            a_pos = start_order.index(a.rsplit('.')[1])
            b_pos = start_order.index(a.rsplit('.')[1])
            return cmp(a_pos,b_pos)
        except:
            return cmp(a,b)
    for mod in sorted(available_modules,cmp=start_order_cmp):
        mod_name = mod.rsplit('.',1)[1]
        if mod_name in cfg['server_modules'] and cfg['server_modules'][mod_name] is True:
            logger.warn('starting %s',mod_name)
            rmod = server_module(mod)
            rmod.start()
            running_modules.append(rmod)
            time.sleep(1) # wait 1 second between starts
    
    # idle until we need to shutdown
    shutdown.wait()

if __name__ == '__main__':
    import argparse

    # Override values with cmdline options
    parser = argparse.ArgumentParser(description='IceProd Server')
    parser.add_argument('-f','--file',dest='file',type=str,default=None)
    parser.add_argument('-d','--daemon',dest='daemon',action='store_true')
    parser.add_argument('action',nargs='?',type=str,default='start',
                        choices=['start','stop','kill','hardkill','restart'])
    parser.add_argument('--pidfile',type=str,default='$I3PROD/var/run/iceprod.pid')
    parser.add_argument('--umask',type=int,default=077)
    args = parser.parse_args()
    
    if args.file:
        if args.daemon and args.action == 'reload':
            raise Exception('Cannot change cfgfile path on reload. Try restarting.')
        cfgfile = os.path.expanduser(os.path.expandvars(args.file))
    else:
        cfgfile = iceprod.server.locateconfig()
    cfgdata = None
    
    # start iceprod
    if args.daemon:
        if args.action in ('start','restart'):
            # try loading cfgfile before daemonizing to catch the bad cfgfile error
            cfgdata = iceprod.server.getconfig(cfgfile)
        
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

