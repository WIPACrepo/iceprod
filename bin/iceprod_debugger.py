#!/usr/bin/env python
"""
Cmd line debugger for IceProd
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import signal
import readline
import parser as code_parser
from functools import partial

from IPython.Shell import IPShellEmbed

import iceprod.server.basic_config
import iceprod.server.RPCinternal


def handle_stop(obj, signum, frame):
    obj.stop()
    sys.exit(1)
def handle_kill(obj, signum, frame):
    obj.kill()
    sys.exit(1)

def set_signals(obj):
    signal.signal(signal.SIGINT, partial(handle_stop,obj))
    signal.signal(signal.SIGQUIT, partial(handle_kill,obj))

def main(cfgfile):
    cfg = iceprod.server.basic_config.BasicConfig()
    cfg.read_file(cfgfile)
    class Response:
        pass
    kwargs = {
        'address':cfg.messaging_url,
        'block':False,
        'service_name':'debug',
        'service_class':Response(),
        'async':True,
    }
    messaging = iceprod.server.RPCinternal.RPCService(**kwargs)
    set_signals(messaging)
    messaging.start()
    
    try:
        IPShellEmbed(argv=[])()
    finally:
        messaging.stop()

if __name__ == '__main__':
    import argparse

    # Override values with cmdline options
    parser = argparse.ArgumentParser(description='IceProd Debugger')
    parser.add_argument('-f','--file',dest='file',type=str,default=None,
                        help='Path to config file')
    args = parser.parse_args()
    
    if args.file:
        cfgfile = os.path.expanduser(os.path.expandvars(args.file))
    else:
        cfgfile = iceprod.server.basic_config.locateconfig()
    
    main(cfgfile)
