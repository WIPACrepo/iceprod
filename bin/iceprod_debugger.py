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

from IPython.terminal.embed import InteractiveShellEmbed

import iceprod.server.basic_config
import iceprod.server.RPCinternal

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
        'async':False,
    }
    messaging = iceprod.server.RPCinternal.RPCService(**kwargs)
    messaging.start()
    
    try:
        InteractiveShellEmbed(banner1='Launching ipython. The `messaging` object is available.')()
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
