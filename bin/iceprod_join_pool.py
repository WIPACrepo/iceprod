#!/usr/bin/env python
"""
Join this site to a pool
"""

from __future__ import absolute_import, division, print_function

import os
import sys
from functools import partial

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
        'service_name':'join_pool',
        'service_class':Response(),
        'async':False,
    }
    messaging = iceprod.server.RPCinternal.RPCService(**kwargs)
    messaging.start()

    master_url = raw_input('master_url:')
    passkey = raw_input('passkey:')
    messaging.config.set(key='master',
                         value={'status':'','url':master_url,'passkey':passkey})


if __name__ == '__main__':
    import argparse

    # Override values with cmdline options
    parser = argparse.ArgumentParser(description='IceProd Server')
    parser.add_argument('-f','--file',dest='file',type=str,default=None,
                        help='Path to config file')
    args = parser.parse_args()

    if args.file:
        cfgfile = os.path.expanduser(os.path.expandvars(args.file))
    else:
        cfgfile = iceprod.server.basic_config.locateconfig()

    main(cfgfile)
