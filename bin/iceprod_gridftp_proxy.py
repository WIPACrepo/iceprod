#!/usr/bin/env python
"""
Set gridftp proxy.
"""

from __future__ import absolute_import, division, print_function

import os
import sys
import logging
import argparse
import getpass

# Assuming that the directory structure remains constant, this will automatically set the python path to find iceprod
bin_dir = os.path.dirname( os.path.abspath(sys.argv[0]) )
root_path = os.path.dirname( bin_dir )
if not root_path in sys.path: sys.path.append( root_path )

from iceprod.server.gridftp import SiteGlobusProxy

def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='IceProd Gridftp Proxy Helper')
    parser.add_argument('-f','--cfgfile',dest='file',type=str,default=None,
                        help='Path to gridftp config file')
    args = parser.parse_args()

    p = SiteGlobusProxy(cfgfile=args.file)
    passphrase = getpass.getpass('passphrase: ')
    p.set_passphrase(passphrase)
    try:
        p.update_proxy()
    except Exception as e:
        logging.error('proxy error', exc_info=True)

if __name__ == '__main__':
    main()
