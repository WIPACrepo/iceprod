"""
Test script for server init scripts
"""

import logging
import os

import iceprod.server

logger = logging.getLogger('server_init_test')


def test_30_salt():
    s = iceprod.server.salt()
    if not isinstance(s,str):
        raise Exception('not a string')

    for _ in range(5):
        for length in range(1,100):
            s = iceprod.server.salt(length)
            if len(s) != length:
                logger.info('len: %d. salt: %s',length,s)
                raise Exception('salt is not correct length')


def test_100_get_pkg_binary():
    ret = iceprod.server.get_pkg_binary('iceprod', 'loader.sh')
    assert ret != None
    assert os.path.exists(ret)
    assert ret.endswith('loader.sh')
