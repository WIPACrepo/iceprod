"""
An interface to communicate with the master asyncronously.
"""

import os
import logging

# requre certifi for TLS cert verification
import certifi

from requests_futures.sessions import FuturesSession
import tornado.gen

from iceprod.core.jsonUtil import json_encode, json_decode

logger = logging.getLogger('master_communication')


@tornado.gen.coroutine
def send_master(cfg, method, session=None, **kwargs):
    """
    Send an asyncronous request to the master.

    This assumes an :ref:`tornado.ioloop.IOLoop` is already running.

    Args:
        cfg (dict): the main configuration dictionary
        method (str): Which method to call on the server
        **kwargs: Keyword arguments to pass to the master method

    Returns:
        str: Response from master
    """
    if ('master' not in cfg or 'url' not in cfg['master'] or
        not cfg['master']['url']):
        raise Exception('no master url, cannot communicate')

    if 'passkey' not in kwargs:
        if 'master' not in cfg or 'passkey' not in cfg['master']:
            raise Exception('no passkey')
        kwargs['passkey'] = cfg['master']['passkey']
        if 'site_id' not in kwargs and 'site_id' in cfg:
            kwargs['site_id'] = cfg['site_id']

    session = FuturesSession(session=session)

    url = cfg['master']['url']
    if url.endswith('/'):
        url += 'jsonrpc'
    else:
        url += '/jsonrpc'
    body = json_encode({'jsonrpc':'2.0',
                        'method':method,
                        'params':kwargs,'id':1})

    logger.info('calling method %s on master', method)
    response = yield session.post(url, timeout=60, data=body,
                                  headers={'Content-Type': 'application/json-rpc'})
    response.raise_for_status()
    ret = json_decode(response.content)
    if 'error' in ret:
        logger.warn('error receiving: %r',ret['error'])
        raise Exception('error: %r'%ret['error'])
    elif 'result' in ret:
        raise tornado.gen.Return(ret['result'])
    else:
        logger.warn('error receiving: no result')
        raise Exception('bad response')
    
