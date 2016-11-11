"""
An interface to communicate with the master asyncronously.
"""

import os
import logging

import certifi

from iceprod.core.jsonUtil import json_encode, json_decode
from tornado.httpclient import AsyncHTTPClient
import tornado.gen

logger = logging.getLogger('master_communication')

@tornado.gen.coroutine
def send_master(cfg,method,**kwargs):
    """
    Send an asyncronous request to the master.

    This assumes an :ref:`tornado.ioloop.IOLoop` is already running.

    :param cfg: the main configuration dictionary
    :param method: Which method to call on the server
    :param **kwargs: Keyword arguments to pass to the master method
    :returns: Response from master
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

    args = {'method': 'POST',
            'connect_timeout': 30,
            'request_timeout': 120,
            'validate_cert': True,
            'ca_certs': certifi.where()}
    http_client = AsyncHTTPClient()
    url = cfg['master']['url']
    if url.endswith('/'):
        url += 'jsonrpc'
    else:
        url += '/jsonrpc'
    body = json_encode({'jsonrpc':'2.0',
                        'method':method,
                        'params':kwargs,'id':1})
    args['body'] = body

    response = yield http_client.fetch(url,**args)
    response.rethrow()
    ret = json_decode(response.body)
    if 'error' in ret:
        logger.warn('error receiving: %r',ret['error'])
        raise Exception('error: %r'%ret['error'])
    elif 'result' in ret:
        raise tornado.gen.Return(ret['result'])
    else:
        logger.warn('error receiving: no result')
        raise Exception('bad response')
    
