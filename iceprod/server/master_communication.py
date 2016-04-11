"""
An interface to communicate with the master asyncronously.
"""

import os
import logging

import certifi

from iceprod.core.jsonUtil import json_encode, json_decode
from tornado.httpclient import AsyncHTTPClient

logger = logging.getLogger('master_communication')

def send_master(cfg,method,callback=None,**kwargs):
    """
    Send an asyncronous request to the master.

    This assumes an :ref:`tornado.ioloop.IOLoop` is already running.

    If a callback is provided, it is given either an exception
    or the response result as an argument.

    :param cfg: the main configuration dictionary
    :param method: Which method to call on the server
    :param callback: Callback to call when finished (optional)
    :param **kwargs: Keyword arguments to pass to the master method
    :returns: A `Future` with the :ref:`tornado.httpclient.HTTPResonse`
    """
    if ('master' not in cfg or 'url' not in cfg['master'] or
        not cfg['master']['url']):
        raise Exception('no master url, cannot communicate')

    if callback:
        def cb(response):
            if response.error:
                logger.warn('error receiving: http error: %r',
                            response.error)
                callback(Exception('http error: %r'%response.error))
                return
            ret = json_decode(response.body)
            if 'error' in ret:
                logger.warn('error receiving: %r',ret['error'])
                callback(Exception('error: %r'%ret['error']))
            elif 'result' in ret:
                callback(ret['result'])
            else:
                logger.warn('error receiving: no result')
                callback(Exception('bad response'))

    if 'passkey' not in kwargs:
        if 'master' not in cfg or 'passkey' not in cfg['master']:
            raise Exception('no passkey')
        kwargs['passkey'] = cfg['master']['passkey']

    args = {'method': 'POST',
            'connect_timeout': 30,
            'request_timeout': 120,
            'validate_cert': True,
            'ca_certs': certifi.where()}
    if callback:
        args['callback'] = cb
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

    return http_client.fetch(url,**args)