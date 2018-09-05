"""
A simple REST json client using `requests`_ for the http connection.

.. _requests: http://docs.python-requests.org

The REST protocol is built on http(s), with the body containing
a json-encoded dictionary as necessary.
"""

import os
import logging
import asyncio

import requests

from .session import AsyncSession,Session
from .jsonUtil import json_encode,json_decode

class Client(object):
    def __init__(self, address, auth_key, timeout=60.0, backoff=True, **kwargs):
        self.address = address
        self.auth_key = auth_key
        self.timeout = timeout
        self.backoff = backoff
        self.kwargs = kwargs
        self.session = None

        self.open() # start session

    def open(self, sync=False):
        """Open the http session"""
        logging.warning('establish REST http session')
        if sync:
            self.session = Session()
        else:
            self.session = AsyncSession()
        self.session.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '+self.auth_key,
        }
        if 'username' in self.kwargs and 'password' in self.kwargs:
            self.session.auth = (self.kwargs['username'], self.kwargs['password'])
        if 'sslcert' in self.kwargs:
            if 'sslkey' in self.kwargs:
                self.session.cert = (self.kwargs['sslcert'], self.kwargs['sslkey'])
            else:
                self.session.cert = self.kwargs['sslcert']
        if 'cacert' in self.kwargs:
            self.session.verify = self.kwargs['cacert']

    def close(self):
        """Close the http session"""
        logging.warning('close REST http session')
        if self.session:
            self.session.close()

    def _prepare(self, method, path, args=None):
        """Internal method for preparing requests"""
        if not args:
            args = {}
        if path.startswith('/'):
            path = path[1:]
        url = os.path.join(self.address, path)
        kwargs = {
            'timeout': self.timeout,
        }
        if method in ('GET','HEAD'):
            # args should be urlencoded
            kwargs['params'] = args
        else:
            kwargs['json'] = args
        return (url, kwargs)

    def _decode(self, content):
        """Internal method for translating response from json"""
        if not content:
            logging.warning('request returned empty string')
            return None
        try:
           return json_decode(content)
        except Exception:
            logging.info('json data: %r', content)
            raise

    async def request(self, method, path, args=None):
        """
        Send request to REST Server.

        Async request - use with coroutines.

        Args:
            method (str): the http method
            path (str): the url path on the server
            args (dict): any arguments to pass

        Returns:
            dict: json dict or raw string
        """
        url, kwargs = self._prepare(method, path, args)
        try:
            r = await asyncio.wrap_future(self.session.request(method, url, **kwargs))
            r.raise_for_status()
            return self._decode(r.content)
        except Exception:
            logging.info('bad request: %s %s %r', method, path, args, exc_info=True)
            raise

    def request_seq(self, method, path, args=None):
        """
        Send request to REST Server.

        Sequential version of `request`.

        Args:
            method (str): the http method
            path (str): the url path on the server
            args (dict): any arguments to pass

        Returns:
            dict: json dict or raw string
        """
        url, kwargs = self._prepare(method, path, args)
        s = self.session
        try:
            self.open(sync=True)
            r = self.session.request(method, url, **kwargs)
            r.raise_for_status()
            return self._decode(r.content)
        finally:
            self.session = s
