"""
A simple REST json client using `requests`_ for the http connection.

.. _requests: http://docs.python-requests.org

The REST protocol is built on http(s), with the body containing
a json-encoded dictionary as necessary.
"""

import os
import logging
import asyncio

from .session import Session
from .jsonUtil import json_encode,json_decode

logger = logging.getLogger('rest_client')

class Client(object):
    def __init__(self, address, auth_key, timeout=60.0, backoff=True, **kwargs):
        self.address = address
        self.auth_key = auth_key
        self.timeout = timeout
        self.backoff = backoff
        self.kwargs = kwargs
        self.session = None

        self.open() # start session

    def open(self):
        """Open the http session"""
        logger.warning('establish REST http session')
        self.session = Session()
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
        logger.warning('close REST http session')
        self.session.close()

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

        # make request to server
        r = await asyncio.wrap_future(self.session.request(method, url, **kwargs))
        r.raise_for_status()

        # translate response from json
        if not r.content:
            logger.warning('request returned empty string')
            return None
        try:
           return json_decode(r.content)
        except Exception:
            logger.info('json data: %r', r.content)
            raise

    def request_seq(self, *args, **kwargs):
        """Sequential version of `request`."""
        loop = asyncio.get_event_loop()
        ret = loop.run_until_complete(self.request(*args, **kwargs))
        loop.close()
        return ret        
