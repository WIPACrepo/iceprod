"""
A simple `JSON-RPC`_ client using `requests`_ for the http connection.

.. _JSON-RPC: http://www.jsonrpc.org/specification
.. _requests: http://docs.python-requests.org

The RPC protocol is built on http(s), with the body containing
a json-encoded dictionary:

Request Object:

* method (string) - Name of the method to be invoked.
* params (dict) - Keyword arguments to the method.

Response Object:

* result (object) - The returned result from the method. This is REQUIRED on
  success, and MUST NOT exist if there was an error.
* error (object) - A description of the error, likely an Exception object.
  This is REQUIRED on error and MUST NOT exist on success.
"""
import logging
import time
import random
from threading import RLock

import requests

from iceprod.core.jsonUtil import json_encode,json_decode

logger = logging.getLogger('jsonrpc')

class Client(object):
    """Raw JSONRPC client object"""
    id = 0
    idlock = RLock()

    def __init__(self,timeout=60.0,address=None,backoff=True,**kwargs):
        if address is None:
            raise Exception('need a valid address')
        self.__timeout = timeout
        self.__address = address
        self.__backoff = backoff
        self.__kwargs = kwargs
        self.open() # start session

    def open(self):
        """Open the http session"""
        logger.warn('establish http session for jsonrpc')
        self.__session = requests.Session()
        if 'username' in self.__kwargs and 'password' in self.__kwargs:
            self.__session.auth = (self.__kwargs['username'], self.__kwargs['password'])
        if 'sslcert' in self.__kwargs:
            if 'sslkey' in self.__kwargs:
                self.__session.cert = (self.__kwargs['sslcert'], self.__kwargs['sslkey'])
            else:
                self.__session.cert = self.__kwargs['sslcert']
        if 'cacert' in self.__kwargs:
            self.__session.verify = self.__kwargs['cacert']

    def close(self):
        """Close the http session"""
        logger.warn('close jsonrpc http session')
        self.__session.close()

    @classmethod
    def newid(cls):
        cls.idlock.acquire()
        id = cls.id
        cls.id += 1
        cls.idlock.release()
        return id

    def request(self,methodname,kwargs):
        """Send request to RPC Server"""
        # check method name for bad characters
        if methodname[0] == '_':
            logger.warning('cannot use RPC for private methods')
            raise Exception('Cannot use RPC for private methods')

        # translate request to json
        body = json_encode({'jsonrpc': '2.0',
                            'method': methodname,
                            'params': kwargs,
                            'id': Client.newid(),
                           }).encode('utf-8')

        # make request to server
        data = None
        for i in range(10):
            try:
                r = self.__session.post(self.__address, timeout=self.__timeout,
                        data=body, headers={'Content-Type': 'application/json-rpc'})
                r.raise_for_status()
                data = r.content
                break
            except Exception:
                logger.warn('error making jsonrpc request for %s', methodname)
                if self.__backoff and i < 2:
                    # try restarting connection, with backoff
                    self.close()
                    sleep_time = random.randint(i*2,(i+1)*30)
                    if isinstance(self.__backoff, (int,float)):
                        sleep_time *= self.__backoff
                    time.sleep(sleep_time)
                    self.open()
                else:
                    raise

        # translate response from json
        if not data:
            logger.warn('request returned empty string')
            return None
        try:
            data = json_decode(data)
        except Exception:
            logger.info('json data: %r',data)
            raise

        if 'error' in data:
            logger.warn('error: %r', data['error'])
            try:
                raise Exception('Error %r: %r    %r'%data['error'])
            except Exception:
                raise Exception('Error %r'%data['error'])
        if 'result' in data:
            if isinstance(data['result'], Exception):
                raise data['result']
            return data['result']
        else:
            logger.info('result not in data: %r', data)
            return None

class JSONRPC:
    """`JSON-RPC`_ client connection.

    Call RPC functions as regular function calls.

    Example::

        rpc = JSONRPC('http://my.server/jsonrpc')
        rpc.set_task_status(task_id,'waiting')
    """
    def __init__(self, address=None, timeout=None, passkey=None, **kwargs):
        """Start the JSONRPC Client."""
        self._address = address
        self._timeout = timeout
        self._passkey = passkey
        self._rpc = None

        self.start(**kwargs)

    def start(self, **kwargs):
        self._rpc = Client(timeout=self._timeout,
                           address=self._address,
                           **kwargs)
        try:
            ret = self._rpc.request('echo', {'value':'e', 'passkey':self._passkey})
        except Exception as e:
            logger.error('error',exc_info=True)
            self.stop()
            raise Exception('JSONRPC communcation did not start.  '
                            'url=%s and passkey=%s'%(self._address,self._passkey))
        if ret != 'e':
            self.stop()
            raise Exception('JSONRPC communication error when starting - '
                            'echo failed (%r).  url=%s and passkey=%s'
                            %(ret,self._address,self._passkey))

    def stop(self):
        """Stop the JSONRPC Client."""
        self._rpc.close()
        self._rpc = None

    def restart(self):
        """Restart the JSONRPC Client."""
        self._rpc.close()
        self._rpc.open()

    def __getattr__(self, name):
        if self._rpc is None:
            raise Exception('JSONRPC connection not started yet')
        class _Method(object):
            def __init__(self, rpc, passkey, name):
                self.rpc = rpc
                self.name = name
                self.passkey = passkey
            def __getattr__(self, name):
                return _Method(self.rpc, self.passkey, "%s.%s"%(self.name,name))
            def __call__(self, *args, **kwargs):
                # add passkey to arguments
                if 'passkey' not in kwargs:
                    kwargs['passkey'] = self.passkey
                # jsonrpc can only handle args or kwargs, not both
                # so turn args into kwargs
                if len(args) > 0 and 'args' not in kwargs:
                    kwargs['args'] = args
                return self.rpc.request(self.name,kwargs)
        return _Method(self._rpc,self._passkey,name)
