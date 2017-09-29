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

    def __init__(self,timeout=60.0,address=None,**kwargs):
        if address is None:
            raise Exception('need a valid address')
        self.__timeout = timeout
        self.__address = address
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
        body = json_encode({'jsonrpc':'2.0','method':methodname,'params':kwargs,'id':Client.newid()})

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
                if i < 2:
                    # try restarting connection, with backoff
                    self.close()
                    time.sleep(random.randint(i*2,(i+1)*30))
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
            return data['result']
        else:
            logger.info('result not in data: %r', data)
            return None

class MetaJSONRPC(type):
    """Metaclass for JSONRPC.  Allows for static class usage."""
    __rpc = None
    __timeout = None
    __address = None
    __passkey = None

    @classmethod
    def start(cls,timeout=None,address=None,passkey=None,**kwargs):
        """Start the JSONRPC Client."""
        if timeout is not None:
            cls.__timeout = timeout
        if address is not None:
            cls.__address = address
        if passkey is not None:
            cls.__passkey = passkey
        cls.__rpc = Client(timeout=cls.__timeout,address=cls.__address,
                           **kwargs)

    @classmethod
    def stop(cls):
        """Stop the JSONRPC Client."""
        cls.__rpc.close()
        cls.__rpc = None

    @classmethod
    def restart(cls):
        """Restart the JSONRPC Client."""
        cls.stop()
        cls.start()

    def __getattr__(cls,name):
        if cls.__rpc is None:
            raise Exception('JSONRPC connection not started yet')
        class _Method(object):
            def __init__(self,rpc,passkey,name):
                self.__rpc = rpc
                self.__name = name
                self.__passkey = passkey
            def __getattr__(self,name):
                return _Method(self.__rpc,"%s.%s"%(self.__name,name))
            def __call__(self,*args,**kwargs):
                # add passkey to arguments
                if 'passkey' not in kwargs:
                    kwargs['passkey'] = self.__passkey
                # jsonrpc can only handle args or kwargs, not both
                # so turn args into kwargs
                if len(args) > 0 and 'args' not in kwargs:
                    kwargs['args'] = args
                #return getattr(self.__rpc,self.__name)(**kwargs)
                return self.__rpc.request(self.__name,kwargs)
        return _Method(cls.__rpc,cls.__passkey,name)

class JSONRPC(object):
    """
    `JSON-RPC`_ client connection.

    Call RPC functions as regular function calls.

    Example::

        JSONRPC.set_task_status(task_id,'waiting')
    """
    __metaclass__ = MetaJSONRPC
    def __getattr__(self,name):
        return getattr(JSONRPC,name)