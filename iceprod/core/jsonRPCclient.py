"""
A simple jsonrpc client using `requests` for the http connection
"""
import logging
from threading import RLock

import requests

from iceprod.core.jsonUtil import json_encode,json_decode


class Client(object):
    """Raw JSONRPC client object"""
    id = 0
    idlock = RLock()

    def __init__(self,timeout=60.0,address=None,**kwargs):
        if address is None:
            raise Exception('need a valid address')
        # establish http session
        self.__session = requests.Session()
        if 'username' in kwargs and 'password' in kwargs:
            s.auth = (kwargs['username'], kwargs['password'])
        if 'sslcert' in kwargs:
            if 'sslkey' in kwargs:
                s.cert = (kwargs['sslcert'], kwargs['sslkey'])
            else:
                s.cert = kwargs['sslcert']
        if 'cacert' in kwargs:
            s.verify = kwargs['cacert']
        # save timeout
        self.__timeout = timeout
        # save address
        self.__address = address

    def close(self):
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
            logging.warning('cannot use RPC for private methods')
            raise Exception('Cannot use RPC for private methods')

        # translate request to json
        body = json_encode({'jsonrpc':'2.0','method':methodname,'params':kwargs,'id':Client.newid()})

        # make request to server
        data = None
        try:
            r = self.__session.post(self.__address, timeout=self.__timeout,
                    data=body, headers={'Content-Type': 'application/json-rpc'})
            r.raise_for_status()
            data = r.content
        except Exception as e:
            logging.warn('error making jsonrpc request: %r',e)
            raise

        # translate response from json
        if not data:
            return None
        try:
            data = json_decode(data)
        except:
            logging.info('json data: %r',data)
            raise

        if 'error' in data:
            try:
                raise Exception('Error %r: %r    %r'%data['error'])
            except:
                raise Exception('Error %r'%data['error'])
        if 'result' in data:
            return data['result']
        else:
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
    JSONRPC client connection.

    Call JSON-RPC functions as regular function calls.

    JSON-RPC spec: http://www.jsonrpc.org/specification

    Example::

        JSONRPC.set_task_status(task_id,'waiting')
    """
    __metaclass__ = MetaJSONRPC
    def __getattr__(self,name):
        return getattr(JSONRPC,name)
