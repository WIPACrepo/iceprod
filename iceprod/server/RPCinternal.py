"""
  RPC calls over ZeroMQ
"""

import os
import sys
import random
import time
from collections import OrderedDict, namedtuple
from threading import Thread,RLock,Event
from functools import partial
from contextlib import contextmanager
import logging

from iceprod.server.zeromq import AsyncSendReceive

logger = logging.getLogger('RPCinternal')

class Serializer():
    """
    A (de)serializer that wraps a certain serialization protocol.
    
    Currently it only supports the standard pickle protocol.
    """
    try:
        import cPickle as pickle
    except ImportError:
        import pickle
    @classmethod
    def serialize(cls, data):
        """Serialize the given data object."""
        return cls.pickle.dumps(data, cls.pickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls, data):
        """Deserializes the given data."""
        return cls.pickle.loads(data)

class MessageFactory():
    """
    A message factory for creating and parsing binary messages.
    
    See the Python library `struct`_ module for info about struct packing.
    
    .. _struct: https://docs.python.org/2/library/struct.html#format-characters
    """
    import binascii,struct,ctypes
    
    # big endian (id, version, sequenceNumber, bodyLen, bodyChecksum,
    #             messageType, headerChecksum)
    HEADERFMT = '!4sBLLLBL'
    HEADERSIZE = struct.calcsize(HEADERFMT)
    # big endian (id, version, sequenceNumber, bodyLen, bodyChecksum,
    #             messageType, serviceLength, serviceName, headerChecksum)
    HEADERFMT_SERVICE = '!4sBLLLBB%dsL'
    ID_TAG = b'IRPC'
    PROTOCOL_VERSION=1
    MAX_SEQ = 4294967295
    MAX_SERVICE = 255
    class MESSAGE_TYPE:
        """Enum:"""
        SERVER, BROADCAST, BROADCAST_ACK, SERVICE, RESPONSE = range(5)
    
    @classmethod
    def getChecksum(cls,data):
        """Get 4 byte checksum"""
        return cls.binascii.crc32(data) & 0xffffffff
        
    @classmethod
    def verifyChecksum(cls,data,checksum):
        """Verify a checksum"""
        return (cls.getChecksum(data) == checksum)
    
    @classmethod
    def createMessage(cls,data,seq,msg_type,service_name=None,serialized=False):
        """
        Convert data to binary (creating header in the process).
        
        :param data: the data to convert
        :param seq: the sequence number
        :param msg_type: the message type
        :param service_name: the service name, optional
        :param serialized: Is the data already serialized? optional,
                defaults to True
        :returns: binary string
        """
        if not serialized:
            data = Serializer.serialize(data)
        if msg_type == cls.MESSAGE_TYPE.SERVICE:
            if not service_name:
                raise Exception('SERVICE message type, but no service name provided')
            header_fmt = cls.HEADERFMT_SERVICE % len(service_name)
            header_size = cls.struct.calcsize(header_fmt)
            args = (cls.ID_TAG, cls.PROTOCOL_VERSION, seq, len(data),
                    cls.getChecksum(data), msg_type,
                    len(service_name), service_name)
        elif msg_type <= cls.MESSAGE_TYPE.RESPONSE:
            header_fmt = cls.HEADERFMT
            header_size = cls.HEADERSIZE
            args = (cls.ID_TAG, cls.PROTOCOL_VERSION, seq, len(data),
                    cls.getChecksum(data), msg_type)
        else:
            raise Exception('invalid message type')
        
        header = cls.ctypes.create_string_buffer(header_size)
        cls.struct.pack_into(header_fmt[:-1],header,0,*args)
        headerChk = cls.getChecksum(header[:cls.HEADERSIZE-4])
        cls.struct.pack_into('!L',header,cls.HEADERSIZE-4,headerChk)
        return [header.raw,data]
    
    @classmethod
    def parseMessageHeader(cls,header):
        """
        Parse the header
        
        :param header: the binary header to parse
        :returns: {sequence_number, body_length, body_checksum, message_type,
                   service_name}
        """
        if not headerData or len(headerData) < cls.HEADERSIZE:
            raise Exception('header data size too small')
        parts = cls.struct.unpack_from(cls.HEADERFMT[:-1], headerData)
        id,ver,seq,body_len,body_chk,msg_type = parts
        if id != cls.ID_TAG or ver != cls.PROTOCOL_VERSION:
            raise Exception('invalid data or unsupported protocol version')
        
        service = None
        if msg_type == cls.MESSAGE_TYPE.SERVICE:
            service_len = cls.struct.unpack_from('!B',headerData,
                                                 cls.HEADERSIZE-4)[0]
            header_size = cls.struct.calcsize(cls.HEADERFMT_SERVICE%service_len)
            service = cls.struct.unpack_from('!%ds'%service_len,headerData,
                                             cls.HEADERSIZE-3)[0]
        elif msg_type <= cls.MESSAGE_TYPE.RESPONSE:
            header_size = cls.HEADERSIZE
        else:
            raise Exception('invalid message type')
        
        # check header checksum
        headerChk = cls.struct.unpack_from('!L',headerData,header_size-4)[0]
        expectedHChk = cls.getChecksum(headerData[:header_size-4])
        if headerChk != expectedHChk:
            raise Exception('header checksum mismatch: expected %s but got %s'%(expectedHChk,headerChk))
        return {'sequence_number':seq,
                'body_length':body_len,
                'body_checksum':body_chk,
                'message_type':msg_type,
                'service_name':service,
               }
    
    @classmethod
    def getMessage(cls,body,header):
        """
        Parse the body.
        
        :param body: raw message body
        :param header: decoded message header
        :returns: decoded data
        """
        if not body:
            raise Exception('body data not given')
        if not header:
            raise Exception('header is empty')
        if not header or len(header) != 5: # check header data validity
            raise Exception('invalid header data')
        if not body:
            raise Exception('bodyData missing')
        if len(body) < header['body_length']:
            raise Exception('body data size mismatch: expected %s but got %s'%(
                            header['body_length'],len(body)))
        body_chksum = cls.getChecksum(body)
        if header['body_checksum'] != body_chksum:
            raise Exception('body checksum mismatch: expected %s but got %s'%(
                            header['body_checksum'],body_chksum))
        return Serializer.deserialize(body)

class Base(AsyncSendReceive):
    """
    ZMQ messaging base.
    
    :param history_length: Amount of history to keep track of, optional
    :param address: Address to connect to
    """
    def __init__(self,history_length=10000,**kwargs):
        super(Base,self).__init__(**kwargs)
        
        # set some variables
        self.history_length = history_length
        self.send_history = OrderedDict()
        self.send_history_lock = RLock()
        self.recv_seq_history = OrderedDict()
        self.recv_seq_history_lock = RLock()
    
    def send(self, data, serialized=False, seq=None, timeout=60.0,
             client_id=None, type=MessageFactory.MESSAGE_TYPE.SERVICE,
             callback=None):
        """
        Send a message.
        
        :param data: The actual data to send
        :param serialzed: Is the data already serialized?, optional
        :param seq: The sequence number to respond to, optional
        :param timeout: The timeout for retrying, optional
        :param client_id: The client id to send to, optional
        :param type: The type of message to send, optional
        :param callback: The callback function, optional
        """
        # check stream
        if self.stream.closed():
            # try to reconnect
            self.restart()
        
        if client_id:
            message = [client_id]
        else:
            message = []
        
        old_request = None
        if type in (MessageFactory.MESSAGE_TYPE.RESPONSE,
                    MessageFactory.MESSAGE_TYPE.BROADCAST_ACK):
            if seq is None:
                raise Exception('RESPONSE must have seq number')
            # format data for sending
            message.extend(MessageFactory.createMessage(data,seq,type,
                                                        serialized=serialized))
            # save message to history
            with self.recv_history_lock:
                self.recv_history[seq] = message
                if len(self.recv_history) >= self.history_length:
                    # hit history length, so kill FIFO message
                    self.recv_history.popitem(last=False)
        elif type in (MessageFactory.MESSAGE_TYPE.SERVICE,
                      MessageFactory.MESSAGE_TYPE.SERVER,
                      MessageFactory.MESSAGE_TYPE.BROADCAST):
            # set timeout
            if not isinstance(timeout,(int,float)):
                timeout = 60.0
            elif timeout < 0.1:
                timeout = 0.1
            # make new sequence number
            seq = random.randint(0,MessageFactory.MAX_SEQ)
            with self.send_history_lock:
                while seq in self.send_history:
                    seq = random.randint(0,MessageFactory.MAX_SEQ)
                cb_err = partial(self._response_timeout,seq)
                tt = self.io_loop.add_timeout((time.time()+timeout),cb_err)
                self.send_history[seq] = (callback,tt)
                if len(self.send_history) >= self.history_length:
                    # hit history length, so kill FIFO message
                    old_request = self.send_history.popitem(last=False)
            # format data for sending
            message.extend(MessageFactory.createMessage(data,seq,type,
                                                        serialized=serialized))
        
        # send message (make sure we're on the ioloop thread)
        self.io_loop.add_callback(partial(super(Base,self).send,message))
        logger.debug('sending message:%s',str(data))
        
        if old_request:
            self._send_history_full(item[0],item[1][0],item[1][1])
    
    def _handle_stream_error(self):
        # stream is corrupted at this point, so reset
        logger.error('stream error on socket. resetting...')
        with self.send_history_lock, self.recv_history_lock:
            for cb,tt in self.send_history.values():
                if cb:
                    try:
                        cb(Exception('socket error'))
                    except Exception:
                        pass
                try:
                    self.io_loop.remove_timeout(tt)
                except Exception:
                    pass
            self.send_history = OrderedDict()
            self.recv_history = OrderedDict()
        self.restart()
    
    def _send_history_full(self,seq,callback,tt):
        logger.info('send history full, popped request for seq %d',seq)
        # remove timeout
        try:
            logger.debug('removing timeout for seq %d',seq)
            self.io_loop.remove_timeout(tt)
        except:
            pass
        if callback:
            try:
                callback(Exception('response timeout'))
            except Exception:
                pass
    
    def _response_timeout(self,seq):
        # TODO: consider doing a few retries before giving up
        logger.info('timeout in send request for seq %d',seq)
        try:
            with self.send_history_lock:
                callback,tt = self.send_history.pop(seq)
        except KeyError as e:
            logger.warning('sequence number not valid: %s',str(seq))
        except Exception as e:
            # generic error
            logger.warning('unknown error with sequence number. %s',str(e))
            self._handle_stream_error()
        else:
            # remove timeout
            try:
                logger.debug('removing timeout for seq %d',seq)
                self.io_loop.remove_timeout(tt)
            except:
                pass
            if callback:
                try:
                    callback(Exception('response timeout'))
                except Exception:
                    pass

class Client(Base):
    """
    ZMQ messaging client/service.
    
    Start a client in a separate thread so it doesn't block the caller.
    
    :param service_name: Name of service, optional
    :param service_callback: Callback for service, optional
        
        Function signature must be fn( data, callback=writer(msg) )
    
    :param address: Address to connect to
    :param history_length: Amount of history to keep track of, optional
    """
    def __init__(self,service_name=None,service_callback=None,**kwargs):
        kwargs['bind'] = False
        kwargs['recv_handler'] = self._get_response
        super(Client,self).__init__(**kwargs)
        
        # set some variables
        self.service_name = service_name
        self.service_callback = service_callback
    
    def start(self,*args,**kwargs):
        """Start the Client"""
        super(Client,self).start(*args,**kwargs)
        
        if self.service_name and self.service_callback:
            data = {'method':'register_service',
                    'params':{'service_name':self.service_name},
                   }
            self.send(data,None,MessageFactory.MESSAGE_TYPE.SERVER)
    
    def stop(self):
        """Stop the Client"""
        if self.service_name and self.service_callback:
            data = {'method':'unregister_service',
                    'params':{'service_name':self.service_name},
                   }
            self.send(data,None,MessageFactory.MESSAGE_TYPE.SERVER,
                      timeout=0.5,
                      callback=super(Client,self).stop)
        else:
            super(Client.self).stop()
    
    def _get_response(self,frames):
        # decode message
        try:
            header = MessageFactory.parseMessageHeader(frames[0])
            data = MessageFactory.getMessage(frames[1], header)
        except Exception as e:
            logger.warning('error getting message: %s',str(e))
            # is this error unrecoverable? probably not
            #self._handle_stream_error()
        else:
            if header['message_type'] == MessageFactory.MESSAGE_TYPE.SERVICE:
                # handle service request
                cb = partial(self.send,seq=header['sequence_number'],
                             msg_type=MessageFactory.MESSAGE_TYPE.RESPONSE)
                if (not header['service_name'] or 
                    header['service_name'] != self.service_name):
                    cb({'error':'service name does not match'})
                else:
                    self.service_callback(data,callback=cb)
                
            elif header['message_type'] == MessageFactory.MESSAGE_TYPE.BROADCAST:
                # handle broadcast request, send an ACK back
                cb = partial(self.send,seq=header['sequence_number'],
                             msg_type=MessageFactory.MESSAGE_TYPE.BROADCAST_ACK)
                self.service_callback(data,callback=cb)
                
            elif (header['message_type'] == MessageFactory.MESSAGE_TYPE.RESPONSE
                  or header['message_type'] == MessageFactory.MESSAGE_TYPE.BROADCAST_ACK):
                # response to one of our messages
                try:
                    with self.send_history_lock:
                        callback,tt = self.send_history.pop(seq)
                except KeyError as e:
                    logger.warn('RESPONSE: sequence number not valid: %s',
                                str(seq))
                except Exception as e:
                    # generic error
                    logger.warn('RESPONSE: unknown error. sequence number: %s',
                                str(e))
                    self._handle_stream_error()
                else:
                    logger.debug('RESPONSE: got msg for seq: %d'%seq)
                    try:
                        logger.debug('RESPONSE: removing timeout for seq %d',
                                     seq)
                        self.io_loop.remove_timeout(tt)
                    except:
                        pass
                    if callback:
                        try:
                            callback(data)
                        except Exception:
                            pass
                
            else:
                logger.warn('invalid message type: %s',msg_type)

class ThreadedClient(Client,Thread):
    pass

class Server(Base):
    """
    ZMQ messaging server.
    
    Start a server in the current thread (blocking).
    
    TODO: handle case of 2+ instances of same service
    
    :param history_length: Amount of history to keep track of
    :param address: Address to bind to
    """
    def __init__(self,**kwargs):
        kwargs['bind'] = True
        kwargs['recv_handler'] = self._get_response
        super(Server,self).__init__(**kwargs)
        
        # define some variables
        self.services = {}
    
    def start(self):
        # start the server
        logger.warning("starting RPCInternal.Server(%s)",self.address)
        self.run()
    
    def _server_handler(self,client_id,msg,callback=None):
        # handle any messages for the server
        try:
            if msg['method'] == 'register_service':
                self.services[msg['params']['service_name']] = client_id
                callback({'result':'success'})
            elif msg['method'] == 'unregister_service':
                try:
                    del self.services[msg['params']['service_name']]
                except KeyError:
                    pass
                callback({'result':'success'})
            elif msg['method'] == 'service_list':
                callback({'result':services.keys()})
            elif msg['method'] == 'stop':
                callback({'result':'success'})
                time.sleep(1)
                self.stop()
            elif msg['method'] == 'kill':
                callback({'result':'success'})
                time.sleep(0.01)
                self.stop()
            else:
                callback({'error':'invalid method'})
        except KeyError:
            callback({'error':'bad message format'})
        except Exception as e:
            logger.info('general _server_handler error',exc_info=True)
            callback({'error':'server error: %s'%e})
    
    def _get_response(self,frames):
        # decode message
        client_id = frames[0]
        try:
            header = MessageFactory.parseMessageHeader(frames[1])
        except Exception as e:
            logger.warning('error getting header: %s',str(e))
            # is this error unrecoverable? probably not
            #self._handle_stream_error()
        else:
            if header['message_type'] == MessageFactory.MESSAGE_TYPE.SERVICE:
                # forward to service
                cb = partial(self.send,seq=header['sequence_number'],
                             client_id=client_id,
                             msg_type=MessageFactory.MESSAGE_TYPE.RESPONSE)
                try:
                    service_id = self.services[header['service_name']]
                except KeyError:
                    cb({'error':'service not registered'})
                else:
                    self.send(frames[2], serialized=True,
                              client_id=service_id, callback=cb,
                              msg_type=MessageFactory.MESSAGE_TYPE.SERVICE)
                
            elif (header['message_type'] == MessageFactory.MESSAGE_TYPE.RESPONSE
                  or header['message_type'] == MessageFactory.MESSAGE_TYPE.BROADCAST_ACK):
                # response to one of our messages
                try:
                    with self.send_history_lock:
                        callback,tt = self.send_history.pop(seq)
                except KeyError as e:
                    logger.warn('RESPONSE: sequence number not valid: %s',
                                str(seq))
                except Exception as e:
                    # generic error
                    logger.warn('RESPONSE: unknown error. sequence number: %s',
                                str(e))
                    self._handle_stream_error()
                else:
                    logger.debug('RESPONSE: got msg for seq: %d'%seq)
                    
                    try:
                        logger.debug('RESPONSE: removing timeout for seq %d',
                                     seq)
                        self.io_loop.remove_timeout(tt)
                    except:
                        pass
                    if callback: # only message_type == RESPONSE
                        try: # this should be a send
                            callback(frames[2],serialized=True)
                        except Exception:
                            pass
                
            elif header['message_type'] == MessageFactory.MESSAGE_TYPE.BROADCAST:
                # broadcast to all registered services
                for service_id in self.services.values():
                    self.send(frames[2], serialized=True, client_id=service_id,
                              msg_type=MessageFactory.MESSAGE_TYPE.BROADCAST)
                self.send({'result':'ack'},seq=header['sequence_number'],
                          msg_type=MessageFactory.MESSAGE_TYPE.BROADCAST_ACK)
                
            elif header['message_type'] == MessageFactory.MESSAGE_TYPE.SERVER:
                # a message for us, so decode it
                try:
                    data = MessageFactory.getMessage(frames[2], header)
                except Exception as e:
                    logger.warning('error getting message: %s',str(e))
                else:
                    cb = partial(self.send,seq=header['sequence_number'],
                                 client_id=client_id,
                                 msg_type=MessageFactory.MESSAGE_TYPE.RESPONSE)
                    self._server_handler(client_id,data,cb)
                
            else:
                logger.warn('invalid message type: %s',msg_type)


class RPCService():
    """
    An RPC version of the :class:`Client`
    
    Once initialized, call like RPCService.service_name.method_name(kwargs)
    with a named parameter 'callback' for results.
    Callback function gets one arg as the result, which could be an 
    Exception class.
    
    RPC service functions are defined in a service_class via either static
    or regular methods. Each function is called with kwargs from the client
    and a 'callback' response function for async.
    
    Example RPC class::
    
        class RPC():
            def test(self,var1,var2,callback):
                # do something with vars
                # return directly for fast calls
                return var1
            def test2(self,var3,var3,callback):
                # do something else with vars
                # very time consuming tasks should be done async, and passed the
                # callback function to return a value
                time_consumer(var3,callback=callback)
            @staticmethod
            def test3(callback):
                # this is a bare method with no args, just a callback
                pass # and we don't even have to use it
    
    :param address: the address of the :class:`Server`
    :param block: whether to block or start a separate thread, optional,
            defaults to True
    :param timeout: request timeout, optional,
            defaults to 60.0 seconds
    :param io_loop: the Tornado io_loop to use, optional,
            defaults to the main loop
    :param service_name: The service name, optional
    :param service_class: The RPC service class, optional
    :param context: A context manager for service calls, optional
    :param history_length: Amount of history to keep track of, optional
    """
    def __init__(self, address=None, block=True, timeout=60.0, io_loop=None,
                 service_name=None, service_class=None, context=None,
                 history_length=None):
        if address is None: # set default here in case we actually get a None address from the user
            address = os.path.join('ipc://',os.getcwd(),'unix_socket.sock')
        self.timeout = timeout
        self.service_class = service_class
        self.context = context
        kwargs = {'address':address}
        if io_loop:
            kwargs['io_loop'] = io_loop
        if service_name and service_class:
            kwargs['service_name'] = service_name
            kwargs['service_callback'] = self.__service_handler
        if history_length:
            kwargs['history_length'] = history_length
        if block:
            self._cl = Client(**kwargs)
        else:
            self._cl = ThreadedClient(**kwargs)
    
    def start(self):
        self._cl.start()
    
    def stop(self):
        self._cl.stop()

    def __repr__(self):
        return ("<RPC Client to %s>" % (str(self._cl.address)))
    
    __str__ = __repr__
    
    def __service_handler(self,data,callback=None):
        logger.debug('got message:%s',str(data))
        """Unpack data from RPC format"""
        try:
            methodname = data['method']
            kwargs = data['params']
        except Exception as e:
            # error unpacking data
            if callback:
                callback({'error':'invalid message format'})
            logger.warning('error unpacking data from message. %s',str(e))
        else:
            # check method name for bad characters
            if methodname[0] == '_':
                if callback:
                    callback({'error':'Cannot use RPC for private methods'})
                logger.warning('cannot use RPC for private methods. %s',str(e))
                return
            
            # check for function
            try:
                func = getattr(self.service_class,methodname)
            except AttributeError:
                if callback:
                    callback({'error':'Method not available'})
                logger.warning('method %s not available',str(methodname))
                return
            kwargs['callback'] = partial(self.__response,callback=callback)
            try:
                if self.context:
                    with self.context():
                        ret = func(**kwargs)
                else:
                    ret = func(**kwargs)
            except Exception as e:
                # error calling function
                if callback:
                    callback({'error':str(e)})
                logger.warning('error calling function specified',exc_info=True)
            else:
                if ret is not None:
                    callback({'result':data})
    
    def __response(self,data,callback=None):
        """Pack data into RPC format"""
        if isinstance(data,Exception):
            logger.warning('error calling function specified: %r',data)
            if callback:
                callback({'error':str(data)})
        elif callback:
            callback({'result':data})
    
    def __callback(self,callback,data):
        """Parse response from server and send to callback"""
        logger.debug('__callback %r',data)
        if callback is not None:
            if 'result' not in data:
                logger.warning('data does not contain a result')
                callback(Exception('data does not contain a result'))
            callback(data['result'])
    
    def __request(self,service,methodname,kwargs):
        """Send request to RPC Server"""
        # check method name for bad characters
        if methodname[0] == '_':
            logger.warning('cannot use RPC for private methods')
            raise Exception('Cannot use RPC for private methods')
    
        # get callback, if available
        try:
            callback = kwargs.pop('callback')
            callback = partial(self.__callback,callback)
        except:
            callback = None
        
        # check what type of request we're making
        request_args = {timeout:self.timeout,
                        callback:callback}
        if service == 'SERVER':
            request_args['type'] = MessageFactory.MESSAGE_TYPE.SERVER
        elif service == 'BROADCAST':
            request_args['type'] = MessageFactory.MESSAGE_TYPE.BROADCAST
        else:
            request_args['service'] = service
        if callback is None:
            # test for async keyword
            try:
                async = kwargs.pop('async')
            except:
                async = True
            if async is False:
                # return like normal function
                logger.debug('async request for %s',methodname)
                def cb(ret=None):
                    cb.ret = ret
                    cb.event.set()
                cb.ret = None
                cb.event = Event()
                cb.event.clear()
                request_args['callback'] = partial(self.__callback,cb)
                # make request to server
                self._cl.send({'method':methodname,'params':kwargs},
                              **request_args)
                # wait until just after timeout for request to finish
                if not cb.event.wait(self.timeout+10):
                    raise Exception('request timed out')
                return cb.ret
        
        # make async request to server
        self._cl.send({'method':methodname,'params':kwargs}, **request_args)
    
    def __getattr__(self,name):
        class _Method:
            def __init__(self,send,service,name):
                self.__send = send
                self.__service = service
                self.__name = name
            def __getattr__(self,name):
                return _Method(self.__send,self.__service,
                               "%s.%s"%(self.__name,name))
            def __call__(self,**kwargs):
                return self.__send(self.__service,self.__name,kwargs)
        class _Service:
            def __init__(self,send,service):
                self.__send = send
                self.__service = service
            def __getattr__(self,name):
                return _Method(self.__send,self.__service,name)
            def __call__(self,**kwargs):
                raise Exception('Service %s, method name not specified'%(
                                self.__service))
        return _Service(self.__request,name)
