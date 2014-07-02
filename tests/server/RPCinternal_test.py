"""
Test script for RPC internal
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('server_tester')

import os, sys, time
import shutil
import tempfile
import random
import struct
from threading import Thread
from contextlib import contextmanager

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

from tornado.ioloop import IOLoop

import iceprod.server
from iceprod.server import zeromq
from iceprod.server import RPCinternal

class RPCinternal_test(unittest.TestCase):
    def setUp(self):
        super(RPCinternal_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(RPCinternal_test,self).tearDown()        

    def test_01_Serializer(self):
        try:
            for data in [str(random.random()),random.random()]:
                s1 = RPCinternal.Serializer.serialize(data)
                s2 = RPCinternal.Serializer.deserialize(s1)
                if data != s2:
                    logger.warning('%r != %r',data,s2)
                    raise Exception('input and output do not match')
            
        except Exception as e:
            logger.error('Error running RPCinternal.Serializer test - %s',str(e))
            printer('Test RPCinternal.Serializer',False)
            raise
        else:
            printer('Test RPCinternal.Serializer')
        
    def test_02_MessageFactory(self):
        try:
            for _ in xrange(10):
                for data in [str(random.random()), random.random(),
                             ''.join(str(random.random()) for _ in range(1000))]:
                    logger.info('data: %r',data)
                    service = 'test_service'
                    msg = RPCinternal.MessageFactory.createMessage(data,1,
                            RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE,
                            service_name=service)
                    logger.info('msg: %r',msg)
                    header = RPCinternal.MessageFactory.parseMessageHeader(msg[0])
                    logger.info('header: %r',header)
                    if header['sequence_number'] != 1:
                        raise Exception('Bad header sequence number')
                    body = RPCinternal.MessageFactory.getMessage(msg[1],header)
                    logger.info('body: %r',body)
                    if body != data:
                        raise Exception('Bad message using getmessage() with header data')
                    if not RPCinternal.MessageFactory.verifyChecksum(RPCinternal.Serializer.serialize(body),header['body_checksum']):
                        logger.info('checksum failed: %s != %s',RPCinternal.MessageFactory.getChecksum(RPCinternal.Serializer.serialize(body)),header['body_checksum'])
                        raise Exception('verifyChecksum() failed')
                    
                    for m in ('SERVER','BROADCAST','BROADCAST_ACK','RESPONSE'):
                        msg = RPCinternal.MessageFactory.createMessage(data,1,
                                getattr(RPCinternal.MessageFactory.MESSAGE_TYPE,m))
                        logger.info('msg: %r',msg)
                        header = RPCinternal.MessageFactory.parseMessageHeader(msg[0])
                        logger.info('header: %r',header)
                        if header['sequence_number'] != 1:
                            raise Exception('Bad header sequence number')
                        body = RPCinternal.MessageFactory.getMessage(msg[1],header)
                        logger.info('body: %r',body)
                        if body != data:
                            raise Exception('Bad message using getmessage() with header data')
                        if not RPCinternal.MessageFactory.verifyChecksum(RPCinternal.Serializer.serialize(body),header['body_checksum']):
                            logger.info('checksum failed: %s != %s',RPCinternal.MessageFactory.getChecksum(RPCinternal.Serializer.serialize(body)),header['body_checksum'])
                            raise Exception('verifyChecksum() failed')
                    
                    # pre-serialize the data
                    s_data = RPCinternal.Serializer.serialize(data)
                    service = 'test_service'
                    msg = RPCinternal.MessageFactory.createMessage(s_data,1,
                            RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE,
                            service_name=service,serialized=True)
                    logger.info('msg: %r',msg)
                    header = RPCinternal.MessageFactory.parseMessageHeader(msg[0])
                    logger.info('header: %r',header)
                    if header['sequence_number'] != 1:
                        raise Exception('Bad header sequence number')
                    body = RPCinternal.MessageFactory.getMessage(msg[1],header)
                    logger.info('body: %r',body)
                    if body != data:
                        raise Exception('Bad message using getmessage() with header data')
                    if not RPCinternal.MessageFactory.verifyChecksum(RPCinternal.Serializer.serialize(body),header['body_checksum']):
                        logger.info('checksum failed: %s != %s',RPCinternal.MessageFactory.getChecksum(RPCinternal.Serializer.serialize(body)),header['body_checksum'])
                        raise Exception('verifyChecksum() failed')
                    
                    for m in ('SERVER','BROADCAST','BROADCAST_ACK','RESPONSE'):
                        msg = RPCinternal.MessageFactory.createMessage(s_data,1,
                                getattr(RPCinternal.MessageFactory.MESSAGE_TYPE,m),
                                serialized=True)
                        logger.info('msg: %r',msg)
                        header = RPCinternal.MessageFactory.parseMessageHeader(msg[0])
                        logger.info('header: %r',header)
                        if header['sequence_number'] != 1:
                            raise Exception('Bad header sequence number')
                        body = RPCinternal.MessageFactory.getMessage(msg[1],header)
                        logger.info('body: %r',body)
                        if body != data:
                            raise Exception('Bad message using getmessage() with header data')
                        if not RPCinternal.MessageFactory.verifyChecksum(RPCinternal.Serializer.serialize(body),header['body_checksum']):
                            logger.info('checksum failed: %s != %s',RPCinternal.MessageFactory.getChecksum(RPCinternal.Serializer.serialize(body)),header['body_checksum'])
                            raise Exception('verifyChecksum() failed')
                            
            
            # bad message type
            try:
                msg = RPCinternal.MessageFactory.createMessage(data,1,100)
            except Exception:
                pass
            else:
                raise Exception('bad message type did not raise Exception')
            
            # no service name
            try:
                msg = RPCinternal.MessageFactory.createMessage(data,1,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE)
            except Exception:
                pass
            else:
                raise Exception('no service name did not raise Exception')
            
        except Exception as e:
            logger.error('Error running RPCinternal.MessageFactory.createMessage test - %s',str(e))
            printer('Test RPCinternal.MessageFactory.createMessage',False)
            raise
        else:
            printer('Test RPCinternal.MessageFactory.createMessage')
    
    def test_03_MessageFactory_getMessage(self):
        try:
            for data in [str(random.random()),random.random()]:
                logger.info('data: %r',data)
                msg = RPCinternal.MessageFactory.createMessage(data,1,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE)
                
                # missing header
                try:
                    RPCinternal.MessageFactory.parseMessageHeader(None)
                except Exception:
                    pass
                else:
                    raise Exception('missing header did not raise an exception')
                
                # header checksum error
                msg2 = list(msg[0])
                if msg2[RPCinternal.MessageFactory.HEADERSIZE-2] != '2':
                    msg2[RPCinternal.MessageFactory.HEADERSIZE-2] = '2' # modify header checksum
                else:
                    msg2[RPCinternal.MessageFactory.HEADERSIZE-2] = '3'
                msg2 = ''.join(msg2)
                
                try:
                    header = RPCinternal.MessageFactory.parseMessageHeader(msg2)
                except:
                    pass
                else:
                    logger.info('msg: %r',msg[0])
                    logger.info('msg2: %r',msg2)
                    raise Exception('header checksum mismatch did not raise exception')
                
                # header ID error
                msg2 = list(msg[0])
                msg2[3] = 'X'
                msg2 = ''.join(msg2)
                
                try:
                    header = RPCinternal.MessageFactory.parseMessageHeader(msg2)
                except:
                    pass
                else:
                    logger.info('msg: %r',msg[0])
                    logger.info('msg2: %r',msg2)
                    raise Exception('header ID mismatch did not raise exception')
                
                # header protocol version error
                msg2 = list(msg[0])
                msg2[5] = 'X'
                msg2 = ''.join(msg2)
                
                try:
                    header = RPCinternal.MessageFactory.parseMessageHeader(msg2)
                except:
                    pass
                else:
                    logger.info('msg: %r',msg[0])
                    logger.info('msg2: %r',msg2)
                    raise Exception('header protocol version mismatch did not raise exception')
                
                # header message type error
                msg2 = list(msg[0])
                msg2[-5] = 'X'
                msg2 = ''.join(msg2)
                
                try:
                    header = RPCinternal.MessageFactory.parseMessageHeader(msg2)
                except:
                    pass
                else:
                    logger.info('msg: %r',msg[0])
                    logger.info('msg2: %r',msg2)
                    raise Exception('header message type mismatch did not raise exception')
                
                # body length error
                msg2 = list(msg[0])
                if msg2[RPCinternal.MessageFactory.HEADERSIZE-11] != '2':
                    msg2[RPCinternal.MessageFactory.HEADERSIZE-11] = '2' # modify body len
                else:   
                    msg2[RPCinternal.MessageFactory.HEADERSIZE-11] = '3'
                msg2 = ''.join(msg2)[:-4]
                msg2 += struct.pack('!L',RPCinternal.MessageFactory.getChecksum(msg2))
                header = RPCinternal.MessageFactory.parseMessageHeader(msg2)
                try:
                    RPCinternal.MessageFactory.getMessage(msg[1],header)
                except Exception:
                    pass
                else:
                    logger.info('msg: %r',msg[0])
                    logger.info('msg2: %r',msg2)
                    raise Exception('body length mismatch did not raise exception')
                
                # body checksum error
                msg2 = list(msg[0])
                if msg2[RPCinternal.MessageFactory.HEADERSIZE-6] != '2':
                    msg2[RPCinternal.MessageFactory.HEADERSIZE-6] = '2' # modify body checksum
                else:   
                    msg2[RPCinternal.MessageFactory.HEADERSIZE-6] = '3'
                msg2 = ''.join(msg2)[:-4]
                msg2 += struct.pack('!L',RPCinternal.MessageFactory.getChecksum(msg2))
                header = RPCinternal.MessageFactory.parseMessageHeader(msg2)
                try:
                    RPCinternal.MessageFactory.getMessage(msg[1],header)
                except Exception:
                    pass
                else:
                    logger.info('msg: %r',msg[0])
                    logger.info('msg2: %r',msg2)
                    raise Exception('body checksum mismatch did not raise exception')
                
                # missing body
                try:
                    RPCinternal.MessageFactory.getMessage(None,None)
                except Exception:
                    pass
                else:
                    raise Exception('missing header did not raise an exception')
                
                # missing header
                try:
                    RPCinternal.MessageFactory.getMessage(msg[1],None)
                except Exception:
                    pass
                else:
                    raise Exception('missing header did not raise an exception')
                
                # missing header data
                header.pop('body_length')
                try:
                    RPCinternal.MessageFactory.getMessage(msg[1],header)
                except Exception:
                    pass
                else:
                    raise Exception('missing header did not raise an exception')
                
        except Exception as e:
            logger.error('Error running RPCinternal.MessageFactory.getMessage Bad Data test - %s',str(e))
            printer('Test RPCinternal.MessageFactory.getMessage',False)
            raise
        else:
            printer('Test RPCinternal.MessageFactory.getMessage')
    
    def test_10_Client(self):
        try:
            def run():
                run.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('run').replace_with(run)
            def restart():
                restart.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('restart').replace_with(restart)
            def stop():
                stop.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('stop').replace_with(stop)
            def setup():
                setup.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('setup').replace_with(setup)
            def send(msg):
                send.messages.append(msg)
            flexmock(zeromq.AsyncSendReceive).should_receive('send').replace_with(send)
            
            class Stream:
                def closed(self):
                    return False
            
            ioloop = IOLoop.current()
            Thread(target=ioloop.start).start()
            kwargs_base = {
                'io_loop':ioloop,
                'address':'localhost'
            }
            
            try:
                # test without service
                run.called = restart.called = stop.called = setup.called = False
                send.messages = []
                kwargs = kwargs_base.copy()
                c = RPCinternal.Client(**kwargs)
                if not c:
                    raise Exception('did not return a client')
                if not setup.called:
                    raise Exception('did not call setup')
                c.stream = Stream()
                
                c.start()
                if not run.called:
                    raise Exception('did not start ioloop')
                if send.messages:
                    raise Exception('non-service client should not send message on start')
                
                for d in (random.random(),''.join(str(random.random()) for _ in range(100))):
                    c.send(d,service='test')
                time.sleep(0.1)
                if len(send.messages) != 2:
                    raise Exception('did not send all messages')
                for d in (random.random(),''.join(str(random.random()) for _ in range(100))):
                    c.send(d,type=RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                time.sleep(0.1)
                if len(send.messages) != 4:
                    raise Exception('did not send all messages2')
                
                c.stop()
                if not stop.called:
                    raise Exception('did not call stop')
                
                c.restart()
                if not restart.called:
                    raise Exception('did not call restart')
                
                run.called = restart.called = stop.called = setup.called = False
                send.messages = []
                kwargs = kwargs_base.copy()
                kwargs['service_name'] = 'tester'
                try:
                    c = RPCinternal.Client(**kwargs)
                except Exception:
                    pass
                else:
                    raise Exception('did not raise exception on service name without callback')
                
                # test service
                def cb(*args,**kwargs):
                    cb.called = [args,kwargs]
                cb.called = None
                kwargs = kwargs_base.copy()
                kwargs['service_name'] = 'tester'
                kwargs['service_callback'] = cb
                c = RPCinternal.Client(**kwargs)
                if not c:
                    raise Exception('did not return a client')
                if not setup.called:
                    raise Exception('did not call setup')
                c.stream = Stream()
                
                c.start()
                if not run.called:
                    raise Exception('did not start ioloop')
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('missed the service name message')
                
                send.messages = []
                c.stop()
                time.sleep(0.6)
                if not stop.called:
                    raise Exception('did not call stop')
                if len(send.messages) != 1:
                    raise Exception('missed the service name message')
                
                # test responses
                for data in [str(random.random()),random.random()]:
                    send.messages = []
                    cb.called = None
                    c.send(data, service='test',
                           type=RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE,
                           callback=cb)
                    time.sleep(0.1)
                    if len(send.messages) != 1:
                        raise Exception('did not send message')
                    header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][0])
                    msg2 = RPCinternal.MessageFactory.createMessage(data,
                            header['sequence_number'],
                            RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE)
                    c._get_response(msg2)
                    if not cb.called:
                        raise Exception('did not call callback')
                    if isinstance(cb.called[0],Exception):
                        raise cb.called[0]
                
                # test service
                for data in [str(random.random()),random.random()]:
                    send.messages = []
                    cb.called = None
                    seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                    msg = RPCinternal.MessageFactory.createMessage(data, seq,
                            RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE,
                            service_name='tester')
                    c._get_response(msg)
                    if not cb.called:
                        raise Exception('did not call callback')
                    if isinstance(cb.called[0],Exception):
                        raise cb.called[0]
                    if 'callback' not in cb.called[1]:
                        raise Exception('callback not in cb kwargs')
                    cb.called[1]['callback']('return_data')
                    time.sleep(0.1)
                    if len(send.messages) != 1:
                        raise Exception('did not send message')
                    header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][0])
                    if header['sequence_number'] != seq:
                        raise Exception('seq number did not match')
                    body = RPCinternal.MessageFactory.getMessage(send.messages[0][1],header)
                    if body != 'return_data':
                        raise Exception('did not get return data')
                
            finally:
                ioloop.stop()
            
        except Exception as e:
            logger.error('Error running RPCinternal.Client test - %s',str(e))
            printer('Test RPCinternal.Client',False)
            raise
        else:
            printer('Test RPCinternal.Client')
    
    def test_11_ThreadedClient(self):
        try:
            def run(*args,**kwargs):
                run.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('run').replace_with(run)
            def restart():
                restart.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('restart').replace_with(restart)
            def stop():
                stop.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('stop').replace_with(stop)
            def setup():
                setup.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('setup').replace_with(setup)
            def send(msg):
                send.messages.append(msg)
            flexmock(zeromq.AsyncSendReceive).should_receive('send').replace_with(send)
            
            class Stream:
                def closed(self):
                    return False
            
            ioloop = IOLoop.current()
            Thread(target=ioloop.start).start()
            kwargs_base = {
                'io_loop':ioloop,
                'address':'localhost'
            }
            
            try:
                run.called = restart.called = stop.called = setup.called = False
                send.messages = []
                kwargs = kwargs_base.copy()
                c = RPCinternal.ThreadedClient(**kwargs)
                if not c:
                    raise Exception('did not return a client')
                if not setup.called:
                    raise Exception('did not call setup')
                c.stream = Stream()
                
                c.start()
                time.sleep(0.1)
                if not run.called:
                    raise Exception('did not start ioloop')
                if send.messages:
                    raise Exception('non-service client should not send message on start')
                
                for d in (random.random(),''.join(str(random.random()) for _ in range(100))):
                    c.send(d,service='test')
                time.sleep(0.1)
                if len(send.messages) != 2:
                    raise Exception('did not send all messages')
                for d in (random.random(),''.join(str(random.random()) for _ in range(100))):
                    c.send(d,type=RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                time.sleep(0.1)
                if len(send.messages) != 4:
                    raise Exception('did not send all messages2')
                
                c.stop()
                if not stop.called:
                    raise Exception('did not call stop')
                
                c.restart()
                time.sleep(0.1)
                if not restart.called:
                    raise Exception('did not call restart')
                
                run.called = restart.called = stop.called = setup.called = False
                send.messages = []
                kwargs = kwargs_base.copy()
                kwargs['service_name'] = 'tester'
                try:
                    c = RPCinternal.ThreadedClient(**kwargs)
                except Exception:
                    pass
                else:
                    raise Exception('did not raise exception on service name without callback')
                
                def cb(*args,**kwargs):
                    cb.called = [args,kwargs]
                cb.called = None
                kwargs = kwargs_base.copy()
                kwargs['service_name'] = 'tester'
                kwargs['service_callback'] = cb
                c = RPCinternal.ThreadedClient(**kwargs)
                if not c:
                    raise Exception('did not return a client')
                if not setup.called:
                    raise Exception('did not call setup')
                c.stream = Stream()
                
                c.start()
                time.sleep(0.1)
                if not run.called:
                    raise Exception('did not start ioloop')
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('missed the service name message')
                
                send.messages = []
                c.stop()
                time.sleep(0.6)
                if not stop.called:
                    raise Exception('did not call stop')
                if len(send.messages) != 1:
                    raise Exception('missed the service name message')
                
                # test responses
                for data in [str(random.random()),random.random()]:
                    send.messages = []
                    cb.called = None
                    c.send(data, service='test',
                           type=RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE,
                           callback=cb)
                    time.sleep(0.1)
                    if len(send.messages) != 1:
                        raise Exception('did not send message')
                    header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][0])
                    msg2 = RPCinternal.MessageFactory.createMessage(data,
                            header['sequence_number'],
                            RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE)
                    c._get_response(msg2)
                    if not cb.called:
                        raise Exception('did not call callback')
                    if isinstance(cb.called[0],Exception):
                        raise cb.called[0]
                
                # test service
                for data in [str(random.random()),random.random()]:
                    send.messages = []
                    cb.called = None
                    seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                    msg = RPCinternal.MessageFactory.createMessage(data, seq,
                            RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE,
                            service_name='tester')
                    c._get_response(msg)
                    if not cb.called:
                        raise Exception('did not call callback')
                    if isinstance(cb.called[0],Exception):
                        raise cb.called[0]
                    if 'callback' not in cb.called[1]:
                        raise Exception('callback not in cb kwargs')
                    cb.called[1]['callback']('return_data')
                    time.sleep(0.1)
                    if len(send.messages) != 1:
                        raise Exception('did not send message')
                    header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][0])
                    if header['sequence_number'] != seq:
                        raise Exception('seq number did not match')
                    body = RPCinternal.MessageFactory.getMessage(send.messages[0][1],header)
                    if body != 'return_data':
                        raise Exception('did not get return data')
            finally:
                ioloop.stop()
            
        except Exception as e:
            logger.error('Error running RPCinternal.ThreadedClient test - %s',str(e))
            printer('Test RPCinternal.ThreadedClient',False)
            raise
        else:
            printer('Test RPCinternal.ThreadedClient')
    
    def test_20_Server(self):
        try:
            def run(*args,**kwargs):
                run.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('run').replace_with(run)
            def restart():
                restart.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('restart').replace_with(restart)
            def stop():
                stop.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('stop').replace_with(stop)
            def setup():
                setup.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('setup').replace_with(setup)
            def send(msg):
                send.messages.append(msg)
            flexmock(zeromq.AsyncSendReceive).should_receive('send').replace_with(send)
            
            class Stream:
                def closed(self):
                    return False
            
            ioloop = IOLoop.current()
            Thread(target=ioloop.start).start()
            kwargs = {
                'io_loop':ioloop
            }
            
            try:
                run.called = restart.called = stop.called = setup.called = False
                send.messages = []
                c = RPCinternal.Server(address='localhost',**kwargs)
                if not c:
                    raise Exception('did not return a client')
                if not setup.called:
                    raise Exception('did not call setup')
                c.stream = Stream()
                
                c.start()
                time.sleep(0.1)
                if not run.called:
                    raise Exception('did not start ioloop')
                if send.messages:
                    raise Exception('server should not send message on start')
                
                for d in (random.random(),''.join(str(random.random()) for _ in range(100))):
                    c.send(d,service='test')
                time.sleep(0.1)
                if len(send.messages) != 2:
                    raise Exception('did not send all messages')
                for d in (random.random(),''.join(str(random.random()) for _ in range(100))):
                    c.send(d,type=RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                time.sleep(0.1)
                if len(send.messages) != 4:
                    raise Exception('did not send all messages2')
                
                c.stop()
                if not stop.called:
                    raise Exception('did not call stop')
                
                c.restart()
                time.sleep(0.1)
                if not restart.called:
                    raise Exception('did not call restart')
                
                try:
                    c = RPCinternal.Server(**kwargs)
                except Exception:
                    pass
                else:
                    raise Exception('did not raise exception on no address')
                
                # test invalid service
                send.messages = []
                data = {'a':1,'b':2}
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVICE,
                        service_name='tester')
                c._get_response([1]+msg)
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('did not send message')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'error':'service not registered'}:
                    raise Exception('did not return invalid service')
                
                # test valid service
                send.messages = []
                c.services['tester'] = random.randint(0,100)
                c._get_response([1]+msg)
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('did not send message')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != data:
                    raise Exception('did not forward data to service')
                data2 = {'c':3,'d':4}
                msg2 = RPCinternal.MessageFactory.createMessage(data2,
                        header['sequence_number'],
                        RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE)
                send.messages = []
                c._get_response([2]+msg2)
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('did not send message')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != data2:
                    logger.info('expected: %r',data2)
                    logger.info('actual: %r',body)
                    raise Exception('did not forward reply to client')
                
                # test broadcast
                send.messages = []
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.BROADCAST)
                c.services['tester'] = random.randint(0,100)
                c.services['tester2'] = random.randint(0,100)
                c._get_response([1]+msg)
                time.sleep(0.1)
                if len(send.messages) != 3:
                    raise Exception('did not send messages and ack')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.BROADCAST:
                    raise Exception('did not resend broadcast')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != data:
                    raise Exception('did not forward data to service')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[1][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.BROADCAST:
                    raise Exception('did not resend2 broadcast')
                body = RPCinternal.MessageFactory.getMessage(send.messages[1][2],header)
                if body != data:
                    raise Exception('did not forward data to service2')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[2][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.BROADCAST_ACK:
                    raise Exception('did not ack broadcast')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[2][2],header)
                if body != {'result':'ack'}:
                    raise Exception('did not forward data to service2')
                
                # test register_service
                send.messages = []
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method':'register_service','params':{'service_name':'test'}}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c.services = {}
                c._get_response([1]+msg)
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'result':'success'}:
                    raise Exception('did not forward data to service2')
                if 'test' not in c.services:
                    raise Exception('did not add service_name')
                
                # test unregister_service
                send.messages = []
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method':'unregister_service','params':{'service_name':'test'}}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c._get_response([1]+msg)
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'result':'success'}:
                    raise Exception('did not forward data to service2')
                if 'test' in c.services:
                    raise Exception('did not remove service_name')
                
                # test unregister_service when already unregistered
                send.messages = []
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method':'unregister_service','params':{'service_name':'test'}}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c.services = {}
                c._get_response([1]+msg)
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'result':'success'}:
                    raise Exception('did not forward data to service2')
                
                # test service_list
                send.messages = []
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method':'service_list'}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c.services = {'tester':1,'tester2':2}
                c._get_response([1]+msg)
                time.sleep(0.1)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if ('result' not in body or len(body['result']) != 2 or
                    set(body['result']) != set(['tester','tester2'])):
                    logger.info('body: %r',body)
                    raise Exception('did not return correct result list')
                
                # test stop
                send.messages = []
                stop.called = False
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method':'stop'}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c._get_response([1]+msg)
                time.sleep(0.15)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'result':'success'}:
                    raise Exception('did not reply with success')
                if not stop.called:
                    raise Exception('did not call stop')
                
                # test kill
                send.messages = []
                stop.called = False
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method':'kill'}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c._get_response([1]+msg)
                time.sleep(0.05)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'result':'success'}:
                    raise Exception('did not reply with success')
                if not stop.called:
                    raise Exception('did not call stop')
                
                # test invalid method
                send.messages = []
                stop.called = False
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method':'invalid'}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c._get_response([1]+msg)
                time.sleep(0.05)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'error':'invalid method'}:
                    raise Exception('did not reply with correct error message')
                
                # test bad message format
                send.messages = []
                stop.called = False
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = {'method1':'stop'}
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                c._get_response([1]+msg)
                time.sleep(0.05)
                if len(send.messages) != 1:
                    raise Exception('did not send reply')
                header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][1])
                if header['message_type'] != RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE:
                    raise Exception('did not send response')
                if header['sequence_number'] != seq:
                    raise Exception('seq number did not match')
                body = RPCinternal.MessageFactory.getMessage(send.messages[0][2],header)
                if body != {'error':'bad message format'}:
                    raise Exception('did not reply with correct error message')
                
                # test invalid message type
                send.messages = []
                stop.called = False
                seq = random.randint(0,RPCinternal.MessageFactory.MAX_SEQ)
                data = 'test'
                msg = RPCinternal.MessageFactory.createMessage(data, seq,
                        RPCinternal.MessageFactory.MESSAGE_TYPE.SERVER)
                msg2 = list(msg[0])
                msg2[-5] = 'X'
                msg2 = ''.join(msg2)
                msg[0] = msg2
                c._get_response([1]+msg)
                time.sleep(0.05)
                if send.messages:
                    raise Exception('sent a message when it should not')
                
            finally:
                ioloop.stop()
            
        except Exception as e:
            logger.error('Error running RPCinternal.Server test - %s',str(e))
            printer('Test RPCinternal.Server',False)
            raise
        else:
            printer('Test RPCinternal.Server')
    
    def test_30_RPCService(self):
        try:
            def run():
                run.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('run').replace_with(run)
            def restart():
                restart.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('restart').replace_with(restart)
            def stop():
                stop.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('stop').replace_with(stop)
            def setup():
                setup.called = True
            flexmock(zeromq.AsyncSendReceive).should_receive('setup').replace_with(setup)
            def send(msg):
                send.messages.append(msg)
            flexmock(zeromq.AsyncSendReceive).should_receive('send').replace_with(send)
            
            class Stream:
                def closed(self):
                    return False
            
            ioloop = IOLoop.current()
            Thread(target=ioloop.start).start()
            kwargs_base = {
                'io_loop':ioloop,
                'address':'localhost'
            }
            
            class ServiceClass:
                def __init__(self):
                    self.methods = {}
                    self.ret = None
                def t1(self,**kwargs):
                    self.methods['t1'] = kwargs
                    if 'callback' in kwargs:
                        if self.ret:
                            kwargs['callback'](self.ret)
                        else:
                            kwargs['callback']()
                    elif self.ret:
                        return self.ret
                def t2(self,**kwargs):
                    self.methods['t2'] = kwargs
                    return 't2_result'
                def e1(self,**kwargs):
                    raise Exception('error')
                def e2(self,**kwargs):
                    if 'callback' in kwargs:
                        kwargs['callback'](Exception('error'))
            
            try:
                run.called = restart.called = stop.called = setup.called = False
                send.messages = []
                def cb(*args,**kwargs):
                    cb.called = [args,kwargs]
                cb.called = None
                kwargs = kwargs_base.copy()
                kwargs['service_name'] = 'tester'
                kwargs['service_class'] = ServiceClass()
                c = RPCinternal.RPCService(**kwargs)
                if not c:
                    raise Exception('did not return a service')
                if not setup.called:
                    raise Exception('did not call setup')
                c._cl.stream = Stream()
                
                c.start()
                if not run.called:
                    raise Exception('did not start ioloop')
                time.sleep(0.1)
                if len(send.messages) != 1:
                    logger.info('messages: %r',send.messages)
                    raise Exception('missed the add service name message')
                
                send.messages = []
                c.stop()
                time.sleep(0.6)
                if not stop.called:
                    raise Exception('did not call stop')
                if len(send.messages) != 1:
                    raise Exception('missed the remove service name message')
                
                # test responses
                for data in [str(random.random()),random.random()]:
                    # service request
                    send.messages = []
                    cb.called = None
                    c.test.method(a=1,b=2,callback=cb)
                    time.sleep(0.1)
                    if len(send.messages) != 1:
                        raise Exception('did not send message')
                    header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][0])
                    body = RPCinternal.MessageFactory.getMessage(send.messages[0][1],header)
                    if body != {'method':'method','params':{'a':1,'b':2}}:
                        logger.info('body: %r',body)
                        raise Exception('rpc body incorrect')
                    response = {'result':data}
                    msg2 = RPCinternal.MessageFactory.createMessage(response,
                            header['sequence_number'],
                            RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE)
                    c._cl._get_response(msg2)
                    time.sleep(0.1)
                    if not cb.called:
                        raise Exception('did not call callback')
                    if cb.called[0][0] != data:
                        logger.info('cb returned %r',cb.called)
                        raise Exception('did not return data')
                    
                    # broadcast
                    send.messages = []
                    c.BROADCAST.method()
                    time.sleep(0.1)
                    if len(send.messages) != 1:
                        raise Exception('did not send message')
                    header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][0])
                    response = {'result':'ack'}
                    msg2 = RPCinternal.MessageFactory.createMessage(response,
                            header['sequence_number'],
                            RPCinternal.MessageFactory.MESSAGE_TYPE.BROADCAST_ACK)
                    c._cl._get_response(msg2)
                    time.sleep(0.1)
                    
                    # server
                    send.messages = []
                    c.SERVER.method()
                    time.sleep(0.1)
                    if len(send.messages) != 1:
                        raise Exception('did not send message')
                    header = RPCinternal.MessageFactory.parseMessageHeader(send.messages[0][0])
                    response = {'result':'success'}
                    msg2 = RPCinternal.MessageFactory.createMessage(response,
                            header['sequence_number'],
                            RPCinternal.MessageFactory.MESSAGE_TYPE.RESPONSE)
                    c._cl._get_response(msg2)
                    time.sleep(0.1)
                
                # timeout
                c.timeout = 0.2
                send.messages = []
                try:
                    ret = c.SERVER.method(async=False)
                except Exception as e:
                    if 'timeout' not in str(e).lower():
                        logger.info('error',exc_info=True)
                        raise Exception('timeout not in exception')
                else:
                    logger.info('ret: %r',ret)
                    raise Exception('request did not time out')
                if len(send.messages) != 1:
                    raise Exception('did not send message')
                
            finally:
                ioloop.stop()
            
            # service handler
            cb.called = None
            c.service_class.methods = {}
            c._cl.service_callback({'method':'t1','params':{}},callback=cb)
            if 't1' not in c.service_class.methods:
                logger.info('methods called: %r',c.service_class.methods)
                raise Exception('did not call method')
            if c.service_class.methods['t1'].keys() != ['callback']:
                raise Exception('kwargs where none given')
            if cb.called[0][0] != {'result':None}:
                logger.info('callback: %r',cb.called)
                raise Exception('did not callback a None result')
            
            # with args
            c.service_class.methods = {}
            c._cl.service_callback({'method':'t1','params':{'a':1,'b':2}})
            if 't1' not in c.service_class.methods:
                logger.info('methods called: %r',c.service_class.methods)
                raise Exception('did not call method')
            if set(c.service_class.methods['t1'].keys()) != set(['callback','a','b']):
                logger.info('methods called: %r',c.service_class.methods)
                raise Exception('kwargs missing')
            if (c.service_class.methods['t1']['a'] != 1 or
                c.service_class.methods['t1']['b'] != 2):
                logger.info('methods called: %r',c.service_class.methods)
                raise Exception('kwargs incorrect')
            
            # return result
            cb.called = None
            c.service_class.methods = {}
            c._cl.service_callback({'method':'t2','params':{}},callback=cb)
            if 't2' not in c.service_class.methods:
                logger.info('methods called: %r',c.service_class.methods)
                raise Exception('did not call method')
            if cb.called[0][0] != {'result':'t2_result'}:
                logger.info('callback: %r',cb.called)
                raise Exception('did not callback t2_result')
            
            # private method
            cb.called = None
            c._cl.service_callback({'method':'_t1','params':{}},callback=cb)
            if ('error' not in cb.called[0][0] or 
                'private' not in cb.called[0][0]['error']):
                logger.info('callback: %r',cb.called[0][0])
                raise Exception('private method call did not fail correctly')
            
            # bad method
            cb.called = None
            c._cl.service_callback({'method':'x1','params':{}},callback=cb)
            if ('error' not in cb.called[0][0] or 
                'available' not in cb.called[0][0]['error']):
                logger.info('callback: %r',cb.called[0][0])
                raise Exception('missing method call did not fail correctly')
            
            # error method
            cb.called = None
            c._cl.service_callback({'method':'e1','params':{}},callback=cb)
            if ('error' not in cb.called[0][0] or 
                'error' not in cb.called[0][0]['error']):
                logger.info('callback: %r',cb.called[0][0])
                raise Exception('missing method call did not fail correctly')
            
            # error method
            cb.called = None
            c._cl.service_callback({'method':'e2','params':{}},callback=cb)
            if ('error' not in cb.called[0][0] or 
                'error' not in cb.called[0][0]['error']):
                logger.info('callback: %r',cb.called[0][0])
                raise Exception('missing method call did not fail correctly')
            
            # with context
            @contextmanager
            def Context():
                yield
            c.context = Context
            cb.called = None
            c.service_class.methods = {}
            c._cl.service_callback({'method':'t1','params':{}},callback=cb)
            if 't1' not in c.service_class.methods:
                logger.info('methods called: %r',c.service_class.methods)
                raise Exception('did not call method')
            if c.service_class.methods['t1'].keys() != ['callback']:
                raise Exception('kwargs where none given')
            if cb.called[0][0] != {'result':None}:
                logger.info('callback: %r',cb.called)
                raise Exception('did not callback a None result')
            
        except Exception as e:
            logger.error('Error running RPCinternal.RPCService test - %s',str(e))
            printer('Test RPCinternal.RPCService',False)
            raise
        else:
            printer('Test RPCinternal.RPCService')
    
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()    
    alltests = glob_tests(loader.getTestCaseNames(RPCinternal_test))
    suite.addTests(loader.loadTestsFromNames(alltests,RPCinternal_test))
    return suite
