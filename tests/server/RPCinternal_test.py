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
                
            finally:
                ioloop.stop()
            
        except Exception as e:
            logger.error('Error running RPCinternal.Server test - %s',str(e))
            printer('Test RPCinternal.Server',False)
            raise
        else:
            printer('Test RPCinternal.Server')
    
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()    
    alltests = glob_tests(loader.getTestCaseNames(RPCinternal_test))
    suite.addTests(loader.loadTestsFromNames(alltests,RPCinternal_test))
    return suite
