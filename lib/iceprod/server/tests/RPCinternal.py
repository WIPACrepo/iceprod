#! /usr/bin/env python
"""
  Test script for RPC internal

  copyright (c) 2012 the icecube collaboration
"""

from __future__ import print_function
try:
    from server_tester import printer, glob_tests, logger
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    import logging
    logging.basicConfig()
    logger = logging.getLogger('server_tester')

import os, sys, time
import shutil
import random
from threading import Thread
from multiprocessing import Process,Queue

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tornado.ioloop import IOLoop

import iceprod.server
from iceprod.server import RPCinternal


class RPCinternal_test(unittest.TestCase):
    def setUp(self):
        super(RPCinternal_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        os.mkdir(self.test_dir)
        
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
            for _ in xrange(1000):
                for data in [str(random.random()),random.random()]:
                    logger.info('data: %r',data)
                    msg = RPCinternal.MessageFactory.createMessage(data,1)
                    logger.info('msg: %r',msg)
                    header = RPCinternal.MessageFactory.parseMessageHeader(msg[:RPCinternal.MessageFactory.HEADERSIZE])
                    logger.info('header: %r',header)
                    if header[0] != 1:
                        raise Exception('Bad header sequence number')
                    body = RPCinternal.MessageFactory.getMessage(msg[RPCinternal.MessageFactory.HEADERSIZE:],header)
                    logger.info('body: %r',body)
                    if body[1] != data:
                        raise Exception('Bad message using getmessage() with header data')
                    if not RPCinternal.MessageFactory.verifyChecksum(RPCinternal.Serializer.serialize(body[1]),header[2]):
                        logger.info('checksum failed: %s != %s',RPCinternal.MessageFactory.getChecksum(RPCinternal.Serializer.serialize(body[1])),header[2])
                        raise Exception('verifyChecksum() failed')
                    body2 = RPCinternal.MessageFactory.getMessage(msg)
                    logger.info('body2: %r',body2)
                    if body2[1] != data:
                        raise Exception('Bad message using getmessage() without header data')
        except Exception as e:
            logger.error('Error running RPCinternal.MessageFactory test - %s',str(e))
            printer('Test RPCinternal.MessageFactory',False)
            raise
        else:
            printer('Test RPCinternal.MessageFactory')
    
    def test_03_MessageFactory_Bad(self):
        try:
            for _ in xrange(1000):
                for data in [str(random.random()),random.random()]:
                    logger.info('data: %r',data)
                    msg = RPCinternal.MessageFactory.createMessage(data,1)
                    #introduce errors
                    msg2 = list(msg)
                    if msg2[RPCinternal.MessageFactory.HEADERSIZE-6] != '2':
                        msg2[RPCinternal.MessageFactory.HEADERSIZE-6] = '2' # modify body checksum
                    else:   
                        msg2[RPCinternal.MessageFactory.HEADERSIZE-6] = '3'
                    if msg2[RPCinternal.MessageFactory.HEADERSIZE-2] != '2':
                        msg2[RPCinternal.MessageFactory.HEADERSIZE-2] = '2' # modify header checksum
                    else:
                        msg2[RPCinternal.MessageFactory.HEADERSIZE-2] = '3'
                    msg2 = ''.join(msg2)
                    logger.info('msg: %r',msg)
                    logger.info('msg2: %r',msg2)
                    try:
                        header = RPCinternal.MessageFactory.parseMessageHeader(msg2[:RPCinternal.MessageFactory.HEADERSIZE])
                    except:
                        pass
                    else:
                        raise Exception('Header passed when it was not supposed to')
                    try:
                        RPCinternal.MessageFactory.getMessage(None)
                    except:
                        pass
                    else:
                        raise Exception('getMessage(None) should have raised an exception but did not')
                    try:
                        body = RPCinternal.MessageFactory.getMessage(msg2)
                    except:
                        pass
                    else:
                        raise Exception('getMessage() passed when it was not supposed to')
        except Exception as e:
            logger.error('Error running RPCinternal.MessageFactory Bad Data test - %s',str(e))
            printer('Test RPCinternal.MessageFactory Bad Data',False)
            raise
        else:
            printer('Test RPCinternal.MessageFactory Bad Data')
    
    def test_10_MessagingServer_threaded(self):
        try:
            addresses = ['127.0.0.1:0','./u:'+os.path.join(self.test_dir,'unix_socket.sock')]
            
            io_client = IOLoop()
            run_client_io = Thread(target=io_client.start)
            run_client_io.start()
            
            class echo_server(RPCinternal.Server):
                def handler(self,stream,address,seq,data):
                    self.response(stream,seq,data)
            
            try:
                for add in addresses:
                    q = Queue()
                    q2 = Queue()
                    def start_server(q,q2):
                        io_server = IOLoop()
                        sv = echo_server(address=add,io_loop=io_server)
                        sv.run()
                        logger.info('start server')
                        q2.put(':'.join(map(str,sv._sockets.values()[0].getsockname())))
                        q.get()
                        sv.stop()
                        logger.info('stop server')
                    
                    try:
                        server_process = Thread(target=start_server,args=(q,q2))
                        server_process.start()
                        a = q2.get()
                        if a.split(':',1)[0] == add.split(':',1)[0]:
                            add = a
                            logger.info('got new address when starting server: %s',a)
                        
                        def ret_func(value):
                            ret_func.ret = value
                        
                        client = RPCinternal.Client(address=add,io_loop=io_client)
                    
                        try:
                            for _ in xrange(10):
                                ret_func.ret = None
                                data = str(random.random()) # do strings
                                client.send(data,callback=ret_func)                    
                                for _ in xrange(1000):
                                    if ret_func.ret is not None:
                                        break
                                    time.sleep(0.01)
                                if ret_func.ret != data:
                                    raise Exception('client.send() failed - %s'%str(ret_func.ret))
                            
                            for _ in xrange(10):
                                ret_func.ret = None
                                data = random.random() # do floats
                                client.send(data,callback=ret_func)                    
                                for _ in xrange(1000):
                                    if ret_func.ret is not None:
                                        break
                                    time.sleep(0.01)
                                if ret_func.ret != data:
                                    raise Exception('client.send() failed - %s'%str(ret_func.ret))
                    
                        finally:
                            client.close()
                    finally:
                        q.put(1)
                        time.sleep(1)
            finally:
                try:
                    io_client.stop()
                except Exception,e:
                    logger.error('failed to stop something - %s',str(e))
                    raise
            
        except Exception as e:
            logger.error('Error running RPCinternal.MessagingServer test - %s',str(e))
            printer('Test RPCinternal.MessagingServer threaded',False)
            raise
        else:
            printer('Test RPCinternal.MessagingServer threaded')
    
    def test_11_MessagingServer_multiprocess(self):
        try:
            addresses = ['127.0.0.1:0','./u:'+os.path.join(self.test_dir,'unix_socket.sock')]   
            
            io_client = IOLoop()
            q = Queue()
            run_client_io = Thread(target=io_client.start)
            run_client_io.start()
            
            class echo_server(RPCinternal.Server):
                def handler(self,stream,address,seq,data):
                    self.response(stream,seq,data)
            
            try:
                for add in addresses:
                    q = Queue()
                    q2 = Queue()
                    def start_server(q,q2):
                        io_server = IOLoop()
                        sv = echo_server(address=add,io_loop=io_server)
                        sv.run()
                        logger.info('start server')
                        q2.put(':'.join(map(str,sv._sockets.values()[0].getsockname())))
                        q.get()
                        sv.stop()
                        logger.info('stop server')
                    
                    try:
                        server_process = Thread(target=start_server,args=(q,q2))
                        server_process.start()
                        a = q2.get()
                        if a.split(':',1)[0] == add.split(':',1)[0]:
                            add = a
                            logger.info('got new address when starting server: %s',a)
                        
                        def ret_func(value):
                            ret_func.ret = value
                        
                        client = RPCinternal.Client(address=add,io_loop=io_client)
                        try:
                            for _ in xrange(10):
                                ret_func.ret = None
                                data = str(random.random()) # do strings
                                client.send(data,callback=ret_func)
                                for _ in xrange(1000):
                                    if ret_func.ret is not None:
                                        break
                                    time.sleep(0.01)
                                if ret_func.ret != data:
                                    raise Exception('client.send() failed - %s'%str(ret_func.ret))
                            
                            for _ in xrange(10):
                                ret_func.ret = None
                                data = random.random() # do floats
                                client.send(data,callback=ret_func)
                                for _ in xrange(1000):
                                    if ret_func.ret is not None:
                                        break
                                    time.sleep(0.01)
                                if ret_func.ret != data:
                                    raise Exception('client.send() failed - %s'%str(ret_func.ret))
                        
                        finally:
                            client.close()
                    finally:
                        q.put(1)
                        time.sleep(1)
            finally:
                try:
                    io_client.stop()
                except Exception as e:
                    logger.error('failed to stop something - %s',str(e))
                    raise
            
        except Exception as e:
            logger.error('Error running RPCinternal.MessagingServer test - %s',str(e))
            printer('Test RPCinternal.MessagingServer multiprocess',False)
            raise
        else:
            printer('Test RPCinternal.MessagingServer multiprocess')
    
    def test_12_MessagingServer_timeout(self):
        try:
            addresses = ['127.0.0.1:0','./u:'+os.path.join(self.test_dir,'unix_socket.sock')]
            
            io_client = IOLoop()
            run_client_io = Thread(target=io_client.start)
            run_client_io.start()
            
            class echo_server(RPCinternal.Server):
                def handler(self,stream,address,seq,data):
                    #self.response(stream,seq,data)
                    pass
            
            try:
                for add in addresses:
                    q = Queue()
                    q2 = Queue()
                    def start_server(q,q2):
                        io_server = IOLoop()
                        sv = echo_server(address=add,io_loop=io_server)
                        sv.run()
                        logger.info('start server')
                        q2.put(':'.join(map(str,sv._sockets.values()[0].getsockname())))
                        q.get()
                        sv.stop()
                        logger.info('stop server')
                    
                    try:
                        server_process = Thread(target=start_server,args=(q,q2))
                        server_process.start()
                        a = q2.get()
                        if a.split(':',1)[0] == add.split(':',1)[0]:
                            add = a
                            logger.info('got new address when starting server: %s',a)
                        
                        def ret_func(value):
                            ret_func.ret = value
                        
                        client = RPCinternal.Client(address=add,io_loop=io_client)
                    
                        try:
                            for _ in xrange(3):
                                ret_func.ret = None
                                data = str(random.random()) # do strings
                                client.send(data,timeout=1.0,callback=ret_func)
                                for _ in xrange(1000):
                                    if ret_func.ret is not None:
                                        break
                                    time.sleep(0.01)
                                if not isinstance(ret_func.ret,Exception):
                                    raise Exception('timeout failed (str) - %s'%str(ret_func.ret))
                            
                            for _ in xrange(3):
                                ret_func.ret = None
                                data = random.random() # do floats
                                client.send(data,timeout=1.0,callback=ret_func)
                                for _ in xrange(1000):
                                    if ret_func.ret is not None:
                                        break
                                    time.sleep(0.01)
                                if not isinstance(ret_func.ret,Exception):
                                    raise Exception('timeout failed (float) - %s'%str(ret_func.ret))
                    
                        finally:
                            client.close()
                    finally:
                        q.put(1)
                        time.sleep(1)
            finally:
                try:
                    io_client.stop()
                except Exception,e:
                    logger.error('failed to stop something - %s',str(e))
                    raise
            
        except Exception as e:
            logger.error('Error running RPCinternal.MessagingServer timeout test - %s',str(e))
            printer('Test RPCinternal.MessagingServer timeout',False)
            raise
        else:
            printer('Test RPCinternal.MessagingServer timeout')
    
    def test_20_RPCServer_threaded(self):
        try:
            addresses = ['127.0.0.1:0','./u:'+os.path.join(self.test_dir,'unix_socket.sock')]   
            
            io_client = IOLoop()
            q = Queue()
            run_client_io = Thread(target=io_client.start)
            run_client_io.start()
            
            def cb_func(callback):
                callback('message from cb_func on RPC server')        
            
            class RPC():
                @staticmethod
                def test(callback):
                    return 'message from RPC server'
                @staticmethod
                def test2(callback):
                    cb_func(callback)
            
            try:
                for add in addresses:
                    q = Queue()
                    q2 = Queue()
                    def start_server(q,q2):
                        io_server = IOLoop()
                        sv = RPCinternal.RPCServer(RPC,address=add,io_loop=io_server)
                        sv.run()
                        logger.info('start server')
                        q2.put(':'.join(map(str,sv._sockets.values()[0].getsockname())))
                        q.get()
                        sv.stop()
                        logger.info('stop server')
                    
                    try:
                        server_process = Thread(target=start_server,args=(q,q2))
                        server_process.start()
                        a = q2.get()
                        if a.split(':',1)[0] == add.split(':',1)[0]:
                            add = a
                            logger.info('got new address when starting server: %s',a)
                        
                        def ret_func(value):
                            ret_func.ret = value
                        
                        client = RPCinternal.RPCClient(address=add,io_loop=io_client)
                        
                        try:
                            ret_func.ret = None
                            client.test(callback=ret_func)
                            for _ in xrange(1000):
                                if ret_func.ret is not None:
                                    break
                                time.sleep(0.01)
                            if ret_func.ret != 'message from RPC server':
                                raise Exception('client.test() failed - %s'%str(ret_func.ret))
                            
                            ret_func.ret = None
                            client.test2(callback=ret_func)
                            for _ in xrange(1000):
                                if ret_func.ret is not None:
                                    break
                                time.sleep(0.01)
                            if ret_func.ret != 'message from cb_func on RPC server':
                                raise Exception('client.test2() failed - %s'%str(ret_func.ret))
                                
                            ret = client.test(async=False)
                            if ret != 'message from RPC server':
                                raise Exception('non-async client.test() failed - %s'%str(ret))
                            
                            ret = client.test2(async=False)
                            if ret != 'message from cb_func on RPC server':
                                raise Exception('non-async client.test2() failed - %s'%str(ret))
                        
                        finally:
                            client.close()
                    finally:
                        q.put(1)
                        time.sleep(1)
            finally:
                try:
                    io_client.stop()
                except Exception,e:
                    logger.error('failed to stop something - %s',str(e))
            
        except Exception as e:
            logger.error('Error running RPCinternal.RPCServer test - %s',str(e))
            printer('Test RPCinternal.RPCServer threaded',False)
            raise
        else:
            printer('Test RPCinternal.RPCServer threaded')
    
    def test_21_RPCServer_multiprocess(self):
        try:
            addresses = ['127.0.0.1:0','./u:'+os.path.join(self.test_dir,'unix_socket.sock')]   
            
            io_client = IOLoop()
            q = Queue()
            run_client_io = Thread(target=io_client.start)
            run_client_io.start()
            
            def cb_func(callback):
                callback('message from cb_func on RPC server')        
            
            class RPC():
                @staticmethod
                def test(callback):
                    return 'message from RPC server'
                @staticmethod
                def test2(callback):
                    cb_func(callback)
            
            try:
                for add in addresses:
                    q = Queue()
                    q2 = Queue()
                    def start_server(q,q2):
                        io_server = IOLoop()
                        sv = RPCinternal.RPCServer(RPC,address=add,io_loop=io_server)
                        sv.run()
                        logger.info('start server')
                        q2.put(':'.join(map(str,sv._sockets.values()[0].getsockname())))
                        q.get()
                        sv.stop()
                        logger.info('stop server')
                    
                    try:
                        server_process = Thread(target=start_server,args=(q,q2))
                        server_process.start()
                        a = q2.get()
                        if a.split(':',1)[0] == add.split(':',1)[0]:
                            add = a
                            logger.info('got new address when starting server: %s',a)
                        
                        def ret_func(value):
                            ret_func.ret = value
                        
                        client = RPCinternal.RPCClient(address=add,io_loop=io_client)
                        
                        try:
                            ret_func.ret = None
                            client.test(callback=ret_func)
                            for _ in xrange(1000):
                                if ret_func.ret is not None:
                                    break
                                time.sleep(0.01)
                            if ret_func.ret != 'message from RPC server':
                                raise Exception('client.test() failed - %s'%str(ret_func.ret))
                            
                            ret_func.ret = None
                            client.test2(callback=ret_func)                
                            for _ in xrange(1000):
                                if ret_func.ret is not None:
                                    break
                                time.sleep(0.01)
                            if ret_func.ret != 'message from cb_func on RPC server':
                                raise Exception('client.test2() failed - %s'%str(ret_func.ret))
                        
                            ret = client.test(async=False)
                            if ret != 'message from RPC server':
                                raise Exception('non-async client.test() failed - %s'%str(ret))
                            
                            ret = client.test2(async=False)
                            if ret != 'message from cb_func on RPC server':
                                raise Exception('non-async client.test2() failed - %s'%str(ret))
                            
                        finally:
                            client.close()
                    finally:
                        q.put(1)
                        time.sleep(1)
            finally:
                try:
                    io_client.stop()
                except Exception,e:
                    logger.error('failed to stop something - %s',str(e))
            
        except Exception as e:
            logger.error('Error running RPCinternal.RPCServer test - %s',str(e))
            printer('Test RPCinternal.RPCServer multiprocess',False)
            raise
        else:
            printer('Test RPCinternal.RPCServer multiprocess')

    def test_22_RPCinternal_RPCServer_timeout(self):
        try:
            addresses = ['127.0.0.1:0','./u:'+os.path.join(self.test_dir,'unix_socket.sock')]   
            
            io_client = IOLoop()
            q = Queue()
            run_client_io = Thread(target=io_client.start)
            run_client_io.start()
            
            def cb_func(callback):
                pass
            
            class RPC():
                @staticmethod
                def test(callback):
                    pass
                @staticmethod
                def test2(callback):
                    cb_func(callback)
            
            try:
                for add in addresses:
                    q = Queue()
                    q2 = Queue()
                    def start_server(q,q2):
                        io_server = IOLoop()
                        sv = RPCinternal.RPCServer(RPC,address=add,io_loop=io_server)
                        sv.run()
                        logger.info('start server')
                        q2.put(':'.join(map(str,sv._sockets.values()[0].getsockname())))
                        q.get()
                        sv.stop()
                        logger.info('stop server')
                    
                    try:
                        server_process = Thread(target=start_server,args=(q,q2))
                        server_process.start()
                        a = q2.get()
                        if a.split(':',1)[0] == add.split(':',1)[0]:
                            add = a
                            logger.info('got new address when starting server: %s',a)
                        
                        def ret_func(value):
                            ret_func.ret = value
                        
                        client = RPCinternal.RPCClient(timeout=1.0,address=add,io_loop=io_client)
                        
                        try:
                            ret_func.ret = None
                            client.test(callback=ret_func)
                            for _ in xrange(1000):
                                if ret_func.ret is not None:
                                    break
                                time.sleep(0.01)
                            if not isinstance(ret_func.ret,Exception):
                                raise Exception('timeout failed (client.test) - %s'%str(ret_func.ret))
                            
                            ret_func.ret = None
                            client.test2(callback=ret_func)
                            for _ in xrange(1000):
                                if ret_func.ret is not None:
                                    break
                                time.sleep(0.01)
                            if not isinstance(ret_func.ret,Exception):
                                raise Exception('timeout failed (client.test2) - %s'%str(ret_func.ret))
                                
                            ret = client.test(async=False)
                            if not isinstance(ret,Exception):
                                raise Exception('timeout failed (client.test async) - %s'%str(ret))
                            
                            ret = client.test2(async=False)
                            if not isinstance(ret,Exception):
                                raise Exception('timeout failed (client.test2 async) - %s'%str(ret))
                        
                        finally:
                            client.close()
                    finally:
                        q.put(1)
                        time.sleep(1)
            finally:
                try:
                    io_client.stop()
                except Exception,e:
                    logger.error('failed to stop something - %s',str(e))
            
        except Exception as e:
            logger.error('Error running RPCinternal.MessagingServer timeout test - %s',str(e))
            printer('Test RPCinternal.MessagingServer timeout',False)
            raise
        else:
            printer('Test RPCinternal.MessagingServer timeout')
    
    
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()    
    alltests = glob_tests(loader.getTestCaseNames(RPCinternal_test))
    suite.addTests(loader.loadTestsFromNames(alltests,RPCinternal_test))
    return suite
