"""
ZeroMQ communication base classes.
"""

from __future__ import absolute_import, division, print_function

import errno
import logging
import random
import string
from zmq.eventloop import ioloop, zmqstream
import zmq
ioloop.install()

logger = logging.getLogger('zeromq')

class ZmqProcess(object):
    """
    This is the base for all processes and offers utility functions
    for setup and creating new streams.
    """
    def __init__(self, io_loop = None):
        super(ZmqProcess,self).__init__()

        self.context = None
        """The ZeroMQ :class:`~zmq.Context` instance."""

        self.io_loop = io_loop
        """PyZMQ's event loop (:class:`~zmq.eventloop.ioloop.IOLoop`)."""

        self._restart = False

    def setup(self):
        """
        Creates a :attr:`context` and an event :attr:`loop` for the process.
        """
        self.context = zmq.Context()
        if not self.io_loop:
            self.io_loop = ioloop.IOLoop()

    def run(self):
        self._restart = True
        while self._restart:
            self._restart = False
            try:
                self.io_loop.start()
            except zmq.ZMQError as e:
                if e.errno == errno.EINTR:
                    self._restart = True
                    logger.warn('ZMQError: EINTR',exc_info=True)
                else:
                    logger.warn('ZMQError',exc_info=True)
                    raise
            except Exception:
                logger.warn('ioloop error',exc_info=True)
                raise
        self.io_loop.close()

    def restart(self):
        self._restart = True
        self.io_loop.stop()

    def stop(self):
        self.io_loop.stop()

    def make_stream(self, sock_type, addr, bind, callback=None, subscribe=b'',
                    identity=None):
        """
        Creates a :class:`~zmq.eventloop.zmqstream.ZMQStream`.

        :param sock_type: The ZeroMQ socket type (e.g. ``zmq.REQ``)
        :param addr: Address to bind or connect to formatted as *host:port*,
                *(host, port)* or *host* (bind to random port), or a list of
                such entries if connecting.
                If *bind* is ``True``, *host* may be:

                - the wild-card ``*``, meaning all available interfaces,
                - the primary IPv4 address assigned to the interface, in its
                  numeric representation or
                - the interface name as defined by the operating system.

                If *bind* is ``False``, *host* may be:

                - the DNS name of the peer or
                - the IPv4 address of the peer, in its numeric representation.

                If *addr* is just a host name without a port and *bind* is
                ``True``, the socket will be bound to a random port.
        :param bind: Binds to *addr* if ``True`` or tries to connect to it
                otherwise.
        :param callback: A callback for
                :meth:`~zmq.eventloop.zmqstream.ZMQStream.on_recv`, optional
        :param subscribe: Subscription pattern for *SUB* sockets, optional,
                defaults to ``b''``.
        :param identity: Identity for *DEALER* sockets, optional,
                defaults to random string.
        :returns: A tuple containg the stream and the port number.

        """
        sock = self.context.socket(sock_type)
        port = None

        if bind:
            # addr may be 'host:port' or ('host', port)
            logger.info('binding to address %s',addr)
            if isinstance(addr, str):
                addr = addr.rsplit(':',1)
            logger.debug('after transformation: %s',addr)
            host, port = addr if len(addr) == 2 else (addr[0], None)
            logger.debug('final check: %s:%s',host,port)
            # Bind/connect the socket
            if port:
                sock.bind('%s:%s' % (host, port))
            else:
                port = sock.bind_to_random_port(host)
            logger.debug('socket bound to %s:%s',host,port)
        else:
            if not isinstance(addr,(list,tuple)):
                addr = [addr]
            for a in addr:
                if isinstance(a, str):
                    a = a.rsplit(':',1)
                    if len(a) == 2:
                        a = '%s:%s' % (a[0], a[1])
                    else:
                        a = '%s' % (a[0],)
                logger.info('attempting to connect to %s',a)
                sock.connect(a)
                logger.debug('socket connected to %s',a)

        # Add a default subscription for SUB sockets
        if sock_type == zmq.SUB:
            sock.setsockopt(zmq.SUBSCRIBE, subscribe)

        # Add an identity for DEALER sockets
        if sock_type == zmq.DEALER:
            if not identity:
                identity = ''.join(random.sample(string.letters+string.digits,10))
            sock.setsockopt(zmq.IDENTITY, identity)
            logger.debug('DEALER identity = %s',identity)

        # Create the stream and add the callback
        stream = zmqstream.ZMQStream(sock, self.io_loop)
        if callback:
            stream.on_recv(callback)

        return stream,port

class AsyncSendReceive(ZmqProcess):
    """
    Create an asyncronous send or receive socket.
    """
    def __init__(self, address, identity=None, bind=False,
                 recv_handler=None, **kwargs):
        super(AsyncSendReceive,self).__init__(**kwargs)

        self.address = address
        self.identity = identity
        self.bind = bind
        self.recv_handler = recv_handler

        self.stream = None
        self.port = None

    def setup(self):
        """Sets up PyZMQ and creates all streams."""
        super(AsyncSendReceive,self).setup()

        # Create the stream and add the message handler
        if self.bind:
            socket = zmq.ROUTER
        else:
            socket = zmq.DEALER
        try:
            self.stream,self.port = self.make_stream(socket, self.address,
                                                     bind=self.bind,
                                                     callback=self.recv_handler,
                                                     identity=self.identity)
        except Exception:
            logger.warn('make_stream error',exc_info=True)
            raise

    def send(self,msg):
        """Sends a message on the stream."""
        if self.stream:
            try:
                logger.debug('sending message %r',msg)
                if isinstance(msg,(list,tuple)):
                    self.stream.send_multipart(msg)
                else:
                    self.stream.send(msg)
            except zmq.ZMQError as e:
                if e.errno == errno.EINTR:
                    logger.warn('ZMQError in send: EINTR',exc_info=True)
                else:
                    logger.warn('ZMQError in send:',exc_info=True)
                    raise
            except Exception:
                logger.warn('Exception in send:',exc_info=True)
                raise
        else:
            logger.error('zmq stream not available')
