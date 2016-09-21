.. index:: RPCinternal_Details
.. _RPCinternal_Details:

RPC Internal Details
====================

Internal RPC relies on :ref:`ZeroMQ` sockets with a binary message protocol
to give speed, strong data integrity, and reliability.

A hub and spoke model is used, such that every client/service maintains a
connection to the server and can send and receive requests over that
connection.

All message passing is asynchronous, allowing requests to proceed in parallel
and out-of-order.

.. _Low_Level_Message_Protocol:

Low-Level Message Protocol
--------------------------

Each message uses ZeroMQ framing to send the header and body. The server
additionally receives the client_id that is connecting as the first element
of the frame.

Header
^^^^^^

The message header is a binary packed string:

* id tag (4 bytes - 'IRPC'): Denotes that this message uses the IceProd RPC
  protocol
* version (1 byte - uint): The protocol version, currently 1.
* sequence number (4 bytes - uint): The sequence number, to help prevent
  duplicates. The same number is expected in the response.
* body length (4 bytes - uint): The length of the message body.
* body checksum (4 bytes - uint): A CRC32 checksum of the body.
* message type (1 byte - uint): The :class:`MessageFactory.MESSAGE_TYPE <iceprod.server.RPCinternal.MessageFactory.MESSAGE_TYPE>`.

  If the message type == SERVICE:
  
    * service length (4 bytes - uint): The length of the service name.
    * service name (string): The name of the service being offered.

  
* header checksum (4 bytes - uint): A CRC32 checksum of the rest of the header.

Body
^^^^

The body is a serialized python object.

The current serialization is pickle (highest protocol). This may cause 
conflicts if different versions of python are used for the sender and
the receiver.

RPC Message Protocol
--------------------

The RPC protocol is built on top of the :ref:`Low_Level_Message_Protocol`.
It controls what is actually in the body of each message.

Request Object:

* method (string) - Name of the method to be invoked.
* params (dict) - Keyword arguments to the method.

Response Object:

* result (object) - The returned result from the method. This is REQUIRED on
  success, and MUST NOT exist if there was an error.
* error (object) - A description of the error, likely an Exception object.
  This is REQUIRED on error and MUST NOT exist on success.

Examples
--------

Here are some worked examples, showing complete headers and bodies.

Client to Server
^^^^^^^^^^^^^^^^

This message is sent from a client to the server, intended for the server.

::

    Client       Server
    [REQ] ---1--> [REP]
          <--2---
    
    1 C : [('IRPC',1,15,0xAC34B281,SERVER,0x8372E49D),
           {'method':'ping','params':{'test':'a test arg'}}]
    1 S : [1,
           ('IRPC',1,15,0xAC34B281,SERVER,0x8372E49D),
           {'method':'ping','params':{'test':'a test arg'}}]
    
    2 S : [1,
           ('IRPC',1,15,0x38294575,RESPONSE,0xC83DF236),
           {'result':'pong'}]
    2 C : [('IRPC',1,15,0x38294575,RESPONSE,0xC83DF236
           {'result':'pong'}]

Client to Service
^^^^^^^^^^^^^^^^^

This message is sent from a client to the service, routed through the server.

::

    Client       Server        serVice
    [REQ] ---1--> [FWD] ---2--> [REP]
          <--4--- [FWD] <--3---
    
    1 C : [('IRPC',1,15,0xAC34B281,SERVICE,4,'Test',0x8372E49D),
           {'method':'ping','params':{'test':'a test arg'}}]
    1 S : [1,
           ('IRPC',1,15,0xAC34B281,SERVICE,4,'Test',0x8372E49D),
           {'method':'ping','params':{'test':'a test arg'}}]
    
    2 S : [2,
           ('IRPC',1,23,0xAC34B281,SERVICE,4,'Test',0x928437BD),
           {'method':'ping','params':{'test':'a test arg'}}]
    2 V : [('IRPC',1,23,0xAC34B281,SERVICE,4,'Test',0x928437BD),
           {'method':'ping','params':{'test':'a test arg'}}]
    
    3 V : [('IRPC',1,23,0x38294575,RESPONSE,0xB83D2018),
           {'result':'pong'}]
    3 S : [2,
           ('IRPC',1,23,0x38294575,RESPONSE,0xB83D2018),
           {'result':'pong'}]
    
    4 S : [1,
           ('IRPC',1,15,0x38294575,RESPONSE,0xC83DF236),
           {'result':'pong'}]
    4 C : [('IRPC',1,15,0x38294575,RESPONSE,0xC83DF236
           {'result':'pong'}]

Client to Broadcast
^^^^^^^^^^^^^^^^^^^

This message is sent from a client to all services, routed through the server.
It expects an ACK from the server, but not from the services.

::

    Client       Server        serVice
    [REQ] ---1--> [REP]
          <--2---
                  [FWD] ---3--> [REP]
                        <--4---
    
    1 C : [('IRPC',1,15,0xAC34B281,BROADCAST,0x19456DA2),
           {'method':'ping','params':{'test':'a test arg'}}]
    1 S : [1,
           ('IRPC',1,15,0xAC34B281,BROADCAST,4,'Test',0x19456DA2),
           {'method':'ping','params':{'test':'a test arg'}}]
    
    2 S : [1,
           ('IRPC',1,15,0x6592DE72,BROADCAST_ACK,0x493837F3),
           {'result':'ack'}]
    2 C : [('IRPC',1,15,0x6592DE72,BROADCAST_ACK,0x493837F3
           {'result':'ack'}]
    
    3 S : [2,
           ('IRPC',1,23,0xAC34B281,BROADCAST,0x5635FA95),
           {'method':'ping','params':{'test':'a test arg'}}]
    3 V : [('IRPC',1,23,0xAC34B281,BROADCAST,0x5635FA95),
           {'method':'ping','params':{'test':'a test arg'}}]
    
    4 V : [('IRPC',1,23,0x33948CD1,BROADCAST_ACK,0x01B75024),
           {'result':None}]
    4 S : [2,
           ('IRPC',1,23,0x33948CD1,BROADCAST_ACK,0x01B75024),
           {'result':None}]

Class Documentation
-------------------

.. autoclass:: iceprod.server.RPCinternal.Serializer

.. autoclass:: iceprod.server.RPCinternal.MessageFactory

.. autoclass:: iceprod.server.RPCinternal.Base

For other class documentation, see :ref:`RPCinternal`.