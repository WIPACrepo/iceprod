.. index:: jsonRPCclient

JSON-RPC
========

.. _jsonRPCprotocol:

JSON-RPC Message Protocol
-------------------------

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

.. _jsonRPCclient:

JSON-RPC Client
---------------

.. automodule:: iceprod.core.jsonRPCclient
   :no-members:
   
.. autoclass:: JSONRPC
   :no-members:

   .. automethod:: MetaJSONRPC.start

   .. automethod:: MetaJSONRPC.stop

   .. automethod:: MetaJSONRPC.restart

.. autoclass:: Client



