JSON-RPC Interface
==================

Advanced users may want to query IceProd from automated scripts or
other programs.  This is possible through a `JSON-RPC`_ API.

.. _JSON-RPC: http://www.jsonrpc.org/specification

.. highlight:: bash

To test that your setup works, an `echo` method is available::

    curl --data-ascii '{"jsonrpc": "2.0", "method": "echo", "params": {"value": "foo"}, "id": 1}' https://iceprod2.icecube.wisc.edu/jsonrpc

See :py:mod:`iceprod.server.dbmethods.rpc` for available methods.

Authentication
--------------

For public dataset monitoring, no authentication is requred.  To view
private datasets, call actions on datasets, or submit new datasets,
authentication with a token is required.

An authentication token can be obtained within the account settings
in the website.  This can be added to requests as the `passkey` parameter::

    curl --data-ascii '{"jsonrpc": "2.0", "method": "echo", "params": {"value": "foo", "passkey": "my_hex_passkey_here"}, "id": 1}' https://iceprod2.icecube.wisc.edu/jsonrpc

.. danger::

   Anyone with the authentication token is basically you.  It is valid 
   for any action that does not require :ref:`Two Factor Authentication`.
   Be careful with the token!
