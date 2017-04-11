.. index:: Global_Queueing
.. _Global_Queueing:

Global Queueing
===============

Global queueing is initiated on the "client" iceprod server. In the 
:ref:`config`, the master url must be set to enable global queueing.

Protocol
--------

The "client" will send a json-rpc request to the "master" iceprod
server, calling the `queue_master` method with the available
resources as a parameter.  The "master" will then find appropriate
task matches and respond with such.

For more details on the exact protocol, see :ref:`jsonRPCprotocol`.

