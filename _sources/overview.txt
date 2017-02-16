Overview
========

IceProd is broken up into two main parts, the core and server.

Core
----

The core of IceProd is what actually runs on compute nodes.  It is capable of
running in two different modes:

Single Task Mode
^^^^^^^^^^^^^^^^

In this mode, IceProd is given a config file at startup.  It will only run
that task, then exit.  This is the legacy mode.

Many Task Mode
^^^^^^^^^^^^^^

In this mode, IceProd downloads task config files from the server.  It will
download a task, process it, then download another until the server tells it
to stop.  This is a good mode for groups of tasks that have the same large
dependencies; the dependency can be downloaded once and used multiple times.

This mode can also be called "Pilot Mode" since it calls back to the server
with the resources available, only matching those jobs that can likely run
there.

Server
------

The server of IceProd has many responsibilities:

* Queue tasks onto grids
* Store all info in local DB
* Host website for users
* Communicate securely with running tasks
* Communicate securely with other IceProd sites
