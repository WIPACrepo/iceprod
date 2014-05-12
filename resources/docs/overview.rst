.. index:: Overview
.. _Overview:

Overview
========

IceProd is broken up into three main parts:

1. Core
2. Server
3. Modules

Core
----

The core of IceProd is what actually runs on compute nodes.  It is capable of running in two different modes:

Single Task Mode
^^^^^^^^^^^^^^^^

In this mode, IceProd is given a config file at startup.  It will only run that task, then exit.  This is the standard mode.

Many Task Mode
^^^^^^^^^^^^^^

In this mode, IceProd downloads task config files from the server.  It will download a task, process it, then download another until the server tells it to stop.  This is a good mode for groups of tasks that have the same large dependencies; the dependency can be downloaded once and used multiple times.


Server
------

The server of IceProd has many responsibilities:

* Queue tasks onto grids
* Store all info in local DB
* Host website for DB info
* Communicate securely with running tasks
* Communicate securely with other IceProd sites
* (master only) Queue multi-site datasets to individual sites at task level
* (master only) Store all DB info for connected sites


Modules
-------

The modules part of IceProd contains a library of standard modules that can be run by tasks.  They are more fully featured than a simple script because they obtain the full working environment of the IceProd core, containing module parameters and general settings.  These are designed to be base classes which can be subclassed by other scripts, though some can be used standalone.
