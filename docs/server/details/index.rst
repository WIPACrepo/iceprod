.. index:: Technical_Server
.. _Technical_Server:

IceProd Server
==============

The server handles many different tasks and has several independent modules to take care of them.  It uses `Python Multiprocessing <http://docs.python.org/2/library/multiprocessing.html>`_ to prevent anything in one module from inadvertently affecting another module.  The main process handles startup of each module.  It can also reload, restart, stop, or kill the modules.

IceProd can be reloaded to update a configuration variable.  This will only reload the affected parts of IceProd, while letting the rest continue running.

Configuration
-------------

The :ref:`BasicConfig` handles basic startup of IceProd, and has only the necessary things for that task.
Mostly, this is which modules to start by default, the address of the internal RPC messaging server, and logging information.

The rest of the configuration is in :ref:`config`, which are stored as a dictionary and dumped to a json file on every modification.  These can be accessed individually via internal RPC, but are usually locally cached in bulk during module startup.
Updates are pushed out to all modules when changes occur.

Queueing
--------

This is where IceProd actually submits tasks to a grid.  Various grid architectures are supported through plugins:

* `HTCondor <http://research.cs.wisc.edu/htcondor/>`_ (primary)
* `PBS <http://en.wikipedia.org/wiki/Portable_Batch_System>`_ / `Torque <http://www.adaptivecomputing.com/products/open-source/torque/>`_
* `Cream <http://www.eu-emi.eu/products/-/asset_publisher/1gkD/content/cream-2>`_

The queueing module can submit to multiple grids at once since multiple plugins can be active at the same time.

.. toctree::
    :maxdepth: 3
    
    global_queueing

Database
--------

IceProd stores most of its information in a database.  By default, it uses SQLite and a few local database files.  For a master site that must handle the information of a large number of sites, MySQL can be used as a database (though this will take some external setup).

There is a primary database and an archive database, so as to keep current information small and fast while still providing access to older information.

The database structure is given in :ref:`dbtables`.


Website
-------

The website is the main way to communicate with IceProd, both from jobs and as a user.

.. toctree::
    :maxdepth: 3
    
    async
    website
    
Internal RPC
------------

RPC that is internal to the server is handled by an RPC service created on top of ZeroMQ sockets.  This is the primary link between different components of the server.

.. toctree::
    :maxdepth: 3

    zeromq
    rpcinternal

Proxying
--------

Proxying is taken care of by Squid Cache, if configured.  IceProd can start its own squid or use an external caching server.


Other Utilities
---------------

OpenSSL
^^^^^^^

OpenSSL can be used to make a local CA certificate, make a regular certificate signed by a CA certificate, or verify a certificate.  This is mostly used by the master to let other IceProd instances into the trusted pool, and by the server to give tasks the appropriate CA cert.

Scheduler
^^^^^^^^^

The scheduler can be used like cron, to run assigned tasks at specific intervals.  It will mostly be used to update graphs and run other timed interactions.


