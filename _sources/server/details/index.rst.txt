.. index:: Technical_Server
.. _Technical_Server:

IceProd Server Details
======================

The server handles many different tasks and has several independent modules to
take care of them.  It uses asynchronous programming to avoid blocking on I/O.
The server daemon handles startup of each module.  It can also reload, restart,
stop, or kill the modules.

IceProd can be reloaded to update a configuration variable.  This will only
reload the affected parts of IceProd, while letting the rest continue running.


Configuration
-------------

The configuration is in :ref:`config`, which is stored as a
dictionary and dumped to a json file on every modification.

Queueing
--------

This is where IceProd actually submits tasks to a grid.  Various grid
architectures are supported through plugins:

* `HTCondor <http://research.cs.wisc.edu/htcondor/>`_ (primary)
* `PBS <http://en.wikipedia.org/wiki/Portable_Batch_System>`_ / `Torque <http://www.adaptivecomputing.com/products/open-source/torque/>`_
* `Cream <http://www.eu-emi.eu/products/-/asset_publisher/1gkD/content/cream-2>`_

The queueing module can submit to multiple grids at once since multiple
plugins can be active at the same time.

.. toctree::
    :maxdepth: 3

    submit_to_queue
    task_relationships
    lifecycles
    global_queueing

Database
--------

IceProd stores most of its information in a database.  By default, it uses
SQLite and a few local database files.  For a master site that must handle
the information of a large number of sites, MySQL can be used as a database
(though this will take some external setup).

The database structure is given in :ref:`dbtables`.


Website
-------

The website is the main way to communicate with IceProd, both from jobs and
as a user.

.. toctree::
    :maxdepth: 3

    async
    website
    auth
    user_accounts

Proxying
--------

Proxying is taken care of by Squid Cache, if configured.  IceProd can start
its own squid or use an external caching server.

Other Utilities
---------------

OpenSSL
^^^^^^^

OpenSSL can be used to make a local CA certificate, make a regular certificate
signed by a CA certificate, or verify a certificate.  This is mostly used by
sites to create self-signed certificates.

Scheduler
^^^^^^^^^

The scheduler can be used like cron, to run assigned tasks at specific
intervals.  It will mostly be used to update graphs and run other timed
interactions.


