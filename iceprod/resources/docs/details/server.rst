.. index:: Technical_Server
.. _Technical_Server:

IceProd Server
==============

The server handles many different tasks and has several independent modules to take care of them.  It uses `Python Multiprocessing <http://docs.python.org/2/library/multiprocessing.html>`_ to prevent anything in one module from inadvertantly affecting another module.  The main process handles startup and communication between each module.  It can also reload, restart, stop, or kill the modules.

IceProd can be reloaded to update a configuration variable.  This will only reload the affected parts of IceProd, while letting the rest continue running.


Queueing
--------

This is where IceProd actually submits tasks to a grid.  Various grid architectures are supported through plugins:

* `HTCondor <http://research.cs.wisc.edu/htcondor/>`_ (primary)
* `PBS <http://en.wikipedia.org/wiki/Portable_Batch_System>`_ / `Torque <http://www.adaptivecomputing.com/products/open-source/torque/>`_
* `Cream <http://www.eu-emi.eu/products/-/asset_publisher/1gkD/content/cream-2>`_

The queueing module can submit to multiple grids at once since multiple plugins can be active at the same time.


Database
--------

IceProd stores most of its information in a database.  By default, it uses SQLite and a few local database files.  For a master site that must handle the information of a large number of sites, MySQL can be used as a database (though this will take some external setup).

There is a primary database and an archive database, so as to keep current information small and fast while still providing access to older information.

The database structure is given in :mod:`iceprod.server.modules.db`.


Website
-------

The website is the main way to communicate with IceProd.  It handles several different jobs:

* Human
    * dataset submission
    * dataset editing, other in progress actions
    * viewing progress
* Computer
    * running task communications
    * site-to-site communications
    * file downloads
    * file proxying

In order to handle the many hundreds or thousands of requests it could get, the website was built on asynchronous principles.

Async
^^^^^

For IceProd, the main method of going asynchronous is by `callback functions <http://en.wikipedia.org/wiki/Asynchronous_I/O#Callback_functions>`_.  Instead of waiting for a long function to return, it is instead given a callback which it calls when finished.  This allows the main thread to continue processing additional requests.  While this would not work for compute-bound applications, network I/O lends itself very well to this because of the high latencies between network events.  Thus, as long as the main thread can check for a new request every few microseconds, it can also run a large number of shorter functions at the same time.

In practice, the main benefit comes when interacting with the filesystem or database.  Both of these actions would normally spend a large number of clock cycles waiting for a return value, but can now do useful work in the meantime.  Eventually the original action will be finished and call the callback; this gets scheduled on the main thread and runs shortly.

Tornado
"""""""

IceProd's main access to asynchronous I/O is through `Tornado <http://www.tornadoweb.org>`_.  This is the basic framework for the website, and provides utility functions for asynchronous sockets that are used in several other places.  Tornado also allows us to make http downloads asynchronously.

GridFTP
"""""""

Using the C API for the `Globus Toolkit <http://www.globus.org/toolkit>`_ allows us to use GridFTP asynchronously as well.  This has been integrated into Tornado's callback mechanism in Python with mostly complete pybindings for ease of use.

Callback Details
""""""""""""""""

Functions that accept callbacks generally take the form of::

    def foo(arg1,arg2,callback):
        # do some work
        callback(result)

The callback argument should be a function capable of handling the result it is given.

For I/O applications, it is often necessary to pass connection details to the callback function.  There are two approaches:

1. Quick and Dirty::

    def foo(arg1,callback,callback_args):
        # do work
        callback(result,callback_args)
    def cb(result,args):
        # handle result
    foo(1,cb,{'handle':None})

|
|     This passes callback arguments through the worker function using a second argument.  It is used often in lower level languages where things must be compiled to binary before running.

2. Functional Programming::

    def foo(arg1,callback):
        # do work
        callback(result)
    def cb(result,args={}):
        # handle result
    foo(1,partial(cb,args={'handle':None}))

|
|     In functional programming, function signatures can be changed by filling in only some of the arguments and treating that as a new function.  Python allows this with the ``functools.partial()`` built-in.

Most of the code in the IceProd server uses the functional programming style, though there is some of the first style in the GridFTP python bindings.
    
Internal RPC
^^^^^^^^^^^^

RPC that is internal to the server is handled by an RPC service created on top of Tornado sockets.  This is the primary link between different components of the server and the database module.  

Proxying
^^^^^^^^

The website provides proxying and caching services to http or gridftp urls.  This allows a site to cache particular files and serve a copy out to all the jobs.

Task Communication
^^^^^^^^^^^^^^^^^^

Tasks communicate with the server using a json-rpc interface built into the website.  For most communications, this involves talking with the database using the internal RPC.

Site-to-Site Communication
^^^^^^^^^^^^^^^^^^^^^^^^^^

Communication between sites also use the json-rpc interface in the website.

Human Interaction
^^^^^^^^^^^^^^^^^

The website can modify things in the database using ajax and the json-rpc interface in the website.

Nginx
^^^^^

For security, the website uses nginx as a front end.  Nginx handles all SSL certificate checking, static files, and file uploading before proxying the request to Tornado.  Nginx has been proven to be a very robust web server, with over 10% of the web (and growing) using it.  It is also the recommended front end for production Tornado sites.


Other Utilities
---------------

OpenSSL
^^^^^^^

OpenSSL can be used to make a local CA certificate, make a regular certificate signed by a CA certificate, or verify a certificate.  This is mostly used by the master to let other IceProd instances into the trusted pool, and by the server to give tasks the appropriate CA cert.

Scheduler
^^^^^^^^^

The scheduler can be used like cron, to run assigned tasks at specific intervals.  It will mostly be used to update graphs and run other timed interactions.


