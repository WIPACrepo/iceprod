.. index:: Quickstart
.. _Quickstart:

Quickstart
==========

tl;dr
-----

For the impatient::

    python setup.py install --user


Getting Started
---------------

Welcome!  This guide will assume that you want to set up IceProd locally on a new machine.

Download the Project
^^^^^^^^^^^^^^^^^^^^

To download the project, check it out from svn::

    $ svn co http://code.icecube.wisc.edu/svn/sandbox/dschultz/iceprod2 iceprod

If you do not need to stay up to date, you may also get an exported version::

    $ svn export http://code.icecube.wisc.edu/svn/sandbox/dschultz/iceprod2 iceprod

Build the Project
^^^^^^^^^^^^^^^^^

The main requirement of IceProd is Python 2.7 or greater.  Python 3 is supported.

IceProd acts like a normal python project, and with the help of `setuptools`
can download and install all required dependencies. If you do not have `setuptools`
installed, you will need to install the dependencies yourself.

Required Dependencies
"""""""""""""""""""""

* tornado >= 3.0
* pyzmq

If using python < 3.2, the following is also required:

* futures
* backports.ssl_match_hostname

Recommended Dependencies
""""""""""""""""""""""""

* setproctitle
* pycurl
* openssl
* pyasn1

Installation Example
""""""""""""""""""""

The easy way to install dependencies is with pip.  Then install IceProd::

    $ pip install --user tornado pyzmq setproctitle pycurl openssl pyasn1
    $ python setup.py install --user

This will install IceProd and all dependencies into your home directory, under ``~/.local``.


How to Run the Server
"""""""""""""""""""""

To run the server, use the following commands::

    $ cd iceprod
    $ export PYTHONPATH=.
    $ python bin/iceprod_server.py

