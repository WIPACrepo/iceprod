.. index:: Quickstart
.. _Quickstart:

Quickstart
==========

tl;dr
-----

For the impatient::

    pip install --user https://github.com/WIPACrepo/iceprod.git


Getting Started
---------------

Welcome!  This guide will assume that you want to set up IceProd locally on a
new machine.

Download the Project
^^^^^^^^^^^^^^^^^^^^

To download the project, check it out from github::

    $ git clone https://github.com/WIPACrepo/iceprod.git

Build the Project
^^^^^^^^^^^^^^^^^

The main requirement of IceProd is Python 2.7 or greater.  Python 3 is supported.

IceProd acts like a normal python project, and with the help of `setuptools`
can download and install all required dependencies. If you do not have `setuptools`
installed, you will need to install the dependencies yourself.

Required Dependencies
"""""""""""""""""""""

* tornado >= 4.2
* pyzmq
* jsonschema

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

This will install IceProd and all dependencies into your home directory,
under ``~/.local``.


How to Run the Server
"""""""""""""""""""""

To run the server::

    $ python bin/iceprod_server.py

Disabling SSL
"""""""""""""

To disable SSL support (if you don't have `nginx` installed),
modify `iceprod_config.json` with the following option::

    {"system":{"ssl":false}}

Setting the website password
""""""""""""""""""""""""""""

To edit the website password for admin pages,
modify `iceprod_config.json` with the following option::

    {"webserver":{"password":"my-secret-password-here"}}
