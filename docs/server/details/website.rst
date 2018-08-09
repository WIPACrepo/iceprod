Website
=======

The website is the main way to communicate with IceProd.  It handles several
different jobs:

* Human
    * dataset submission
    * dataset editing, other in progress actions
    * viewing progress
* Computer
    * running task communications
    * site-to-site communications
    * file downloads
    * file proxying

In order to handle the many hundreds or thousands of requests it could get,
the website was built on :ref:`async`.

Task Communication
------------------

Tasks communicate with the server using a json-rpc interface built into the
website.  For most communications, this involves talking with the database
using the internal RPC.

Site-to-Site Communication
--------------------------

Communication between sites also use the json-rpc interface in the website.

Human Interaction
-----------------

The website can modify things in the database using ajax and the json-rpc
interface in the website.

Nginx
-----

The website uses nginx as a front end proxy.  Nginx handles all SSL
certificate checking, static files, and file uploading before proxying the
request to Tornado.

Note that Apache can also be used separately, while Nginx config is built-in.

Security
--------

Details about user accounts are in :ref:`user_accounts`.

CSRF
""""

To protect against Cross-Site Request Forgery (and other session-stealing
attacks), the following schemes are used:

Headers
^^^^^^^

If available, the `Origin` or `Referer` headers are checked.
If they are not available, JSON-RPC requests are passed while
other requests are blocked.

.. note:: Why are JSON-RPC requests passed?

   This is because JSON-RPC serves multiple purposes.  Only one of them
   is for website requests.  One other is as an API for scripts or other
   external entities.  But the main purpose is for IceProd task and site
   communication.  In order to not hinder these uses, the headers are left
   unchecked.

   Note that JSON-RPC does not use the user login cookie, so it cannnot be
   tricked into running cross-site requests that way.

Login
^^^^^

There are two different types of login, which are treated differently.

Web Login
+++++++++

The initial web login will save a secure cookie to identify the user, but
this will not in itself grant any access to JSON-RPC or secure areas. The
lifetime for the login is measured in days, and is configurable.

When loading a page that needs to make JSON-RPC requests, a very limited
lifetime passkey is generated (assume 15 minutes) and inserted in a hidden
field in the html.  This passkey is then added to JSON-RPC requests.

Secure areas like account details will require a second login before granting
access.

JSON-RPC Login
++++++++++++++

This version directly returns the passkey necessary to execute authenticated
JSON-RPC requests.  It has a fairly limited lifetime and is designed to be
used immediately within the same script.

.. warning::

   Anyone with the passkey can impersonate you for the lifetime of the key.
   Try not to save it to file, and dispose of it when finished.
