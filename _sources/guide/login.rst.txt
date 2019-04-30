Login
=====

Users can log in to IceProd like any other web application.

Authentication is handled by an external plugin
such as `LDAP`_.

.. _LDAP: https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol

.. note::
   :class: icecube

   LDAP authentication is set up for IceCube users.


Two Factor Authentication
-------------------------

Certain pages and actions require additional authentication.  When a new
account is registered with IceProd, two-factor authentication must be
set up.

IceProd uses time-based one-time passwords (`RFC 6238`_), as used by
Google, GitHub, and others.  Using a phone app such as Google Authenticator,
a barcode image specific to each user can be scanned and registered.  The
app will then generate short codes valid for 30 seconds each.

Example barcode image:

.. image:: ../static/2fa_barcode.png

.. _RFC 6238: https://tools.ietf.org/html/rfc6238