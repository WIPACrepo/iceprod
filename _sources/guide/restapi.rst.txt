REST API Interface
==================

Advanced users may want to query IceProd from automated scripts or
other programs.  This is possible through a REST API.

.. highlight:: bash

See :ref:`IceProd REST API` for available urls.

Authentication
--------------

To view datasets, call actions on datasets, or submit new datasets,
authentication with a token is required.

An authentication token can be obtained within the account settings
in the website.  This can be added to requests as the `Authorization` header.
As an example, here is a query to get the list of all datasets::

    curl -XGET -H 'Authorization: bearer XXXXXXXX-your-token-here-XXXXX' https://iceprod2-api.icecube.wisc.edu/datasets

.. danger::

   Anyone with the authentication token is basically you.  It is valid 
   for any action that does not require :ref:`Two Factor Authentication`.
   Be careful with the token!


