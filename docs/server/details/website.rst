
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
^^^^^^^^^^^^^^^^^^

Tasks communicate with the server using a json-rpc interface built into the 
website.  For most communications, this involves talking with the database 
using the internal RPC.

Site-to-Site Communication
^^^^^^^^^^^^^^^^^^^^^^^^^^

Communication between sites also use the json-rpc interface in the website.

Human Interaction
^^^^^^^^^^^^^^^^^

The website can modify things in the database using ajax and the json-rpc 
interface in the website.

Nginx
^^^^^

For security, the website uses nginx as a front end.  Nginx handles all SSL 
certificate checking, static files, and file uploading before proxying the 
request to Tornado.  Nginx has been proven to be a very robust web server, 
with over 10% of the web (and growing) using it.  It is also the recommended 
front end for production Tornado sites.
