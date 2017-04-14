Dev Notes
=========

Server
------

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
