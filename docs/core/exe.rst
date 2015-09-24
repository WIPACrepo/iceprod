.. index:: Exe
.. _Exe:

Execution
=========

.. automodule:: iceprod.core.exe
   :no-members:
   :no-undoc-members:

Environment Functions
---------------------
The internal IceProd environment, designed to be nested and clean up after itself.

.. autoclass:: Config

.. autofunction:: setupenv

.. autofunction:: destroyenv


Object Setup
------------
Download/upload and setup the main IceProd objects (Resources, Data, Classes).

.. autofunction:: downloadResource

.. autofunction:: downloadData

.. autofunction:: uploadData

.. autofunction:: setupClass


Run Functions
-------------
The main execution flow goes through here.

.. autofunction:: runtask

.. autofunction:: runtray

.. autofunction:: runmodule

.. autofunction:: run_module


Functions for JSONRPC
---------------------

.. autofunction:: setupjsonRPC

.. autofunction:: downloadtask

.. autofunction:: finishtask

.. autofunction:: taskerror

.. autofunction:: uploadLogging

.. autofunction:: uploadLog

.. autofunction:: uploadErr

.. autofunction:: uploadOut
