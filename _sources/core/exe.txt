Execution Functions
===================

.. automodule:: iceprod.core.exe
   :no-members:
   :no-undoc-members:

Environment Functions
---------------------

The internal IceProd environment, designed to be nested and clean up after itself.

.. autoclass:: iceprod.core.exe.Config

.. autofunction:: iceprod.core.exe.setupenv

Run Functions
-------------

The main execution flow goes through here.

.. autofunction:: iceprod.core.exe.runtask

.. autofunction:: iceprod.core.exe.runtray

.. autofunction:: iceprod.core.exe.runmodule

.. autofunction:: iceprod.core.exe.run_module

Object Setup
------------

Download/upload and setup the main IceProd objects (Resources, Data, Classes).

.. autofunction:: iceprod.core.exe.downloadResource

.. autofunction:: iceprod.core.exe.downloadData

.. autofunction:: iceprod.core.exe.uploadData

.. autofunction:: iceprod.core.exe.setupClass

