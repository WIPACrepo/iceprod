.. index:: Dataclasses
.. _Dataclasses:



.. automodule:: iceprod.core.dataclasses
   :no-members:
   :no-undoc-members:

Dataclasses
===========

XML Configuration Objects
-------------------------
The building blocks of how datasets, jobs, and tasks are configured.

.. autoclass:: Job 
   :members:

.. autoclass:: Steering

.. autoclass:: _TaskCommon

.. autoclass:: Task 
   :show-inheritance:

.. autoclass:: Tray 
   :show-inheritance:

.. autoclass:: Module
   :show-inheritance:

.. autoclass:: Parameter

.. autoclass:: Class

.. autoclass:: Project

.. autoclass:: _ResourceCommon

.. autoclass:: Resource
   :show-inheritance:

.. autoclass:: Data
   :show-inheritance:


Metadata XML Objects
--------------------
Metadata for NSF requirements

.. autoclass:: DifPlus

.. autoclass:: Dif

.. autoclass:: Plus

.. autoclass:: Personnel

.. autoclass:: DataCenter


Other Objects
-------------

.. autoexception:: NoncriticalError
   :no-inherited-members:

.. autoclass:: IFace

.. autoclass:: PycURL

