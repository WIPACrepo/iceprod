.. index:: Technical_Extras
.. _Technical_Extras:

Extras
======

Site Pools
----------

Sites can connect to each other in a *pool* to create a network of sites.  This allows datasets and resources to be used across multiple sites.  One of the main requirements of a pool is the election of a master.  When a pool is first created by connecting two nodes together, IceProd will ask which node should be the master.


The Role of the Master
----------------------

The master stores a copy of all dataset and task information in the pool.  This allows it to display and control all actions in the pool.

The master is also a central repository for resources needed by datasets running on multiple sites or sites other than the one they are submitted from.  An external gridftp server can be designated as the repository location for the master to use.

Queueing to Multiple Sites
^^^^^^^^^^^^^^^^^^^^^^^^^^

Queueing a dataset to multiple sites is handled by the master.  Whenever a site asks the master for more tasks (in the :ref:`queueing_module`), the master will assign waiting tasks to that site.  This is a pull paradigm; there is no push capability.

If the site fails to change the state of an assigned task for more than a timeout period, the task is reassigned to another site.  This allows the task to run even if the site goes down.

Communicating with the Master
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sites communicate with the master via json-rpc through the master website.  If communication fails, commands are queued in a buffer until communication succeeds again.  If the site's server is restarted, this buffer is erased so the site is declared out of sync and must resync with the master before continuing.


The Backup Master
-----------------

The backup master contains a copy of all data contained in the master (except possibly the most recent changes).  Its job is to take over the duties of the master temporarily while the master is offline.  When the master comes back online, it first resyncs with the backup master before resuming duties.  In the event that the master has been replaced, the old master will become a regular site.

Note that the backup master is a configurable option that can be disabled, and by default is disabled on smaller site pools.

Exceptions to Backup Master Synchronization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many of the operations of the master are not guaranteed to be mirrored immediately on the backup master because of performance limitations.  Instead they are queued for bulk transfer and the operation can succeed before the backup master is updated.

For these limited types of operations, the backup master is updated before the operation is completed:

* Creating a new dataset
* Altering a dataset configuration

Backup Master Failure
^^^^^^^^^^^^^^^^^^^^^

In the event of backup master failure, a set timeout limit is enforced before a new backup master is elected among the connected sites.


Joining a Pool
--------------

Sites can join a pool at any time, but it is best to do it close to the creation of the site.  This is because there is a small probability that the randomly generated site id will already be taken in the pool, causing all global ids (dataset ids, task ids, and others) to be updated with a new site id.

Global Site Ids
^^^^^^^^^^^^^^^

Each id must be globally unique across all sites in the pool.  However, each site can create new datasets and other objects independently.  Using a standard autoincrement id would fail under this case.  Instead, we partition the allowable range of ids by using the site id, which identifies which partition we are in.

Example:

|
|    Site A has a site_id = 0, so has a range of [0-1)x10^12
|    Site B has a site_id = 1, so has a range of [1-2)x10^12
|      
|    When site A creates a new global id, the id is i+0x10^12    
|    When site B creates a new global id, the id is i+1x10^12

Because sites can be started and run independently before connecting to a pool, we need to assign them a random id from a decently large space to lower the risk of collision (it might still happen, but very infrequently).  So we need > 10^9 possible sites and > 10^12 possible ids.
    
This presents an additional challenge: integers are not large enough to handle that much range (they go up to 10^18).  The solution is to use a string of characters as the id.  [a-zA-Z0-9] should be plenty.  This gives us the following possibilities:

    12 chars gives 10^21 ids
        * 10^9 sites with 10^12 ids for each DB table
    15 chars gives 10^26 ids
        * 10^10 sites with 10^16 ids for each DB table
    20 chars gives 10^35 ids
        * Only if you want to be very future proof
    
The 15 char length was selected for IceProd 2.  This is hard coded into :class:`iceprod.server.GlobalID`.  If this is changed, all old DB data is lost and you will not be able to connect properly to a pool using a different value.  So make sure this is set for your use case before starting to use IceProd
    
Pool Security
-------------

All communication is encrypted with SSL certificates.  The master is capable of creating its own CA certificate, or a CA certificate can be given at formation (note: the certificate must be able to sign other certificates).  When new sites join the pool, they receive a certificate signed by the master to use for all communications.  Tasks receive a CA certificate bundle that includes the master and site certificates, allowing them to verify that they are communicating with a server in the pool.
