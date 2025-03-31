Overview
========

IceProd is broken up into several parts:

Core
----

The core of IceProd is what actually runs tasks. It has been overhauled
to better work directly with batch systems, so writes out a batch file
and then executes that. This batch file can be submitted to batch
systems for parallel execution.

Server
------

The primary purpose of the server is to interact with a batch system,
keeping tasks flowing. Currently only HTCondor is supported,
but it would not be too hard to add additional batch systems.

The reasoning behind this support is that IceCube currently uses
HTCondor as a federated overlay grid in conjunction with the
Open Science Grid, to distribute work all around the world.
While other sites may run other batch systems, to us it looks
like one large HTCondor pool.

REST API
--------

As it says, this is an HTTP-based API providing access to the
IceProd database. Everything else uses this to store and
retrieve state.

One critical piece here is authentication and authorization.
Only users or system components that are allowed to have
access to certain things do so. For example, one user
can see the dataset of another user, but not edit it.

Scheduled Tasks
---------------

If you're familar with ``cron``, that's what this is. Periodic
scripts to clean up various things, either due to status changes
or based on timeouts. Especially useful for handling failures,
to keep datasets from getting stuck in limbo.
