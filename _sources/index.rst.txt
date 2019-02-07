.. title:: IceProd

IceProd
=======

IceProd is a Python framework for distributed management of batch jobs.
It runs as a layer on top of other batch middleware, such as `HTCondor`_,
and can pool together resources from different batch systems. The primary
purpose is to coordinate and administer many large sets of jobs at once,
keeping a history of the entire job lifecycle.

.. _HTCondor: http://research.cs.wisc.edu/htcondor/

Installation
------------

.. highlight:: bash

Most users will not need to install IceProd, as they will use an already
running instance.  For advanced users and administrators, you can install
with ``pip``:

.. parsed-literal::

    pip install https\://github.com/WIPACrepo/iceprod/archive/v\ |version|.tar.gz

**Optional dependencies**: Nginx, Globus toolkit, MySQL

.. note::
   :class: icecube

    For IceCube users with CVMFS access, IceProd is already installed. To load
    the environment execute::

        /cvmfs/icecube.wisc.edu/iceprod/latest/env-shell.sh

Documentation
-------------

.. toctree::
    :maxdepth: 3
    :titlesonly:

    overview
    guide/index
    admin/index

Developer Documentation
-----------------------

.. toctree::
    :maxdepth: 3
    :titlesonly:

    core/index
    server/index
