.. index:: Quickstart
.. _Quickstart:

Quickstart
==========

tl;dr
-----

For the impatient::

    configure
    make install


Getting Started
---------------

Welcome!  This guide will assume that you want to set up IceProd locally on a new machine.

Download the Metaproject
^^^^^^^^^^^^^^^^^^^^^^^^

To download the metaproject, check it out from svn::

$ svn co http://code.icecube.wisc.edu/svn/metaprojects/iceprod/trunk src

If you do not need to stay up to date, you may also get an exported version::

$ svn export http://code.icecube.wisc.edu/svn/metaprojects/iceprod/trunk src

Build the Metaproject
^^^^^^^^^^^^^^^^^^^^^

The IceProd metaproject uses autoconf tools to download and build all dependencies.  The basic steps are::

$ ./configure
$ make
$ make install

This will install IceProd in the default directory of $HOME/iceprod.

One key option is specifying a --prefix to confiure, telling it to build in a different directory than the default directory.  A standard setup will have the following directory structure::

    /
        src/
        build/

You would then run configure from within the src directory like such::

$ ./configure --prefix=../build

