Running Manually
================

For those times where you need to debug a dataset configuration,
or test before submitting it, running manually is a good choice.

Make sure to load IceProd and dependencies into your PYTHONPATH.

.. note::
   :class: icecube

   IceProd is available on cvmfs::

      eval $(/cvmfs/icecube.opensciencegrid.org/iceprod/latest/setup.sh)

1. First, download the config to a file (we'll call it `config.json`).
2. Then, run iceprod with special flags::

    python -m iceprod.core.i3exec -f config.json -d --offline

More options can be seen by calling with `-h`, or at :py:mod:`iceprod.core.i3exec`.
