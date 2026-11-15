Dataset Submission
==================

Submission Types
----------------

.. ~ There are three types of submission: basic, advanced, and expert.

.. ~ Basic View
.. ~ ^^^^^^^^^^

.. ~ The basic view is designed for the case of "I want to run this script."

.. ~ It requires a URL (http or gridftp) to the script. All other boxes are
.. ~ optional, though filling in the description is highly recommended.

.. ~ Then enter the number of jobs and hit submit.

.. ~ Advanced View
.. ~ ^^^^^^^^^^^^^

.. ~ The advanced view is for running multiple tasks and more complex scenarios.
.. ~ It is currently a direct translation from the json config to html structure.

.. ~ This is under revision to make it easier to use.  Comments welcome.

.. ~ Expert View
.. ~ ^^^^^^^^^^^

The only submission type right now is the raw json configuration. Copying/pasting
the configuration is very easy here.

.. warning:: Be careful of json syntax errors!

Macros
------

IceProd has several built-in macros relating to what task is actually running.

`$(dataset_id)`
    The dataset_id string.

`$(dataset)`
    The dataset_id in numerical form.

    .. note:: Numbering starts at 20000 for IceProd 2 datasets.
       :class: icecube

`$(jobs_submitted)`
    The number of jobs in the dataset.

`$(job)`
    The job index within the dataset. Within the range `[0, jobs_submitted)`.

`$(task_id)`
    The task_id string. Guaranteed to be unique to the running task.

`$(task)`
    The task name.

Env Expansion
-------------

IceProd can expand the following env vars when using an `env_shell` in the module:

* OS_ARCH
* SROOT
* I3_BUILD
* I3_SRC

Use `$environ(XXXX)` in the json config to insert the env variable in the module
src or arguments.

Script Checks
-------------

IceProd converts each task into a shell script, ``task_runner.sh``.

When a dataset is submitted or its config is edited, IceProd generates the
script for each task (with dummy values for job index and ids) and rejects
the config if:

* the script has a shell syntax error, or
* the script contains an IceProd expression that was not expanded.

The most common cause is a typo or a missing parameter, such as
``$steering(gcdfile)`` when the steering parameter is named ``gcd``.
IceProd leaves an expression it cannot expand as-is, and the shell then
fails with ``syntax error near unexpected token `('``. The error message
includes the task, tray, and module, along with the offending line.

Tasks of datasets that already exist are also checked for syntax errors
before being submitted to HTCondor. A task that fails this check is
marked ``failed`` instead of running, and the error is in its log.
