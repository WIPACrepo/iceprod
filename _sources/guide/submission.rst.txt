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
