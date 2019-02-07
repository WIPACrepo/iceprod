.. index:: submit_to_queue
.. _submit_to_queue:

From Submission to Queue
========================

The following is an explanation of how tasks go from a user
submission to queued on a grid.

Dataset Submission
------------------

The submit process takes a dataset config as input, usually
through the website. It then creates the following entries:

* dataset
* config
* task_rel - one for each task

Note that if the IceProd instance you submit through does not
know about a dataset dependency, submission will fail. Try submitting
through the master instance in that case.

Buffering
---------

Jobs and tasks get buffered, or allocated, when there is empty
space on a queue that they can run at. Whole jobs are buffered at
once, so more than the requested number of tasks may be buffered.

The following entries are made:

* search
* task
* job

Algorithm
"""""""""

* Get the possible datasets we can buffer from

  * Get number of jobs submitted for each dataset
  * Subtract off already buffered jobs

* Get task_rel for each dataset

* For each dataset, and potential job to buffer:

  * Generate task dependencies for each task

    * Get a task_rel for the current task
    * If missing a task_rel, load it
    * Figure out which task is referenced for each task_rel dependency

  * If a dependency is not met, do not buffer
  * Else, buffer the job and associated tasks

Queueing
--------

Once a task is in `waiting` or `idle`, it can be queued
(pending dependencies and requirements).  See :ref:`Lifecycles` for the
actual happenings from this point on.