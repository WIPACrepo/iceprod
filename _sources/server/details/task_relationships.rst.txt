.. index:: task_relationships
.. _task_relationships:

Task Relationships
==================

Submission
----------

On submission in the config file, task dependencies are of the
form ``[dataset_id].(name|index)`` and come in a json list
so you can depend on more than one thing. When the dataset_id is
excluded, the dependency is local to the current dataset.

This means these are all valid:

* 1
* generate
* d1.1
* d1.generate
* 1,2,3
* generate,propagate
* d1.level2,d2.level2

The default pattern is to have sequential tasks in a dataset
depend on the previous task.

Within task_rel
---------------

Internally within the `task_rel` table, dependencies are of the
form ``task_rel_id[,task_rel_id...]``, a comma-separated-list of
task_rel_ids.

Within task
-----------

In the task table itself, dependencies are of the form
``task_id[,task_id...]``, a comma-separated-list of task_ids. This
gives quick lookup of whether the dependencies have been completed
already.