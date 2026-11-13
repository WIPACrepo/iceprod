Dataset Monitoring
==================

The dataset page lists some basic settings for the dataset,
with a link to view the entire configuration.

The next section is the list of tasks that IceProd has buffered.
Not all tasks within the dataset are buffered immediately, to save
on space in the database and make queries faster.

Another section lists the task status by task name. This is useful
for multi-task datasets, to see how many jobs have reached each
level of the dataset.

Finally, there are completion statistics broken down by task name.
The average, minimum, and maximum successful processing times are displayed,
along with the efficiency.  Efficiency is defined as:

    efficiency = successful processing time / total time spent on a task

where total time includes processing time that resulted in an error.


Dataset Status
--------------

A dataset is always in one of these statuses:

* **processing**: at least one job is processing.
* **suspended**: the dataset has been put on hold.
* **errors**: no jobs are active, and there are jobs with errors.
* **complete**: all jobs and tasks are complete.
* **obsolete**: the dataset is no longer relevant.

The dataset page has buttons to change the status: *Reset* and *Hard Reset*
return a dataset to processing, and *Suspend* puts it on hold.

Obsolete Datasets
^^^^^^^^^^^^^^^^^

Use *Mark Obsolete* on the dataset page to hide a dataset you no longer need.
Any running jobs and tasks are suspended. Obsolete datasets are hidden from
the dataset list and the overview page, but the dataset page still works.
To list them, select ``obsolete`` in the status filter on the dataset list.

To bring an obsolete dataset back, use *Restore* on the dataset page.
This sets the dataset to suspended; from there it can be reset as usual.

Advanced users can also reset or hard reset an obsolete dataset directly
through the REST API, which moves it straight back to processing.
