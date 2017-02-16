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
