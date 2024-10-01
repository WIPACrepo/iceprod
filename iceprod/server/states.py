"""Dataset, job, and task statuses and state machines, and related things."""

from functools import lru_cache, partial

DATASET_STATUS = {
    'processing': ['suspended', 'errors', 'complete'],
    'suspended': ['processing'],
    'errors': ['processing', 'suspended'],
    'complete': [],
}
"""Dataset Statuses

* processing: at least one job is processing.
* suspended: the dataset has been put on hold.
* errors: no jobs are active, and there are jobs with errors.
* complete: all jobs and tasks are complete.
"""

DATASET_STATUS_START = 'processing'
"""Initial dataset status"""

JOB_STATUS = {
    'processing': ['suspended', 'errors', 'complete'],
    'suspended': ['processing'],
    'errors': ['processing'],
    'complete': [],
}
"""Job Statuses

* processing: at least one task is in an active state.
* suspended: a task was put on hold.
* errors: one of the tasks has an error.
* complete: all tasks are complete.
"""

JOB_STATUS_START = 'processing'
"""Initial job status"""

TASK_STATUS = {
    'idle': ['waiting', 'suspended'],
    'waiting': ['queued', 'idle', 'suspended'],
    'queued': ['processing', 'idle', 'waiting', 'suspended', 'failed'],
    'processing': ['idle', 'waiting', 'suspended', 'failed', 'complete'],
    'suspended': ['idle', 'waiting'],
    'failed': ['idle', 'waiting'],
    'complete': [],
}
"""Task Statuses

* idle: task in IceProd DB, either lower priority or dependencies not met.
* waiting: task in Iceprod DB, ready to run.
* queued: task is queued in HTCondor.
* processing: task is processing in HTCondor.
* suspended: task was manually put on hold.
* failed: task has an error.
* complete: task has completed successfully.
"""

TASK_STATUS_START = 'idle'
"""Initial task status"""


def get_all_prev_statuses(states, status):
    ret = []
    for k,v in states.items():
        if status in v:
            ret.append(k)
    return ret


def status_sort(states, status):
    try:
        return list(states).index(status)
    except ValueError:
        return len(states)


dataset_prev_statuses = lru_cache(partial(get_all_prev_statuses, DATASET_STATUS))
dataset_status_sort = lru_cache(partial(status_sort, DATASET_STATUS))

job_prev_statuses = lru_cache(partial(get_all_prev_statuses, JOB_STATUS))
job_status_sort = lru_cache(partial(status_sort, JOB_STATUS))

task_prev_statuses = lru_cache(partial(get_all_prev_statuses, TASK_STATUS))
task_status_sort = lru_cache(partial(status_sort, TASK_STATUS))
