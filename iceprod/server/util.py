"""Utility functions"""

from datetime import datetime

def datetime2str(dt):
    """Convert a datetime object to ISO 8601 string"""
    return dt.isoformat()

def nowstr():
    """Get an ISO 8601 string of the current time in UTC"""
    return datetime.utcnow().isoformat()

def str2datetime(st):
    """Convert a ISO 8601 string to datetime object"""
    if '.' in st:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S")

dataset_statuses = ['processing', 'truncated', 'suspended', 'errors', 'complete']
job_statuses = ['processing', 'suspended', 'errors', 'complete']
task_statuses = ['idle', 'waiting', 'queued', 'processing', 'reset',
                 'suspended', 'failed', 'complete']

def dataset_status_sort(st):
    try:
        return dataset_statuses.index(st)
    except ValueError:
        return len(dataset_statuses)

def job_status_sort(st):
    try:
        return job_statuses.index(st)
    except ValueError:
        return len(job_statuses)

def task_status_sort(st):
    try:
        return task_statuses.index(st)
    except ValueError:
        return len(task_statuses)