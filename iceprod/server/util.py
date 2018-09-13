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

def status_sort(st):
    statuses = ['idle', 'waiting', 'queued', 'processing', 'reset',
                'suspended', 'failed', 'complete']
    try:
        return statuses.index(st)
    except ValueError:
        return len(statuses)