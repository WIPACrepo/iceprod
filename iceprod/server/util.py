"""Utility functions"""
from datetime import datetime, UTC


def datetime2str(dt):
    """Convert a datetime object to ISO 8601 string"""
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')


def nowstr():
    """Get an ISO 8601 string of the current time in UTC"""
    return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')


def str2datetime(st):
    """Convert a ISO 8601 string to datetime object"""
    if '.' in st:
        return datetime.strptime(st, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return datetime.strptime(st, "%Y-%m-%dT%H:%M:%S")
