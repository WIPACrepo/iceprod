"""
Task Queue
==========

Utilities for fairly queueing tasks based on resource usage.
"""

from __future__ import absolute_import, division, print_function

import logging


logger = logging.getLogger('task_queue')


def sched_prio(resources, time_in_queue=0):
    """
    Weight the priority to prefer smaller resource requests.
    As a task stays longer in the queue, increase priority.

    Best priority is 0, worse increases to infinity.

    Args:
        resources (dict): resources dict
        time_in_queue (float): the time this task has spent in the queue

    Returns:
        float: priority
    """
    return max(0, sum(resources.values()) - time_in_queue/600)


def get_queue(resources):
    """
    Determine which queue this task belongs in.

    Args:
        resources (dict): resources dict

    Returns:
        str: queue name
    """
    if resources['gpu']:
        return 'gpu'
    elif resources['memory'] >= 4:
        return 'memory'
    else:
        return 'default'


def get_queue_for_pilot(resources):
    """
    Determine which queue this pilot resource belongs in.

    Args:
        resources (dict): resources dict

    Returns:
        str: queue name
    """
    if resources['gpu']:
        return 'gpu'
    elif resources['memory'] >= 8:
        return 'memory'
    else:
        return 'default'
