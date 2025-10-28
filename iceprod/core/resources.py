"""
Manage resources like CPUs and Memory.  Default values, detection, tracking,
policy, etc.
"""

from collections import OrderedDict
import logging
import math
import os


# default rounding bins for resources
RESOURCE_BINS = {
    'cpu': list(range(1, 1000)),
    'gpu': list(range(0, 100)),
    'memory': [x/10. for x in list(range(5, 50, 5)) + list(range(50, 200, 10)) + list(range(200, 1000, 40)) + list(range(1000, 10000, 100))],
    'disk': list(range(1, 10)) + list(range(10, 50, 2)) + list(range(50, 100, 5)) + list(range(100, 2000, 20)),
    'time': [x/60. for x in list(range(10, 60, 10)) + list(range(60, 360, 15)) + list(range(360, 1440, 60)) + list(range(1440, 20240, 240))],
}


class Resources:
    """
    Task (and node) resource definition and tracking.
    """

    # ordering based on priority for matching
    defaults = OrderedDict([
        ('gpu', []),
        ('memory', 2.0),
        ('disk', 10.0),
        ('time', 8.0),
        ('cpu', 1),
    ])
    """Default resource values

    cpu
        integer

    gpu
        fungible - a list of unique items

    memory
        float, in GB

    disk
        float, in GB

    time
        float, in hours
    """

    overusage_limits = {
        'cpu': {'ignore': 2., 'allowed': 4., 'movingavg':10},
        'gpu': {'ignore': 1.5, 'allowed': 1.5, 'movingavg':10},
        'memory': {'ignore': 0, 'allowed': 10.},
        'disk': {'ignore': 0, 'allowed': 10.},
        'time': {'ignore': 0, 'allowed': 10.},
    }
    """Overusage limits

    ignore
        Ignore extra usage up to this ratio over the allocated resource.

    allowed
        Allow extra usage if there are available resources, up to this ratio.

    movingavg
        Track a moving average of this many measurements, to avoid spikes.
    """


def du(path):
    """
    Perform a "du" on a path, getting the disk usage.

    Args:
        path (str): The path to analyze

    Returns:
        int: bytes used
    """
    logging.info('du of %s', path)
    total = 0
    for root,dirs,files in os.walk(path):
        for d in list(dirs):
            p = os.path.join(root,d)
            if os.path.islink(p):
                dirs.remove(d)
        for f in files:
            p = os.path.join(root,f)
            if not os.path.islink(p):
                total += os.path.getsize(p)
    logging.info('du of %s finished: %r', path, total)
    return total


def group_hasher(resources):
    """
    Hash a set of resources into a binned group.
    """
    ret = 0
    if 'cpu' in resources:
        ret = int(resources['cpu'])
    if 'gpu' in resources:
        if isinstance(resources['gpu'],(int,float)):
            ret ^= int(resources['gpu'])*100
        else:
            ret ^= len(resources['gpu'])*100
    if 'memory' in resources:
        ret ^= int(math.log(resources['memory'])*math.e)*1000
    if 'disk' in resources:
        ret ^= int(math.log(resources['disk'])*math.e)*1000000
    if 'time' in resources:
        ret ^= int(resources['time']) * 1000000000
    if 'os' in resources:
        ret ^= hash(resources['os']) & (0b11111111 << 32)
    return ret


def sanitized_requirements(reqs, use_defaults=False):
    """
    Sanitize a dict of requirements (resources) for a task.

    Args:
        reqs (dict): dict of requirements
    Returns:
        dict: sanitized requirements
    """
    reqs = {k.lower():v for k,v in reqs.items()}
    ret = {}
    all_keys = set(reqs).union(Resources.defaults)
    for k in all_keys:
        if k in reqs:
            try:
                if k in Resources.defaults:
                    if isinstance(Resources.defaults[k], (int,list)):
                        ret[k] = int(reqs[k])
                    elif isinstance(Resources.defaults[k], float):
                        ret[k] = float(reqs[k])
                else:
                    ret[k] = reqs[k]
            except ValueError:
                pass
        if use_defaults and k not in ret and k in Resources.defaults:
            if isinstance(Resources.defaults[k], (int,float,str)):
                ret[k] = Resources.defaults[k]
    return ret


def rounded_requirements(reqs, bins=None):
    """
    Round requirements into bins for submit systems to have
    fewer "choices" to consider for job matching.

    Args:
        reqs (dict): dict of requirements
        bins (dict): dict of binnings to use (None for defaults)
    Returns:
        dict: rounded requirements
    """
    if not bins:
        bins = RESOURCE_BINS

    def round_up(num, bins):
        """Round up to the next bin value"""
        for b in bins:
            if num <= b*1.05:  # within 5%
                return b
        raise Exception('num too big for bin sizes')
    ret = {}
    for k in reqs:
        v = reqs[k]
        if k in bins:
            v = round_up(v, bins[k])
        ret[k] = v
    return ret
