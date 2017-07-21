"""
Manage resources like CPUs and Memory.  Default values, detection, tracking,
policy, etc.
"""

from __future__ import absolute_import, division, print_function

import os
import time
import math
from copy import deepcopy

import subprocess
import tempfile
import shutil
from collections import deque, defaultdict, OrderedDict
import logging

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger('resources')

class Resources:
    """
    Task (and node) resource definition and tracking.
    """

    # ordering based on priority for matching
    defaults = OrderedDict([
        ('gpu', []),
        ('memory', 1.0),
        ('disk', 10.0),
        ('time', 1.0),
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

    def __init__(self, raw=None, debug=False):
        #: last measurements for resources
        self.history = {}

        #: number of values if we take a moving average
        self.num_values = 10

        if debug:
            self.lookup_intervals = {
                'children':10,
                'cpu':0.1,
                'gpu':1,
                'memory':0.1,
                'disk':30,
                'time':1,
            }
            logger.setLevel(logging.DEBUG)
        else:
            #: time intervals when to check resources, vs using cached values
            self.lookup_intervals = {
                'children':60,
                'cpu':1,
                'gpu':1,
                'memory':1,
                'disk':180,
                'time':1,
            }
            logger.setLevel(logging.INFO)

        #: start time for resource tracking
        self.start_time = time.time()/3600

        #: total resources controlled by the pilot
        self.total = {
            'cpu':get_cpus(),
            'gpu':get_gpus(),
            'memory':get_memory()-0.1, # trim auto-totals to prevent going over
            'disk':get_disk()-0.1,
            'time':self.start_time+get_time()-0.1, # end time
        }
        if raw:
            for r in raw:
                if (r not in self.defaults
                    or (isinstance(self.defaults[r], (float,int))
                        and not isinstance(raw[r], (float,int)))
                    or (isinstance(self.defaults[r], list)
                        and not isinstance(raw[r], list))
                    ):
                    logger.error('bad type of supplied resource: %s=%r', r, raw[r])
                else:
                    v = raw[r]
                    if isinstance(self.defaults[r], int):
                        v = int(v)
                    elif isinstance(self.defaults[r], float):
                        v = float(v)
                    elif isinstance(self.defaults[r], list):
                        v = deepcopy(v)
                    if r == 'time':
                        v = time.time()/3600+v-0.1
                    logger.info('setting %s to %r', r, v)
                    self.total[r] = v
        logger.warn('total resources: %r', self.total)

        #: available resources that are unclaimed
        self.available = deepcopy(self.total)

        #: resources allocated for each task
        self.claimed = {} # dict of task_id:{resource}

        #: maximum usage for each claim
        # dict of task_id:{resource:{max,cnt,avg}}
        self.used = defaultdict(lambda:defaultdict(lambda:{'max':0.,'cnt':0,'avg':0.}))

    def get_available(self):
        """
        Get available resources for new tasks to match against.

        Returns:
            dict: resources
        """
        ret = deepcopy(self.available)
        for r in ret:
            if isinstance(ret[r], list):
                ret[r] = len(ret[r])
        ret['time'] -= time.time()/3600
        return ret

    def get_claimed(self):
        """
        Get claimed resoruces for reporting purposes.

        Returns:
            dict: resources
        """
        ret = {
            'cpu':0,
            'gpu':0,
            'memory':0.,
            'disk':0.,
            'time':0,
        }
        for claim in self.claimed:
            claim_resources = self.claimed[claim]['resources']
            for r in claim_resources:
                if r == 'time':
                    ret[r] = max(ret[r],claim_resources[r])
                elif isinstance(claim_resources[r], list):
                    ret[r] += len(claim_resources[r])
                else:
                    ret[r] += claim_resources[r]
        if ret['time'] > 1:
            ret['time'] -= time.time()/3600
        return ret

    def claim(self, task_id, resources=None):
        """
        Attempt to claim resources for a task.

        Args:
            task_id (str): task_id that wants to use resources
            resources (dict): resources to claim (None = all)

        Raises:
            If claim cannot be satisfied, raise Exception

        Returns:
            dict: claimed resources
        """
        now = time.time()/3600
        claim = {
            'cpu':0,
            'gpu':[],
            'memory':0.,
            'disk':0.,
            'time':self.total['time'],
        }
        if not resources:
            logger.info('claiming all avaialble resources for %s', task_id)
            claim = deepcopy(self.available)
        else:
            logger.info('asking for %r', resources)
            for r in resources:
                if r not in self.available:
                    raise Exception('bad resource type: %r'%r)
                val = resources[r]
                if isinstance(self.defaults[r], (int,list)):
                    val = int(val)
                elif isinstance(self.defaults[r], float):
                    val = float(val)

                if r == 'time':
                    val = now + val

                if isinstance(self.available[r], (int,float)):
                    if val > self.available[r]:
                        raise Exception('not enough {} resources available: {} > {}'.format(
                                r, val, self.available[r]))
                    elif val > 0:
                        claim[r] = val
                elif isinstance(self.defaults[r], list):
                    if val > len(self.available[r]):
                        raise Exception('not enough {} resources available: {} > {}'.format(
                                r, val, len(self.available[r])))
                    elif val > 0:
                        claim[r] = self.available[r][:val]

        # now that the claim is valid, remove resources from available
        for r in claim:
            if r in ('time',): # unclaimable
                continue
            if isinstance(claim[r], (int,float)):
                self.available[r] -= claim[r]
            elif isinstance(claim[r], list):
                for v in claim[r]:
                    self.available[r].remove(v)

        self.claimed[task_id] = {
            'resources': deepcopy(claim),
            'process': None,
            'tmpdir': None,
        }

        # now send back to user
        claim['time'] -= now
        logger.info('granted %r', claim)
        return claim

    def release(self, task_id):
        """
        Release a claim.

        Args:
            task_id (str): the task_id
        """
        if task_id not in self.claimed:
            logger.warn('release: task_id %s not in claimed', task_id)
            return
        claim = self.claimed[task_id]
        for r in claim['resources']:
            v = claim['resources'][r]
            if r in ('time',): # unclaimable
                continue
            if isinstance(v, (int,float)):
                self.available[r] += v
            elif isinstance(v, list):
                for vv in v:
                    self.available[r].append(vv)
        del self.claimed[task_id]
        if task_id in self.used:
            del self.used[task_id]

    def register_process(self, task_id, process, tmpdir):
        """
        Register a process and tmpdir for an already claimed task.

        Args:
            task_id (str): the task_id
            process (:py:class:`psutil.Process`): a psutil process object
            tmpdir (str): temporary directory in use by process
        """
        if task_id not in self.claimed:
            logger.warn('register: task_id %s not in claimed', task_id)
            return
        self.claimed[task_id].update({
            'process': process,
            'tmpdir': tmpdir,
        })

    def check_claims(self, force=False):
        """
        Check all the claims for resource overusage.

        Args:
            force (bool): check all resources now, without caching

        Returns:
            dict: dict of task_ids:reasons that go over usage
        """
        ret = {}
        for task_id in self.claimed:
            claim = self.claimed[task_id]
            try:
                usage = self.get_usage(task_id, force=force)
                logger.debug('%s is using %r', task_id, usage)
            except Exception:
                logger.warn('error getting usage for %r', task_id,
                            exc_info=True)
                continue
            usage_time = usage['time'] - self.history[task_id]['create_time']
            for r in usage:
                if r == 'gpu':
                    claim_r = 100*len(claim['resources'][r])
                    avail_r = len(self.available[r])
                else:
                    claim_r = claim['resources'][r]
                    avail_r = self.available[r]
                overusage = usage[r]-claim_r
                if overusage > 0:
                    overusage_percent = usage[r]*1.0/claim_r
                    limit = self.overusage_limits[r]
                    if overusage_percent < limit['ignore']:
                        logger.info('ignoring overusage of %s for %s', r, task_id)
                    elif (r == 'time' and usage[r] < avail_r):
                        logger.info('managable overusage of time for %s', task_id)
                    elif (r != 'time' and overusage < avail_r
                          and overusage_percent < limit['allowed']):
                        logger.info('managable overusage of %s for %s', r, task_id)
                    else:
                        ret[task_id] = 'Resource overusage for {}: {}'.format(r,
                                usage_time if r == 'time' else usage[r])
                        break
            for r in usage:
                v = usage[r]
                u = self.used[task_id][r]
                if r == 'time':
                    v = usage_time
                u['avg'] = (v + u['cnt'] * u['avg'])/(u['cnt']+1)
                u['cnt'] += 1
                if v > u['max']:
                    u['max'] = v
        return ret

    def get_usage(self, task_id, force=False):
        """
        Get the resources used by this task.  Measure the process and
        its children.

        Will use cached values as necessary for speed, unless forced.

        Args:
            task_id (str): the task_id
            force (bool): check all resources now

        Returns:
            dict: measurements
        """
        if not psutil:
            raise Exception('psutil not available. cannot track resources')

        if task_id not in self.claimed:
            raise Exception('unknown claim for %s'%task_id)

        process = self.claimed[task_id]['process']
        if not process:
            raise Exception('no process to examine')

        tmpdir = self.claimed[task_id]['tmpdir']
        if not tmpdir:
            raise Exception('no tmpdir to examine')

        now = time.time()
        if task_id not in self.history:
            self.history[task_id] = {
                'children': [],
                'children_last_lookup': now-100000,
                'cpu': deque(maxlen=self.num_values),
                'cpu_last_lookup': now-100000,
                'memory': deque(maxlen=self.num_values),
                'memory_last_lookup': now-100000,
                'disk': 0,
                'disk_last_lookup': now-100000,
                'gpu': 0,
                'gpu_last_lookup': now-100000,
                'create_time':process.create_time()/3600,
                'time': 0,
            }
        task = self.history[task_id]

        # recheck process children
        if force or now - task['children_last_lookup'] > self.lookup_intervals['children']:
            task['children_last_lookup'] = now
            task['children'][:] = process.children(recursive=True)
            logger.debug('children_lookup')

        lookups = {}
        for r in ('cpu','memory','disk','gpu'):
            if force or now - task[r+'_last_lookup'] > self.lookup_intervals[r]:
                task[r+'_last_lookup'] = now
                lookups[r] = True
            else:
                lookups[r] = False

        # get current values
        processes = [process]+task['children']
        mem = 0
        cpu = 0
        for p in processes:
            try:
                with p.oneshot():
                    if lookups['cpu']:
                        cpu += p.cpu_percent()
                    if lookups['memory']:
                        mem += p.memory_info().rss
            except Exception:
                pass
        gpu = 0
        if lookups['gpu']:
            for gpu_id in set(self.claimed[task_id]['resources']['gpu']):
                gpu_id = ''.join(filter(lambda x: x.isdigit() or x==',', gpu_id))
                val = get_gpu_utilization_by_id(gpu_id)['utilization']
                if val != -1:
                    gpu += val
        used_resources = {
            'cpu': cpu/100.0 if cpu else None,
            'memory': mem/1000000000.0 if mem else None,
            'disk': du(tmpdir)/1000000000.0 if lookups['disk'] else None,
            'gpu':  gpu/100.0 if lookups['gpu'] else None,
            'time': now/3600,
        }
        logger.debug('used_resources: %r', used_resources)

        # now average for those that need it
        ret = {}
        for r in ('cpu','memory','disk','gpu','time'):
            if isinstance(task[r], deque):
                if used_resources[r] is not None:
                    task[r].append(used_resources[r])
                ret[r] = 0 if not task[r] else sum(task[r])/len(task[r])
            else:
                if used_resources[r] is not None:
                    task[r] = used_resources[r]
                ret[r] = task[r]
        logger.debug('ret: %r', ret)
        return ret

    def get_peak(self, task_id):
        """
        Get peak resource usage.

        Args:
            task_id (str): the task_id

        Returns:
            dict: resources
        """
        if task_id not in self.used:
            return {}
        return {r:self.used[task_id][r]['max'] for r in self.used[task_id]}

    def get_final(self, task_id):
        """
        Get final resource usage.

        Args:
            task_id (str): the task_id

        Returns:
            dict: resources
        """
        if task_id not in self.used:
            return None
        ret = {}
        for r in self.used[task_id]:
            if r == 'gpu':
                ret[r] = self.used[task_id][r]['avg']
            else:
                ret[r] = self.used[task_id][r]['max']
        return ret

    @classmethod
    def set_env(cls, resources):
        """Set the environment for any resources that need it.

        Args:
            resources (dict): Resources to set
        """
        if 'gpu' in resources and resources['gpu']:
            # strip all non-numbers:
            val = ','.join(set(resources['gpu']))
            val = ''.join(filter(lambda x: x.isdigit() or x==',', val))
            os.environ['CUDA_VISIBLE_DEVICES'] = val
            os.environ['GPU_DEVICE_ORDINAL'] = val
        else:
            os.environ['CUDA_VISIBLE_DEVICES'] = '9999'
            os.environ['GPU_DEVICE_ORDINAL'] = '9999'

def get_cpus():
    """Detect the number of available (allocated) cpus."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                line = line.strip()
                if line and line.split('=')[0].strip().lower() == 'totalcpus':
                    ret = int(float(line.split('=')[1]))
                    logger.info('got cpus from machine ad: %r',ret)
                    break
        except Exception:
            pass
    if (not ret) and 'NUM_CPUS' in os.environ:
        try:
            ret = int(float(os.environ['NUM_CPUS']))
            logger.info('got cpus from NUM_CPUS: %r',ret)
        except Exception:
            pass
    if not ret:
        return Resources.defaults['cpu']
    else:
        return ret

def get_gpus():
    """Detect the available (allocated) gpus.

    Returns:
        list: a list of gpus
    """
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                line = line.strip()
                if line and line.split('=')[0].strip().lower() == 'assignedgpus':
                    ret = line.split('=')[1].strip(' "').split(',')
                    logger.info('got gpus from machine ad: %r',ret)
                    break
        except Exception:
            pass
    if (not ret) and 'CUDA_VISIBLE_DEVICES' in os.environ:
        try:
            ret = [x.strip() for x in os.environ['CUDA_VISIBLE_DEVICES'].split(',') if x.strip()]
            logger.info('got gpus from CUDA_VISIBLE_DEVICES: %r',ret)
        except Exception:
            pass
    if (not ret) and 'GPU_DEVICE_ORDINAL' in os.environ:
        try:
            ret = [x.strip() for x in os.environ['GPU_DEVICE_ORDINAL'].split(',') if x.strip()]
            logger.info('got gpus from GPU_DEVICE_ORDINAL: %r',ret)
        except Exception:
            pass
    if (not ret) and '_CONDOR_AssignedGPUs' in os.environ:
        try:
            ret = [x.strip() for x in os.environ['_CONDOR_AssignedGPUs'].split(',') if x.strip()]
            logger.info('got gpus from _CONDOR_AssignedGPUs: %r',ret)
        except Exception:
            pass
    if (not ret) and 'NUM_GPUS' in os.environ:
        try:
            ret = [str(x) for x in range(int(os.environ['NUM_GPUS']))]
            logger.info('got gpus from NUM_GPUS: %r',ret)
        except Exception:
            pass
    if not ret:
        return deepcopy(Resources.defaults['gpu'])
    else:
        return ret

def get_memory():
    """Detect the amount of available (allocated) memory (in GB)."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                line = line.strip()
                if line and line.split('=')[0].strip().lower() == 'totalmemory':
                    ret = float(line.split('=')[1])/1000.
                    logger.info('got memory from machine ad: %r',ret)
                    break
        except Exception:
            pass
    if (not ret) and 'NUM_MEMORY' in os.environ:
        try:
            ret = float(os.environ['NUM_MEMORY'])
            logger.info('got memory from NUM_MEMORY: %r',ret)
        except Exception:
            pass
    if not ret:
        return Resources.defaults['memory']
    else:
        return ret

def get_disk():
    """Detect the amount of available (allocated) disk (in GB)."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                line = line.strip()
                if line and line.split('=')[0].strip().lower() == 'totaldisk':
                    ret = float(line.split('=')[1])/1000000.
                    logger.info('got disk from machine ad: %r',ret)
                    break
        except Exception:
            pass
    if (not ret) and 'NUM_DISK' in os.environ:
        try:
            ret = float(os.environ['NUM_DISK'])
            logger.info('got disk from NUM_DISK: %r',ret)
        except Exception:
            pass
    if not ret:
        return Resources.defaults['disk']
    else:
        return ret

def get_time():
    """Detect the time allocated for the job."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                line = line.strip()
                if line and line.split('=')[0].strip().lower() == 'timetolive':
                    ret = float(line.split('=')[1])/3600
                    logger.info('got time from machine ad: %r',ret)
                    break
        except Exception:
            pass
    if (not ret)and 'NUM_TIME' in os.environ:
        try:
            ret = float(os.environ['NUM_TIME'])
            logger.info('got time from NUM_TIME: %r',ret)
        except Exception:
            pass
    if not ret:
        return Resources.defaults['time']
    else:
        return ret

def get_gpu_utilization_by_id(gpu_id):
    """Get gpu utilization based on gpu id"""
    ret = {'utilization':-1,'power':-1}
    try:
        out = subprocess.check_output(['nvidia-smi','-q','-i',str(gpu_id),'-d','UTILIZATION,POWER'])
        for line in out.split('\n'):
            line = line.strip()
            if (not line) or ':' not in line:
                continue
            k,v = [x.strip() for x in line.rsplit(':',1)]
            if k == 'Gpu':
                ret['utilization'] = int(v.replace('%','').strip())
            elif k == 'Power Draw':
                ret['power'] = float(v.replace('W','').strip())
    except Exception:
        logger.info('nvidia-smi failed for gpu %s', gpu_id)
    return ret

def du(path):
    """
    Perform a "du" on a path, getting the disk usage.

    Args:
        path (str): The path to analyze

    Returns:
        int: bytes used
    """
    logger.info('du of %s', path)
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
    logger.info('du of %s finished: %r', path, total)
    return total

def group_hasher(resources):
    """
    Hash a set of resources into a binned group.
    """
    ret = int(resources['cpu'])
    if isinstance(resources['gpu'],(int,long,float)):
        ret ^= int(resources['gpu'])*100
    else:
        ret ^= len(resources['gpu'])*100
    ret ^= int(math.log(resources['memory'])*math.e)*1000
    ret ^= int(math.log(resources['disk'])*math.e)*1000000
    ret ^= int(resources['time'])*1000000000
    return ret
