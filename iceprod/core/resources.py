"""
Manage resources like CPUs and Memory.  Default values, detection, tracking,
policy, etc.
"""

import os
import time
import math
from copy import deepcopy

import subprocess
from collections import deque, defaultdict, OrderedDict
import logging

try:
    import psutil
except ImportError:
    psutil = None

try:
    import classad
except ImportError:
    def classad_to_dict(text):
        ret = {}
        for line in text.split('\n'):
            if (not line) or '=' not in line:
                continue
            parts = line.split('=',1)
            ret[parts[0].strip().lower()] = parts[1].strip()
        return ret
else:
    def classad_to_dict(text):
        ret = {}
        c = classad.parseOne(text)
        for k in c.keys():
            try:
                ret[k.lower()] = c.eval(k)
            except TypeError:
                ret[k.lower()] = c[k]
        return ret

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
                'cpu':1,
                'gpu':30,
                'memory':1,
                'disk':30,
                'time':1,
            }
        else:
            #: time intervals when to check resources, vs using cached values
            self.lookup_intervals = {
                'children':60,
                'cpu':10,
                'gpu':300,
                'memory':10,
                'disk':180,
                'time':10,
            }

        #: start time for resource tracking
        self.start_time = time.time()

        #: total resources controlled by the pilot
        self.total = {
            'cpu':get_cpus(),
            'gpu':get_gpus(),
            'memory':get_memory()-0.1,  # trim auto-totals to prevent going over
            'disk':get_disk()-0.1,
            'time':get_time()-0.1,
        }
        if raw:
            for r in raw:
                if (r not in self.defaults
                    or (isinstance(self.defaults[r], (float,int))
                        and not isinstance(raw[r], (float,int)))
                    or (isinstance(self.defaults[r], list)
                        and not isinstance(raw[r], list))
                    ):  # noqa: E125
                    logging.error('bad type of supplied resource: %s=%r', r, raw[r])
                else:
                    v = raw[r]
                    if isinstance(self.defaults[r], int):
                        v = int(v)
                    elif isinstance(self.defaults[r], float):
                        v = float(v)
                    elif isinstance(self.defaults[r], list):
                        v = deepcopy(v)
                    if r == 'time':
                        v -= 0.1
                    logging.info('setting %s to %r', r, v)
                    self.total[r] = v
        logging.warning('total resources: %r', self.total)

        #: available resources that are unclaimed
        self.available = deepcopy(self.total)

        #: resources allocated for each task
        self.claimed = {}  # dict of task_id:{resource}

        #: maximum usage for each claim
        # dict of task_id:{resource:{max,cnt,avg}}
        self.used = defaultdict(lambda:defaultdict(lambda:{'max':0.,'cnt':0,'avg':0.}))

        #: site info - shouldn't change while running
        self.site = get_site()

    def get_available(self):
        """
        Get available resources for new tasks to match against.

        Returns:
            dict: resources
        """
        # update available time
        self.available['time'] = self.total['time'] - (time.time()-self.start_time)/3600

        ret = deepcopy(self.available)
        for r in ret:
            if isinstance(ret[r], list):
                ret[r] = len(ret[r])
        return ret

    def get_claimed(self):
        """
        Get claimed resources for reporting purposes.

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
        # update available time
        self.available['time'] = self.total['time'] - (time.time()-self.start_time)/3600
        claim = {
            'cpu':0,
            'gpu':[],
            'memory':0.,
            'disk':0.,
            'time':self.available['time'],
        }
        if not resources:
            logging.info('claiming all avaialble resources for %s', task_id)
            claim = deepcopy(self.available)
        else:
            logging.info('asking for %r', resources)
            for r in resources:
                if r not in self.available:
                    raise Exception('bad resource type: %r'%r)
                val = resources[r]
                if isinstance(self.defaults[r], (int,list)):
                    val = int(val)
                elif isinstance(self.defaults[r], float):
                    val = float(val)

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
            if r in ('time',):  # unclaimable
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
        logging.info('granted %r', claim)
        return claim

    def release(self, task_id):
        """
        Release a claim.

        Args:
            task_id (str): the task_id
        """
        if task_id not in self.claimed:
            logging.warning('release: task_id %s not in claimed', task_id)
            return
        claim = self.claimed[task_id]
        for r in claim['resources']:
            v = claim['resources'][r]
            if r in ('time',):  # unclaimable
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
            logging.warning('register: task_id %s not in claimed', task_id)
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
        # update available time
        self.available['time'] = self.total['time'] - (time.time()-self.start_time)/3600
        ret = {}
        for task_id in self.claimed:
            claim = self.claimed[task_id]
            try:
                usage = self.get_usage(task_id, force=force)
                logging.debug('%s is using %r', task_id, usage)
            except psutil.NoSuchProcess:
                logging.info('process has exited for %r', task_id)
                continue
            except Exception:
                logging.info('error getting usage for %r', task_id,
                             exc_info=True)
                continue
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
                        logging.info('ignoring overusage of %s for %s', r, task_id)
                    elif (r == 'time' and avail_r > 0):
                        logging.info('managable overusage of time for %s', task_id)
                    elif (r != 'time' and overusage < avail_r
                          and overusage_percent < limit['allowed']):
                        logging.info('managable overusage of %s for %s', r, task_id)
                    else:
                        ret[task_id] = 'Resource overusage for {}: {}'.format(r, usage[r])
                        break
            for r in usage:
                v = usage[r]
                u = self.used[task_id][r]
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
            logging.debug('create task')
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
                'create_time':process.create_time(),
                'time': 0,
            }
        task = self.history[task_id]
        logging.debug('task: %r', task)

        # recheck process children
        if force or now - task['children_last_lookup'] > self.lookup_intervals['children']:
            task['children_last_lookup'] = now
            task['children'][:] = process.children(recursive=True)
            logging.debug('children_lookup')

        lookups = {}
        for r in ('cpu','memory','disk','gpu'):
            if force or now - task[r+'_last_lookup'] > self.lookup_intervals[r]:
                task[r+'_last_lookup'] = now
                lookups[r] = True
            else:
                lookups[r] = False
        logging.debug('lookups: %r', lookups)

        # get current values
        processes = [process]+task['children']
        mem = 0
        cpu = 0
        if lookups['cpu'] or lookups['memory']:
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
                gpu_id = ''.join(filter(lambda x: x.isdigit() or x == ',', gpu_id))
                val = get_gpu_utilization_by_id(gpu_id)['utilization']
                if val != -1:
                    gpu += val
        used_resources = {
            'cpu': cpu/100.0 if cpu else None,
            'memory': mem/1000000000.0 if mem else None,
            'disk': du(tmpdir)/1000000000.0 if lookups['disk'] else None,
            'gpu': gpu/100.0 if lookups['gpu'] else None,
            'time': (now-task['create_time'])/3600,
        }
        logging.debug('used_resources: %r', used_resources)

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
        logging.debug('ret: %r', ret)
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
    def set_env(cls, resources, env=os.environ):
        """Set the environment for any resources that need it.

        Args:
            resources (dict): Resources to set
            env (dict): Environ to update (default: os.environ)
        """
        def replace(s):
            if s.startswith('CUDA'):
                s = s.replace('CUDA','')
            elif s.startswith('OCL'):
                s = s.replace('OCL','')
            return s
        if 'gpu' in resources and resources['gpu']:
            val = ','.join(replace(x) for x in set(resources['gpu']))
            env['CUDA_VISIBLE_DEVICES'] = val
            env['GPU_DEVICE_ORDINAL'] = val
            env['ROCR_VISIBLE_DEVICES'] = val
        else:
            env['CUDA_VISIBLE_DEVICES'] = '9999'
            env['GPU_DEVICE_ORDINAL'] = '9999'
            env['ROCR_VISIBLE_DEVICES'] = '9999'


def get_cpus():
    """Detect the number of available (allocated) cpus."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            ads = classad_to_dict(open('.machine.ad').read())
        except Exception:
            pass
        else:
            if 'totalcpus' in ads:
                try:
                    ret = int(float(ads['totalcpus']))
                    logging.info('got cpus from machine ad: %r',ret)
                except Exception:
                    pass
    if (not ret) and 'NUM_CPUS' in os.environ:
        try:
            ret = int(float(os.environ['NUM_CPUS']))
            logging.info('got cpus from NUM_CPUS: %r',ret)
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
            ads = classad_to_dict(open('.machine.ad').read())
        except Exception:
            pass
        else:
            if 'assignedgpus' in ads:
                try:
                    ret = ads['assignedgpus'].strip(' "').split(',')
                    logging.info('got gpus from machine ad: %r',ret)
                except Exception:
                    pass
            for k in ads:
                if 'cuda' in k.lower() or 'ocl' in k.lower():
                    logging.warning('.machine.ad: %s=%s', k, ads[k])
    if (not ret) and 'CUDA_VISIBLE_DEVICES' in os.environ:
        try:
            ret = [x.strip() for x in os.environ['CUDA_VISIBLE_DEVICES'].split(',') if x.strip()]
            logging.info('got gpus from CUDA_VISIBLE_DEVICES: %r',ret)
        except Exception:
            pass
    if (not ret) and 'GPU_DEVICE_ORDINAL' in os.environ:
        try:
            ret = [x.strip() for x in os.environ['GPU_DEVICE_ORDINAL'].split(',') if x.strip()]
            logging.info('got gpus from GPU_DEVICE_ORDINAL: %r',ret)
        except Exception:
            pass
    if (not ret) and 'ROCR_VISIBLE_DEVICES' in os.environ:
        try:
            ret = [x.strip() for x in os.environ['ROCR_VISIBLE_DEVICES'].split(',') if x.strip()]
            logging.info('got gpus from ROCR_VISIBLE_DEVICES: %r',ret)
        except Exception:
            pass
    if (not ret) and '_CONDOR_AssignedGPUs' in os.environ:
        try:
            ret = [x.strip() for x in os.environ['_CONDOR_AssignedGPUs'].split(',') if x.strip()]
            logging.info('got gpus from _CONDOR_AssignedGPUs: %r',ret)
        except Exception:
            pass
    if (not ret) and 'NUM_GPUS' in os.environ:
        try:
            ret = [str(x) for x in range(int(os.environ['NUM_GPUS']))]
            logging.info('got gpus from NUM_GPUS: %r',ret)
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
            ads = classad_to_dict(open('.machine.ad').read())
        except Exception:
            pass
        else:
            if 'totalmemory' in ads:
                try:
                    ret = float(ads['totalmemory'])/1000.
                    logging.info('got memory from machine ad: %r',ret)
                except Exception:
                    pass
    if (not ret) and 'NUM_MEMORY' in os.environ:
        try:
            ret = float(os.environ['NUM_MEMORY'])
            logging.info('got memory from NUM_MEMORY: %r',ret)
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
            ads = classad_to_dict(open('.machine.ad').read())
        except Exception:
            pass
        else:
            if 'totaldisk' in ads:
                try:
                    ret = float(ads['totaldisk'])/1000000.
                    logging.info('got disk from machine ad: %r',ret)
                except Exception:
                    pass
    if (not ret) and 'NUM_DISK' in os.environ:
        try:
            ret = float(os.environ['NUM_DISK'])
            logging.info('got disk from NUM_DISK: %r',ret)
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
            ads = classad_to_dict(open('.machine.ad').read())
        except Exception:
            logging.debug('failed to get classads from .machine.ad', exc_info=True)
        else:
            if 'pyglidein_time_to_live' in ads:
                try:
                    ret = float(ads['pyglidein_time_to_live'])/3600
                    logging.info('got pyglidein_time_to_live from machine ad: %r',ret)
                except Exception:
                    logging.info('failed to get pyglidein_time_to_live', exc_info=True)
            if (not ret) and 'glidein_max_walltime' in ads:
                try:
                    ret = float(ads['glidein_max_walltime'])/3600
                    logging.info('got glidein_max_walltime from machine ad: %r',ret)
                except Exception:
                    logging.info('failed to get glidein_max_walltime', exc_info=True)
            if (not ret) and 'timetolive' in ads:
                try:
                    ret = float(ads['timetolive'])/3600
                    logging.info('got timetolive from machine ad: %r',ret)
                except Exception:
                    logging.info('failed to get timetolive', exc_info=True)
    if (not ret) and 'NUM_TIME' in os.environ:
        try:
            ret = float(os.environ['NUM_TIME'])
            logging.info('got time from NUM_TIME: %r',ret)
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
        out = subprocess.check_output(['nvidia-smi','-q','-i',str(gpu_id),'-d','UTILIZATION,POWER']).decode('utf-8')
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
        logging.info('nvidia-smi failed for gpu %s', gpu_id, exc_info=True)
    return ret


def get_site():
    """Detect the site we are running on."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            ads = classad_to_dict(open('.machine.ad').read())
        except Exception:
            logging.debug('failed to get classads from .machine.ad', exc_info=True)
        else:
            if 'GLIDEIN_Site' in ads:
                try:
                    ret = ads['GLIDEIN_Site']
                    logging.info('got GLIDEIN_Site from machine ad: %r',ret)
                except Exception:
                    logging.info('failed to get GLIDEIN_Site', exc_info=True)
            if (not ret) and 'GLIDEIN_SiteResource' in ads:
                try:
                    ret = ads['GLIDEIN_SiteResource']
                    logging.info('got GLIDEIN_SiteResource from machine ad: %r',ret)
                except Exception:
                    logging.info('failed to get GLIDEIN_SiteResource', exc_info=True)
            if (not ret) and 'GLIDEIN_ResourceName' in ads:
                try:
                    ret = ads['GLIDEIN_ResourceName']
                    logging.info('got GLIDEIN_ResourceName from machine ad: %r',ret)
                except Exception:
                    logging.info('failed to get GLIDEIN_ResourceName', exc_info=True)
    if (not ret) and 'GLIDEIN_Site' in os.environ:
        try:
            ret = os.environ['GLIDEIN_Site']
            logging.info('got site from GLIDEIN_Site: %r',ret)
        except Exception:
            pass
    if (not ret) and 'Site' in os.environ:
        try:
            ret = os.environ['Site']
            logging.info('got site from Site: %r',ret)
        except Exception:
            pass
    if not ret:
        return ''
    else:
        return ret


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
