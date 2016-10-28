"""
Utilities for IceProd functionality.
"""

from __future__ import absolute_import, division, print_function

import os
import time

import subprocess
import tempfile
import shutil

class NoncriticalError(Exception):
    """An exception that can be logged and then ignored."""
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        if self.value:
            return 'NoncriticalError(%r)'%(self.value)
        else:
            return 'NoncriticalError()'
    def __reduce__(self):
        return (NoncriticalError,(self.value,))


#: The types of node resources, with defaults
Node_Resources = {
    'cpu': 1,
    'gpu': 0,
    'memory': 1, # in GB
    'disk': 1, # in GB
}

#: The types of task resources, with defaults
Task_Resources = Node_Resources.copy()
Task_Resources['time'] = 24 # in hours

#: Overusage limits
Task_Resource_Overusage = {
    'cpu': {'ignore': 2., 'allowed': 2.}, # allow up to 2x without checking
    'gpu': {'ignore': 1.5, 'allowed': 1.5},
    'memory': {'ignore': 0, 'allowed': 3.}, # allow up to 3x if available
    'disk': {'ignore': 0, 'allowed': 10.},
    'time': {'ignore': 0, 'allowed': 2.},
}

def get_cpus():
    """Detect the number of available (allocated) cpus."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'cpus':
                    ret = int(line.split('=')[1])
                    break
        except Exception:
            pass
    if ret is None and 'NUM_CPUS' in os.environ:
        try:
            ret = int(os.environ['NUM_CPUS'])
        except Exception:
            pass
    if ret is None:
        return Node_Resources['cpu']
    else:
        return ret

def get_gpus():
    """Detect the number of available (allocated) gpus."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'gpus':
                    ret = int(line.split('=')[1])
                    break
        except Exception:
            pass
    if ret is None and 'NUM_GPUS' in os.environ:
        try:
            ret = int(os.environ['NUM_GPUS'])
        except Exception:
            pass
    if ret is None and 'CUDA_VISIBLE_DEVICES' in os.environ:
        try:
            ret = int(len(os.environ['CUDA_VISIBLE_DEVICES'].split(',')))
        except Exception:
            pass
    if ret is None and 'GPU_DEVICE_ORDINAL' in os.environ:
        try:
            ret = int(len(os.environ['GPU_DEVICE_ORDINAL'].split(',')))
        except Exception:
            pass
    if ret is None and '_CONDOR_AssignedGPUs' in os.environ:
        try:
            ret = int(len(os.environ['_CONDOR_AssignedGPUs'].split(',')))
        except Exception:
            pass
    if ret is None:
        return Node_Resources['gpu']
    else:
        return ret

def get_memory():
    """Detect the amount of available (allocated) memory (in GB)."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'memory':
                    ret = int(line.split('=')[1])/1000.
                    break
        except Exception:
            pass
    if ret is None and 'NUM_MEMORY' in os.environ:
        try:
            ret = int(os.environ['NUM_MEMORY'])
        except Exception:
            pass
    if ret is None:
        return Node_Resources['memory']
    else:
        return ret

def get_disk():
    """Detect the amount of available (allocated) disk (in GB)."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'disk':
                    ret = int(line.split('=')[1])/1000000.
                    break
        except Exception:
            pass
    if ret is None and 'NUM_DISK' in os.environ:
        try:
            ret = int(os.environ['NUM_DISK'])
        except Exception:
            pass
    if ret is None:
        return Node_Resources['disk']
    else:
        return ret

def get_time():
    """Detect the time allocated for the job."""
    ret = None
    if os.path.exists('.machine.ad'):
        try:
            max_time = None
            age = None
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'GLIDEIN_Max_Walltime':
                    max_time = int(line.split('=')[1])
                elif line and line.split('=')[0].strip().lower() == 'MonitorSelfAge':
                    age = int(line.split('=')[1])
                if max_time and age:
                    break
            if max_time and age:
                ret = (max_time - age) // 3600
        except Exception:
            pass
    if ret is None and 'NUM_TIME' in os.environ:
        try:
            ret = int(os.environ['NUM_TIME'])
        except Exception:
            pass
    if ret is None:
        return Task_Resources['time']
    else:
        return ret

def get_node_resources():
    return {
        'cpu':get_cpus(),
        'gpu':get_gpus(),
        'memory':get_memory(),
        'disk':get_disk(),
    }

def get_task_resources():
    return {
        'cpu':get_cpus(),
        'gpu':get_gpus(),
        'memory':get_memory(),
        'disk':get_disk(),
        'time':get_time(),
    }

class IFace(object):
    """A network interface object
    
       :ivar name: ' '
       :ivar encap: ' '
       :ivar mac: ' '
       :ivar link: []
       :ivar rx_packets: 0
       :ivar tx_packets: 0
       :ivar rx_bytes: 0
       :ivar tx_bytes: 0
    """
    def __init__(self):
        self.name = ''
        self.encap = ''
        self.mac = ''
        self.link = [] # list of dicts
        self.rx_packets = 0
        self.tx_packets = 0
        self.rx_bytes = 0
        self.tx_bytes = 0
    
    def __eq__(self,other):
        return (self.name == other.name and
                self.encap == other.encap and
                self.mac == other.mac and
                self.link == other.link)
    def __ne__(self,other):
        return not self.__eq__(other)
        
    def __str__(self):
        ret = 'Interface name='+self.name+' encap='+self.encap+' mac='+self.mac
        for l in self.link:
            ret += '\n '
            for k in l.keys():
                ret += ' '+k+'='+l[k]
        ret += '\n  RX packets='+str(self.rx_packets)+' TX packets='+str(self.tx_packets)
        ret += '\n  RX bytes='+str(self.rx_bytes)+' TX bytes='+str(self.tx_bytes)
        return ret
