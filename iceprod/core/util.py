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

def get_cpus():
    """Detect the number of available (allocated) cpus."""
    ret = 1
    flag = False
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'cpus':
                    ret = int(line.split('=')[1])
                    flag = True
                    break
        except Exception:
            pass
    if not flag and 'NUM_CPUS' in os.environ:
        try:
            ret = int(os.environ['NUM_CPUS'])
        except Exception:
            pass
    return ret

def get_gpus():
    """Detect the number of available (allocated) gpus."""
    ret = 1
    flag = False
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'gpus':
                    ret = int(line.split('=')[1])
                    flag = True
                    break
        except Exception:
            pass
    if not flag and 'NUM_GPUS' in os.environ:
        try:
            ret = int(os.environ['NUM_GPUS'])
        except Exception:
            pass
    if not flag and 'CUDA_VISIBLE_DEVICES' in os.environ:
        try:
            ret = int(len(os.environ['CUDA_VISIBLE_DEVICES'].split(',')))
        except Exception:
            pass
    if not flag and 'GPU_DEVICE_ORDINAL' in os.environ:
        try:
            ret = int(len(os.environ['GPU_DEVICE_ORDINAL'].split(',')))
        except Exception:
            pass
    if not flag and '_CONDOR_AssignedGPUs' in os.environ:
        try:
            ret = int(len(os.environ['_CONDOR_AssignedGPUs'].split(',')))
        except Exception:
            pass
    return ret

def get_memory():
    """Detect the amount of available (allocated) memory (in GB)."""
    ret = 1
    flag = False
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'memory':
                    ret = int(line.split('=')[1])/1000
                    flag = True
                    break
        except Exception:
            pass
    if not flag and 'NUM_MEMORY' in os.environ:
        try:
            ret = int(os.environ['NUM_MEMORY'])
        except Exception:
            pass
    return ret

def get_disk():
    """Detect the amount of available (allocated) disk (in GB)."""
    ret = 1
    flag = False
    if os.path.exists('.machine.ad'):
        try:
            for line in open('.machine.ad'):
                if line and line.split('=')[0].strip().lower() == 'disk':
                    ret = int(line.split('=')[1])/1000000
                    flag = True
                    break
        except Exception:
            pass
    if not flag and 'NUM_DISK' in os.environ:
        try:
            ret = int(os.environ['NUM_DISK'])
        except Exception:
            pass
    return ret

#: The types of node resources, with detection methods
Node_Resources = {
    'cpu':get_cpus(),
    'gpu':get_gpus(),
    'memory':get_memory(),
    'disk':get_disk(),
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

# TODO: consider moving this to its own module, with more general naming
try:
    import pycurl
except ImportError:
    class PycURL(object):
        """An object to download/upload files using cURL"""
        def __init__(self):
            self.opts = {'connect-timeout':'30',
                         'cert-type':'PEM',
                         'key-type':'PEM',
                         'fail':None,
                         'max-time':'300',
                         'location':None,
                         'max-redirs':'5',
                         'post301':None,
                         'post302':None,
                         'post303':None,
                        }
        
        def put(self, url, filename, username=None, password=None,
                sslcert=None, sslkey=None, cacert=None):
            """Upload a file using POST"""
            opts = self.opts
            opts.update({
                'url':url,
                'data-binary':'@'+filename,
                'max-time':'1800',
            })
            if username:
                if password:
                    opts['user'] = str(username)+':'+str(password)
                else:
                    opts['user'] = str(username)+':'
            if sslcert:
                opts['cert'] = str(sslcert)
            if sslkey:
                opts['key'] = str(sslkey)
            if cacert:
                opts['cacert'] = str(cacert)
            cmd = ['curl']
            for k in opts:
                if opts[k] is None:
                    cmd.append('--'+k)
                else:
                    cmd.append('--'+k+' '+opts[k])
            subprocess.check_call(cmd)
        
        def fetch(self, url, filename, username=None, password=None,
                sslcert=None, sslkey=None, cacert=None):
            """Download a file using GET"""
            opts = self.opts
            opts.update({
                'url':url,
                'output':filename,
            })
            if username:
                if password:
                    opts['user'] = str(username)+':'+str(password)
                else:
                    opts['user'] = str(username)+':'
            if sslcert:
                opts['cert'] = str(sslcert)
            if sslkey:
                opts['key'] = str(sslkey)
            if cacert:
                opts['cacert'] = str(cacert)
            cmd = ['curl']
            for k in opts:
                if opts[k] is None:
                    cmd.append('--'+k)
                else:
                    cmd.append('--'+k+' '+opts[k])
            subprocess.check_call(cmd)
        
        def post(self, url, writefunc, username=None, password=None, 
                sslcert=None, sslkey=None, cacert=None, headerfunc=None,
                postbody=None, timeout=None):
            """Download a file using POST, output to writefunc"""
            if not writefunc or not callable(writefunc):
                raise Exception('Write function invalid: %s'%str(writefunc))
            # make some tempfiles
            tmp_dir = tempfile.mkdtemp(dir=os.getcwd())
            filename = os.path.join(tmp_dir,'download')
            headerfunc_file = os.path.join(tmp_dir,'headers')
            try:
                opts = self.opts
                opts.update({
                    'url':url,
                    'output':filename,
                })
                if postbody:
                    opts['data'] = postbody
                if headerfunc and callable(headerfunc):
                    opts['dump-header'] = headerfunc_file
                if timeout:
                    opts['max-time'] = str(timeout)
                if username:
                    if password:
                        opts['user'] = str(username)+':'+str(password)
                    else:
                        opts['user'] = str(username)+':'
                if sslcert:
                    opts['cert'] = str(sslcert)
                if sslkey:
                    opts['key'] = str(sslkey)
                if cacert:
                    opts['cacert'] = str(cacert)
                cmd = ['curl']
                for k in opts:
                    if opts[k] is None:
                        cmd.append('--'+k)
                    else:
                        cmd.append('--'+k+' '+opts[k])
                subprocess.check_call(cmd)
                if headerfunc and callable(headerfunc):
                    headerfunc(open(headerfunc_file).read())
                writefunc(open(filename).read())
            finally:
                shutil.rmtree(tmp_dir)
        
else:
    class PycURL(object):
        """An object to download/upload files using pycURL"""
        def __init__(self):
            self.curl = pycurl.Curl()
            self.curl.setopt(pycurl.FOLLOWLOCATION, 1)
            self.curl.setopt(pycurl.MAXREDIRS, 5)
            self.curl.setopt(pycurl.CONNECTTIMEOUT, 30)
            self.curl.setopt(pycurl.TIMEOUT, 300) # timeout after 300 seconds (5 min)
            self.curl.setopt(pycurl.NOSIGNAL, 1)
            self.curl.setopt(pycurl.NOPROGRESS, 1)
            self.curl.setopt(pycurl.SSLCERTTYPE, 'PEM')
            self.curl.setopt(pycurl.SSLKEYTYPE, 'PEM')
            self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
            self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
            self.curl.setopt(pycurl.FAILONERROR, True)
        
        def put(self, url, filename, username=None, password=None,
                sslcert=None, sslkey=None, cacert=None):
            """Upload a file using POST"""
            try:
                self.curl.setopt(pycurl.URL, url)
                self.curl.setopt(pycurl.HTTPPOST, [('file',(pycurl.FORM_FILE, filename))])
                self.curl.setopt(pycurl.TIMEOUT, 1800) # use longer timeout for uploads
                if username:
                    if password:
                        self.curl.setopt(pycurl.USERPWD, str(username)+':'+str(password))
                    else:
                        self.curl.setopt(pycurl.USERPWD, str(username)+':')
                if sslcert:
                    self.curl.setopt(pycurl.SSLCERT, str(sslcert))
                if sslkey:
                    self.curl.setopt(pycurl.SSLKEY, str(sslkey))
                if cacert:
                    self.curl.setopt(pycurl.CAINFO, str(cacert))
                self.curl.perform()
                error_code = self.curl.getinfo(pycurl.HTTP_CODE)
                if error_code not in (200,304):
                    raise NoncriticalError('HTTP error code: %d'%error_code)
            except:
                raise
            finally:
                self.curl.setopt(pycurl.TIMEOUT, 300)
                if username:
                    self.curl.setopt(pycurl.USERPWD, '')
                if sslcert:
                    self.curl.setopt(pycurl.SSLCERT, '')
                if sslkey:
                    self.curl.setopt(pycurl.SSLKEY, '')
                if cacert:
                    self.curl.setopt(pycurl.CAINFO, '')
        
        def fetch(self, url, filename, username=None, password=None,
                sslcert=None, sslkey=None, cacert=None):
            """Download a file using GET"""
            fp = open(filename,'wb')
            error = None
            try:
                self.curl.setopt(pycurl.URL, url)
                self.curl.setopt(pycurl.WRITEDATA, fp)
                if username:
                    if password:
                        self.curl.setopt(pycurl.USERPWD, str(username)+':'+str(password))
                    else:
                        self.curl.setopt(pycurl.USERPWD, str(username)+':')
                if sslcert:
                    self.curl.setopt(pycurl.SSLCERT, str(sslcert))
                if sslkey:
                    self.curl.setopt(pycurl.SSLKEY, str(sslkey))
                if cacert:
                    self.curl.setopt(pycurl.CAINFO, str(cacert))
                self.curl.perform()
                error_code = self.curl.getinfo(pycurl.HTTP_CODE)
                if error_code not in (200,304):
                    raise NoncriticalError('HTTP error code: %d'%error_code)
            except:
                error = True
                raise
            finally:
                fp.close()
                if error:
                    os.remove(filename)
                if username:
                    self.curl.setopt(pycurl.USERPWD, '')
                if sslcert:
                    self.curl.setopt(pycurl.SSLCERT, '')
                if sslkey:
                    self.curl.setopt(pycurl.SSLKEY, '')
                if cacert:
                    self.curl.setopt(pycurl.CAINFO, '')
        
        def post(self, url, writefunc, username=None, password=None, 
                sslcert=None, sslkey=None, cacert=None, headerfunc=None,
                postbody=None, timeout=None):
            """Download a file using POST, output to writefunc"""
            if not writefunc or not callable(writefunc):
                raise Exception('Write function invalid: %s'%str(writefunc))
            try:
                self.curl.setopt(pycurl.URL, url)
                if postbody:
                    self.curl.setopt(pycurl.POST,1)
                    self.curl.setopt(pycurl.POSTFIELDS, postbody)
                if headerfunc and callable(headerfunc):
                    self.curl.setopt(pycurl.HEADERFUNCTION,headerfunc)
                self.curl.setopt(pycurl.WRITEFUNCTION,writefunc)
                if timeout:
                    self.curl.setopt(pycurl.TIMEOUT, timeout)
                if username:
                    if password:
                        self.curl.setopt(pycurl.USERPWD, str(username)+':'+str(password))
                    else:
                        self.curl.setopt(pycurl.USERPWD, str(username)+':')
                if sslcert:
                    self.curl.setopt(pycurl.SSLCERT, str(sslcert))
                if sslkey:
                    self.curl.setopt(pycurl.SSLKEY, str(sslkey))
                if cacert:
                    self.curl.setopt(pycurl.CAINFO, str(cacert))
                self.curl.perform()
                error_code = self.curl.getinfo(pycurl.HTTP_CODE)
                if error_code not in (200,304):
                    raise NoncriticalError('HTTP error code: %d'%error_code)
            except:
                raise
            finally:
                if timeout:
                    self.curl.setopt(pycurl.TIMEOUT, 300)
                if username:
                    self.curl.setopt(pycurl.USERPWD, '')
                if sslcert:
                    self.curl.setopt(pycurl.SSLCERT, '')
                if sslkey:
                    self.curl.setopt(pycurl.SSLKEY, '')
                if cacert:
                    self.curl.setopt(pycurl.CAINFO, '')

