"""
  A set of classes common to IceProd.

  copyright (c) 2012 the icecube collaboration
"""

import os, time
import pycurl
from collections import OrderedDict
import cPickle as pickle
from json import dumps as tojson, loads as fromjson

### The XML Configuration Objects ###

class Job(object):
    """Holds all information about a job
    
       :ivar dataset: 0
       :ivar parent_id: 0
       :ivar xml_version: 3
       :ivar iceprod_version: 2.0
       :ivar options: {} -- a dict of parameters to pass to task runner, name is key
       :ivar steering: None
       :ivar tasks: OrderedDict()
       :ivar difplus: None
       :ivar description: str()
       :ivar categories: []
    """
    def __init__(self):
        self.dataset      = 0
        self.parent_id    = 0
        self.xml_version  = 3
        self.iceprod_version = 2.0
        self.options      = {}
        self.steering     = None
        self.tasks        = OrderedDict()
        self.difplus      = None
        self.description  = ''
        self.categories   = []

class Steering(object):
    """Holds all information that goes in the steering section of a configuration
    
        :ivar parameters: {}
        :ivar batchsys: {} -- a dict of dicts of parameteres (one dict for each batchsys)
        :ivar system: {} -- just specialized parameters
        :ivar resources: []
        :ivar data: []
    """
    def __init__(self):
        self.parameters   = {}
        self.batchsys     = {}
        self.system       = {}
        self.resources    = []
        self.data         = []

class _TaskCommon(object):
    """Holds common attributes used by task, tray, module
    
        :ivar name: None
        :ivar resources: []
        :ivar data: [] 
        :ivar classes: []
        :ivar projects: []
        :ivar parameters: {}
    """
    def __init__(self):
        self.name         = None
        self.resources    = []
        self.data         = []
        self.classes      = []
        self.projects     = []
        self.parameters   = {}

class Task(_TaskCommon):
    """Holds all information about a task
    
        :ivar depends: [] -- a list of task names
        :ivar batchsys: {} -- a dict of dicts of parameteres (one dict for each batchsys)
        :ivar trays: OrderedDict()
    """
    def __init__(self):
        super(Task,self).__init__()
        self.depends      = []
        self.batchsys     = {}
        self.trays        = OrderedDict()

class Tray(_TaskCommon):
    """ Holds all information about a tray
    
        :ivar iterations: 1
        :ivar modules: OrderedDict()
    """
    def __init__(self):
        super(Tray,self).__init__()
        self.iterations   = 1
        self.modules      = OrderedDict()

class Module(_TaskCommon):
    """Holds all information about a module
    
        :ivar running_class: None -- the python class or function to call
        :ivar src: None -- src of class or script
        :ivar args: None -- args to give to class or src if not an iceprod module
    """
    def __init__(self):
        super(Module,self).__init__()
        self.running_class = None
        self.src           = None
        self.args          = None

class Parameter(object):
    """A parameter object
    
        :ivar name: None -- required
        :ivar value: None -- required
        :ivar type: None -- optional (bool,int,float,string,json,pickle)
    """
    def __init__(self,name=None,value=None,type=None):
        self.name         = name
        self.type         = type
        if type is None:
            # try guessing type
            if isinstance(value,bool):
                type = 'bool'
            elif isinstance(value,basestring):
                type = 'basestring'
            elif isinstance(value,int):
                type = 'int'
            elif isinstance(value,float):
                type = 'float'
            elif isinstance(value,(list,tuple)):
                type = 'list'
            elif isinstance(value,dict):
                type = 'dict'
            elif isinstance(value,set):
                type = 'set'
                value = list(value)
        self.type = type
        if type is None:
            self.value = value
        if type in ('b','bool'):
            self.value = str(value)
        elif type in ('s','str','string','unicode','basestring','u'):
            self.value = value
        elif type == 'pickle':
            try:
                self.value = pickle.dumps(value)
            except:
                self.value = str(value)
        else:
            try:
                self.value = tojson(value)
            except:
                self.value = str(value)
    
    def get(self):
        """Get the value in the correct type"""
        if not self.type:
            return self.value
        elif self.type in ('b','bool'):
            return (self.value.lower() in 'true' or self.value)
        elif self.type in ('s','str','string','unicode','basestring','u'):
            return self.value
        elif self.type == 'pickle':
            try:
                return pickle.loads(self.value)
            except:
                return self.value
        else:
            try:
                ret = fromjson(self.value)
            except:
                ret = self.value
            if self.type == 'set':
                try:
                    ret = set(ret)
                except:
                    pass
            return ret

class Class(object):
    """A class object, downloaded from a url
    
        :ivar name: None -- required
        :ivar src: None -- if downloaded from url
        :ivar resource_name: None -- if present in resource object
        :ivar recursive: False
        :ivar libs: None -- if more than default lib directory
        :ivar env_vars: None
    """
    def __init__(self):
        self.name         = None
        self.src          = None
        self.resource_name = None
        self.recursive    = False
        self.libs         = None
        self.env_vars     = None

class Project(object):
    """A project object, shipped with IceProd
    
        :ivar class_name: None -- required
        :ivar name: None -- optional
    """
    def __init__(self):
        self.name         = None
        self.class_name   = None # required

class _ResourceCommon(object):
    """Holds common attributes used by Resource and Data
    
        :ivar remote: None
        :ivar local: None
        :ivar compression: None
    """
    compression_options = ['none','gzip','gz','bzip','bz2','lzma']

    def __init__(self):
        self.remote       = None
        self.local        = None
        self.compression  = None

class Resource(_ResourceCommon):
    """A resource object, representing a file to download
    
        :ivar arch: None
    """
    def __init__(self):
        super(Resource,self).__init__()
        self.arch         = None

class Data(_ResourceCommon):
    """A data object, representing input and/or output of data
    
        :ivar type: None -- required
        :ivar movement: None -- required
    """
    type_options = ['permanent','tray_temp','task_temp','job_temp','dataset_temp','site_temp']
    movement_options = ['input','output','both']
    
    def __init__(self):
        super(Data,self).__init__()
        self.type         = None
        self.movement     = None
    
    def storage_location(self,env):
        """Get storage location"""
        type = self.type.lower()
        if type not in Data.type_options:
            raise Exception('Data.type is undefined')
        if 'parameters' in env and type in env['parameters']:
            return env['parameters'][type].value
        elif type == 'permanent':
            if 'parameters' in env and 'data_url' in env['parameters']:
                return env['parameters']['data_url'].value
            else:
                raise Exception('data_url not defined in env[\'parameters\']')
        else:
            raise Exception('%s not defined in env' % type)

class DifPlus(object):
    """A DifPlus object
    
        :ivar dif: None
        :ivar plus: None
    """
    def __init__(self):
        self.dif          = None
        self.plus         = None

class Dif(object):
    """A Dif object
       :ivar entry_id: None
       :ivar entry_title: None
       :ivar parameters: ' '
       :ivar iso_topic_category: 'geoscientificinformation'
       :ivar data_ceter: None
       :ivar summary: ' '
       :ivar metadata_name: '[CEOS IDN DIF]'
       :ivar metadata_version: '9.4'
       :ivar personnel: None
       :ivar sensor_name: 'ICECUBE'
       :ivar source_name: 'SIMULATION'
       :ivar dif_creation_date: time.strftime("%Y-%m-%d")
    """
    valid_parameters = [ 
        "SPACE SCIENCE > Astrophysics > Neutrinos", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Atmospheric", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Extraterrestrial Point Source", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Gamma Ray Burst", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > WIMPS", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Diffuse Source", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Extreme High Energy", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Super Nova", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Cosmic Ray Muon Component", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Tau", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Cascades", 
        "SPACE SCIENCE > Astrophysics > Neutrinos > Galactic Plane", 
        "SPACE SCIENCE > Astrophysics > Cosmic Rays", 
        "SPACE SCIENCE > Astrophysics > Cosmic Rays > Composition", 
        "SPACE SCIENCE > Astrophysics > Cosmic Rays > Air Shower", 
        "SPACE SCIENCE > Astrophysics > Cosmic Rays > Cosmic Ray Muons", 
        "SPACE SCIENCE > Astrophysics > Cosmic Rays > Moon Shadow", 
        "SPACE SCIENCE > Engineering > Sensor Characteristics", 
        "SPACE SCIENCE > Engineering > Sensor Characteristics > Photomultiplier Tubes", 
        "SPACE SCIENCE > Engineering > Sensor Characteristics > Digital Optical Modules", 
        "EARTH SCIENCE > Cryosphere > Glaciers/Ice Sheets", 
        "EARTH SCIENCE > Cryosphere > Glaciers/Ice Sheets > Hot Water Drilling", 
        "EARTH SCIENCE > Cryosphere > Glaciers/Ice Sheets > Hot Water Drilling > Hole Drilling", 
        "EARTH SCIENCE > Cryosphere > Glaciers/Ice Sheets > Hot Water Drilling > Hole Refreeze"
    ]
    valid_source_name = { 
        "SIMULATION":"Data which are numerically generated",
        "EXPERIMENTAL":"Data with an instrumentation based source"
    }
    valid_sensor_name = { 
        "AMANDA-A":"Prototype Antarctic Muon and Neutrino Detector Array",
        "AMANDA":"Antarctic Muon and Neutrino Detector Array",
        "SPASE-1":"South Pole Air Shower Experiment 1",
        "SPASE-2":"South Pole Air Shower Experiment 2",
        "VULCAN":"South Pole Air-Cherenkov Telescope",
        "RICE":"Radio Ice Cherenkov Experiment",
        "GASP":"Gamma Astronomy-South Pole",
        "ICECUBE":"IceCube",
        "ICETOP":"IceTop",
        "EHWD":"Enhanced Hot Water Drill",
        "SPTR":"South Pole TDRSS Relay",
        "RPSC-MET":"Raytheon Polar Services Corporation Meteorology"
    }
    
    def __init__(self):
        self.entry_id     = None
        self.entry_title  = None
        self.parameters   = ''
        self.iso_topic_category = 'geoscientificinformation'
        self.data_ceter   = None
        self.summary      = ''
        self.metadata_name = '[CEOS IDN DIF]'
        self.metadata_version = '9.4'
        self.personnel    = None
        self.sensor_name  = 'ICECUBE'
        self.source_name  = 'SIMULATION'
        self.dif_creation_date = time.strftime("%Y-%m-%d")

class Plus(object):
    """A Plus object
    
       :ivar start: None
       :ivar end: None
       :ivar category: None
       :ivar subcategory: None
       :ivar run_number: None
       :ivar i3db_key: None
       :ivar simdb_key: None
       :ivar project: OrderedDict() -- {name: version}
       :ivar steering_file: None
       :ivar log_file: None
       :ivar command_line: None
    """
    
    valid_category = [
        "unclassified",
        "generated", 
        "propagated",
        "unbiased", 
        "filtered", 
        "calibration", 
        "monitoring", 
        "webcam", 
        "hole", 
        "TestDAQ",
        "FAT", 
        "log",
        "upmu",
        "minbias",
        "cascades",
        "high-energy", 
        "wimp", 
        "GRB" 
    ]
    
    def __init__(self):
        self.start        = None
        self.end          = None
        self.category     = None
        self.subcategory  = None
        self.run_number   = None
        self.i3db_key     = None
        self.simdb_key    = None
        self.project      = OrderedDict()
        self.steering_file = None
        self.log_file     = None
        self.command_line = None

class Personnel(object):
    """A Personnel object
    
       :ivar role: None
       :ivar first_name: None
       :ivar last_name: None
       :ivar email: None
    """
    def __init__(self):
        self.role         = None
        self.first_name   = None
        self.last_name    = None
        self.email        = None

class DataCenter(object):
    """A Data Center object
    
       :ivar name: 'UWI-MAD/A3RI > Antarctic Astronomy and Astrophysics Research Institute, University of Wisconsin, Madison'
       :ivar personnel: None
    """
    def __init__(self):
        self.name         = 'UWI-MAD/A3RI > Antarctic Astronomy and Astrophysics Research Institute, University of Wisconsin, Madison'
        self.personnel    = None


### Other objects ###

class NoncriticalError(Exception):
    """An exception that can be logged and then ignored"""
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return 'NoncriticalError(%r)'%(self.value)
    def __reduce__(self):
        return (NoncriticalError,(self.value,))

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

