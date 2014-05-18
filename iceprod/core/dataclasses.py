"""
A set of classes that holds dataset configuration data.

Each class is based on a dictionary and only contains simple elements for 
easy serialization to json (or other formats). A full dataset configuration
can be had by serializing the :class:`Job` class.

The `convert` method of each class will turn any regular dictionary objects
into special `dataclasses` objects.

The `valid` method of each class will test the validity of the data to be
an actual dataset config.
"""

from __future__ import absolute_import, division, print_function

import os
import time

from numbers import Number, Integral
try:
    String = basestring
except NameError:
    String = str

class Job(dict):
    """
    Holds all information about a running job.
    
    If the `options` are empty, this is the same as a dataset
    configuration.
    
    :ivar dataset: 0
    :ivar parent_id: 0
    :ivar version: 3
    :ivar options: {} -- a dict of parameters to pass to the task runner
    :ivar steering: None
    :ivar tasks: []
    :ivar difplus: None
    :ivar description: ""
    :ivar categories: []
    """
    def __init__(self,*args,**kwargs):
        self['dataset']     = 0
        self['parent_id']   = 0
        self['version']     = 3
        self['options']     = {}
        self['steering']    = None
        self['tasks']       = []
        self['difplus']     = None
        self['description'] = ''
        self['categories']  = []
        super(Job,self).__init__(*args,**kwargs)
    
    def convert(self):
        if not isinstance(self['steering'],Steering):
            tmp = Steering(self['steering'])
            tmp.convert()
            self['steering'] = tmp
        for i,t in enumerate(self['tasks']):
            if not isinstance(t,Task):
                tmp = Task(t)
                tmp.convert()
                self['tasks'][i] = tmp
        if not isinstance(self['difplus'],DifPlus):
            tmp = DifPlus(self['difplus'])
            tmp.convert()
            self['difplus'] = tmp
    
    def valid(self):
        try:
            return (isinstance(self['dataset'],(Number,String)) and
                    isinstance(self['parent_id'],(Number,String)) and
                    isinstance(self['version'],Number) and
                    self['version'] >= 3 and
                    isinstance(self['options'],dict) and
                    isinstance(self['steering'],Steering) and
                    self['steering'].valid() and
                    isinstance(self['tasks'],list) and
                    all(isinstance(t,Task) and t.valid() for t in self['tasks']) and
                    isinstance(self['difplus'],DifPlus) and
                    self['difplus'].valid() and
                    isinstance(self['description'],String) and
                    isinstance(self['categories'],list)
                   )
        except Exception:
            return False

class Steering(dict):
    """
    Holds all information that goes in the steering section of a configuration.
    
    :ivar parameters: {}
    :ivar batchsys: {} -- a dict of dicts of parameteres (one dict for each batchsys)
    :ivar system: {} -- just specialized parameters
    :ivar resources: []
    :ivar data: []
    """
    def __init__(self,*args,**kwargs):
        self['parameters'] = {}
        self['batchsys']   = {}
        self['system']     = {}
        self['resources']  = []
        self['data']       = []
        super(Steering,self).__init__(*args,**kwargs)
    
    def convert(self):
        for i,r in enumerate(self['resources']):
            if not isinstance(r,Resource):
                tmp = Resource(r)
                tmp.convert()
                self['resources'][i] = tmp
        for i,d in enumerate(self['data']):
            if not isinstance(d,Data):
                tmp = Data(d)
                tmp.convert()
                self['data'][i] = tmp
    
    def valid(self):
        try:
            return (isinstance(self['parameters'],dict) and
                    isinstance(self['batchsys'],dict) and
                    all(isinstance(b,dict) for b in self['batchsys']) and
                    isinstance(self['system'],dict) and
                    isinstance(self['resources'],list) and
                    all(isinstance(r,Resources) and r.valid() for r in self['resources']) and
                    isinstance(self['data'],list) and
                    all(isinstance(d,Data) and d.valid() for d in self['data'])
                   )
        except Exception:
            return False

class _TaskCommon(dict):
    """
    Holds common attributes used by task, tray, module.
    
    :ivar name: None
    :ivar resources: []
    :ivar data: [] 
    :ivar classes: []
    :ivar projects: []
    :ivar parameters: {}
    """
    def __init__(self,*args,**kwargs):
        self['name']       = None
        self['resources']  = []
        self['data']       = []
        self['classes']    = []
        self['projects']   = []
        self['parameters'] = {}
        super(_TaskCommon,self).__init__(*args,**kwargs)
    
    def convert(self):
        for i,r in enumerate(self['resources']):
            if not isinstance(r,Resource):
                tmp = Resource(r)
                tmp.convert()
                self['resources'][i] = tmp
        for i,d in enumerate(self['data']):
            if not isinstance(d,Data):
                tmp = Data(d)
                tmp.convert()
                self['data'][i] = tmp
        for i,c in enumerate(self['classes']):
            if not isinstance(c,Class):
                tmp = Class(c)
                tmp.convert()
                self['classes'][i] = tmp
        for i,p in enumerate(self['projects']):
            if not isinstance(p,Project):
                tmp = Project(p)
                tmp.convert()
                self['projects'][i] = tmp
    
    def valid(self):
        try:
            return ((self['name'] is None or isinstance(self['name'],String)) and
                    isinstance(self['resources'],list) and
                    all(isinstance(r,Resources) and r.valid() for r in self['resources']) and
                    isinstance(self['data'],list) and
                    all(isinstance(d,Data) and d.valid() for d in self['data']) and
                    isinstance(self['classes'],list) and
                    all(isinstance(c,Class) and c.valid() for c in self['classes']) and
                    isinstance(self['projects'],list) and
                    all(isinstance(p,Project) and p.valid() for p in self['projects']) and
                    isinstance(self['parameters'],dict)
                   )
        except Exception:
            return False

class Task(_TaskCommon):
    """
    Holds all information about a task.
    
    :ivar depends: [] -- a list of task names
    :ivar batchsys: {} -- a dict of dicts of parameteres (one dict for each batchsys)
    :ivar trays: []
    """
    def __init__(self,*args,**kwargs):
        self['depends']  = []
        self['batchsys'] = {}
        self['trays']    = []
        super(Task,self).__init__(*args,**kwargs)
    
    def convert(self):
        super(Task,self).convert()
        for i,t in enumerate(self['trays']):
            if not isinstance(t,Tray):
                tmp = Tray(t)
                tmp.convert()
                self['trays'][i] = tmp
    
    def valid(self):
        try:
            return (super(Task,self).valid() and
                    isinstance(self['depends'],list) and
                    all(isinstance(r,(String,Number)) for r in self['depends']) and
                    isinstance(self['batchsys'],dict) and
                    isinstance(self['trays'],list) and
                    all(isinstance(t,Tray) and t.valid() for t in self['trays'])
                   )
        except Exception:
            return False

class Tray(_TaskCommon):
    """
    Holds all information about a tray.
    
    :ivar iterations: 1
    :ivar modules: []
    """
    def __init__(self,*args,**kwargs):
        self['iterations'] = 1
        self['modules']    = []
        super(Tray,self).__init__(*args,**kwargs)
    
    def convert(self):
        super(Task,self).convert()
        for i,m in enumerate(self['modules']):
            if not isinstance(m,Module):
                tmp = Module(m)
                tmp.convert()
                self['modules'][i] = tmp
    
    def valid(self):
        try:
            return (super(Task,self).valid() and
                    isinstance(self['iterations'],Integral) and
                    isinstance(self['modules'],list) and
                    all(isinstance(m,Module) and m.valid() for m in self['modules'])
                   )
        except Exception:
            return False

class Module(_TaskCommon):
    """
    Holds all information about a module.
    
    :ivar running_class: None -- the python class or function to call
    :ivar src: None -- src of class or script
    :ivar args: None -- args to give to class or src if not an iceprod module
    """
    def __init__(self,*args,**kwargs):
        self['running_class'] = None
        self['src']           = None
        self['args']          = None
        super(Module,self).__init__(*args,**kwargs)
    
    def convert(self):
        super(Task,self).convert()
    
    def valid(self):
        try:
            return (super(Task,self).valid() and
                    (self['running_class'] is None or isinstance(self['running_class'],String)) and
                    (self['src'] is None or isinstance(self['src'],String))
                   )
        except Exception:
            return False

class Class(dict):
    """
    A class object, downloaded from a url.
    
    :ivar name: None -- required
    :ivar src: None -- if downloaded from url
    :ivar resource_name: None -- if present in resource object
    :ivar recursive: False
    :ivar libs: None -- if more than default lib directory
    :ivar env_vars: None
    """
    def __init__(self,*args,**kwargs):
        self['name']          = None
        self['src']           = None
        self['resource_name'] = None
        self['recursive']     = False
        self['libs']          = None
        self['env_vars']      = None
        super(Class,self).__init__(*args,**kwargs)
    
    def convert(self):
        pass
    
    def valid(self):
        try:
            return ((self['name'] is None or isinstance(self['name'],String)) and
                    (self['src'] is None or isinstance(self['src'],String)) and
                    (self['resource_name'] is None or isinstance(self['resource_name'],String)) and
                    (self['recursive'] is True or self['recursive'] is False) and
                    (self['libs'] is None or isinstance(self['libs'],String)) and
                    (self['env_vars'] is None or isinstance(self['env_vars'],String))
                   )
        except Exception:
            return False

class Project(dict):
    """
    A project object, shipped with IceProd.
    
    :ivar class_name: None -- required
    :ivar name: None -- optional
    """
    def __init__(self,*args,**kwargs):
        self['name']       = None
        self['class_name'] = None # required
        super(Project,self).__init__(*args,**kwargs)
    
    def convert(self):
        pass
    
    def valid(self):
        try:
            return ((self['name'] is None or isinstance(self['name'],String)) and
                    (self['class_name'] is None or isinstance(self['class_name'],String))
                   )
        except Exception:
            return False

class _ResourceCommon(dict):
    """
    Holds common attributes used by Resource and Data.
    
    :ivar remote: None
    :ivar local: None
    :ivar compression: None
    """
    compression_options = ['none','gzip','gz','bzip','bz2','lzma']

    def __init__(self,*args,**kwargs):
        self['remote']      = None
        self['local']       = None
        self['compression'] = None
        super(_ResourceCommon,self).__init__(*args,**kwargs)
    
    def convert(self):
        pass
    
    def valid(self):
        try:
            return ((self['remote'] is None or isinstance(self['remote'],String)) and
                    (self['local'] is None or isinstance(self['local'],String)) and
                    (self['compression'] is None or self['compression'] is True or
                     self['compression'] is False or
                     self['compression'] in self.compression_options)
                   )
        except Exception:
            return False

class Resource(_ResourceCommon):
    """
    A resource object, representing a file to download.
    
    :ivar arch: None
    """
    def __init__(self,*args,**kwargs):
        self['arch'] = None
        super(Resource,self).__init__(*args,**kwargs)
    
    def convert(self):
        pass
    
    def valid(self):
        try:
            return (super(Resource,self).valid() and
                    (self['arch'] is None or isinstance(self['arch'],String))
                   )
        except Exception:
            return False

class Data(_ResourceCommon):
    """
    A data object, representing input and/or output of data.
    
    :ivar type: None -- required
    :ivar movement: None -- required
    """
    type_options = ['permanent','tray_temp','task_temp','job_temp','dataset_temp','site_temp']
    movement_options = ['input','output','both']
    
    def __init__(self,*args,**kwargs):
        self['type']     = None
        self['movement'] = None
        super(Data,self).__init__(*args,**kwargs)
    
    def convert(self):
        pass
    
    def valid(self):
        try:
            return (super(Data,self).valid() and
                    (self['type'] is None or self['type'] in self.type_options) and
                    (self['movement'] is None or self['movement'] in self.movement_options)
                   )
        except Exception:
            return False
    
    def storage_location(self,env):
        """
        Get storage location from the environment.
        
        :param env: environment
        :returns: storage location as a string, or raises Exception
        """
        type = self.type.lower()
        if type not in Data.type_options:
            raise Exception('Data.type is undefined')
        if 'parameters' in env and type in env['parameters']:
            return env['parameters'][type]
        elif type == 'permanent':
            if 'parameters' in env and 'data_url' in env['parameters']:
                return env['parameters']['data_url']
            else:
                raise Exception('data_url not defined in env[\'parameters\']')
        else:
            raise Exception('%s not defined in env' % type)

class DifPlus(dict):
    """
    A DifPlus object.
    
    :ivar dif: None
    :ivar plus: None
    """
    def __init__(self,*args,**kwargs):
        self['dif']  = None
        self['plus'] = None
        super(DifPlus,self).__init__(*args,**kwargs)
    
    def convert(self):
        if self['dif'] and not isinstance(self['dif'],Dif):
            tmp = Dif(self['dif'])
            tmp.convert()
            self['dif'] = tmp
        if self['plus'] and not isinstance(Plus):
            tmp = Plus(self['plus'])
            tmp.convert()
            self['plus'] = tmp
    
    def valid(self):
        try:
            return ((self['dif'] is None or (isinstance(self['dif'],Dif) and
                     self['dif'].valid())) and
                    (self['plus'] is None or (isinstance(self['plus'],Plus) and
                     self['plus'].valid()))
                   )
        except Exception:
            return False

class Dif(dict):
    """
    A Dif object.
    
   :ivar entry_id: None
   :ivar entry_title: None
   :ivar parameters: ' '
   :ivar iso_topic_category: 'geoscientificinformation'
   :ivar data_ceter: None
   :ivar summary: ' '
   :ivar metadata_name: '[CEOS IDN DIF]'
   :ivar metadata_version: '9.4'
   :ivar personnel: []
   :ivar sensor_name: 'ICECUBE'
   :ivar source_name: 'SIMULATION'
   :ivar dif_creation_date: time.strftime("%Y-%m-%d")
    """
    # TODO: move these to the DB, or somewhere IceCube-specific
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
    
    def __init__(self,*args,**kwargs):
        self['entry_id']     = None
        self['entry_title']  = None
        self['parameters']   = ''
        self['iso_topic_category'] = 'geoscientificinformation'
        self['data_ceter']   = None
        self['summary']      = ''
        self['metadata_name'] = '[CEOS IDN DIF]'
        self['metadata_version'] = '9.4'
        self['personnel']    = []
        self['sensor_name']  = 'ICECUBE'
        self['source_name']  = 'SIMULATION'
        self['dif_creation_date'] = time.strftime("%Y-%m-%d")
        super(Dif,self).__init__(*args,**kwargs)
    
    def convert(self):
        for i,p in enumerate(self['personnel']):
            if not isinstance(p,Personnel):
                tmp = Personnel(p)
                tmp.convert()
                self['personnel'][i] = tmp
    
    def valid(self):
        try:
            return ((self['entry_id'] is None or
                     isinstance(self['entry_id'],(Number,String))) and
                    (self['entry_title'] is None or
                     isinstance(self['entry_title'],String)) and
                    isinstance(self['parameters'],String) and
                    isinstance(self['iso_topic_category'],String) and
                    (self['data_ceter'] is None or
                     (isinstance(self['data_ceter'],DataCenter) and
                      self['data_center'].valid())) and
                    isinstance(self['summary'],String) and
                    isinstance(self['metadata_name'],String) and
                    isinstance(self['metadata_version'],(Number,String)) and
                    any(isinstance(p,Personnel) and p.valid() for p in self['personnel']) and
                    isinstance(self['sensor_name'],String) and
                    isinstance(self['source_name'],String) and
                    isinstance(self['dif_creation_date'],(Number,String))
                   )
        except Exception:
            return False


class Plus(dict):
    """
    A Plus object.
    
   :ivar start: None
   :ivar end: None
   :ivar category: None
   :ivar subcategory: None
   :ivar run_number: None
   :ivar i3db_key: None
   :ivar simdb_key: None
   :ivar project: [] -- [{name: version}]
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
    
    def __init__(self,*args,**kwargs):
        self['start']        = None
        self['end']          = None
        self['category']     = None
        self['subcategory']  = None
        self['run_number']   = None
        self['i3db_key']     = None
        self['simdb_key']    = None
        self['project']      = []
        self['steering_file'] = None
        self['log_file']     = None
        self['command_line'] = None
        super(Plus,self).__init__(*args,**kwargs)
    
    def convert(self):
        pass
    
    def valid(self):
        try:
            return ((self['start'] is None or
                     isinstance(self['start'],(Number,String))) and
                    (self['end'] is None or
                     isinstance(self['end'],(Number,String))) and
                    (self['category'] is None or
                     isinstance(self['category'],String)) and
                    (self['subcategory'] is None or
                     isinstance(self['subcategory'],String)) and
                    (self['run_number'] is None or
                     isinstance(self['run_number'],(Number,String))) and
                    (self['i3db_key'] is None or
                     isinstance(self['i3db_key'],(Number,String))) and
                    (self['simdb_key'] is None or
                     isinstance(self['simdb_key'],(Number,String))) and
                    (self['project'] is None or
                     isinstance(self['project'],(Number,String))) and
                    (self['steering_file'] is None or
                     isinstance(self['steering_file'],String)) and
                    (self['log_file'] is None or
                     isinstance(self['log_file'],String)) and
                    (self['command_line'] is None or
                     isinstance(self['command_line'],String))
                   )
        except Exception:
            return False


class Personnel(dict):
    """
    A Personnel object.
    
   :ivar role: None
   :ivar first_name: None
   :ivar last_name: None
   :ivar email: None
    """
    def __init__(self,*args,**kwargs):
        self['role']       = None
        self['first_name'] = None
        self['last_name']  = None
        self['email']      = None
        super(Personnel,self).__init__(*args,**kwargs)
    
    def convert(self):
        pass
    
    def valid(self):
        try:
            return ((self['role'] is None or
                     isinstance(self['role'],String)) and
                    (self['first_name'] is None or
                     isinstance(self['first_name'],String)) and
                    (self['last_name'] is None or
                     isinstance(self['last_name'],String)) and
                    (self['email'] is None or
                     isinstance(self['email'],String))
                   )
        except Exception:
            return False

class DataCenter(dict):
    """
    A Data Center object.
    
   :ivar name: None
   :ivar personnel: []
    """
    valid_names = ['UWI-MAD/A3RI > Antarctic Astronomy and Astrophysics Research Institute, University of Wisconsin, Madison']
    
    def __init__(self,*args,**kwargs):
        self['name']      = None
        self['personnel'] = []
        super(DataCenter,self).__init__(*args,**kwargs)
    
    def convert(self):
        for i,p in enumerate(self['personnel']):
            if not isinstance(p,Personnel):
                tmp = Personnel(p)
                tmp.convert()
                self['personnel'][i] = tmp
    
    def valid(self):
        try:
            return ((self['name'] is None or
                     isinstance(self['name'],String)) and
                    any(isinstance(p,Personnel) and p.valid() for p in self['personnel'])
                   )
        except Exception:
            return False
