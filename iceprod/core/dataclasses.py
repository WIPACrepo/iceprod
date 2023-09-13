"""
A set of classes that holds dataset configuration data.

Each class is based on a dictionary and only contains simple elements for
easy serialization to json (or other formats). A full dataset configuration
can be had by serializing the :class:`Job` class.

The `convert` method of each class will turn any regular dictionary objects
into special `dataclasses` objects.

The `valid` method of each class will test the validity of the data to be
an actual dataset config.

The `output` method of each class will create json with info on each
dataclass, to be used in javascript.
"""

import time

from numbers import Number, Integral
String = str


# pluralizations for keys that are not classes here
_plurals = {
    'Option': 'Options',
    'Category': 'Categories',
    'Parameter': 'Parameters',
    'System': 'System',
    'Dependence': 'Depends'
}


class Job(dict):
    """
    Holds all information about a running job.

    If the `options` are empty, this is the same as a dataset
    configuration.

    :ivar version: 3
    :ivar options: {} -- a dict of parameters to pass to the task runner
    :ivar steering: None
    :ivar tasks: []
    :ivar difplus: None
    :ivar description: ""
    :ivar categories: []
    """
    plural = 'Jobs'

    def __init__(self,*args,**kwargs):
        self['version'] = 3
        self['options'] = {}
        self['steering'] = None
        self['tasks'] = []
        self['difplus'] = None
        self['description'] = ''
        self['categories'] = []
        super(Job,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'tasks':
                ret[n] = [self[n],'Task']
            elif n == 'difplus':
                ret[n] = [self[n],'DifPlus']
            elif n == 'steering':
                ret[n] = [self[n],'Steering']
            elif n == 'categories':
                ret[n] = [self[n],'']
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        if (self['steering'] is not None and
                not isinstance(self['steering'],Steering)):
            tmp = Steering(self['steering'])
            tmp.convert()
            self['steering'] = tmp
        for i,t in enumerate(self['tasks']):
            if not isinstance(t,Task):
                tmp = Task(t)
                tmp.convert()
                self['tasks'][i] = tmp
        if (self['difplus'] is not None and
                not isinstance(self['difplus'],DifPlus)):
            tmp = DifPlus(self['difplus'])
            tmp.convert()
            self['difplus'] = tmp

    def valid(self):
        try:
            return (
                isinstance(self['version'],Number)
                and self['version'] >= 3
                and isinstance(self['options'],dict)
                and (self['steering'] is None or (
                    isinstance(self['steering'],Steering)
                    and self['steering'].valid()))
                and isinstance(self['tasks'],list)
                and all(isinstance(t,Task) and t.valid() for t in self['tasks'])
                and (self['difplus'] is None or (
                    isinstance(self['difplus'],DifPlus)
                    and self['difplus'].valid()))
                and isinstance(self['description'],String)
                and isinstance(self['categories'],list)
            )
        except Exception:
            return False


class Steering(dict):
    """
    Holds all information that goes in the steering section of a configuration.

    :ivar parameters: {}
    :ivar batchsys: None
    :ivar system: {} -- just specialized parameters
    :ivar resources: []
    :ivar data: []
    """
    plural = 'Steering'

    def __init__(self,*args,**kwargs):
        self['parameters'] = {}
        self['batchsys'] = None
        self['system'] = {}
        self['resources'] = []
        self['data'] = []
        super(Steering,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'resources':
                ret[n] = [self[n],'Resource']
            elif n == 'data':
                ret[n] = [self[n],'Data']
            elif n == 'batchsys':
                ret[n] = [self[n],'Batchsys']
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        if (self['batchsys'] is not None and
                not isinstance(self['batchsys'],Batchsys)):
            tmp = Batchsys(self['batchsys'])
            tmp.convert()
            self['batchsys'] = tmp
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
            return (
                isinstance(self['parameters'],dict)
                and (self['batchsys'] is None or (
                    isinstance(self['batchsys'],Batchsys)
                    and self['batchsys'].valid()))
                and isinstance(self['system'],dict)
                and isinstance(self['resources'],list)
                and all(isinstance(r,Resource) and r.valid() for r in self['resources'])
                and isinstance(self['data'],list)
                and all(isinstance(d,Data) and d.valid() for d in self['data'])
            )
        except Exception:
            return False


class Batchsys(dict):
    """
    Holds information for running on grid/cluster types.
    Designed as a dict of dicts, one for each grid/cluster type.
    """
    plural = "Batchsys"

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        return {'*':{}}

    def convert(self):
        pass

    def valid(self):
        try:
            return all(isinstance(v,dict) for v in self.values())
        except Exception:
            return False


class _TaskCommon(dict):
    """
    Holds common attributes used by task, tray, module.

    :ivar name: ''
    :ivar resources: []
    :ivar data: []
    :ivar classes: []
    :ivar parameters: {}
    """
    def __init__(self,*args,**kwargs):
        self['name'] = ''
        self['resources'] = []
        self['data'] = []
        self['classes'] = []
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

    def valid(self):
        try:
            return (
                isinstance(self['name'],String)
                and isinstance(self['resources'],list)
                and all(isinstance(r,Resource) and r.valid() for r in self['resources'])
                and isinstance(self['data'],list)
                and all(isinstance(d,Data) and d.valid() for d in self['data'])
                and isinstance(self['classes'],list)
                and all(isinstance(c,Class) and c.valid() for c in self['classes'])
                and isinstance(self['parameters'],dict)
            )
        except Exception:
            return False


class Requirement(dict):
    plural = 'Requirements'

    def __init__(self, *args,**kwargs):
        self['cpu'] = None
        self['gpu'] = None
        self['memory'] = None
        self['disk'] = None
        self['os'] = None
        self['site'] = None
        super(Requirement,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            ret[n] = self[n]
        return ret

    def convert(self):
        pass

    def valid(self):
        return (
            (self['cpu'] is None or isinstance(self['cpu'],(String,Number)))
            and (self['gpu'] is None or isinstance(self['gpu'],(String,Number)))
            and (self['memory'] is None or isinstance(self['memory'],(String,Number)))
            and (self['disk'] is None or isinstance(self['disk'],(String,Number)))
            and (self['os'] is None or isinstance(self['os'],String))
            and (self['site'] is None or isinstance(self['site'],String))
        )


class Task(_TaskCommon):
    """
    Holds all information about a task.

    :ivar depends: [] -- a list of task names
    :ivar batchsys: None
    :ivar trays: []
    :ivar requirements: {} -- a dict of requirements
    :ivar task_files: False -- whether to use the task files API
    """
    plural = 'Tasks'

    def __init__(self,*args,**kwargs):
        self['depends'] = []
        self['batchsys'] = None
        self['trays'] = []
        self['requirements'] = Requirement()
        self['task_files'] = False
        self['container'] = None
        super(Task,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'resources':
                ret[n] = [self[n],'Resource']
            elif n == 'data':
                ret[n] = [self[n],'Data']
            elif n == 'classes':
                ret[n] = [self[n],'Class']
            elif n == 'depends':
                ret[n] = [self[n],'']
            elif n == 'batchsys':
                ret[n] = [self[n],'Batchsys']
            elif n == 'trays':
                ret[n] = [self[n],'Tray']
            elif n == 'requirements':
                ret[n] = [self[n],'Requirement']
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        super(Task,self).convert()
        if (self['batchsys'] is not None and
                not isinstance(self['batchsys'],Batchsys)):
            tmp = Batchsys(self['batchsys'])
            tmp.convert()
            self['batchsys'] = tmp
        for i,t in enumerate(self['trays']):
            if not isinstance(t,Tray):
                tmp = Tray(t)
                tmp.convert()
                self['trays'][i] = tmp

    def valid(self):
        try:
            return (
                super(Task,self).valid()
                and isinstance(self['depends'],list)
                and all(isinstance(r,(String,Number)) for r in self['depends'])
                and (self['batchsys'] is None or (
                    isinstance(self['batchsys'],Batchsys)
                    and self['batchsys'].valid()))
                and isinstance(self['trays'],list)
                and all(isinstance(t,Tray) and t.valid() for t in self['trays'])
                and isinstance(self['requirements'],Requirement)
                and self['requirements'].valid()
                and isinstance(self['task_files'],bool)
                and self['container'] is None or isinstance(self['container'], str)
            )
        except Exception:
            return False


class Tray(_TaskCommon):
    """
    Holds all information about a tray.

    :ivar iterations: 1
    :ivar modules: []
    """
    plural = 'Trays'

    def __init__(self,*args,**kwargs):
        self['iterations'] = 1
        self['modules'] = []
        super(Tray,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'resources':
                ret[n] = [self[n],'Resource']
            elif n == 'data':
                ret[n] = [self[n],'Data']
            elif n == 'classes':
                ret[n] = [self[n],'Class']
            elif n == 'modules':
                ret[n] = [self[n],'Module']
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        super(Tray,self).convert()
        for i,m in enumerate(self['modules']):
            if not isinstance(m,Module):
                tmp = Module(m)
                tmp.convert()
                self['modules'][i] = tmp

    def valid(self):
        try:
            return (
                super(Tray,self).valid()
                and isinstance(self['iterations'],Integral)
                and isinstance(self['modules'],list)
                and all(isinstance(m,Module) and m.valid() for m in self['modules'])
            )
        except Exception:
            return False


class Module(_TaskCommon):
    """
    Holds all information about a module.

    :ivar running_class: None -- the python class or function to call
    :ivar src: None -- src of class or script
    :ivar args: None -- args to give to class or src if not an iceprod module
    :ivar env_shell: None -- src of script which sets env and calls arg
    :ivar env_clear: True -- clear the env before calling the module
                              (calls env_shell after clearing, if defined)
    :ivar configs: None -- any json config files that should be written
                             (format is {filename: data})

    Note that `env_clear` should be used carefully, as it wipes out
    any loaded classes.
    """
    plural = 'Modules'

    def __init__(self,*args,**kwargs):
        self['running_class'] = ''
        self['src'] = ''
        self['args'] = ''
        self['env_shell'] = ''
        self['env_clear'] = True
        self['configs'] = {}
        super(Module,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'resources':
                ret[n] = [self[n],'Resource']
            elif n == 'data':
                ret[n] = [self[n],'Data']
            elif n == 'classes':
                ret[n] = [self[n],'Class']
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        super(Module,self).convert()

    def valid(self):
        try:
            return (
                super(Module,self).valid()
                and isinstance(self['running_class'],String)
                and isinstance(self['src'],String)
                and isinstance(self['env_shell'],String)
                and isinstance(self['env_clear'],bool)
                and isinstance(self['configs'],dict)
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
    plural = 'Classes'

    def __init__(self,*args,**kwargs):
        self['name'] = ''
        self['src'] = ''
        self['resource_name'] = ''
        self['recursive'] = False
        self['libs'] = ''
        self['env_vars'] = ''
        super(Class,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            ret[n] = self[n]
        return ret

    def convert(self):
        pass

    def valid(self):
        try:
            return (
                isinstance(self['name'],String)
                and isinstance(self['src'],String)
                and isinstance(self['resource_name'],String)
                and isinstance(self['recursive'],bool)
                and isinstance(self['libs'],String)
                and isinstance(self['env_vars'],String)
            )
        except Exception:
            return False


class _ResourceCommon(dict):
    """
    Holds common attributes used by Resource and Data.

    :ivar remote: ''
    :ivar local: ''
    :ivar compression: False
    """
    compression_options = [False,True,'none','gzip','gz','bzip','bz2','lzma']

    def __init__(self,*args,**kwargs):
        self['remote'] = ''
        self['local'] = ''
        self['compression'] = False
        self['transfer'] = True
        super(_ResourceCommon,self).__init__(*args,**kwargs)

    def convert(self):
        pass

    def valid(self):
        try:
            return (
                isinstance(self['remote'],String)
                and isinstance(self['local'],String)
                and self['compression'] in self.compression_options
                and isinstance(self['transfer'],(String,Number,bool))
            )
        except Exception:
            return False

    def do_transfer(self):
        """
        Test if we should actually transfer the file.
        """
        ret = True
        if isinstance(self['transfer'], bool):
            ret = self['transfer']
        elif isinstance(self['transfer'], String):
            t = self['transfer'].lower()
            if t in ('n','no','not','f','false'):
                ret = False
            elif t in ('m','maybe','exists') or t.startswith('if'):
                ret = 'maybe'
        elif isinstance(self['transfer'], Number):
            if self['transfer'] == 0:
                ret = False
        return ret


class Resource(_ResourceCommon):
    """
    A resource object, representing a file to download.

    :ivar arch: None
    """
    plural = 'Resources'

    def __init__(self,*args,**kwargs):
        self['arch'] = ''
        super(Resource,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'compression':
                ret[n] = [self[n],self.compression_options]
            ret[n] = self[n]
        return ret

    def convert(self):
        super(Resource,self).convert()

    def valid(self):
        try:
            return (
                super(Resource,self).valid()
                and isinstance(self['arch'],String)
            )
        except Exception:
            return False


class Data(_ResourceCommon):
    """
    A data object, representing input and/or output of data.

    :ivar type: 'permanent' -- required
    :ivar movement: 'both' -- required
    """
    plural = 'Data'
    type_options = ['permanent','tray_temp','task_temp','job_temp','dataset_temp','site_temp']
    movement_options = ['input','output','both']

    def __init__(self,*args,**kwargs):
        self['type'] = 'permanent'
        self['movement'] = 'both'
        super(Data,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'compression':
                ret[n] = [self[n],self.compression_options]
            elif n == 'type':
                ret[n] = [self[n],self.type_options]
            elif n == 'movement':
                ret[n] = [self[n],self.movement_options]
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        super(Data,self).convert()

    def valid(self):
        try:
            return (
                super(Data,self).valid()
                and self['type'] in self.type_options
                and self['movement'] in self.movement_options
            )
        except Exception:
            return False

    def storage_location(self,env):
        """
        Get storage location from the environment.

        :param env: environment
        :returns: storage location as a string, or raises Exception
        """
        type = self['type'].lower()
        if type not in Data.type_options:
            raise Exception('Data.type is undefined')
        if 'options' in env and type in env['options']:
            return env['options'][type]
        elif type == 'permanent':
            if 'options' in env and 'data_url' in env['options']:
                return env['options']['data_url']
            else:
                raise Exception('data_url not defined in env[\'options\']')
        else:
            raise Exception('%s not defined in env' % type)

    def filecatalog(self,env):
        """
        Get filecatalog from the environment.

        :param env: environment
        :returns: FileCatalog object, or raises Exception
        """
        type = self['type'].lower()
        if type not in Data.type_options:
            raise Exception('Data.type is undefined')
        elif type in ('job_temp','dataset_temp','site_temp'):
            if 'options' in env and 'filecatalog_temp' in env['options']:
                return env['options']['filecatalog_temp']
        elif type == 'permanent':
            if 'options' in env and 'filecatalog' in env['options']:
                return env['options']['filecatalog']
        raise Exception('%s not defined in env' % type)


class DifPlus(dict):
    """
    A DifPlus object.

    :ivar dif: None
    :ivar plus: None
    """
    plural = 'DifPlus'

    def __init__(self,*args,**kwargs):
        self['dif'] = None
        self['plus'] = None
        super(DifPlus,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'dif':
                ret[n] = [self[n],'Dif']
            elif n == 'plus':
                ret[n] = [self[n],'Plus']
            else:
                ret[n] = self[n]
        return ret

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
            return (
                (self['dif'] is None or (isinstance(self['dif'],Dif) and self['dif'].valid()))
                and (self['plus'] is None or (isinstance(self['plus'],Plus) and self['plus'].valid()))
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
    plural = 'Dif'
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
        self['entry_id'] = None
        self['entry_title'] = None
        self['parameters'] = ''
        self['iso_topic_category'] = 'geoscientificinformation'
        self['data_ceter'] = None
        self['summary'] = ''
        self['metadata_name'] = '[CEOS IDN DIF]'
        self['metadata_version'] = '9.4'
        self['personnel'] = []
        self['sensor_name'] = 'ICECUBE'
        self['source_name'] = 'SIMULATION'
        self['dif_creation_date'] = time.strftime("%Y-%m-%d")
        super(Dif,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'personnel':
                ret[n] = [self[n],'Personnel']
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        for i,p in enumerate(self['personnel']):
            if not isinstance(p,Personnel):
                tmp = Personnel(p)
                tmp.convert()
                self['personnel'][i] = tmp

    def valid(self):
        try:
            return (
                (self['entry_id'] is None or isinstance(self['entry_id'],(Number,String)))
                and (self['entry_title'] is None or isinstance(self['entry_title'],String))
                and isinstance(self['parameters'],String)
                and isinstance(self['iso_topic_category'],String)
                and (self['data_ceter'] is None or (
                    isinstance(self['data_ceter'],DataCenter)
                    and self['data_center'].valid())
                )
                and isinstance(self['summary'],String)
                and isinstance(self['metadata_name'],String)
                and isinstance(self['metadata_version'],(Number,String))
                and all(isinstance(p,Personnel) and p.valid() for p in self['personnel'])
                and isinstance(self['sensor_name'],String)
                and isinstance(self['source_name'],String)
                and isinstance(self['dif_creation_date'],(Number,String))
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
    plural = 'Plus'

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
        self['start'] = None
        self['end'] = None
        self['category'] = None
        self['subcategory'] = None
        self['run_number'] = None
        self['i3db_key'] = None
        self['simdb_key'] = None
        self['project'] = []
        self['steering_file'] = None
        self['log_file'] = None
        self['command_line'] = None
        super(Plus,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'project':
                ret[n] = [self[n],{}]
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        pass

    def valid(self):
        try:
            return (
                (self['start'] is None or isinstance(self['start'],(Number,String)))
                and (self['end'] is None or isinstance(self['end'],(Number,String)))
                and (self['category'] is None or isinstance(self['category'],String))
                and (self['subcategory'] is None or isinstance(self['subcategory'],String))
                and (self['run_number'] is None or isinstance(self['run_number'],(Number,String)))
                and (self['i3db_key'] is None or isinstance(self['i3db_key'],(Number,String)))
                and (self['simdb_key'] is None or isinstance(self['simdb_key'],(Number,String)))
                and all((
                    isinstance(p,dict)
                    and all(isinstance(k,String) for k in p.keys())
                    and all(isinstance(v,(Number,String)) for v in p.values())
                ) for p in self['project'])
                and (self['steering_file'] is None or isinstance(self['steering_file'],String))
                and (self['log_file'] is None or isinstance(self['log_file'],String))
                and (self['command_line'] is None or isinstance(self['command_line'],String))
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
    plural = 'Personnel'

    def __init__(self,*args,**kwargs):
        self['role'] = None
        self['first_name'] = None
        self['last_name'] = None
        self['email'] = None
        super(Personnel,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            ret[n] = self[n]
        return ret

    def convert(self):
        pass

    def valid(self):
        try:
            return (
                (self['role'] is None or isinstance(self['role'],String))
                and (self['first_name'] is None or isinstance(self['first_name'],String))
                and (self['last_name'] is None or isinstance(self['last_name'],String))
                and (self['email'] is None or isinstance(self['email'],String))
            )
        except Exception:
            return False


class DataCenter(dict):
    """
    A Data Center object.

   :ivar name: None
   :ivar personnel: []
    """
    plural = 'DataCenter'
    valid_names = ['UWI-MAD/A3RI > Antarctic Astronomy and Astrophysics Research Institute, University of Wisconsin, Madison']

    def __init__(self,*args,**kwargs):
        self['name'] = None
        self['personnel'] = []
        super(DataCenter,self).__init__(*args,**kwargs)

    def output(self):
        """Output dict with values and (optionally) the object name for
        new objects."""
        ret = {}
        for n in self:
            if n == 'personnel':
                ret[n] = [self[n],'Personnel']
            else:
                ret[n] = self[n]
        return ret

    def convert(self):
        for i,p in enumerate(self['personnel']):
            if not isinstance(p,Personnel):
                tmp = Personnel(p)
                tmp.convert()
                self['personnel'][i] = tmp

    def valid(self):
        try:
            return (
                (self['name'] is None or isinstance(self['name'],String))
                and all(isinstance(p,Personnel) and p.valid() for p in self['personnel'])
            )
        except Exception:
            return False
