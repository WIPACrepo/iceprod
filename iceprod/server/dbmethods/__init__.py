"""
Database methods
"""

import logging
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable
import inspect

import iceprod.server

logger = logging.getLogger('dbmethods')

class DBMethods():
    """The actual methods to be called on the database.
    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument."""
    def __init__(self,db):
        self.db = db
        self.subclasses = []
        self.methods = {}
        
        # find all subclasses
        raw_types = iceprod.server.listmodules('iceprod.server.dbmethods')
        logger.info('available modules: %r',raw_types)
        index = -1
        for r in raw_types:
            # try instantiating the module
            try:
                self.subclasses.append(iceprod.server.run_module(r,self))
                index += 1
            except Exception:
                logger.error('Error importing module',exc_info=True)
            else:
                for m,obj in inspect.getmembers(self.subclasses[-1],inspect.ismethod):
                    if m.startswith('_'):
                        continue
                    if m in self.methods:
                        logger.critical('duplicate method name in dbmethods: %s',m)
                        raise Exception('duplicate method name in dbmethods: %s'%m)
                    self.methods[m] = index
    
    def __getattr__(self,name):
        if name in self.methods:
            return getattr(self.subclasses[self.methods[name]],name)
        else:
            raise AttributeError("DBMethods instance has no attribute '%s'"%name)

class _Methods_Base():
    """Base class for DB methods classes."""
    def __init__(self,parent):
        self.db = parent.db
        self.parent = parent
    
    def _list_to_dict(self,table,input):
        """Convert an input that is a list of values from a table 
           into a dict of values from that table."""
        if isinstance(table,basestring):
            if table not in self.db.tables:
                raise Exception('bad table')
            keys = self.db.tables[table]
        elif isinstance(table,Iterable):
            if not set(table) <= set(self.db.tables):
                raise Exception('bad table')
            keys = reduce(lambda a,b:a+self.db.tables[b].keys(), table, [])
        else:
            raise Exception('bad table type')
        
        ret = OrderedDict()
        try:
            for i,k in enumerate(keys):
                ret[k] = input[i]
        except:
            logger.warn('error making table %s dict from return values %r',
                         table,input)
            raise
        return ret

def filtered_input(input):
    """Filter input to sql in cases where we can't use bindings.
       Just remove all " ' ; : ? characters, since
       those won't be needed in proper names"""
    def filter(s):
        if isinstance(s, str):
            return s.replace("'","").replace('"',"").replace(';','').replace(':','').replace('?','')
        elif isinstance(s, (int,long,real,complex)):
            return s
        else: # if it's not a basic type, discard it
            return ''
        
    if isinstance(input, list):
        return map(filter,input)
    elif isinstance(input,dict):
        ret = {}
        for x in input:
            ret[filter(x)] = filter(input[x])
        return ret
    elif isinstance(input,OrderedDict):
        ret = OrderedDict()
        for x in input:
            ret[filter(x)] = filter(input[x])
        return ret
    else:
        return filter(input)

def datetime2str(dt):
    """Convert a datetime object to ISO 8601 string"""
    return dt.isoformat()
def str2datetime(st):
    """Convert a ISO 8601 string to datetime object"""
    if '.' in st:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S")

