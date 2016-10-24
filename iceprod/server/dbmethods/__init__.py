"""
Database methods
"""

import logging
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable
import inspect
from functools import update_wrapper, partial

import tornado.ioloop

from iceprod.functools_future import partialmethod

from iceprod.core.dataclasses import String, Number
import iceprod.server

logger = logging.getLogger('dbmethods')

def dbmethod(*args,**kwargs):
    def make_wrapper(obj):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 60
        def wrapper(*args, **kwargs):
            defaults = dict(kwargs.pop('_defaults'))
            if 'callback' in kwargs:
                defaults['ignore_callback'] = False
                defaults['callback'] = kwargs['callback']
                defaults['timeout_handle'] = None
                ioloop = tornado.ioloop.IOLoop.current()
                def cb(defaults,*args,**kwargs):
                    if not defaults['ignore_callback']:
                        defaults['callback'](*args,**kwargs)
                    defaults['ignore_callback'] = True
                    if defaults['timeout_handle']:
                        def remove_timeout():
                            ioloop.remove_timeout(defaults['timeout_handle'])
                        ioloop.add_callback(remove_timeout)
                kwargs['callback'] = partial(cb,defaults)
                def cb2(defaults,*args,**kwargs):
                    if not defaults['ignore_callback']:
                        defaults['callback'](Exception('timeout'))
                    defaults['ignore_callback'] = True
                if 'timeout' in kwargs:
                    defaults['timeout'] = kwargs.pop('timeout')
                defaults['timeout_handle'] = ioloop.add_timeout(
                        timedelta(seconds=defaults['timeout']),
                        partial(cb2,defaults))
            logger.info('args: %r',args)
            logger.info('kwargs: %r',kwargs)
            try:
                ret = obj(*args,**kwargs)
            except Exception as e:
                logger.info('got exception from dbmethod', exc_info=True)
                defaults['ignore_callback'] = True
                raise
            if ret is not None:
                defaults['ignore_callback'] = True
            return ret
        if (obj.func_code.co_argcount > 0 and
            obj.func_code.co_varnames[0] == 'self'):
            obj2 = partialmethod(wrapper,_defaults=kwargs)
        else:
            obj2 = update_wrapper(partial(wrapper,_defaults=kwargs),obj,
                    ('__name__','__module__','__doc__'),('__dict__',))
        return obj2
    if kwargs:
        return make_wrapper
    else:
        return make_wrapper(*args)

def authorization(auth_role=None, match_user=False, site_valid=False):
    """Authorization decorator.

    Args:
        auth_role (str, list): The role name(s) to match against
        match_user (bool): Match logged in user with kwargs `user_id`
        site_valid (bool): Valid for site queries
    """
    def make_wrapper(obj):
        def wrapper(self, *args, **kwargs):
            auth = kwargs.pop('_auth')
            auth_role = auth['auth_role']
            match_user = auth['match_user']
            site_valid = auth['site_valid']

            # get user and role
            user = None
            role = None
            site_auth = site_valid
            if 'passkey' in kwargs:
                passkey = kwargs.pop('passkey')
                
                if 'site_id' in kwargs:
                    # authorize site
                    site_id = kwargs.pop('site_id')
                    if site_valid:
                        site_auth = self.db.('auth_authorize_site',
                                                       site=site_id, key=passkey)
                else:
                    # authorize task
                    user_auth = yield self.db_call('auth_authorize_task', key=passkey)
                    
            elif 'cookie_id' in kwargs:
                user_id = kwargs.pop('cookie_id')

            # check authorization
            if auth_role:
                if not role:
                    logger.debug('no role to match')
                    raise Exception('authorization failure')
                if not isinstance(auth_role,list):
                    auth_role = [auth_role]
                if all(role != r for r in auth_role):
                    logger.debug('role match failure: %r!=%r', role, auth_role)
                    raise Exception('authorization failure')
            if match_user:
                if not user:
                    logger.debug('no user to match')
                    raise Exception('authorization failure')
                kwargs_user = kwargs.pop('user', False)
                if user != kwargs_user:
                    logger.debug('user match failure: %r!=%r', user, kwargs_user)
                    raise Exception('authorization failure')

            # run function
            return obj(self, *args,**kwargs)

        if (obj.func_code.co_argcount > 0 and
            obj.func_code.co_varnames[0] == 'self'):
            obj2 = partialmethod(wrapper,_auth=kwargs)
        else:
            obj2 = update_wrapper(partial(wrapper,_auth=kwargs),obj,
                    ('__name__','__module__','__doc__'),('__dict__',))
        return obj2
    return make_wrapper

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
                for m,obj in inspect.getmembers(self.subclasses[-1],callable):
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

    def _list_to_dict(self,table,input_row):
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
                ret[k] = input_row[i]
        except:
            logger.warn('error making table %s dict from return values %r',
                         table,input_row)
            raise
        return ret

    def _bulk_select(self, conn, sql, bindings):
        """
        Select many items by id.

        sql should have %s for where bindings are inserted.
        """
        if not isinstance(bindings,list):
            bindings = list(bindings)
        while bindings:
            bindings2 = bindings[:900]
            logger.info('bulk select %s %d',sql,len(bindings2))
            bindings = bindings[900:]
            sql2 = sql%(','.join('?' for _ in bindings2))
            ret = self.db._db_read(conn,sql2,bindings2,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            for row in ret:
                yield row

    def _send_to_master(self, updates, callback=None):
        """Send an update to the master"""
        try:
            self.db.messaging.master_updater.add(arg=updates, callback=callback)
        except Exception:
            logger.warn('_send_to_master() error',exc_info=True)

    def _is_master(self):
        """Test if this is the master"""
        return ('master' in self.db.cfg and
                'status' in self.db.cfg['master'] and
                self.db.cfg['master']['status'])


def filtered_input(input_data):
    """Filter input to sql in cases where we can't use bindings.
       Just remove all " ' ; : ? characters, since
       those won't be needed in proper names"""
    def filter(s):
        if isinstance(s, String):
            return s.replace("'","").replace('"',"").replace(';','').replace(':','').replace('?','')
        elif isinstance(s, Number):
            return s
        else: # if it's not a basic type, discard it
            return ''

    if isinstance(input_data, list):
        return map(filter,input_data)
    elif isinstance(input_data,dict):
        ret = {}
        for x in input_data:
            ret[filter(x)] = filter(input_data[x])
        return ret
    elif isinstance(input_data,OrderedDict):
        ret = OrderedDict()
        for x in input_data:
            ret[filter(x)] = filter(input_data[x])
        return ret
    else:
        return filter(input_data)

def datetime2str(dt):
    """Convert a datetime object to ISO 8601 string"""
    return dt.isoformat()
def nowstr():
    """Get an ISO 8601 string of the current time in UTC"""
    return datetime.utcnow().isoformat()
def str2datetime(st):
    """Convert a ISO 8601 string to datetime object"""
    if '.' in st:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return datetime.strptime( st, "%Y-%m-%dT%H:%M:%S")

