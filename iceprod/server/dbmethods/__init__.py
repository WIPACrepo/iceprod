"""
Database methods
"""

import logging
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable
import inspect
from functools import update_wrapper, partial

import tornado.gen
from tornado.concurrent import run_on_executor

from iceprod.functools_future import partialmethod

from iceprod.core.dataclasses import String, Number
import iceprod.server

logger = logging.getLogger('dbmethods')


def authorization(**kwargs):
    """Authorization decorator.

    Args:
        auth_role (str, list): The role name(s) to match against
        match_user (bool): Match logged in user with kwargs `user_id`
        site_valid (bool): Valid for site queries
    """
    def make_wrapper(obj):
        def wrapper(self, *args, **kwargs):
            auth = kwargs.pop('_auth')
            auth_role = auth.pop(['auth_role'], None)
            auth_user = auth.pop(['auth_user'], None)
            site_valid = auth.pop(['site_valid'], False)

            # get user and role
            user = None
            role = None
            site_auth = False
            passkey_auth = False
            if 'passkey' in kwargs:
                passkey = kwargs.pop('passkey')
                
                if 'site_id' in kwargs:
                    # authorize site
                    site_id = kwargs.pop('site_id')
                    if site_valid:
                        site_auth = self.parent.db('auth_authorize_site',
                                                   site=site_id, key=passkey)
                else:
                    # authorize task
                    passkey_auth = yield self.parent.db_call('auth_authorize_task', key=passkey)
                    
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
            ret = yield obj(self, *args,**kwargs)
            raise tornado.gen.Return(ret)

        if (obj.func_code.co_argcount > 0 and
            obj.func_code.co_varnames[0] == 'self'):
            obj2 = partialmethod(wrapper,_auth=kwargs)
        else:
            obj2 = update_wrapper(partial(wrapper,_auth=kwargs),obj,
                    ('__name__','__module__','__doc__'),('__dict__',))
        return obj2
    return make_wrapper


class _Methods_Base():
    """Base class for DB methods classes."""
    def __init__(self,parent):
        self.parent = parent
        self.io_loop = parent.io_loop
        self.executor = parent.executor

    def _list_to_dict(self,table,input_row):
        """Convert an input that is a list of values from a table
           into a dict of values from that table."""
        tables = self.parent.db.tables
        if isinstance(table,basestring):
            if table not in tables:
                raise Exception('bad table')
            keys = tables[table]
        elif isinstance(table,Iterable):
            if not set(table) <= set(tables):
                raise Exception('bad table')
            keys = reduce(lambda a,b: a+tables[b].keys(), table, [])
        else:
            raise Exception('bad table type')

        try:
            return OrderedDict(zip(keys,input_row))
        except:
            logger.warn('error making table %s dict from return values %r',
                         table,input_row)
            raise

    def _bulk_select(self, sql, bindings):
        """
        Select many items by id.

        sql should have %s for where bindings are inserted.

        Args:
            sql (str): An sql template
            bindings (iterable): The bindings to iterate over

        Returns:
            list: A list of :class:`Future` objects
        """
        if not isinstance(bindings,list):
            bindings = list(bindings)
        ret = []
        while bindings:
            bindings2 = bindings[:990]
            logger.info('bulk select %s %d',sql,len(bindings2))
            bindings = bindings[990:]
            sql2 = sql%(','.join('?' for _ in bindings2))
            ret.append(self.parent.db.query(sql2, bindings2))
        return ret

    @tornado.gen.coroutine
    def _send_to_master(self, updates):
        """Send an update to the master"""
        if 'master_updater' in self.parent.modules:
            try:
                yield self.parent.modules['master_updater']['add'](updates)
            except Exception:
                logger.warn('_send_to_master() error',exc_info=True)

    def _is_master(self):
        """Test if this is the master"""
        return ('master' in self.parent.db.cfg and
                'status' in self.parent.db.cfg['master'] and
                self.parent.db.cfg['master']['status'])

    @run_on_executor
    def _executor_wrapper(self, func):
        """
        Run function inside executor

        Args:
            func (callable): function to wrap
        """
        return func()


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

