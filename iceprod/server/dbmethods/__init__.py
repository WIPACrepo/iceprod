"""
Database methods
"""

import logging
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable, namedtuple
import inspect
from functools import update_wrapper, partial, reduce

import cachetools
import cachetools.keys

import tornado.gen
import tornado.concurrent
from tornado.concurrent import run_on_executor
import concurrent.futures

try:
    from functools import partialmethod
except ImportError:
    from iceprod.functools_future import partialmethod

from iceprod.core.dataclasses import String, Number
import iceprod.server

logger = logging.getLogger('dbmethods')


def _is_member_func(obj):
    """Detect if obj is a member func.

    Can also detect decorator-wrapped member funcs.
    """
    while (('future' in obj.__code__.co_varnames
            and 'yielded' in obj.__code__.co_varnames)
           or 'coro' in obj.__code__.co_varnames):
        obj = obj.__closure__[0].cell_contents
    return (obj.__code__.co_argcount > 0 and
            obj.__code__.co_varnames[0] == 'self')

CacheInfo = namedtuple('CacheInfo',['hits','misses','maxsize','currsize'])
def memcache(size=1024, ttl=None):
    """Caching decorator.

    Instrumented like :py:func:`functools.lru_cache` with
    `cache_clear()` and `cache_info()`.

    Args:
        size (int): Max number of entries
        ttl (int): Time to live (default None = infinite)
    """
    if ttl:
        cache = cachetools.TTLCache(size, ttl)
    else:
        cache = cachetools.LRUCache(size)
    info = {'hits':0, 'misses': 0, 'maxsize':size, 'currsize':0}

    def make_wrapper(obj):
        @tornado.gen.coroutine
        def wrapper(self, *args, **kwargs):
            key = cachetools.keys.hashkey(*args,**kwargs)
            if key in cache:
                info['hits'] += 1
                logger.debug('%r: cache hit for %r', obj.__name__, key)
                raise tornado.gen.Return(cache[key])
            else:
                info['misses'] += 1
            if _is_member_func(obj):
                ret = obj(self, *args, **kwargs)
            else:
                ret = obj(*args, **kwargs)
            if isinstance(ret, (tornado.concurrent.Future, concurrent.futures.Future)):
                ret = yield tornado.gen.with_timeout(timedelta(seconds=120),ret)
            cache[key] = ret
            raise tornado.gen.Return(ret)
        if _is_member_func(obj):
            obj2 = wrapper
        else:
            obj2 = update_wrapper(partial(wrapper, None), obj,
                    ('__name__','__module__','__doc__'),('__dict__',))
        # instrument like functools.lru_cache
        def cache_clear():
            logger.debug('clearing cache for %s', obj.__name__)
            for k in list(cache):
                del cache[k]
        def cache_info():
            info['currsize'] = len(cache)
            return CacheInfo(**info)
        obj2.cache_clear = cache_clear
        obj2.cache_info = cache_info
        return obj2
    return make_wrapper

def authorization(**kwargs):
    """Authorization decorator.

    Must be used on a member function of dbmethods in order
    to access the DB for authorization information.

    Non-decorator optional args:
        passkey (str): the passkey (for site, task, or user)
        cookie_id (str): the user_id supplied by a cookie
        site_id (str): the site id

    Args:
        site (bool): Valid for site queries
        user (str, list, callable): The user id(s) to match against
        role (str, list, callable): The role id(s) to match against
        expression (str): The expression of site, user, and role
    """
    def make_wrapper(obj):
        @tornado.gen.coroutine
        def wrapper(self, *args, **kwargs):
            try:
                # take a copy so we don't destroy future calls
                auth = kwargs.pop('_auth').copy()
                auth_site = auth.pop('site', False)
                auth_user = auth.pop('user', None)
                auth_role = auth.pop('role', None)
                auth_expression = auth.pop('expression', None)
                if not auth_expression:
                    enabled_auths = []
                    if auth_site:
                        enabled_auths.append('site')
                    if auth_user:
                        enabled_auths.append('user')
                    if auth_role:
                        enabled_auths.append('role')
                    auth_expression = ' and '.join(enabled_auths)
                    if not auth_expression:
                        raise Exception('cannot have no authorization')

                successful_site = False
                successful_user = False
                successful_role = False
                
                passkey = kwargs.pop('passkey', None)
                cookie_id = kwargs.pop('cookie_id', None)
                site_id = kwargs.pop('site_id', None)

                # check for site validity
                if auth_site and passkey and site_id:
                    # authorize site
                    successful_site = yield self.parent.db_call('auth_authorize_site',
                                                                site=site_id, key=passkey)

                # get username, roles
                username = None
                roles = []
                if passkey and not site_id:
                    ret = yield self.parent.db_call('auth_get_passkey_membership',
                                                    key=passkey)
                    username = ret['username']
                    roles = ret['roles']
                elif cookie_id:
                    ret = yield self.parent.db_call('auth_get_user_membership',
                                                    user_id=cookie_id)
                    username = ret['username']
                    roles = ret['roles']

                # check for user validity
                if auth_user is True and username: # if we just want any logged in user
                    successful_user = True
                elif auth_user and isinstance(auth_user, str):
                    if auth_user == username:
                        successful_user = True
                elif (auth_user and isinstance(auth_user, Iterable)
                      and username in auth_user):
                    successful_user = True
                elif callable(auth_user):
                    if _is_member_func(auth_user):
                        auth_user = partial(auth_user, self)
                    ret = auth_user(username, *args, **kwargs)
                    if isinstance(ret, (tornado.concurrent.Future, concurrent.futures.Future)):
                        ret = yield ret
                    successful_user = ret
                else:
                    logger.warning('auth_user undefined: %r', auth_user)

                # check for role validity
                if auth_role and isinstance(auth_role, str):
                    if auth_role in roles:
                        successful_role = True
                elif auth_role and isinstance(auth_role, Iterable):
                    if set(auth_role).intersection(roles):
                        successful_role = True
                elif callable(auth_role):
                    if _is_member_func(auth_role):
                        auth_role = partial(auth_role, self)
                    ret = auth_role(roles, *args, **kwargs)
                    if isinstance(ret, (tornado.concurrent.Future, concurrent.futures.Future)):
                        ret = yield ret
                    successful_role = ret
                else:
                    logger.warning('auth_role undefined: %r', auth_role)

                # check auth expression
                auth_expression = auth_expression.replace('site', 'True' if successful_site else 'False')
                auth_expression = auth_expression.replace('user', 'True' if successful_user else 'False')
                auth_expression = auth_expression.replace('role', 'True' if successful_role else 'False')
                logger.debug('auth expression: %r', auth_expression)
                ret = eval(auth_expression)
                if not ret:
                    raise Exception('failed auth expression')
            except Exception:
                logger.warning('error in authorization', exc_info=True)
                raise Exception('authorization error')
            else:
                # run function
                ret = obj(self, *args,**kwargs)
                if isinstance(ret, (tornado.concurrent.Future, concurrent.futures.Future)):
                    ret = yield tornado.gen.with_timeout(timedelta(seconds=120),ret)
                raise tornado.gen.Return(ret)

        return partialmethod(wrapper,_auth=kwargs)
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
        if isinstance(table,str):
            if table not in tables:
                raise Exception('bad table')
            keys = tables[table]
        elif isinstance(table,Iterable):
            if not set(table) <= set(tables):
                raise Exception('bad table')
            keys = reduce(lambda a,b: a+list(tables[b].keys()), table, [])
        else:
            raise Exception('bad table type')

        try:
            return OrderedDict(zip(keys,input_row))
        except Exception:
            logger.warning('error making table %s dict from return values %r',
                         table,input_row)
            raise

    def _bulk_select(self, sql, bindings, extra_bindings=None, num=990):
        """
        Select many items by id.

        sql should have %s for where bindings are inserted.

        Args:
            sql (str): An sql template
            bindings (iterable): The bindings to iterate over
            extra_bindings (iterable): Extra bindings to add to each query

        Returns:
            list: A list of :class:`Future` objects
        """
        if not isinstance(bindings,list):
            bindings = list(bindings)
        ret = []
        while bindings:
            bindings2 = bindings[:num]
            logger.info('bulk select %s %d',sql,len(bindings2))
            bindings = bindings[num:]
            if extra_bindings:
                cnt = sql.split('%s',1)[0].count('?')
            sql2 = sql%(','.join('?' for _ in bindings2))
            if extra_bindings:
                extra_bindings = list(extra_bindings)
                bindings2 = extra_bindings[:cnt] + bindings2 + extra_bindings[cnt:]
            ret.append(self.parent.db.query(sql2, bindings2))
        return ret

    @tornado.gen.coroutine
    def _send_to_master(self, updates):
        """Send an update to the master"""
        if 'master_updater' in self.parent.modules:
            try:
                yield self.parent.modules['master_updater']['add'](updates)
            except Exception:
                logger.warning('_send_to_master() error',exc_info=True)

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

