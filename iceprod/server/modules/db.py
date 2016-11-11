"""
Database server module
"""
from __future__ import absolute_import, division, print_function

from threading import Thread, Event, Condition, Lock
import os
import logging
import time
import random
import json
from itertools import izip
from functools import partial
from contextlib import contextmanager
from collections import OrderedDict, Iterable

import tornado.locks
from tornado.concurrent import run_on_executor

from concurrent.futures import ThreadPoolExecutor

from iceprod.server import module
from iceprod.server import dbmethods
from iceprod.server import GlobalID, get_pkgdata_filename

logger = logging.getLogger('modules_db')

class db(module.module):
    """
    Handle all interaction with the local database
    """
    def __init__(self,*args,**kwargs):
        # run default init
        super(db,self).__init__(*args,**kwargs)
        self.db = None

        # find all dbmethods
        raw_types = iceprod.server.listmodules('iceprod.server.dbmethods')
        logger.info('available dbmethod modules: %r',raw_types)
        for r in raw_types:
            c = iceprod.server.run_module(r,self)
            for m,obj in inspect.getmembers(c, callable):
                if m.startswith('_'):
                    continue
                if m in self.services:
                    logger.critical('duplicate method name in dbmethods: %s',m)
                    raise Exception('duplicate method name in dbmethods: %s'%m)
                self.services[m] = obj

    def start(self):
        """Start thread"""
        super(db,self).start()
        try:
            t = self.cfg['db']['type']
            if t.lower() == 'mysql':
                logger.info('attempting to start MySQL db')
                self.db = MySQL(self)
            elif t.lower() == 'sqlite':
                logger.info('attempting to start SQLite db')
                self.db = SQLite(self)
            else:
                raise Exception('Unknown database type: %s'%t)
        except Exception:
            logger.critical('failed to start db', exc_info=True)
            self.modules['daemon'].stop()

    def stop(self):
        self.db = None
        super(db,self).stop()

    def kill(self):
        self.db = None
        super(db,self).kill()

def read_db_conf(field_name=None):
    """
    Read the DB conf data from file.

    Args:
        field_name (string): (optional) field to return
    Returns:
        Either a specific field, or all fields in a dict
    """
    if not read_db_conf.cache:
        filename = get_pkgdata_filename('iceprod.server','data/etc/db_config.json')
        if not os.path.exists(filename):
            raise Exception('cannot find db_config.json')
        d = json.load(open(filename))
        # strip comments out of tables
        for t in d['tables']:
            tmp = OrderedDict()
            for row in d['tables'][t]:
                tmp[row[0]] = row[1]
            d['tables'][t] = tmp
        read_db_conf.cache = d
    if not field_name:
        return read_db_conf.cache
    if field_name in read_db_conf.cache:
        return read_db_conf.cache[field_name]
    raise Exception('cannot find field name')
read_db_conf.cache = None

class DBAPI(object):
    """
    API for database interaction

    Config data (such as tables) are found at :ref:`dbconfig`.
    """

    # define tables
    tables = read_db_conf('tables')
    archive_tables = read_db_conf('archive_tables')
    status_options = read_db_conf('status_options')

    ### basic functions ###

    def __init__(self, parent):
        # set cfg
        self.cfg = parent.cfg
        self.modules = parent.modules
        self.io_loop = parent.io_loop
        self.locks = {}

        # set up threads and connections for each thread
        nthreads = self.cfg['db']['nthreads']
        self.executor = ThreadPoolExecutor(nthreads)
        self._connections = [self._dbsetup() for _ in range(nthreads)]

        # indexes
        self.indexes = {}
        for k in self.tables:
            v = self.tables[k].keys()[0]
            if v.replace('_id','_offset') in self.tables['setting']:
                self.indexes[k] = v

        self._setup_tables()
        self.init()

    def init(self):
        """
        Initialize the settings table, if necessary.
        """
        conn = self._dbsetup()
        try:
            site_id = self.cfg['site_id']
            sql = 'select * from setting where setting_id = 0'
            ret = self._db_read(conn, sql)
            if ret and len(ret) >= 1:
                return
            # table is not initialized, so do so
            sql = 'insert into setting ('
            sql += ','.join(self.tables['setting'].keys())
            sql += ') values ('
            sql += ','.join(['?' for _ in self.tables['setting'].keys()])
            sql += ')'
            bindings = tuple()
            for key in self.tables['setting']:
                if key == 'setting_id':
                    bindings += (0,)
                elif key.endswith('_last'):
                    bindings += (GlobalID.int2char(0),)
                elif key.endswith('_offset'):
                    bindings += (GlobalID.globalID_gen(0,site_id),)
                else:
                    raise Exception('unexpected settings key: %s'%key)
            self._db_write(conn,sql,bindings)
        except:
            logger.error('settings table init failed', exc_info=True)
            raise

    def acquire_lock(self, lock_name='_none_'):
        """
        Yield a :py:class:`tornado.locks.Lock` for a specific task.

        This is designed to be used in a context-manager:

            with (yield db.acquire_lock('task1')):
                # do something

        Args:
            lock_name (str): The name of the task
        """
        if lock_name not in self.locks:
            self.locks[lock_name] = tornado.locks.Lock()
        return self.locks[lock_name]

    @run_on_executor
    def increment_id(self, table_name):
        """
        Increment the id of a table, returning the old value.

        Args:
            table_name (str): The name of the table

        Returns:
            str: A table id
        """
        conn = self.connections.pop()
        try:
            return self._increment_id_helper(conn, table_name)
        finally:
            self.connections.append(conn)

    @run_on_executor
    def query(self, sql, bindings=tuple()):
        """
        Execute a database query.

        If this is a read (SELECT) query, it will return an iterator
        over rows. If this is a write (INSERT, UPDATE, REPLACE) query,
        it will return `None`.

        Args:
            sql (str): The query to execute
            bindings (tuple): The bindings, if any

        Returns:
            iterator or None
        """
        conn = self.connections.pop()
        try:
            if sql.lower().startswith('select'):
                return self._db_read(self, conn, sql, bindings)
            else:
                self._db_write(self, conn, sql, bindings)
        finally:
            self.connections.append(conn)

    ### Functions that must be overwritten in subclasses ###

    def _setup_tables(self):
        """Set up tables if they are not present"""
        raise NotImplementedError()

    def _dbsetup(self):
        """Set up database connection"""
        raise NotImplementedError()

    def _db_read(self,conn,sql,bindings):
        """Do a read query from the database"""
        raise NotImplementedError()
    def _db_write(self,conn,sql,bindings):
        """Do a write query from the database"""
        raise NotImplementedError()

    def _increment_id_helper(self, conn, table_name):
        """
        Increment the id of the table, returning the id.

        Args:
            conn (DB connection): The database connection
            table_name (str): The table name to increment on

        Returns:
            str: an id
        """
        raise NotImplementedError()

try:
    import apsw
except ImportError:
    logger.warn('Cannot import apsw. SQLite db not available')
else:
    class SQLite(DBAPI):
        """SQLite 3 implementation of DBAPI"""
        def __init__(self, *args, **kwargs):
            super(SQLite,self).__init__(*args,**kwargs)

        def _setup_tables(self):
            conn = self._dbsetup()
            for table_name in self.tables.keys():
                sql_create = ' ('
                sql_select = ' '
                sep = ''
                cols = self.tables[table_name].keys()
                for col in cols:
                    sql_create += sep+'"'+col+'"'
                    sql_select += sep+'"'+col+'"'
                    if sep == '':
                        sql_create += ' PRIMARY KEY' # make first column the primary key
                        sep = ', '
                sql_create += ')'
                scols = set(cols)
                with conn as c:
                    cur = c.cursor()
                    try:
                        curcols = set()
                        for cid,name,type,notnull,dflt,pk in cur.execute("pragma table_info("+table_name+")"):
                            curcols.add(name)
                        if not curcols or len(curcols) < 1:
                            # table does not exist
                            logger.info('create table '+table_name+sql_create)
                            cur.execute('create table '+table_name+sql_create)
                        elif curcols != scols:
                            # table not the same
                            logger.info('modify table '+table_name)

                            # get similar cols
                            keepcols = curcols & scols

                            full_sql = 'create temporary table '+table_name+'_backup '+sql_create+';'
                            full_sql += 'insert into '+table_name+'_backup select '+','.join(keepcols)+' from '+table_name+';'
                            full_sql += 'drop table '+table_name+';'
                            full_sql += 'create table '+table_name+sql_create+';'
                            full_sql += 'insert into '+table_name+' select '+sql_select+' from '+table_name+'_backup;'
                            full_sql += 'drop table '+table_name+'_backup;'
                            cur.execute(full_sql)
                        else:
                            # table is good
                            logger.info('table '+table_name+' already exists')
                    except apsw.Error:
                        # something went wrong
                        logger.warn('setup tables error', exc_info=True)
                        raise

        def _dbsetup(self):
            logger.debug('_dbsetup()')
            kwargs = {}
            if ('db' in self.cfg and 'sqlite_cachesize' in self.cfg['db'] and
                isinstance(self.cfg['db']['sqlite_cachesize'],int)):
                kwargs['statementcachesize'] = self.cfg['db']['sqlite_cachesize']
            conn = apsw.Connection(self.cfg['db']['name'], **kwargs)
            conn.cursor().execute('PRAGMA journal_mode = WAL')
            conn.cursor().execute('PRAGMA synchronous = OFF')
            conn.setbusytimeout(100)
            return conn

        def _db_query(self, cur, sql, bindings=None):
            """Make a db query and do error handling"""
            logger.info('running query %s',sql)
            for i in range(10):
                try:
                    if bindings is not None:
                        cur.execute(sql, bindings)
                    else:
                        cur.execute(sql)
                except (apsw.BusyError, apsw.LockedError):
                    # try again for transient errors, but with random
                    # exponential backoff up to a minute
                    backoff = 0.1*random.uniform(2**(i-1), 2**i)
                    logger.warn('database busy/locked, backoff %f', backoff)
                    time.sleep(backoff)
                    continue
                except apsw.Error:
                    raise # just kill for other db errors
                return
            raise Exception('database busy/locked and timeout')

        def _db_read(self, conn, sql, bindings):
            ret = None
            try:
                with conn as c:
                    cur = c.cursor()
                    self._db_query(cur, sql, bindings)
                    ret = cur.fetchall()
            except:
                logger.info('sql: %r', sql)
                logger.info('bindings: %r', bindings)
                logger.warning('error in _db_read', exc_info=True)
                raise
            logger.debug('_db_read returns %r', ret)
            return ret

        def _db_write(self,conn,sql,bindings,archive_conn,archive_sql,archive_bindings):
            try:
                with conn as c:
                    cur = c.cursor()
                    if isinstance(sql, basestring):
                        self._db_query(cur, sql, bindings)
                    elif isinstance(sql, Iterable):
                        for s,b in izip(sql, bindings):
                            self._db_query(cur, s, b)
                    else:
                        logger.info('sql: %r', sql)
                        raise Exception('sql is an unknown type')
            except:
                logger.info('sql: %r', sql)
                logger.info('bindings: %r', bindings)
                logger.warning('error in _db_write', exc_info=True)
                raise

        def _increment_id_helper(self, conn, table_name):
            new_id = None
            with conn as c:
                cur = c.cursor()
                if table_name+'_offset' in self.tables['setting']:
                    # global id
                    self._db_query(cur,'select '+table_name+'_offset from setting',tuple())
                    ret = cur.fetchall()
                    if (not ret) or not ret[0]:
                        raise Exception('bad return value')
                    site_id = self.cfg['site_id']
                    old_id = ret[0][0]
                    old_id = GlobalID.localID_ret(old_id,type='int')
                    new_id = GlobalID.globalID_gen(old_id+1,site_id)
                    self._db_query(cur,'update setting set '+table_name+'_offset = ?',(new_id,))
                elif table_name+'_last' in self.tables['setting']:
                    # local id
                    self._db_query(cur,'select '+table_name+'_last from setting',tuple())
                    ret = cur.fetchall()
                    if (not ret) or not ret[0]:
                        raise Exception('bad return value')
                    old_id = ret[0][0]
                    new_id = GlobalID.int2char(GlobalID.char2int(old_id)+1)
                    self._db_query(cur,'update setting set '+table_name+'_last = ?',(new_id,))
                else:
                    raise Exception('not in setting table')
            return new_id

try:
    import MySQLdb
except ImportError:
    logger.warn('Cannot import MySQLdb. Trying pymysql')
    try:
        import pymysql as MySQLdb
    except ImportError:
        logger.warn('Cannot import pymysql. MySQL db not available')
        MySQLdb = None
if MySQLdb:
    class MySQL(DBAPI):
        """MySQL 5 implementation of DBAPI"""

        def _setup_tables(self):
            conn = self._dbsetup()
            dbname = self.cfg['db']['name']
            for table_name in self.tables.keys():
                sql_create = ' ('
                sql_select = ' '
                sep = ''
                cols = self.tables[table_name].keys()
                for col in cols:
                    sql_create += sep+'`'+col+'`'
                    sql_select += sep+'`'+col+'`'
                    t = self.tables[table_name][col]
                    if t == 'str':
                        sql_create += ' VARBINARY(255) NOT NULL '
                    elif t == 'int':
                        sql_create += ' INT NOT NULL DEFAULT 0 '
                    elif t == 'bool':
                        sql_create += ' BOOL NOT NULL DEFAULT 0 '
                    elif t == 'float':
                        sql_create += ' DOUBLE NOT NULL DEFAULT 0.0 '
                    elif t == 'Text':
                        sql_create += ' TEXT NOT NULL DEFAULT "" '
                    elif t == 'MediumText':
                        sql_create += ' MEDIUMTEXT NOT NULL DEFAULT "" '
                    if sep == '':
                        sql_create += ' PRIMARY KEY' # make first column the primary key
                        sep = ', '
                sql_create += ')'
                scols = set(cols)
                try:
                    cur = conn.cursor()
                    try:
                        curcols = set()
                        curdatatypes = {}
                        cur.execute("""select column_name, column_type from information_schema.columns where table_name = '"""+table_name+"""' and table_schema = '"""+dbname+"""'""")
                        for name,datatype in cur.fetchall():
                            curcols.add(name)
                            curdatatypes[name] = datatype
                        if not curcols or len(curcols) < 1:
                            # table does not exist
                            logger.info('create table '+table_name)
                            cur.execute('create table '+table_name+sql_create+' CHARACTER SET utf8 COLLATE utf8_general_ci')
                        elif curcols != scols:
                            # table not the same
                            logger.info('modify table '+table_name)

                            # differences
                            rmcols = curcols - scols
                            addcols = []
                            for x in cols:
                                if x not in curcols:
                                    t = '`'+self.tables[table_name][x]+'`'
                                    if t == 'str':
                                        x += ' VARBINARY(255) NOT NULL '
                                    elif t == 'int':
                                        x += ' INT NOT NULL DEFAULT 0 '
                                    elif t == 'bool':
                                        x += ' BOOL NOT NULL DEFAULT 0 '
                                    elif t == 'float':
                                        x += ' DOUBLE NOT NULL DEFAULT 0.0 '
                                    elif t == 'Text':
                                        x += ' TEXT NOT NULL DEFAULT "" '
                                    elif t == 'MediumText':
                                        x += ' MEDIUMTEXT NOT NULL DEFAULT "" '
                                    addcols.append(x)

                            full_sql = 'alter table '+table_name+' '
                            if addcols:
                                full_sql += 'add column '
                                full_sql += ', add column '.join(col for col in addcols)
                            if addcols and rmcols:
                                full_sql += ', '
                            if rmcols:
                                full_sql += 'drop column '
                                full_sql += ', drop column '.join('`'+col+'`' for col in rmcols)
                            cur.execute(full_sql)
                        else:
                            # table is good
                            logger.info('table '+table_name+' already exists')
                    except MySQLdb.MySQLError:
                        # something went wrong
                        logger.warning('error', exc_info=True)
                        raise
                except:
                    try:
                        conn.rollback()
                    except:
                        pass
                    raise
                else:
                    conn.commit()

        def _dbsetup(self):
            name = self.cfg['db']['name']
            mysql_address = self.cfg['db']['mysql_address']
            mysql_port = None
            if ':' in mysql_address:
                mysql_port = int(mysql_address.rsplit(':',1)[1])
                mysql_address = mysql_address.rsplit(':',1)[0]
            mysql_username = self.cfg['db']['mysql_username']
            mysql_password = self.cfg['db']['mysql_password']
            kwargs = {'host':mysql_address,
                      'db':name,
                      'user':mysql_username,
                      'passwd':mysql_password,
                      'connect_timeout':10,
                      'use_unicode':True,
                      'charset':'utf8'}
            if mysql_port is not None:
                kwargs['port'] = mysql_port
            return MySQLdb.Connection(**kwargs)

        def _convert_to_mysql(self, sql, bindings):
            num = sql.count('?')
            if num < 1:
                return (sql, bindings)

            if bindings is None:
                raise Exception('bindings is None, but expected %d bindings'%(num,))
            elif num != len(bindings):
                raise Exception('wrong number of bindings - expected %d and got %d'%(num,len(bindings)))

            pieces = sql.replace('%','%%').split('?')
            newsql = '%s'.join(pieces)

            return (newsql, bindings)

        def _db_query(self, con, sql, bindings=None):
            """Make a db query and do error handling"""
            sql, bindings = self._convert_to_mysql(sql, bindings)
            try:
                if bindings is not None:
                    con.execute(sql, bindings)
                else:
                    con.execute(sql)
            except MySQLdb.MySQLError:
                raise # just kill for other db errors

        def _db_read(self, conn, sql, bindings):
            """Do a read query from the database"""
            ret = None
            try:
                cur = conn.cursor()
                self._db_query(cur,sql,bindings)
                ret = cur.fetchall()
            except:
                logger.warning('error reading', exc_info=True)
                try:
                    conn.rollback()
                except:
                    pass
                raise
            else:
                conn.commit()
            return ret

        def _db_write(self, conn, sql, bindings):
            try:
                cur = conn.cursor()
                if isinstance(sql,basestring):
                    self._db_query(cur,sql,bindings)
                elif isinstance(sql,Iterable):
                    for s,b in izip(sql,bindings):
                        self._db_query(cur,s,b)
                else:
                    raise Exception('sql is an unknown type')
            except:
                logger.warning('error writing', exc_info=True)
                try:
                    conn.rollback()
                except:
                    pass
                raise
            else:
                conn.commit()

        def _increment_id_helper(self, conn, table_name):
            new_id = None
            try:
                cur = conn.cursor()
                if table_name+'_offset' in self.tables['setting']:
                    # global id
                    self._db_query(cur,'select '+table_name+'_offset from setting',tuple())
                    ret = cur.fetchall()
                    site_id = self.cfg['site_id']
                    old_id = ret[0][0]
                    old_id = GlobalID.localID_ret(old_id,type='int')
                    new_id = GlobalID.globalID_gen(old_id+1,site_id)
                    self._db_query(cur,'update setting set '+table_name+'_offset = ?',(new_id,))
                elif table_name+'_last' in self.tables['setting']:
                    # local id
                    cur = conn.cursor()
                    self._db_query(cur,'select '+table_name+'_last from setting',tuple())
                    ret = cur.fetchall()
                    old_id = ret[0][0]
                    new_id = GlobalID.int2char(GlobalID.char2int(old_id)+1)
                    self._db_query(cur,'update setting set '+table_name+'_last = ?',(new_id,))
                else:
                    raise Exception('not in setting table')
            except:
                logger.warning('error incrementing id', exc_info=True)
                try:
                    conn.rollback()
                except:
                    pass
                raise
            else:
                conn.commit()
            return new_id
