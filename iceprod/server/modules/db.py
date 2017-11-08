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
from functools import partial
from contextlib import contextmanager
from collections import OrderedDict, Iterable
import inspect
from datetime import timedelta

import tornado.locks
import tornado.gen
from tornado.concurrent import run_on_executor

from concurrent.futures import ThreadPoolExecutor

import iceprod.server
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
                if m in self.service:
                    logger.critical('duplicate method name in dbmethods: %s',m)
                    raise Exception('duplicate method name in dbmethods: %s'%m)
                self.service[m] = obj

    def start(self):
        """Start database"""
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
            self.modules['daemon']['stop']()

    def stop(self):
        self.db = None
        super(db,self).stop()

    def kill(self):
        self.db = None
        super(db,self).kill()

class DBResetError(Exception):
    pass

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
    indices = read_db_conf('indices')
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
        self._inc_id_connection = self._dbsetup('setting')

        # indexes
        self.indexes = {}
        for k in self.tables:
            v = list(self.tables[k].keys())[0]
            if v.replace('_id','_offset') in self.tables['setting']:
                self.indexes[k] = v

        self._setup_tables()
        self.init()

    def init(self):
        """
        Initialize the settings table, if necessary.
        """
        conn = self._inc_id_connection
        try:
            site_id = self.cfg['site_id']
            sql = 'select * from setting where setting_id = 0'
            ret = self._db_read(conn, sql, tuple())
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
        except Exception:
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
        logger.info('acquire_lock(%s)', lock_name)
        if lock_name not in self.locks:
            self.locks[lock_name] = tornado.locks.Lock()
        return self.locks[lock_name].acquire(timeout=timedelta(seconds=50))

    @tornado.gen.coroutine
    def increment_id(self, table_name):
        """
        Increment the id of a table, returning the old value.

        Args:
            table_name (str): The name of the table

        Returns:
            str: A table id
        """
        try:
            ret = self._increment_id_helper(self._inc_id_connection, table_name)
        except DBResetError:
            # connection needs a reset
            logger.warn('resetting db connection')
            self._inc_id_connection.close()
            self._inc_id_connection = self._dbsetup()
            ret = self._increment_id_helper(self._inc_id_connection, table_name)
        raise tornado.gen.Return(ret)

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
        if isinstance(sql, str):
            reading = sql.lower().strip().startswith('select')
        elif isinstance(sql, Iterable):
            reading = any(s.lower().strip().startswith('select') for s in sql)
        conn = self._connections.pop()
        try:
            if reading:
                return self._db_read(conn, sql, bindings)
            else:
                self._db_write(conn, sql, bindings)
        except DBResetError:
            # connection needs a reset
            logger.warn('resetting db connection')
            conn.close()
            conn = self._dbsetup()
            # retry query
            if reading:
                return self._db_read(conn, sql, bindings)
            else:
                self._db_write(conn, sql, bindings)
        finally:
            self._connections.append(conn)

    ### Functions that must be overwritten in subclasses ###

    def _setup_tables(self):
        """Set up tables if they are not present"""
        raise NotImplementedError()

    def _dbsetup(self, dbname=None):
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
                sql_create += ') WITHOUT ROWID'
                sql_index_create = []
                if table_name in self.indices:
                    for col in self.indices[table_name]:
                        name = col.replace(',','_')
                        sql_index_create.append('CREATE INDEX IF NOT EXISTS '+name+'_index ON '+table_name+' ('+col+')')
                scols = set(cols)
                with (conn if table_name != 'setting' else self._inc_id_connection) as c:
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

                            sql = 'create table '+table_name+'_backup '+sql_create
                            logger.info(sql)
                            cur.execute(sql)
                            sql = 'insert into '+table_name+'_backup ('+(','.join(keepcols))+') select '+','.join(keepcols)+' from '+table_name
                            logger.info(sql)
                            cur.execute(sql)
                            sql = 'drop table '+table_name
                            logger.info(sql)
                            cur.execute(sql)
                            sql = 'alter table '+table_name+'_backup rename to '+table_name
                            logger.info(sql)
                            cur.execute(sql)
                        else:
                            # table is good
                            logger.info('table '+table_name+' already exists')
                        # try for indices
                        for query in sql_index_create:
                            cur.execute(query)
                    except apsw.Error:
                        # something went wrong
                        logger.warn('setup tables error', exc_info=True)
                        raise

        def _dbsetup(self, dbname=None):
            logger.debug('_dbsetup()')
            cachesize = -50000
            kwargs = {
                'statementcachesize':1000,
            }
            if ('db' in self.cfg and 'sqlite_statementcachesize' in self.cfg['db'] and
                isinstance(self.cfg['db']['sqlite_statementcachesize'],int)):
                kwargs['statementcachesize'] = self.cfg['db']['sqlite_statementcachesize']
            if ('db' in self.cfg and 'sqlite_cachesize' in self.cfg['db'] and
                isinstance(self.cfg['db']['sqlite_cachesize'],int)):
                cachesize = -1*self.cfg['db']['sqlite_cachesize']
            name = 'name'
            if dbname:
                name += '_'+dbname
            conn = apsw.Connection(self.cfg['db'][name], **kwargs)
            conn.cursor().execute('PRAGMA journal_mode = WAL')
            conn.cursor().execute('PRAGMA synchronous = OFF')
            conn.cursor().execute('PRAGMA automatic_index = OFF')
            conn.cursor().execute('PRAGMA cache_size = %d'%cachesize)
            conn.setbusytimeout(100)
            return conn

        def _db_query(self, cur, sql, bindings=None):
            """Make a db query and do error handling"""
            logger.debug('running query %s',sql)
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
                except apsw.Error as e:
                    raise DBResetError(str(e))
                return
            raise Exception('database busy/locked and timeout')

        def _db_read(self, conn, sql, bindings=None):
            ret = None
            try:
                with conn as c:
                    cur = c.cursor()
                    self._db_query(cur, sql, bindings)
                    ret = cur.fetchall()
            except Exception:
                logger.info('sql: %r', sql)
                logger.info('bindings: %r', bindings)
                logger.warning('error in _db_read', exc_info=True)
                raise
            logger.debug('_db_read returns %r', ret)
            return ret

        def _db_write(self, conn, sql, bindings=None):
            try:
                with conn as c:
                    cur = c.cursor()
                    if isinstance(sql, str):
                        self._db_query(cur, sql, bindings)
                    elif isinstance(sql, Iterable):
                        for s,b in zip(sql, bindings):
                            self._db_query(cur, s, b)
                    else:
                        logger.info('sql: %r', sql)
                        raise Exception('sql is an unknown type')
            except Exception:
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
                    else:
                        raise Exception('unknown column type: %r'%t)
                    if sep == '':
                        sql_create += ' PRIMARY KEY' # make first column the primary key
                        sep = ', '
                sql_create += ') CHARACTER SET utf8 COLLATE utf8_general_ci'
                sql_index_create = []
                if table_name in self.indices:
                    for col in self.indices[table_name]:
                        name = col.replace(',','_')
                        sql_index_create.append('CREATE INDEX '+name+'_index ON '+table_name+' ('+col+')')
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
                            cur.execute('create table '+table_name+sql_create)
                            for sql in sql_index_create:
                                cur.execute(sql)
                        elif curcols != scols:
                            # table not the same
                            logger.info('modify table '+table_name)

                            # get similar cols
                            keepcols = curcols & scols

                            sql = 'create table '+table_name+'_backup '+sql_create
                            logger.info(sql)
                            cur.execute(sql)
                            sql = 'insert into '+table_name+'_backup ('+(','.join(keepcols))+') select '+','.join(keepcols)+' from '+table_name
                            logger.info(sql)
                            cur.execute(sql)
                            sql = 'drop table '+table_name
                            logger.info(sql)
                            cur.execute(sql)
                            sql = 'alter table '+table_name+'_backup rename to '+table_name
                            logger.info(sql)
                            cur.execute(sql)
                            for sql in sql_index_create:
                                cur.execute(sql)
                        else:
                            # table is good
                            logger.info('table '+table_name+' already exists')
                    except MySQLdb.MySQLError:
                        # something went wrong
                        logger.warning('error', exc_info=True)
                        raise
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise
                else:
                    conn.commit()

        def _dbsetup(self, dbname=None):
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
            logger.debug('running query %s',sql)
            sql, bindings = self._convert_to_mysql(sql, bindings)
            try:
                if bindings is not None:
                    con.execute(sql, bindings)
                else:
                    con.execute(sql)
            except (MySQLdb.InterfaceError, MySQLdb.OperationalError,
                    MySQLdb.InternalError) as e:
                raise DBResetError(str(e))
            except MySQLdb.MySQLError:
                raise # just kill for other db errors

        @staticmethod
        def _convert_to_unicode(rows):
            ret = []
            for row in rows:
                r = []
                for obj in row:
                    if isinstance(obj, bytes):
                        try:
                            r.append(obj.decode('utf-8'))
                        except Exception:
                            r.append(obj)
                    else:
                        r.append(obj)
                ret.append(r)
            return ret

        def _db_read(self, conn, sql, bindings):
            """Do a read query from the database"""
            ret = None
            try:
                cur = conn.cursor()
                self._db_query(cur,sql,bindings)
                ret = self._convert_to_unicode(cur.fetchall())
            except Exception:
                logger.warning('error reading', exc_info=True)
                try:
                    conn.rollback()
                except (MySQLdb.InterfaceError, MySQLdb.OperationalError,
                        MySQLdb.InternalError) as e:
                    raise DBResetError(str(e))
                except Exception:
                    pass
                raise
            else:
                conn.commit()
            return ret

        def _db_write(self, conn, sql, bindings):
            try:
                cur = conn.cursor()
                if isinstance(sql,str):
                    self._db_query(cur,sql,bindings)
                elif isinstance(sql,Iterable):
                    for s,b in zip(sql,bindings):
                        self._db_query(cur,s,b)
                else:
                    raise Exception('sql is an unknown type')
            except Exception:
                logger.warning('error writing', exc_info=True)
                try:
                    conn.rollback()
                except (MySQLdb.InterfaceError, MySQLdb.OperationalError,
                        MySQLdb.InternalError) as e:
                    raise DBResetError(str(e))
                except Exception:
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
            except Exception:
                logger.warning('error incrementing id', exc_info=True)
                try:
                    conn.rollback()
                except (MySQLdb.InterfaceError, MySQLdb.OperationalError,
                        MySQLdb.InternalError) as e:
                    raise DBResetError(str(e))
                except Exception:
                    pass
                raise
            else:
                conn.commit()
            return new_id
