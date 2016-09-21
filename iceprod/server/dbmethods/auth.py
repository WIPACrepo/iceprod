"""
Authorization and security database methods
"""

import logging
from datetime import datetime,timedelta
from functools import partial
import uuid

from iceprod.core.dataclasses import Number,String

from iceprod.server.dbmethods import dbmethod,_Methods_Base,datetime2str,str2datetime,nowstr

logger = logging.getLogger('dbmethods.auth')

try:
    import ldap3
except ImportError:
    logger.info('cannot use ldap auth')


class auth(_Methods_Base):
    """
    The authorization / security DB methods.

    Takes a handle to a subclass of iceprod.server.modules.db.DBAPI
    as an argument.
    """
    @dbmethod
    def auth_get_site_auth(self,site_id,callback=None):
        """Get the auth_key for the selected site (usually the current site).
        Returns the auth_key"""
        sql = 'select auth_key from site where site_id = ?'
        bindings = (site_id,)
        cb = partial(self._auth_get_site_auth_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _auth_get_site_auth_callback(self,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                if len(ret) < 1:
                    callback(Exception('No site match for current site name'))
                elif len(ret) > 1:
                    callback(Exception('More than one site match for current site name'))
                elif ret[0][0] is None:
                    # DB will return None if column is empty
                    callback(Exception('Row does not have both site and key'))
                else:
                    callback(ret[0][0])

    @dbmethod
    def auth_authorize_site(self,site,key,callback=None):
        """Validate site and key for authorization.
        Returns True/Exception"""
        sql = 'select site_id,auth_key from site where site_id = ?'
        bindings = (site,)
        cb = partial(self._auth_authorize_site_callback,key,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _auth_authorize_site_callback(self,key,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                if len(ret) < 1:
                    callback(Exception('No site match for current site id'))
                elif len(ret) > 1:
                    callback(Exception('More than one site match for current site id'))
                elif len(ret[0]) < 2 or ret[0][1] is None:
                    # DB will return None if column is empty
                    callback(Exception('Row does not have both site and key'))
                else:
                    callback(key == ret[0][1])

    @dbmethod
    def auth_authorize_task(self,key,callback=None):
        """Validate key for authorization.
        Returns True/Exception"""
        sql = 'select auth_key,expire from passkey where auth_key = ?'
        bindings = (key,)
        cb = partial(self._auth_authorize_task_callback,key,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _auth_authorize_task_callback(self,key,ret,callback=None):
        if callback:
            if isinstance(ret,Exception):
                callback(ret)
            else:
                if len(ret) < 1:
                    callback(Exception('No match for passkey'))
                elif len(ret) > 1:
                    callback(Exception('More than one match for passkey'))
                elif len(ret[0]) < 2 or ret[0][1] is None:
                    # DB will return None if column is empty
                    callback(Exception('Row does not have both key and expiration time'))
                else:
                    k = ret[0][0]
                    d = str2datetime(ret[0][1])
                    if k != key:
                        callback(Exception('Passkey returned from db does not match key'))
                    elif d < datetime.now():
                        callback(Exception('Passkey is expired'))
                    else:
                        callback(True)

    @dbmethod
    def auth_new_passkey(self, expiration=3600, user_id=None, callback=None):
        """Make a new passkey.  Default expiration in 1 hour."""
        if isinstance(expiration,Number):
            expiration = datetime.utcnow()+timedelta(seconds=expiration)
        elif not isinstance(expiration,datetime):
            raise Exception('bad expiration')
        if not user_id:
            user_id = ''

        passkey_id = self.db.increment_id('passkey')
        passkey = uuid.uuid4().hex
        sql = 'insert into passkey (passkey_id, auth_key, expire, user_id) '
        sql += 'values (?,?,?,?)'
        bindings = (passkey_id, passkey, datetime2str(expiration), user_id)
        cb = partial(self._auth_new_passkey_callback,passkey,callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _auth_new_passkey_callback(self,passkey,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(passkey)

    @dbmethod
    def auth_get_passkey(self,passkey,callback=None):
        """Get the expiration datetime of a passkey"""
        if not passkey:
            raise Exception('bad expiration')

        sql = 'select expire from passkey where auth_key = ?'
        bindings = (passkey,)
        cb = partial(self._auth_get_passkey_callback,callback=callback)
        self.db.sql_read_task(sql,bindings,callback=cb)
    def _auth_get_passkey_callback(self,ret,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        elif (not ret) or (not ret[0]) or not ret[0][0]:
            callback(Exception('get_passkey did not return a passkey'))
        else:
            try:
                expiration = str2datetime(ret[0][0])
            except Exception as e:
                callback(e)
            else:
                callback(expiration)

    @dbmethod
    def add_site_to_master(self,site_id,callback=None):
        """Add a remote site to the master and return a new passkey"""
        passkey = uuid.uuid4().hex
        sql = 'insert into site (site_id,auth_key) values (?,?)'
        bindings = (site_id,passkey)
        cb = partial(self._add_site_to_master_callback,passkey,
                     callback=callback)
        self.db.sql_write_task(sql,bindings,callback=cb)
    def _add_site_to_master_callback(self,passkey,ret=None,callback=None):
        if isinstance(ret,Exception):
            callback(ret)
        else:
            callback(passkey)

    @dbmethod
    def auth_user_create(self, username, passwd, name=None, email=None,
                         callback=None):
        if 'ldap' in self.db.cfg['system'] and self.db.cfg['system']['ldap']:
            callback(Exception('cannot create a user with ldap'))
        else:
            cb = partial(self._auth_user_create_internal, username, passwd,
                         callback=callback)
            self.db.blocking_task('user', cb)

    def _auth_user_create_internal(self, username, passwd, name=None,
                                   email=None, callback=None):
        if not name:
            name = username
        if not email:
            email = ''
        try:
            conn,archive_conn = self.db._dbsetup()
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
            from cryptography.hazmat.backends import default_backend
            backend = default_backend()
            salt = os.urandom(64)
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=128,
                salt=salt,
                iterations=100000,
                backend=backend
            )
            db_hash = kdf.derive(passwd)

            user_id = self.db._increment_id_helper('user',conn)
            sql = 'insert into user (user_id, username, salt, hash, name, email) '
            sql += 'values (?,?,?,?,?,?)'
            bindings = (user_id, username, salt, db_hash, name, email)
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
        except:
            logger.info('internal auth failure', exc_info=True)
            callback(Exception('authentication failure'))
        else:
            callback(user_id)

    @dbmethod
    def auth_user(self, username, passwd, callback=None):
        """Authenticate a username and password"""
        if 'ldap' in self.db.cfg['system'] and self.db.cfg['system']['ldap']:
            cb = partial(self._auth_user_ldap, username, passwd,
                         callback=callback)
        else:
            cb = partial(self._auth_user_internal, username, passwd,
                         callback=callback)
        self.db.blocking_task('user', cb)

    def _auth_user_ldap(self, username, passwd, callback=None):
        try:
            server = ldap3.Server(self.db.cfg['system']['ldap']['uri'])
            base = self.db.cfg['system']['ldap']['base']
            ldap_conn = ldap3.Connection(server, 'uid={},{}'.format(username, base),
                                    passwd, auto_bind=True)
            ldap_conn.search(base, '(uid={})'.format(username), attributes=['cn','email'])
            name = ldap_conn.entries[0].cn
            email = ldap_conn.entries[0].email

            conn,archive_conn = self.db._dbsetup()
            sql = 'select user_id from user where username=?'
            bindings = (username,)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            if (not ret) or not ret[0]:
                user_id = self.db._increment_id_helper('user',conn)
                sql = 'insert into user (user_id,username,email,groups,last_login_time) '
                sql += 'values (?,?,?,?,?)'
                bindings = (user_id,username,email,'',nowstr())
            else:
                user_id = ret[0][0]
                sql = 'update user set last_login_time=? where user_id=?'
                bindings = (nowstr(),user_id)
            ret = self.db._db_write(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            callback({'id': user_id, 'name': name, 'email': email})
        except:
            logger.info('ldap auth failure', exc_info=True)
            callback(Exception('authentication failure'))

    def _auth_user_internal(self, username, passwd, callback=None):
        try:
            conn,archive_conn = self.db._dbsetup()
            sql = 'select user_id,name,salt,hash,email from user where username=?'
            bindings = (username,)
            ret = self.db._db_read(conn,sql,bindings,None,None,None)
            if isinstance(ret,Exception):
                raise ret
            if (not ret) or not ret[0]:
                raise Exception('cannot find username')
            user_id, name, salt, db_hash, email = ret[0]

            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
            from cryptography.hazmat.backends import default_backend
            backend = default_backend()
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=128,
                salt=salt,
                iterations=100000,
                backend=backend
            )
            kdf.verify(passwd, db_hash)

            sql = 'update user set last_login_time=? where user_id=?'
            bindings = (nowstr(),user_id)
            ret = self.db._db_write(conn,sql,bindings,None,None,None)

            callback({'id': user_id, 'name': name, 'email': email})
        except:
            logger.info('internal auth failure', exc_info=True)
            callback(Exception('authentication failure'))
