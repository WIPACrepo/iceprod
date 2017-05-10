"""
Authorization and security database methods
"""

import logging
from datetime import datetime,timedelta
from functools import partial
import uuid

import tornado.gen

from iceprod.core.dataclasses import Number,String

from iceprod.server.dbmethods import _Methods_Base,datetime2str,str2datetime,nowstr,memcache

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
    @memcache(size=4096, ttl=300)
    def auth_get_site_auth(self, site_id):
        """
        Get the auth_key for the selected site.

        The selected site is usually the current site.

        Args:
            site_id (str): site id

        Returns:
            str: the auth_key
        """
        sql = 'select auth_key from site where site_id = ?'
        bindings = (site_id,)
        ret = yield self.parent.db.query(sql, bindings)
        if not ret:
            raise Exception('No site match for current site name')
        elif len(ret) > 1:
            raise Exception('More than one site match for current site name')
        elif ret[0][0] is None:
            # DB will return None if column is empty
            raise Exception('Row does not have both site and key')
        else:
            raise tornado.gen.Return(ret[0][0])

    @memcache(size=4096, ttl=300)
    @tornado.gen.coroutine
    def auth_authorize_site(self, site_id, key):
        """
        Validate site and key for authorization.

        Raises Exception if not valid.
        """
        ret = yield self.auth_get_site_auth(site_id)
        if key != ret:
            raise Exception("key does not match")

    @memcache(size=4096, ttl=300)
    @tornado.gen.coroutine
    def auth_authorize_task(self, key):
        """
        Validate key for authorization.

        Raises Exception if not valid.
        """
        sql = 'select auth_key,expire from passkey where auth_key = ?'
        bindings = (key,)
        ret = yield self.parent.db.query(sql, bindings)
        if len(ret) < 1:
            raise Exception('No match for passkey')
        elif len(ret) > 1:
            raise Exception('More than one match for passkey')
        elif len(ret[0]) < 2 or ret[0][1] is None:
            # DB will return None if column is empty
            raise Exception('Row does not have both key and expiration time')
        else:
            k = ret[0][0]
            d = str2datetime(ret[0][1])
            if k != key:
                raise Exception('Passkey returned from db does not match key')
            elif d < datetime.now():
                raise Exception('Passkey is expired')
            else:
                raise tornado.gen.Return(True)

    @tornado.gen.coroutine
    def auth_new_passkey(self, expiration=3600, user_id=None):
        """Make a new passkey.  Default expiration in 1 hour."""
        if isinstance(expiration,Number):
            expiration = datetime.utcnow()+timedelta(seconds=expiration)
        elif not isinstance(expiration,datetime):
            raise Exception('bad expiration')
        if not user_id:
            user_id = ''

        passkey_id = yield self.parent.db.increment_id('passkey')
        passkey = uuid.uuid4().hex
        sql = 'insert into passkey (passkey_id, auth_key, expire, user_id) '
        sql += 'values (?,?,?,?)'
        bindings = (passkey_id, passkey, datetime2str(expiration), user_id)
        yield self.parent.db.query(sql, bindings)
        raise tornado.gen.Return(passkey)

    @memcache(size=65536, ttl=3600)
    @tornado.gen.coroutine
    def auth_get_passkey(self, passkey):
        """Get the expiration datetime of a passkey"""
        if not passkey:
            raise Exception('missing key')

        sql = 'select expire from passkey where auth_key = ?'
        bindings = (passkey,)
        ret = yield self.parent.db.query(sql, bindings)
        if (not ret) or (not ret[0]) or not ret[0][0]:
            raise Exception('get_passkey did not return a passkey')
        raise tornado.gen.Return(str2datetime(ret[0][0]))

    @tornado.gen.coroutine
    def add_site_to_master(self, site_id):
        """Add a remote site to the master and return a new passkey"""
        passkey = uuid.uuid4().hex
        sql = 'insert into site (site_id,auth_key) values (?,?)'
        bindings = (site_id,passkey)
        yield self.parent.db.query(sql, bindings)
        raise tornado.gen.Return(passkey)

    @tornado.gen.coroutine
    def auth_user_create(self, username, passwd, name=None, email=None):
        if 'ldap' in self.parent.db.cfg['system'] and self.parent.db.cfg['system']['ldap']:
            raise Exception('cannot create a user with ldap')
        else:
            with (yield self.parent.db.acquire_lock('user')):
                yield self._auth_user_create_internal(username, passwd, name, email)

    @tornado.gen.coroutine
    def _auth_user_create_internal(self, username, passwd, name=None,
                                   email=None):
        if not name:
            name = username
        if not email:
            email = ''
        try:
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

            user_id = yield self.parent.db.increment_id('user')
            sql = 'insert into user (user_id, username, salt, hash, name, email) '
            sql += 'values (?,?,?,?,?,?)'
            bindings = (user_id, username, salt, db_hash, name, email)
            yield self.parent.db.query(sql, bindings)
        except:
            logger.info('internal auth failure', exc_info=True)
            raise Exception('authentication failure')
        else:
            raise tornado.gen.Return(user_id)

    @tornado.gen.coroutine
    def auth_user(self, username, passwd):
        """Authenticate a username and password"""
        with (yield self.parent.db.acquire_lock('user')):
            if 'ldap' in self.parent.db.cfg['system'] and self.parent.db.cfg['system']['ldap']:
                yield self._auth_user_ldap(username, passwd)
            else:
                yield self._auth_user_internal(username, passwd)

    @tornado.gen.coroutine
    def _auth_user_ldap(self, username, passwd):
        try:
            server = ldap3.Server(self.parent.db.cfg['system']['ldap']['uri'])
            base = self.parent.db.cfg['system']['ldap']['base']
            ldap_conn = ldap3.Connection(server, 'uid={},{}'.format(username, base),
                                    passwd, auto_bind=True)
            ldap_conn.search(base, '(uid={})'.format(username), attributes=['cn','email'])
            name = ldap_conn.entries[0].cn
            email = ldap_conn.entries[0].email

            sql = 'select user_id from user where username=?'
            bindings = (username,)
            ret = yield self.parent.db.query(sql, bindings)
            if (not ret) or not ret[0]:
                user_id = yield self.parent.db.increment_id('user')
                sql = 'insert into user (user_id,username,email,groups,last_login_time) '
                sql += 'values (?,?,?,?,?)'
                bindings = (user_id,username,email,'',nowstr())
            else:
                user_id = ret[0][0]
                sql = 'update user set last_login_time=? where user_id=?'
                bindings = (nowstr(),user_id)
            yield self.parent.db.query(sql, bindings)
            ret = {'id': user_id, 'name': name, 'email': email}
        except:
            logger.info('ldap auth failure', exc_info=True)
            raise Exception('authentication failure')
        else:
            raise tornado.gen.Return(ret)

    @tornado.gen.coroutine
    def _auth_user_internal(self, username, passwd):
        try:
            sql = 'select user_id,name,salt,hash,email from user where username=?'
            bindings = (username,)
            ret = yield self.parent.db.query(sql, bindings)
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
            yield self.parent.db.query(sql, bindings)
            ret = {'id': user_id, 'name': name, 'email': email}
        except:
            logger.info('internal auth failure', exc_info=True)
            raise Exception('authentication failure')
        else:
            raise tornado.gen.Return(ret)
