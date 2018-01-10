import os
import time
import random
import binascii
import socket
from threading import Thread,Event,Condition
import logging
from contextlib import contextmanager
from functools import partial, wraps
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from datetime import timedelta

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen

import tornado.concurrent
import concurrent.futures

import motor
import ldap3

from iceprod.server.rest import RESTHandler, RESTHandlerSetup

logger = logging.getLogger('rest.auth')

def setup(config):
    """
    Setup method for Auth REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`tornado.server.config`.

    Returns: list of routes for auth, which can be passed to
             :py:class:`tornado.web.Application`.
    """
    cfg_auth = config.get('rest',{}).get('auth',{})
    db_name = cfg_auth.get('database','mongodb://localhost:27017')
    handler_cfg = RESTHandlerSetup(config)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(db_name),
    })
    ldap_cfg = dict(handler_cfg)
    ldap_cfg.update({
        'ldap_uri': cfg_auth.get('ldap_uri', ''),
        'ldap_base': cfg_auth.get('ldap_base', ''),
    })

    return [
        (r'/roles', MultiRoleHandler, handler_cfg),
        (r'/roles/(.*)', RoleHandler, handler_cfg),
        (r'/ldap', LDAPHandler, ldap_cfg),
    ]


class AuthHandler(RESTHandler):
    """
    Base handler for Auth REST API. 
    """
    def initialize(self, database=None, auth=None, **kwargs):
        super(AuthHandler, self).initialize(**kwargs)
        self.db = database

class MultiRoleHandler(AuthHandler):
    """
    Handle multi-role requests.
    """
    @tornado.gen.coroutine
    def get(self):
        """Get a list of roles."""
        self.write({})
        self.finish()

class RoleHandler(AuthHandler):
    """
    Handle individual role requests.
    """
    @tornado.gen.coroutine
    def put(self, role):
        """Create a role."""
        self.send_error(400, "Error")

    @tornado.gen.coroutine
    def get(self, role):
        """Get a role."""
        self.send_error(404, "Role not found")

    @tornado.gen.coroutine
    def delete(self, role):
        """Get a role."""
        self.send_error(404, "Role not found")
    

class LDAPHandler(AuthHandler):
    """
    Handle LDAP authentication.
    """ 
    def initialize(self, ldap_uri=None, ldap_base=None, **kwargs):
        super(LDAPHandler, self).initialize(**kwargs)
        self.ldap_uri = ldap_uri
        self.ldap_base = ldap_base

    @tornado.gen.coroutine
    def post(self):
        """Validate LDAP login, creating a token."""
        try:
            username = self.get_argument('username')
            password = self.get_argument('password')
            conn = ldap3.Connection(self.ldap_uri, 'uid={},{}'.format(username, self.ldap_base),
                                    password, auto_bind=True)
        except Exception:
            self.send_error(403, "Login failed")
        else:
            # get user info from DB


            # create token
            tok = self.auth.create_token(username)
            self.write(tok)
            self.finish()


        


