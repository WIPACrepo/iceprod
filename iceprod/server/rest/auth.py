import os
import time
import random
import binascii
import socket
from threading import Thread,Event,Condition
import logging
import json
import uuid
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

import pymongo
import motor
import ldap3

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization, catch_error

logger = logging.getLogger('rest.auth')

def setup(config):
    """
    Setup method for Auth REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for auth, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_auth = config.get('rest',{}).get('auth',{})
    db_name = cfg_auth.get('database','mongodb://localhost:27017')

    # add indexes
    db = pymongo.MongoClient(db_name).auth
    if 'name_index' not in db.roles.index_information():
        db.roles.create_index('name', name='name_index', unique=True)
    if 'group_id_index' not in db.groups.index_information():
        db.groups.create_index('group_id', name='group_id_index', unique=True)
    if 'user_id_index' not in db.users.index_information():
        db.users.create_index('user_id', name='user_id_index', unique=True)

    handler_cfg = RESTHandlerSetup(config)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(db_name).auth,
    })
    ldap_cfg = dict(handler_cfg)
    ldap_cfg.update({
        'ldap_uri': cfg_auth.get('ldap_uri', ''),
        'ldap_base': cfg_auth.get('ldap_base', ''),
    })

    return [
        (r'/roles', MultiRoleHandler, handler_cfg),
        (r'/roles/(\w+)', RoleHandler, handler_cfg),
        (r'/groups', MultiGroupHandler, handler_cfg),
        (r'/groups/(\w+)', GroupHandler, handler_cfg),
        (r'/users', MultiUserHandler, handler_cfg),
        (r'/users/(\w+)', UserHandler, handler_cfg),
        (r'/users/(\w+)/groups', UserGroupsHandler, handler_cfg),
        (r'/ldap', LDAPHandler, ldap_cfg),
    ]


class AuthHandler(RESTHandler):
    """
    Base handler for Auth REST API. 
    """
    def initialize(self, database=None, **kwargs):
        super(AuthHandler, self).initialize(**kwargs)
        self.db = database

class MultiRoleHandler(AuthHandler):
    """
    Handle multi-role requests.
    """
    @authorization(roles=['admin'])
    async def get(self):
        """
        Get a list of roles.

        Returns:
            dict: {'results': list of roles}
        """
        ret = await self.db.roles.find(projection={'_id':False}).to_list(length=1000)
        self.write({'results':ret})
        self.finish()

class RoleHandler(AuthHandler):
    """
    Handle individual role requests.
    """
    @authorization(roles=['admin'])
    async def put(self, role_name):
        """
        Add/modify a role.

        Body should contain all necessary fields for a role.

        Args:
            role_name (str): the role to add/modify

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'name' not in data:
            data['name'] = role_name
        elif data['name'] != role_name:
            raise tornado.web.HTTPError(400, 'role name mismatch')
        ret = await self.db.roles.find_one_and_replace({'name':role_name},
                data, upsert=True)
        self.write({})
        self.finish()

    @authorization(roles=['admin'])
    async def get(self, role_name):
        """
        Get a role.

        Args:
            role_name (str): the role to get

        Returns:
            dict: role info
        """
        ret = await self.db.roles.find_one({'name':role_name},
                projection={'_id':False})
        if not ret:
            self.send_error(404, "Role not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin'])
    async def delete(self, role_name):
        """
        Delete a role.

        Args:
            role_name (str): the role to delete

        Returns:
            dict: empty dict
        """
        await self.db.roles.delete_one({'name':role_name})
        self.write({})
        self.finish()

class MultiGroupHandler(AuthHandler):
    """
    Handle multi-group requests.
    """
    @authorization(roles=['admin'])
    async def get(self):
        """
        Get a list of groups.

        Returns:
            dict: {'results': list of groups}
        """
        ret = await self.db.groups.find(projection={'_id':False}).to_list(length=1000)
        self.write({'results':ret})
        self.finish()

    @authorization(roles=['admin'])
    async def post(self):
        """
        Add a group.

        Body should contain all necessary fields for a group.
        """
        data = json.loads(self.request.body)
        data['group_id'] = uuid.uuid1().hex
        ret = await self.db.groups.insert_one(data)
        self.set_status(201)
        self.set_header('Location', '/groups/'+data['group_id'])
        self.write({'result': '/groups/'+data['group_id']})
        self.finish()

class GroupHandler(AuthHandler):
    """
    Handle individual group requests.
    """
    @authorization(roles=['admin'])
    async def get(self, group_id):
        """
        Get a group.

        Args:
            group_id (str): the group to get

        Returns:
            dict: group info
        """
        ret = await self.db.groups.find_one({'group_id':group_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, "Group not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin'])
    async def delete(self, group_id):
        """
        Delete a group.

        Args:
            group_id (str): the group to delete

        Returns:
            dict: empty dict
        """
        await self.db.groups.delete_one({'group_id':group_id})
        self.write({})
        self.finish()

class MultiUserHandler(AuthHandler):
    """
    Handle multi-user requests.
    """
    @authorization(roles=['admin'])
    async def get(self):
        """
        Get a list of users.

        Returns:
            dict: {'results': list of users}
        """
        ret = await self.db.users.find(projection={'_id':False}).to_list(length=1000)
        self.write({'results':ret})
        self.finish()

    @authorization(roles=['admin'])
    async def post(self):
        """
        Add a user.

        Body should contain all necessary fields for a user.
        """
        data = json.loads(self.request.body)
        data['user_id'] = uuid.uuid1().hex
        ret = await self.db.users.insert_one(data)
        self.set_status(201)
        self.set_header('Location', '/users/'+data['user_id'])
        self.write({'result': '/users/'+data['user_id']})
        self.finish()

class UserHandler(AuthHandler):
    """
    Handle individual user requests.
    """
    @authorization(roles=['admin'])
    async def get(self, user_id):
        """
        Get a user.

        Args:
            user_id (str): the user to get

        Returns:
            dict: user info
        """
        ret = await self.db.users.find_one({'user_id':user_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, "User not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin'])
    async def delete(self, user_id):
        """
        Delete a user.

        Args:
            user_id (str): the user to delete

        Returns:
            dict: empty dict
        """
        await self.db.users.delete_one({'user_id':user_id})
        self.write({})
        self.finish()

class UserGroupsHandler(AuthHandler):
    """
    Handle groups for an individual user.
    """
    @authorization(roles=['admin'])
    async def get(self, user_id):
        """
        Get the groups for a user.

        Args:
            user_id (str): the user

        Returns:
            dict: {results: list of groups}
        """
        ret = await self.db.users.find_one({'user_id':user_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, "User not found")
        else:
            if 'groups' in ret:
                self.write({'results':ret['groups']})
            else:
                self.write({'results':[]})
            self.finish()

    @authorization(roles=['admin'])
    async def post(self, user_id):
        """
        Add a group to a user.

        Body should contain {'group': <group_name>}

        Args:
            user_id (str): the user

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'group' not in data:
            raise tornado.web.HTTPError(400, 'missing group')
        ret = await self.db.groups.find_one({'group_id': data['group']},
                projection=['_id'])
        if not ret:
            raise tornado.web.HTTPError(400, 'invalid group')
        ret = await self.db.users.find_one_and_update({'user_id':user_id},
                {'$addToSet': {'groups': data['group']}})
        self.write({})
        self.finish()

    @authorization(roles=['admin'])
    async def put(self, user_id):
        """
        Set the groups for a user.

        Body should contain {'groups': [ <group_name> ] }

        Args:
            user_id (str): the user

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'groups' not in data:
            raise tornado.web.HTTPError(400, 'missing groups')
        for g in data['groups']:
            ret = await self.db.groups.find_one({'group_id': g},
                    projection=['_id'])
            if not ret:
                raise tornado.web.HTTPError(400, 'invalid group')
        ret = await self.db.users.find_one_and_update({'user_id':user_id},
                {'$set': {'groups': data['groups']}})
        self.write({})
        self.finish()

class LDAPHandler(AuthHandler):
    """
    Handle LDAP authentication.
    """ 
    def initialize(self, ldap_uri=None, ldap_base=None, **kwargs):
        super(LDAPHandler, self).initialize(**kwargs)
        self.ldap_uri = ldap_uri
        self.ldap_base = ldap_base

    @catch_error
    async def post(self):
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


        


