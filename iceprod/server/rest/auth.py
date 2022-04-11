import logging
import json
import uuid
import time

import tornado.web
import pymongo
import motor
import ldap3

from iceprod.server.rest import RESTHandler, RESTHandlerSetup, authorization, authenticated, catch_error

logger = logging.getLogger('rest.auth')

def setup(config, *args, **kwargs):
    """
    Setup method for Auth REST API.

    Sets up any database connections or other prerequisites.

    Args:
        config (dict): an instance of :py:class:`iceprod.server.config`.

    Returns:
        list: Routes for auth, which can be passed to :py:class:`tornado.web.Application`.
    """
    cfg_rest = config.get('rest',{}).get('auth',{})
    db_cfg = cfg_rest.get('database',{})

    # add indexes
    db = pymongo.MongoClient(**db_cfg).auth
    if 'name_index' not in db.roles.index_information():
        db.roles.create_index('name', name='name_index', unique=True)
    if 'name_index' not in db.groups.index_information():
        db.groups.create_index('name', name='name_index', unique=True)
    if 'user_id_index' not in db.users.index_information():
        db.users.create_index('user_id', name='user_id_index', unique=True)
    if 'username_index' not in db.users.index_information():
        db.users.create_index('username', name='username_index', unique=True)
    if 'dataset_id_index' not in db.auths_dataset.index_information():
        db.auths_dataset.create_index('dataset_id', name='dataset_id_index', unique=True)

    handler_cfg = RESTHandlerSetup(config, *args, **kwargs)
    handler_cfg.update({
        'database': motor.motor_tornado.MotorClient(**db_cfg).auth,
    })
    ldap_cfg = dict(handler_cfg)
    ldap_cfg.update({
        'ldap_uri': cfg_rest.get('ldap_uri', ''),
        'ldap_base': cfg_rest.get('ldap_base', ''),
    })

    return [
        (r'/roles', MultiRoleHandler, handler_cfg),
        (r'/roles/(?P<role_name>\w+)', RoleHandler, handler_cfg),
        (r'/groups', MultiGroupHandler, handler_cfg),
        (r'/groups/(?P<group_name>.*)', GroupHandler, handler_cfg),
        (r'/users', MultiUserHandler, handler_cfg),
        (r'/users/(?P<user_id>\w+)', UserHandler, handler_cfg),
        (r'/users/(?P<user_id>\w+)/roles', UserRolesHandler, handler_cfg),
        (r'/users/(?P<user_id>\w+)/groups', UserGroupsHandler, handler_cfg),
        (r'/ldap', LDAPHandler, ldap_cfg),
        (r'/create_token', CreateTokenHandler, handler_cfg),
        (r'/auths/(?P<dataset_id>\w+)', AuthDatasetHandler, handler_cfg),
        (r'/auths/(?P<dataset_id>\w+)/actions/(?P<action>\w+)', AuthDatasetActionHandler, handler_cfg),
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

    @authorization(roles=['admin','client'])
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
            self.send_error(404, reason="Role not found")
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
    @authorization(roles=['admin','client'])
    async def get(self):
        """
        Get a list of groups.

        Returns:
            dict: {'results': list of groups}
        """
        ret = await self.db.groups.find(projection={'_id':False}).to_list(length=10000)
        self.write({'results':ret})
        self.finish()

class GroupHandler(AuthHandler):
    """
    Handle individual group requests.
    """
    @authorization(roles=['admin'])
    async def put(self, group_name):
        """
        Add/modify a group.

        Body should contain all necessary fields for a group.

        Args:
            group_name (str): the group to add/modify

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'name' not in data:
            data['name'] = group_name
        elif data['name'] != group_name:
            raise tornado.web.HTTPError(400, 'group name mismatch')
        ret = await self.db.groups.find_one_and_replace({'name':group_name},
                data, upsert=True)
        logger.info('%r', ret)
        self.write({})
        self.finish()

    @authorization(roles=['admin','client'])
    async def get(self, group_name):
        """
        Get a group.

        Args:
            group_name (str): the group to get

        Returns:
            dict: group info
        """
        ret = await self.db.groups.find_one({'name':group_name},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="Group not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin'])
    async def delete(self, group_name):
        """
        Delete a group.

        Args:
            group_name (str): the group to delete

        Returns:
            dict: empty dict
        """
        await self.db.groups.delete_one({'name':group_name})
        self.write({})
        self.finish()

class MultiUserHandler(AuthHandler):
    """
    Handle multi-user requests.
    """
    @authorization(roles=['admin','client'])
    async def get(self):
        """
        Get a list of users.

        Body args:
            username: username to filter by

        Returns:
            dict: {'results': list of users}
        """
        filters = {}
        try:
            data = json.loads(self.request.body)
            if 'username' in data:
                filters['username'] = data['username']
        except Exception:
            pass

        ret = await self.db.users.find(filters, projection={'_id':False}).to_list(length=1000)
        self.write({'results':ret})
        self.finish()

    @authorization(roles=['admin'])
    async def post(self):
        """
        Add a user.

        Body should contain all necessary fields for a user.
        """
        data = json.loads(self.request.body)
        if 'username' not in data:
            raise tornado.web.HTTPError(400, reason='missing username')
        ret = await self.db.users.find_one({'username': data['username']})
        if ret:
            raise tornado.web.HTTPError(400, reason='duplicate username')

        user_id = uuid.uuid1().hex
        data['user_id'] = user_id
        if 'groups' not in data:
            data['groups'] = []
        if 'roles' not in data:
            data['roles'] = []
        if 'priority' not in data:
            data['priority'] = 0.5
        ret = await self.db.users.insert_one(data)
        self.set_status(201)
        self.set_header('Location', f'/users/{user_id}')
        self.write({'result': f'/users/{user_id}'})
        self.finish()

class UserHandler(AuthHandler):
    """
    Handle individual user requests.
    """
    @authorization(roles=['admin','client'])
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
            self.send_error(404, reason="User not found")
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

class UserRolesHandler(AuthHandler):
    """
    Handle roles for an individual user.
    """
    @authorization(roles=['admin'])
    async def put(self, user_id):
        """
        Set the roles for a user.

        Body should contain {'roles': [ <role_name> ] }

        Args:
            user_id (str): the user

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'roles' not in data:
            raise tornado.web.HTTPError(400, reason='missing roles')
        for role_name in data['roles']:
            ret = await self.db.roles.find_one({'name': role_name},
                    projection=['_id'])
            if not ret:
                raise tornado.web.HTTPError(400, reason='invalid role name')
        ret = await self.db.users.find_one_and_update({'user_id':user_id},
                {'$set': {'roles': data['roles']}})
        self.write({})
        self.finish()

class UserGroupsHandler(AuthHandler):
    """
    Handle groups for an individual user.
    """
    @authorization(roles=['admin','client'])
    async def get(self, user_id):
        """
        Get the groups for a user.

        Args:
            user_name (str): the user

        Returns:
            dict: {results: list of groups}
        """
        ret = await self.db.users.find_one({'user_id':user_id},
                projection={'_id':False})
        if not ret:
            self.send_error(404, reason="User not found")
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
            raise tornado.web.HTTPError(400, reason='missing group')
        ret = await self.db.groups.find_one({'name': data['group']},
                projection=['_id'])
        if not ret:
            raise tornado.web.HTTPError(400, reason='invalid group')
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
            ret = await self.db.groups.find_one({'name': g},
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
        """
        Validate LDAP login, creating a token.

        Body should contain {'username', 'password'}

        Returns:
            dict: {'token': auth token, 'roles':[roles], 'current_role':role}
        """
        try:
            data = json.loads(self.request.body)
            username = data['username']
            password = data['password']
            conn = ldap3.Connection(self.ldap_uri, 'uid={},{}'.format(username, self.ldap_base),
                                    password, auto_bind=True)
        except Exception:
            self.send_error(403, reason="Login failed")
        else:
            # get user info from DB
            ret = await self.db.users.find_one({'username':username})
            if not ret:
                # create generic user
                ret = {
                    'user_id': uuid.uuid1().hex,
                    'username': username,
                    'groups': ['users'],
                    'roles': ['user'],
                }
                await self.db.users.insert_one(ret)

            # create token
            token_data = {
                'username': username,
                'role': 'user' if 'user' in ret['roles'] else 'anonymous',
                'groups': ret['groups'],
            }
            tok = self.auth.create_token(username, type='user', payload=token_data)
            if isinstance(tok, bytes):
                tok = tok.decode('utf-8')
            self.write({
                'token': tok,
                'username': username,
                'roles': ret['roles'],
                'current_role': token_data['role'],
                'groups': token_data['groups'],
            })
            self.finish()

class CreateTokenHandler(AuthHandler):
    """
    Handle new token creation from an existing token.
    """
    @authorization(roles=['user','admin','client'])
    async def post(self):
        """
        Create a new token based on the existing token.

        Refresh is not allowed.  This is a sub-token with no
        extra expiration time.

        Body (json) args:
            type (str): the token type
            role (str): the role for the new token
            exp (int): expiration time (in seconds, from now)

        Returns:
            dict: {'result': token}
        """
        if 'username' not in self.auth_data:
            raise tornado.web.HTTPError(400, reason='invalid username')
        if 'role' not in self.auth_data:
            raise tornado.web.HTTPError(400, reason='invalid role')
        if 'type' not in self.auth_data or self.auth_data['type'] not in ('user','system'):
            raise tornado.web.HTTPError(400, reason='invalid token type')

        # get user info from DB
        ret = await self.db.users.find_one({'username':self.auth_data['username']})
        if not ret:
            raise tornado.web.HTTPError(400, reason='invalid username')

        # get args
        args = json.loads(self.request.body)

        # calculate expiration
        max_exp = self.auth_data['exp']-time.time()
        try:
            exp = int(args.get('exp', max_exp))
        except Exception:
            raise tornado.web.HTTPError(400, reason='invalid expiration')
        else:
            if exp < 0 or exp > max_exp:
                raise tornado.web.HTTPError(400, reason='invalid expiration')

        # create token
        data = {'username': self.auth_data['username']}
        tok_type = args.get('type', 'temp')
        logger.debug('token type: %r', tok_type)
        if tok_type == 'temp':
            # temp token for current role
            if self.auth_data['role'] not in ('admin','user'):
                raise tornado.web.HTTPError(400, reason='invalid role')
            if self.auth_data['role'] not in ret['roles']:
                raise tornado.web.HTTPError(400, reason='invalid role request')
            data.update({
                'role': self.auth_data['role'],
                'groups': ret['groups'] if 'groups' in ret else [],
            })
        elif tok_type == 'user':
            # switching roles
            if 'admin' not in ret['roles']:
                raise tornado.web.HTTPError(400, reason='invalid role')
            role = args.get('role', None)
            if (not role) or role not in ('admin','user'):
                raise tornado.web.HTTPError(400, reason='invalid role request')
            data.update({
                'role': role,
                'groups': ret['groups'] if 'groups' in ret else [],
            })
        elif tok_type == 'system':
            # iceprod internal tokens
            if self.auth_data['role'] not in ('admin','client'):
                raise tornado.web.HTTPError(400, reason='invalid role')
            role = args.get('role', None)
            if (not role) or not ((self.auth_data['role'] == 'admin' and role in ('system','client'))
                                  or (self.auth_data['role'] == 'client' and role == 'pilot')):
                raise tornado.web.HTTPError(400, reason='invalid role request')
            data.update({
                'role': role,
                'groups': [],
            })
        else:
            raise tornado.web.HTTPError(400, reason='invalid token type request')

        logger.debug('making new token with payload: %r', data)
        tok = self.auth.create_token(data['username'], expiration=exp,
                                     type=tok_type, payload=data)
        if isinstance(tok, bytes):
            tok = tok.decode('utf-8')
        self.write({'result': tok})

class AuthDatasetHandler(AuthHandler):
    """
    Handle dataset authorization rules.
    """
    @authorization(roles=['admin'])
    async def get(self, dataset_id):
        """
        Get the authorization rules for a dataset.

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: {'read_groups': [ <group_id> ], 'write_groups': [ <group_id> ] }
        """
        ret = await self.db.auths_dataset.find_one({'dataset_id':dataset_id},
                projection={'_id':False,'dataset_id':False})
        if not ret:
            self.send_error(404, reason="Dataset not found")
        else:
            self.write(ret)
            self.finish()

    @authorization(roles=['admin','system']) # let other components (/datasets) set this
    async def put(self, dataset_id):
        """
        Set the authorization rules for a dataset.

        Body should contain `{'read_groups': [ <group_name> ], 'write_groups': [ <group_name> ] }`

        Args:
            dataset_id (str): the dataset

        Returns:
            dict: empty dict
        """
        data = json.loads(self.request.body)
        if 'read_groups' not in data:
            raise tornado.web.HTTPError(400, reason='missing read_groups')
        if 'write_groups' not in data:
            raise tornado.web.HTTPError(400, reason='missing write_groups')
        for group in set(data['read_groups'])|set(data['write_groups']):
            ret = await self.db.groups.find_one({'name': group},
                    projection=['_id'])
            if not ret:
                raise tornado.web.HTTPError(400, reason='invalid group name')
        ret = await self.db.auths_dataset.find_one_and_update({'dataset_id':dataset_id},
                {'$set': {'read_groups': data['read_groups'], 'write_groups': data['write_groups']}},
                upsert=True)
        self.write({})
        self.finish()

class AuthDatasetActionHandler(AuthHandler):
    """
    Handle dataset authorization rules.
    """
    @authenticated
    @catch_error
    async def get(self, dataset_id, action):
        """
        Check the auth token against the authorization rules for a dataset.

        Returns a 403 error on authorization failure.

        Args:
            dataset_id (str): the dataset
            action (str): 'read' or 'write'

        Returns:
            dict: empty dict
        """
        if action not in ('read','write'):
            raise tornado.web.HTTPError(400, reason='invalid action')
        if 'groups' not in self.auth_data:
            raise tornado.web.HTTPError(400, reason='invalid auth token')
        ret = await self.db.auths_dataset.find_one({'dataset_id':dataset_id},
                projection={'_id':False})
        if not ret:
            self.send_error(403, reason="Dataset not found")
        elif action+'_groups' not in ret:
            self.send_error(403, reason="Action not found")
        elif not set(ret[action+'_groups'])&set(self.auth_data['groups']):
            self.send_error(403, reason="Denied")
        else:
            self.write({})
            self.finish()
