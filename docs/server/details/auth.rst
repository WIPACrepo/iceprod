RPC Authorization
=================

After we've been authenticated, we need to be authorized.  This happens
at the rpc method level.  There are several general categories,
including public, login, user, role, etc.

Public Access
    Public rpc methods can be accessed by anyone.  These are usually read-only
    and provide basic access.

Login Access
    Methods that can be accessed by anyone with an account.  So everyone but
    the general public.  A very minor security bar.

User Access
    Match against a user. Typically for modifying your own user account
    information.

Role Access
    These rpc methods depend on the logged in user being a member of a role.
    Typically this is because a dataset has been marked private and is
    restricted to the role that submitted it.

Site Access
    Another site (usually a slave instance) calling the method.
    Sites have special passkeys to allow this access.

Authorization Implementation
----------------------------

A decorator for RPC methods is used.  For example, a public access::

    # public method
    def rpc_public_foo(self):
        pass

If you only need a logged in user, access is set up as::

    @authorization(user=True)
    def rpc_logged_in(self):
        pass

Specifc user ids can also be used::

    @authorization(user=['123'])
    def rpc_logged_in(self):
        pass

Role based access is similarly easy::

    @authorization(role=['admin']):
    def rpc_admin_func(self):
        pass

Site access can be enabled with a flag::

    @authorization(site=True)
    def rpc_site_foo(self):
        pass

Dynamic Matching
""""""""""""""""

Non-static mappings are more difficult. A two-step solution is needed:

#. Get logged in user/role.
#. Verify that credential against the database.

This can't all be done in the decorator, so we need a helper function::

    @tornado.gen.coroutine
    def _rpc_set_user_email_helper(self, auth_user, user_id, *args, **kwargs):
        """
        Args:
            auth_user (str): the user_id of the logged in user
            user_id (str): an argument to the original function
        """
        raise tornado.gen.Return(auth_user == user_id)

    @authorization(auth_user=_rpc_set_user_email_helper)
    def rpc_set_user_email(self, user_id, email):
        # set email address

Role checks are generally more difficult, involving a database lookup::

    @tornado.gen.coroutine
    def _rpc_edit_dataset_helper(self, auth_role, dataset_id, *args, **kwargs):
        """
        Args:
            auth_grole (list): role_ids of logged in user
            dataset_id (str): dataset to modify
        """
        sql = 'select group_id from dataset where dataset_id = ?'
        bindings = (dataset_id,)
        ret = yield self.parent.db.query(sql, bindings)
        sql = 'select name from groups where groups_id = ?'
        bindings = (ret[0][0],)
        ret = yield self.parent.db.query(sql, bindings)
        sql = 'select groups_prefix from role where role_id in ('
        sql += ','.join('?' for _ in auth_role)+')'
        bindings = tuple(auth_role)
        ret2 = yield self.parent.db.query(sql, bindings)
        success = any(row[0] in ret[0][0] for row in ret2)
        raise tornado.gen.Return(success)

    @authorization(auth_group=_rpc_edit_dataset_helper)
    def rpc_edit_dataset(self, dataset_id, config):
        # edit dataset config

Authorization Expression
------------------------

If more than one condition is needed, the authorization expression comes
into play::

    @authorization(user=True, site=True, expression='user or site')
    def rpc_user_or_site(self):
        pass

The expression replaces the keywords `user`, `role`, `site` with the
evaluated authorizations, then evaluates the expression.  Since this is
directly evaludated in python, no user input should ever go into this.

The default expression is any supplied authorization and-ed together.
