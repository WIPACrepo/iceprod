RPC Authorization
=================

After we've been authenticated, we need to be authorized.  This happens
at the rpc method level.  There are several general categories,
including public, login, group membership, role, username, etc.

Public Access
    Public rpc methods can be accessed by anyone.  These are usually read-only
    and provide basic access.

Login Access
    Methods that can be accessed by anyone with an account.  So everyone but
    the general public.  A very minor security bar.

Group Access
    These rpc methods depend on the logged in user being a member of a group.
    Typically this is because a dataset has been marked private and is
    restricted to the group that submitted it.

Role Access
    The only current role-based access is for administrators.

Username Access
    Typically for modifying your own user account information.

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

    @authorization(auth_user=True)
    def rpc_logged_in(self):
        pass

Specifc user names can also be used::

    @authorization(auth_user=['john.doe'])
    def rpc_logged_in(self):
        pass

Groups can be specified in a similar way::

    @authorization(auth_group=['icetop'])
    def rpc_icetop_dataset(self):
        pass

Role based access is similarly easy::

    @authorization(auth_role=['admin']):
    def rpc_admin_func(self):
        pass

Site access can be enabled with a flag::

    @authorization(site_valid=True)
    def rpc_site_foo(self):
        pass

Dynamic Matching
""""""""""""""""

Non-static mappings are more difficult. A two-step solution is needed:

#. Get logged in user/group/role.
#. Verify that credential against the database.

This can't all be done in the decorator, so we need a helper function::

    def _rpc_set_user_email_helper(self, auth_user, user_id, *args, **kwargs):
        """
        Args:
            auth_user (str): the user_id of the logged in user
            user_id (str): an argument to the original function
        """
        return auth_user == user_id

    @authorization(auth_user=_rpc_set_user_email_helper)
    def rpc_set_user_email(self, user_id, email):
        # set email address

Group checks are generally more difficult, involving a database lookup::

    @tornado.gen.coroutine
    def _rpc_edit_dataset_helper(self, auth_group, dataset_id, *args, **kwargs):
        """
        Args:
            auth_group (list): group_ids of logged in user
            dataset_id (str): dataset to modify
        """
        sql = 'select groups_id from dataset where dataset_id = ?'
        bindings = (dataset_id,)
        ret = yield self.parent.db.query(sql, bindings)
        raise tornado.gen.Return(ret[0][0] in auth_group)

    @authorization(auth_group=_rpc_edit_dataset_helper)
    def rpc_edit_dataset(self, user_id, email):
        # set email address
