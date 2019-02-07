REST Authorization
==================

After a token has been authenticated, it needs to be authorized.  This happens
at the method level.  There are two main categories of authorization:

Role Based Access Control
    These methods depend on the token having a certain role.  This is most
    often used for admin-only methods, or for internal IceProd usage
    that is not meant for user access.

Attribute Based Access Control
    These methods match a dataset's attributes against the token's
    attributes - specifically the groups of both.  If a token belongs
    to the same group (or a higher level group containing this sub-group)
    as the dataset, it has permission.
    Note that datasets have separate read and write access, so a dataset
    can be marked as public readable and group-only writable.

Both of these access controls can be combined on a single method, allowing
a certain role complete access, as well as tokens that match an attribute.

.. note::

   Public access isn't mentioned directly.  Instead, it is the lack of either
   access control protection that marks a method as public.

Authorization Implementation
----------------------------

A decorator for RPC methods is used.  For example, a public access::

    # public method
    async def rpc_public_foo(self):
        pass

If you only need a logged in user, access is set up as::

    @authorization(roles=['user'])
    async def rpc_logged_in(self):
        pass

Other role based access is similarly easy::

    @authorization(roles=['admin']):
    async def rpc_admin_func(self):
        pass

Internal IceProd access can be enabled with a role.  Note that
the `client` role is for IceProd clients that run scheduled tasks,
while `pilot` is for pilot workers.

::

    @authorization(roles=['client','pilot'])
    async def rpc_site_foo(self):
        pass

For dataset attributes, access is like::

    @authorization(attrs=['dataset_id:read'])
    async def rpc_get_task(self, dataset_id, task_id):
        pass
