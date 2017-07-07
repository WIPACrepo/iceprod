Roles
=====

Roles are general categorizations for users.  Each user may belong to
more than one role.

When submitting a dataset, one role must be selected.  This enables
a group prefix, allowing the dataset to belong to any sub-group.  It also
can set common options like the gridftp proxy.

Groups
------

Groups are mostly for dataset priority controls.  A role may alter
the priority of any sub-groups under the groups prefix of that role.

Administrators have a groups prefix of `/`, allowing them to control all
groups.

Example Roles
-------------

As an example, user Bob has the roles `user` and `icetop`.

When submitting a dataset and selecting the `user` role, the groups prefix
is set to `/user/Bob` and Bob's gridftp proxy is used.

When submitting a dataset and selecting the `icetop` role, the groups prefix
is set to `/icetop` and the icetop production gridftp proxy is used.
