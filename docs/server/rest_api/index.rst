IceProd REST API
================

.. toctree::
   :hidden:

   Datasets <datasets>
   Config <config>
   Jobs <jobs>
   Tasks <tasks>
   Task Stats <task_stats>
   Logs <logs>
   Grids <grids>
   Pilots <pilots>
   Auth <auth>

:doc:`Datasets <datasets>`
--------------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
GET     /datasets                                       get a dict of datasets
POST    /datasets                                       create new dataset
GET     /datasets/<dataset_id>                          get a dataset
PUT     /datasets/<dataset_id>/status                   update dataset status
PUT     /datasets/<dataset_id>/description              update dataset description
======  ==============================================  ==================================================================

:doc:`Config <config>`
----------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
GET     /datasets/<dataset_id>/config                   get a config
PUT     /datasets/<dataset_id>/config                   create/update a config
======  ==============================================  ==================================================================

:doc:`Jobs <jobs>`
------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
GET     /datasets/<dataset_id>/jobs                     get a list of jobs, filtered by dataset_id
GET     /datasets/<dataset_id>/jobs/<job_id>            get a job
PUT     /datasets/<dataset_id>/jobs/<job_id>/status     set a job status
GET     /datasets/<dataset_id>/job_summaries/status     get a summary of jobs grouped by status
======  ==============================================  ==================================================================

Jobs (internal use only)
^^^^^^^^^^^^^^^^^^^^^^^^

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
POST    /jobs                                           create a job
GET     /jobs/<job_id>                                  get a job
PATCH   /jobs/<job_id>                                  update a job
======  ==============================================  ==================================================================

:doc:`Tasks <tasks>`
--------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
GET     /datasets/<dataset_id>/tasks                    get a list of tasks, filtered by dataset_id, job_id, grid_id
GET     /datasets/<dataset_id>/tasks/<task_id>          get a task
PUT     /datasets/<dataset_id>/tasks/<task_id>/status   set a task status
GET     /datasets/<dataset_id>/task_summaries/status    get a summary of tasks grouped by status
======  ==============================================  ==================================================================

Tasks (internal use only)
^^^^^^^^^^^^^^^^^^^^^^^^^

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
POST    /tasks                                          create a task
GET     /tasks/<task_id>                                get a task
PATCH   /tasks/<task_id>                                update a task
======  ==============================================  ==================================================================


:doc:`Task Stats <task_stats>`
------------------------------

======  ====================================================================  ==================================================================
Method  Path                                                                  Description
======  ====================================================================  ==================================================================
GET     /datasets/<dataset_id>/tasks/<task_id>/task_stats                     get a list of task stats
GET     /datasets/<dataset_id>/tasks/<task_id>/task_stats/<task_stat_id>      get a task stat
======  ====================================================================  ==================================================================

Task Stats (internal use only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
POST    /tasks/<task_id>/task_stats                     create a task stat
======  ==============================================  ==================================================================

:doc:`Logs <logs>`
------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
POST    /datasets/<dataset_id>/logs                     create a log
GET     /datasets/<dataset_id>/logs/<log_id>            get a log
======  ==============================================  ==================================================================

Logs (internal use only)
^^^^^^^^^^^^^^^^^^^^^^^^

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
POST    /logs                                           create a log
GET     /logs/<log_id>                                  get a log
======  ==============================================  ==================================================================

:doc:`Grids <grids>`
--------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
GET     /grids                                          get a list of grids
POST    /grids                                          create a new grid
GET     /grids/<grid_id>                                get a grid
======  ==============================================  ==================================================================

:doc:`Pilots <pilots>`
----------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
GET     /pilots                                         get a list of pilots, filtered by grid_id
POST    /pilots                                         create a pilot
GET     /pilots/<pilot_id>                              get a pilot
DELETE  /pilots/<pilot_id>                              delete a pilot
PUT     /pilots/<pilot_id>/tasks                        set all tasks in the pilot
======  ==============================================  ==================================================================

:doc:`Auth <auth>`
------------------

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
GET     /roles                                          get a list of roles
PUT     /roles/<role_name>                              create a role
GET     /roles/<role_name>                              get a role
DELETE  /roles/<role_name>                              delete a role
GET     /groups                                         get a list of groups
POST    /groups                                         add a group
GET     /groups/<group_id>                              get a group
DELETE  /groups/<group_id>                              delete a group
GET     /users                                          get a list of users
POST    /users                                          add a user
GET     /users/<user_id>                                get a user
DELETE  /users/<user_id>                                delete a user
GET     /users/<user_id>/groups                         get the groups for a user
POST    /users/<user_id>/groups                         add a group to a user
PUT     /users/<user_id>/groups                         set the groups for a user
PUT     /users/<user_id>/roles                          set the roles for a user
POST    /ldap                                           create a token from an LDAP username/password lookup
======  ==============================================  ==================================================================

Internal auth
^^^^^^^^^^^^^

======  ==============================================  ==================================================================
Method  Path                                            Description
======  ==============================================  ==================================================================
PUT     /auths/<dataset_id>                             set authorization rules for dataset
GET     /auths/<dataset_id>                             get authorization rules for dataset
GET     /auths/<dataset_id>/actions/read                does the current token have read access to this dataset?
GET     /auths/<dataset_id>/actions/write               does the current token have write access to this dataset?
======  ==============================================  ==================================================================

