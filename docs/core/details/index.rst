.. index:: Technical_Core
.. _Technical_Core:

IceProd Core
============

The fundamental design of the core is to run a task composed of trays and modules.  The general heirarchy looks like::

    task
    |
    |- tray1
       |
       |- module1
       |
       |- module2
    |
    |- tray2
       |
       |- module3
       |
       |- module4

Parameters can be defined at every level, and each level is treated as a scope (such that inner scopes inherit from outer scopes).  This is accomplished via an internal environment for each scope.

Internal Environment
--------------------

The internal envoronment (env) is a dictionary composed of several objects:

* shell environment
* parameters
* resources
* data
* classes
* projects
* deletions
* uploads
    
To keep the scope correct a new dictionary is created for every level, then the inheritable objects are shallow copied (to 1 level) into the new env.  The deletions are not inheritable (start empty for each scope), and the shell environment is set at whatever the previous scope currently has.

Parameters
^^^^^^^^^^

Parameters are defined directly as an object, or as a string pointing to another object.  They can use the IceProd meta-language to be defined in relation to other parameters specified in inherited scopes, or as eval or sprinf functions.

Resources and Data
^^^^^^^^^^^^^^^^^^

Resources and Data are similar in that they handle extra files that modules may create or use.  The difference is that resources are only for use, such as pre-built lookup tables, while data can be input and/or output.  Compression can be automatically handled by IceProd.  Both resources and data are defined in the environment as strings to their file location.

Classes
^^^^^^^

This is where external software gets added.  The software can be an already downloaded resource or just a url to download.  All python files get added to the python path and binary libraries get symlinked into a directory on the LD_LIBRARY_PATH.  Note that if there is more than one copy of the same shared library file, only the most recent one is in scope.  Classes are defined in the environment as strings to their file location.

Projects
^^^^^^^^

These are ties to :ref:`IceProdModules`.  Defining a project causes it to be imported and available directly from the env object::

    env['projects']['test'].testing()
    
Deletions
^^^^^^^^^

These are files that should be deleted when the scope ends.

Uploads
^^^^^^^

These are files that should be uploaded when the scope ends.  Mostly Data objects that are used as output.

Running a Module
----------------

Modules are run in a forked process to prevent segfaults from killing IceProd.  Their stdout and stderr is dumped into the log file with prefixes on each line to designate its source.  Any error or the return value is returned to the main process via a Queue.

If a module defines a src, that is assumed to be a Class which should be added to the env.  The running_class is where the exact script or binary is chosen.  It can match several things:

* A fully defined python module.class import (also takes module.function)
* A python class defined in the src provided
* A class name defined in any of the loaded `Projects`_
* A regular python script
* An executable of some type (this is run in a subprocess with shell execution disabled)

Task Execution
--------------

The main work unit is a task, so the core itself can be thought of as a task executor.  The main executable i3exec.py has a ``runner()`` function which does exactly that.  The general outline is:

1. Load dataset configuration
2. Set log level
3. Set some default options if not set in dataset configuration
4. Set up global env based on the dataset configuration
5. Run tasks
    * If a task option is specified in the dataset configuration, follow that:
        
        If the task is specified by name or number, run only that task.  If there is a problem finding the task specified, raise a critical error.
        
    * Otherwise, run all tasks in the dataset configuration in the order they were written

6. Destroy the global env, uploading and deleting files as needed
7. Upload the log, error, and output files if specified in options

Many Task Mode
--------------

The main executable i3.exec.py has the option to run directly on a dataset configuration file or to query the server for dataset configuration files to run on.  If a dataset configuration file is not given as a argument, it will assume many task mode and query the server.  Whichever mode is used, they both run the same task execution detailed above.

