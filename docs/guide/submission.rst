Dataset Submission
==================

Submission Types
----------------

.. ~ There are three types of submission: basic, advanced, and expert.

.. ~ Basic View
.. ~ ^^^^^^^^^^

.. ~ The basic view is designed for the case of "I want to run this script."

.. ~ It requires a URL (http or gridftp) to the script. All other boxes are
.. ~ optional, though filling in the description is highly recommended.

.. ~ Then enter the number of jobs and hit submit.

.. ~ Advanced View
.. ~ ^^^^^^^^^^^^^

.. ~ The advanced view is for running multiple tasks and more complex scenarios.
.. ~ It is currently a direct translation from the json config to html structure.

.. ~ This is under revision to make it easier to use.  Comments welcome.

.. ~ Expert View
.. ~ ^^^^^^^^^^^

The only submission type right now is the raw json configuration. Copying/pasting
the configuration is very easy here.

Config Editor
^^^^^^^^^^^^^

The submit and config pages use a code editor (Monaco, the editor from VS Code) with:

* line numbers, bracket matching, and folding
* search (`Ctrl+F`) and go to line (`Ctrl+G`)
* live checks while typing:

  * json syntax errors are underlined in red
  * problems found by the dataset config schema (unknown keys, wrong types,
    missing required fields) are underlined in yellow
  * hover over an underline to see the message

The in-editor checks are advisory: they never block a submission, and the
server's validation is the final check. If the server rejects a config because
of a json syntax error, the error names the line and column, and the editor
jumps to it.

If the editor can't load (for example, JavaScript is disabled), the page falls
back to a plain text box.

.. note:: In Firefox, the browser console may show "Content-Security-Policy"
   errors from `NetUtil.sys.mjs` for Monaco's `*.worker-*.js` files while the
   developer tools are open. These come from the developer tools, not the page,
   and can be ignored.

Config Versions
^^^^^^^^^^^^^^^

Each config has a `version` field that selects the schema it is validated against.

* New submissions are always validated and stored as the latest version,
  whatever `version` says. The editor on the submit page checks against the
  latest schema and warns if `version` is different.
* A config without a `version` is treated as an old config from before
  versioning, and uses version 3.1. This fallback is only for old configs.
  When editing an existing dataset, the editor checks against the config's own
  `version`.

The schemas are available at `/schemas/v3/dataset_vX.Y.schema.json` on the website.
The unversioned `/schemas/v3/dataset.schema.json` is the 3.1 fallback for old configs.

Macros
------

IceProd has several built-in macros relating to what task is actually running.

`$(dataset_id)`
    The dataset_id string.

`$(dataset)`
    The dataset_id in numerical form.

    .. note:: Numbering starts at 20000 for IceProd 2 datasets.
       :class: icecube

`$(jobs_submitted)`
    The number of jobs in the dataset.

`$(job)`
    The job index within the dataset. Within the range `[0, jobs_submitted)`.

`$(task_id)`
    The task_id string. Guaranteed to be unique to the running task.

`$(task)`
    The task name.

Env Expansion
-------------

IceProd can expand the following env vars when using an `env_shell` in the module:

* OS_ARCH
* SROOT
* I3_BUILD
* I3_SRC

Use `$environ(XXXX)` in the json config to insert the env variable in the module
src or arguments.

Script Checks
-------------

IceProd converts each task into a shell script, ``task_runner.sh``.

When a dataset is submitted or its config is edited, IceProd generates the
script for each task (with dummy values for job index and ids) and rejects
the config if:

* the script has a shell syntax error, or
* the script contains an IceProd expression that was not expanded.

The most common cause is a typo or a missing parameter, such as
``$steering(gcdfile)`` when the steering parameter is named ``gcd``.
IceProd leaves an expression it cannot expand as-is, and the shell then
fails with ``syntax error near unexpected token `('``. The error message
includes the task, tray, and module, along with the offending line.

Tasks of datasets that already exist are also checked for syntax errors
before being submitted to HTCondor. A task that fails this check is
marked ``failed`` instead of running, and the error is in its log.
