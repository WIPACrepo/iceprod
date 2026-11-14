REST API Interface
==================

Advanced users may want to query IceProd from automated scripts or
other programs.  This is possible through a REST API.

.. highlight:: bash

See :ref:`IceProd REST API` for available urls.

Authentication
--------------

To view datasets, call actions on datasets, or submit new datasets,
authentication with a token is required.

An authentication token can be obtained using the python package
`wipac-rest-tools`. For example::

    python3 -m venv venv
    . venv/bin/activate
    pip install wipac-rest-tools
    curl -o get_token.py https://raw.githubusercontent.com/WIPACrepo/rest-tools/master/examples/get_device_credentials_token.py
    python get_device_credentials_token.py iceprod-public

The access token will be valid for 1 hour.

This can be added to requests as the `Authorization` header.
As an example, here is a query to get the list of all datasets::

    curl -XGET -H 'Authorization: bearer XXXXXXXX-your-token-here-XXXXX' https://iceprod2-api.icecube.wisc.edu/datasets

.. danger::

   Anyone with the authentication token is basically you. Be careful with the token!


Scripting
^^^^^^^^^

When writing a python script, you can use our rest client to automatically
manage tokens for you. Just install `wipac-rest-tools` as shown above.
There are also sync and async code options:

Sync::

    from rest_tools.client import SavedDeviceGrantAuth
    api = SavedDeviceGrantAuth(
        address='https://api.iceprod.wisc.edu',
        token_url='https://keycloak.icecube.wisc.edu/auth/realms/IceCube',
        filename='.iceprod-auth',
        client_id='iceprod-public'
    )
    # get a list of datasets
    result = api.request_seq('GET', '/datasets', {})
    for dataset, metadata in result.items():
        # do something with the dataset

Async::

    from rest_tools.client import SavedDeviceGrantAuth
    api = SavedDeviceGrantAuth(
        address='https://api.iceprod.wisc.edu',
        token_url='https://keycloak.icecube.wisc.edu/auth/realms/IceCube',
        filename='.iceprod-auth',
        client_id='iceprod-public'
    )
    # get a list of datasets
    result = await api.request('GET', '/datasets', {})
    for dataset, metadata in result.items():
        # do something with the dataset


Filtering tasks and task stats
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Task stats cannot be filtered by task name or job index directly.
Instead, first filter the tasks, then fetch the stats for those task ids.
The bulk task stats endpoint accepts at most 100 task ids per request,
so request them in chunks::

    # select complete "propagate" tasks for the first 1000 jobs
    tasks = api.request_seq('GET', f'/datasets/{dataset_id}/tasks', {
        'status': 'complete',
        'name': 'propagate',
        'job_index_min': 0,
        'job_index_max': 999,
        'keys': 'task_id',
    })
    task_ids = list(tasks)

    # fetch the latest stats, at most 100 task ids per request
    task_stats = {}
    for i in range(0, len(task_ids), 100):
        args = {'last': 'true', 'task_ids': '|'.join(task_ids[i:i+100])}
        for stat in api.request_stream('GET', f'/datasets/{dataset_id}/bulk/task_stats', args):
            task_stats[stat['task_id']] = stat

The ``job_index_min`` and ``job_index_max`` filters are inclusive, and also
work on ``/datasets/{dataset_id}/jobs``.

A complete command-line version is available in
`resources/filtered_task_stats.py <https://github.com/WIPACrepo/iceprod/blob/master/resources/filtered_task_stats.py>`_.
