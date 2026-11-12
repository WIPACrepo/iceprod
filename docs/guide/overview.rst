User Overview
=============

IceProd provides two ways to see how many tasks and jobs you have, and how
many resources they use, across all of your datasets.

Website overview (current snapshot)
-----------------------------------

The **Overview** page (``/overview``, linked in the top menu) shows live
counts for your *active* datasets (status ``processing``, ``suspended``,
or ``errors``):

* **Totals** - task counts by status and job counts by status, summed over
  all matching datasets.
* **Tasks by Dataset** - one row per dataset with task counts by status,
  linking to the dataset page.

By default the page shows datasets owned by you.  Use the sidebar filters
to select other users, groups, or dataset statuses.  You can only see
datasets you have read access to.

These are point-in-time numbers.  To see usage *over time* (for example,
the average number of running tasks over the last week), use the Grafana
dashboard below.  If configured, the overview page links directly to it,
pre-filtered to the selected users and groups.

Grafana dashboard (usage over time)
-----------------------------------

The *IceProd User Overview* Grafana dashboard plots, filtered by user and
group:

* Processing tasks, now and averaged over the selected time range.
* CPUs and GPUs requested by processing tasks, now and averaged.  This is
  the closest measure of "how many slots/nodes am I using".
* Tasks and jobs by status over time.
* Processing tasks by dataset and by task name.
* Resources requested by queued tasks (waiting for a slot).

To estimate how long a production will take, set the time range to a
representative period and read the *average* panels.

.. note::

   Resource numbers are the *requested* resources (task requirements),
   not measured usage.  Tasks without an explicit requirement count as
   1 CPU, 0 GPUs, and 2 GB memory.

Administrator setup
-------------------

Metrics come from the ``dataset_monitor`` scheduled task
(:py:mod:`iceprod.scheduled_tasks.dataset_monitor`), which exports, for
each ``processing`` dataset:

* ``iceprod_jobs{dataset, username, group, status}``
* ``iceprod_tasks{dataset, taskname, username, group, status}``
* ``iceprod_task_resources{dataset, username, group, status, resource}``
  where ``status`` is ``queued`` or ``processing`` and ``resource`` is
  ``cpu``, ``gpu``, or ``memory`` (GB).

To install the dashboard:

1. Import ``resources/grafana/user_overview.json`` into Grafana and select
   the Prometheus data source that scrapes ``dataset_monitor``.
2. Set the dashboard *Lookback* variable to at least the
   ``dataset_monitor`` CronJob interval.  Because the CronJob is only
   scraped while it runs, queries carry the last value forward for this
   window.
3. Set the website environment variable ``GRAFANA_DASHBOARD_URL`` to the
   dashboard URL (e.g. ``https://grafana.example/d/iceprod-user-overview``)
   to enable links from the overview page.
