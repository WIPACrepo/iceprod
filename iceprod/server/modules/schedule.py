"""
The schedule module is a basic cron for IceProd, executing tasks
at various intervals or fixed times.
"""
import logging

import iceprod.server
from iceprod.server import module
from iceprod.server.globus import SiteGlobusProxy

logger = logging.getLogger('modules_schedule')


class schedule(module.module):
    """
    Run the schedule module, which handles periodic tasks.

    Tasks live in individual modules in `iceprod.server.scheduled_tasks.*`.
    """
    def start(self):
        super(schedule,self).start()

        try:
            # make sure the gridftp proxy is set up
            proxy_kwargs = {}
            if 'gridftp_cfgfile' in self.cfg['queue']:
                proxy_kwargs['cfgfile'] = self.cfg['queue']['gridftp_cfgfile']
            proxy = SiteGlobusProxy(**proxy_kwargs)
            proxy.update_proxy()
        except Exception:
            logger.warning('Failed to get gridftp proxy')

        # find scheduled tasks, which will run at built-in delays
        task_names = iceprod.server.listmodules('iceprod.server.scheduled_tasks')
        self.tasks = [iceprod.server.run_module(n,self) for n in task_names]
