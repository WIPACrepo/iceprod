"""
Interface for configuring modules
"""

from __future__ import absolute_import, division, print_function

import logging

from statsd import StatsClient

logger = logging.getLogger('module')

class FakeStatsClient(object):
    def __init__(self, *args, *kwargs):
        self._prefix = ''
    def __getattr__(self, name):
        def foo(*args, **kwargs):
            pass
        return foo

class module(object):
    """
    This is an abstract class representing a server module.

    :param cfg: An :class:`IceProdConfig`.
    :param io_loop: An :class:`tornado.ioloop.IOLoop`.
    :param executor: A :class:`concurrent.futures.ThreadPoolExecutor`.
    :param modules: A dict of other module's public services.
    """
    def __init__(self, cfg, io_loop, executor, modules):
        self.cfg = cfg
        self.io_loop = io_loop
        self.executor = executor
        self.statsd = FakeStatsClient()
        self.modules = modules
        self.service = {'start': self.start,
                        'stop': self.stop,
                        'kill': self.kill}

    def start(self):
        """
        Set up a module.

        Note that this is not on the io_loop and should not interact
        with other modules.  Add a callback to the io_loop to do so.
        """
        logger.warn('starting module %s', self.__class__.__name__)
        if 'statsd' in self.cfg and self.cfg['statsd']:
            try:
                self.statsd = StatsClient(self.cfg['statsd'],
                                          prefix=self.cfg['site_id']+'.'+self.__class__.__name__)
            except:
                logger.warn('failed to connect to statsd: %r',
                            self.cfg['statsd'], exc_info=True)

    def stop(self):
        logger.warn('stopping module %s', self.__class__.__name__)

    def kill(self):
        logger.warn('killing module %s', self.__class__.__name__)
