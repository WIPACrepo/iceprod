"""
Interface for configuring modules
"""

from __future__ import absolute_import, division, print_function

import logging

from statsd import StatsClient
import requests

logger = logging.getLogger('module')

class FakeStatsClient(object):
    def __init__(self, *args, **kwargs):
        self._prefix = ''
    def __getattr__(self, name):
        def foo(*args, **kwargs):
            pass
        return foo

class ElasticClient(object):
    def __init__(self, hostname, basename='iceprod'):
        self.session = requests.Session()
        # try a connection
        r = self.session.get(hostname)
        r.raise_for_status()
        # concat hostname and basename
        self.hostname = hostname+'/'+basename+'/'
    def put(self, name, index_name, data):
        r = None
        try:
            kwargs = {}
            if isinstance(data,dict):
                kwargs['json'] = data
            else:
                kwargs['data'] = data
            r = self.session.put(self.hostname+name+'/'+index_name, **kwargs)
            r.raise_for_status()
        except Exception:
            logger.warn('cannot put %s/%s to elasticsearch at %r', name,
                         index_name, self.hostname, exc_info=True)
            if r:
                logger.info('%r',r.content)

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
        self.elasticsearch = FakeStatsClient()
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

        if 'elasticsearch' in self.cfg and self.cfg['elasticsearch']:
            try:
                self.elastic = ElasticClient(self.cfg['elasticsearch'])
            except:
                logger.warn('failed to connet to elasicsearch: %r',
                            self.cfg['elasticsearch'], exc_info=True)

    def stop(self):
        logger.warn('stopping module %s', self.__class__.__name__)

    def kill(self):
        logger.warn('killing module %s', self.__class__.__name__)
