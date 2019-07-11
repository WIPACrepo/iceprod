"""
Interface for configuring modules
"""

from __future__ import absolute_import, division, print_function

import logging

from statsd import TCPStatsClient
import requests

try:
    import boto3
    import botocore.client
except ImportError:
    boto3 = None

from rest_tools.client import RestClient

logger = logging.getLogger('module')

class FakeStatsClient(object):
    def __init__(self, *args, **kwargs):
        self._prefix = ''
    def __getattr__(self, name):
        def foo(*args, **kwargs):
            pass
        return foo

class StatsClientIgnoreErrors(object):
    def __init__(self, *args, **kwargs):
        kwargs['timeout'] = 0.1
        self._statsclient = TCPStatsClient(*args, **kwargs)
    def __getattr__(self, name):
        def foo(*args, **kwargs):
            try:
                try:
                    return getattr(self._statsclient, name)(*args, **kwargs)
                except BrokenPipeError:
                    self._statsclient.reconnect()
                    return getattr(self._statsclient, name)(*args, **kwargs)
            except Exception:
                logging.info('StatsClient dropped %s %r %r', name, args, kwargs,
                             exc_info=True)
        return foo
        

class ElasticClient(object):
    def __init__(self, hostname, basename='iceprod'):
        self.session = requests.Session()
        # handle auth
        if '@' in hostname:
            self.session.auth = tuple(hostname.split('@')[0].split('://')[1].split(':'))
            hostname = hostname.split('://',1)[0]+'://'+hostname.split('@',1)[1]
        # try a connection
        r = self.session.get(hostname, timeout=5)
        r.raise_for_status()
        # concat hostname and basename
        self.hostname = hostname+'/'+basename+'_'
    def head(self, name, index_name):
        try:
            r = self.session.head(self.hostname+name+'/item/'+index_name, timeout=5)
            r.raise_for_status()
        except Exception:
            return False
        else:
            return True
    def get(self, name, index_name):
        r = self.session.get(self.hostname+name+'/item/'+index_name, timeout=5)
        r.raise_for_status()
        return r.json()
    def post(self, name, index_name, data):
        r = self.session.post(self.hostname+name+'/item/'+index_name, timeout=5)
        r.raise_for_status()
        return r.json()
    def put(self, name, index_name, data):
        r = None
        try:
            kwargs = {'timeout':5}
            if isinstance(data,dict):
                kwargs['json'] = data
            else:
                kwargs['data'] = data
            r = self.session.put(self.hostname+name+'/item/'+index_name, **kwargs)
            r.raise_for_status()
        except Exception:
            logger.warning('cannot put to elasticsearch: %s%s/%s',
                           self.hostname, name, index_name, exc_info=True)
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
        self.s3 = None
        self.rest_client = None
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
        logger.warning('starting module %s', self.__class__.__name__)
        if 'statsd' in self.cfg and self.cfg['statsd']:
            try:
                addr = self.cfg['statsd']
                port = 8125
                if ':' in addr:
                    addr,port = addr.split(':')
                    port = int(port)
                self.statsd = StatsClientIgnoreErrors(addr, port=port,
                        prefix=self.cfg['site_id']+'.'+self.__class__.__name__)
            except Exception:
                logger.warning('failed to connect to statsd: %r',
                            self.cfg['statsd'], exc_info=True)

        if 'elasticsearch' in self.cfg and self.cfg['elasticsearch']:
            try:
                self.elasticsearch = ElasticClient(self.cfg['elasticsearch'])
            except Exception:
                logger.warning('failed to connect to elasicsearch: %r',
                            self.cfg['elasticsearch'], exc_info=True)

        if (boto3 and 's3' in self.cfg and 'access_key' in self.cfg['s3'] and
            'secret_key' in self.cfg['s3']):
            try:
                self.s3 = boto3.client('s3','us-east-1',
                    aws_access_key_id=self.cfg['s3']['access_key'],
                    aws_secret_access_key=self.cfg['s3']['secret_key'],
                    config=botocore.client.Config(max_pool_connections=101))
            except Exception:
                logger.warning('failed to connect to s3: %r',
                            self.cfg['s3'], exc_info=True)
        if ('rest_api' in self.cfg and 'url' in self.cfg['rest_api']
            and 'auth_key' in self.cfg['rest_api']):
            try:
                self.rest_client = RestClient(self.cfg['rest_api']['url'],
                                              self.cfg['rest_api']['auth_key'])
            except Exception:
                logger.warning('failed to connect to rest api: %r',
                               self.cfg['rest_api'], exc_info=True)

    def stop(self):
        logger.warning('stopping module %s', self.__class__.__name__)

    def kill(self):
        logger.warning('killing module %s', self.__class__.__name__)
