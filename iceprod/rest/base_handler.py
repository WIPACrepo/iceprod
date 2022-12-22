import logging

from rest_tools.server import RestHandler
from rest_tools.server import RestHandlerSetup

import iceprod
from iceprod.server.module import FakeStatsClient
from .auth import AttrAuthMixin

logger = logging.getLogger('rest')


def IceProdRestConfig(config, config=None, database=None):
    config['server_header'] = 'IceProd/' + iceprod.__version__
    ret = RestHandlerSetup(config)
    ret['statsd'] = statsd
    ret['database'] = database
    return ret


class APIBase(RestHandler, AttrAuthMixin):
    """Default REST handler"""
    def initialize(self, database=None, statsd=None, **kwargs):
        super().initialize(**kwargs)
        self.db = database
        self.statsd = statsd

    def prepare(self):
        super().prepare()
        if self.statsd:
            self.statsd.incr(f'prepare.{self.__class__.__name__}.{self.request.method}')

    def on_finish(self):
        super().on_finish()
        if self.statsd:
            self.statsd.incr(f'finish.{self.__class__.__name__}.{self.request.method}.{self.get_status()}')

    def get_template_namespace(self):
        namespace = super().get_template_namespace()
        namespace['version'] = iceprod.__version__
        return namespace
