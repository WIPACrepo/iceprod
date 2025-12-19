"""
Some Prometheus utilities.
"""
import asyncio
import time

from prometheus_client import Histogram
from tornado.web import RequestHandler
from wipac_dev_tools.prometheus_tools import GlobalLabels, AsyncPromWrapper


class HistogramBuckets:
    """Prometheus histogram buckets"""

    # DEFAULT = [.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10]

    #: Database bucket centered around 5ms, with outliers up to 10s
    DB = [.001, .0025, .005, .0075, .01, .025, .05, .1, .25, .5, 1, 10]

    #: API bucket centered around 50ms, up to 10s
    API = [.005, .01, .025, .04, .05, .06, .075, .1, .25, .5, 1, 5, 10]

    #: Timer bucket up to 1 second
    SECOND = [.0001, .0005, .001, .0025, .005, .0075, .01, .025, .05, .075, .1, .25, .5, .75, 1]

    #: Timer bucket up to 10 seconds
    TENSECOND = [.001, .0025, .005, .0075, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 10]

    #: Timer bucket up to 1 minute
    MINUTE = [.1, .5, 1, 2.5, 5, 7.5, 10, 15, 20, 25, 30, 45, 60]

    #: Timer bucket up to 10 minutes
    TENMINUTE = [1, 5, 10, 15, 20, 25, 30, 45, 60, 90, 120, 150, 180, 240, 300, 360, 420, 480, 540, 600]

    #: Timer bucket up to 1 hour
    HOUR = [10, 60, 120, 300, 600, 1200, 1800, 2400, 3000, 3600]


class PromRequestMixin(RequestHandler):
    PromHTTPHistogram = Histogram('http_request_duration_seconds', 'HTTP request duration in seconds', labelnames=('method', 'handler', 'status'), buckets=HistogramBuckets.API)

    def prepare(self):
        super().prepare()
        self._prom_start_time = time.monotonic()

    def on_finish(self):
        super().on_finish()
        end_time = time.monotonic()
        self.PromHTTPHistogram.labels(
            method=str(self.request.method).lower(),
            handler=f'{self.__class__.__module__.split(".")[-1]}.{self.__class__.__name__}',
            status=str(self.get_status()),
        ).observe(end_time - self._prom_start_time)


class AsyncMonitor(GlobalLabels):
    SLEEP_TIME = 5

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._task = None

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._monitor())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None

    @AsyncPromWrapper(lambda self: self.prometheus.gauge('asyncio_tasks_running', 'Python asyncio tasks active'))
    async def _monitor(self, prom_gauge):
        while True:
            prom_gauge.set(len(asyncio.all_tasks()))
            await asyncio.sleep(self.SLEEP_TIME)
