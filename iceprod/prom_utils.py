"""
Some Prometheus utilities.
"""


class HistogramBuckets:
    """Prometheus histogram buckets"""

    # DEFAULT = [.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10]

    #: Database bucket centered around 5ms, with outliers up to 10s
    DB = [.001, .002, .003, .004, .005, .006, .007, .008, .009, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10]

    #: API bucket centered around 50ms, up to 10s
    API = [.01, .02, .03, .04, .05, .06, .07, .08, .09, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10]

    #: Timer bucket up to 1 second
    SECOND = [.0001, .0005, .001, .0025, .005, .0075, .01, .025, .05, .075, .1, .25, .5, .75, 1]

    #: Timer bucket up to 1 minute
    MINUTE = [.1, .5, 1, 2.5, 5, 7.5, 10, 15, 20, 25, 30, 45, 60]

    #: Timer bucket up to 10 minutes
    TENMINUTE = [1, 5, 10, 15, 20, 25, 30, 45, 60, 90, 120, 150, 180, 240, 300, 360, 420, 480, 540, 600]

    #: Timer bucket up to 1 hour
    HOUR = [10, 60, 120, 300, 600, 1200, 1800, 2400, 3000, 3600]
