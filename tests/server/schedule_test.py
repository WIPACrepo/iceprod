"""
Test script for schedule
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('schedule_test')

import os, sys, time, random
from datetime import datetime,timedelta

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import iceprod.server
from iceprod.server import schedule
from iceprod.core import to_log


class schedule_test(unittest.TestCase):
    def setUp(self):
        super(schedule_test,self).setUp()
        
    def tearDown(self):
        super(schedule_test,self).tearDown()        

    def test_01_ParseCron(self):
        try:
            now = time.time()
            now2 = datetime.utcfromtimestamp(now)
            new = False
            def cron(t):
                next = schedule.Scheduler.parsecron(t,now,new)
                next2 = schedule.Scheduler.parsecron(t,next)
                return (datetime.utcfromtimestamp(next),timedelta(seconds=(next2 - next)))
            
            new = False # pretend like it just ran now
            
            # test dates
            (next,diff) = cron('every 2 days')
            if next.date()-now2.date() < timedelta(days=2):
                raise Exception('n=F - next is not 2 days after now: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=2):
                raise Exception('n=F - diff is not >= 2 days: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('first thursday of november')
            if next.month != 11 or next.weekday() != 3 or next.day > 7:
                raise Exception('n=F - next is not first thursday of november: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=350):
                raise Exception('n=F - diff is not ~1 year apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st,2nd,5th,eighteenth,22nd')
            if next.day not in (1,2,5,18,22):
                raise Exception('n=F - next is not 1st,2nd,5th,eighteenth,22nd: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('2nd monday')
            if next.weekday() != 0 or next.day <= 7 or next.day > 14:
                raise Exception('n=F - next is not 2nd monday: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=28) or diff >= timedelta(days=42):
                raise Exception('n=F - diff is not ~1 month apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st of month')
            if next.day != 1:
                raise Exception('n=F - next is not 1st of month: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=28) or diff > timedelta(days=42):
                raise Exception('n=F - diff is not ~1 month apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st,3rd sat,sun of feb')
            if next.month != 2 or next.weekday() not in (5,6) or (next.day > 7 and next.day < 14) or next.day > 21:
                raise Exception('n=F - next is not 1st,3rd sat,sun of feb: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=1) and diff != timedelta(days=6) and (diff < timedelta(days=300) or diff > timedelta(days=400)):
                raise Exception('n=F - diff is not ~1 year apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            
            # test times
            (next,diff) = cron('every monday 10:10')
            if next.weekday() != 0 or next.hour != 10 or next.minute != 10:
                raise Exception('n=F - next is not every monday 10:10: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=7):
                raise Exception('n=F - diff is not 1 week apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('every mon at 10:59')
            if next.weekday() != 0 or next.hour != 10 or next.minute != 59:
                raise Exception('n=F - next is not every monday at 10:59: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=7):
                raise Exception('n=F - diff is not 1 week apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('every 2 tue from 10:59 to 11:03')
            if next.weekday() != 1 or next.hour not in (10,11) or next.minute not in (59,0,1,2,3):
                raise Exception('n=F - next is not every 2 tue from 10:59 to 11:03: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=13) or diff > timedelta(days=15):
                raise Exception('n=F - diff is not 2 weeks apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('03:00')
            if next.hour != 3 or next.minute != 0:
                raise Exception('n=F - next is not 03:00: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=1):
                raise Exception('n=F - diff is not 1 day apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st of sep,mar 01:00')
            if next.hour != 1 or next.day != 1 or next.month not in (3,9):
                raise Exception('n=F - next is not 1st hour of sep,mar: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=30*5) or diff > timedelta(days=30*7):
                raise Exception('n=F - diff is not 6 months apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            
            
            new = True # the event has not run before, so run asap
            
            # test dates
            (next,diff) = cron('every 2 days')
            if diff < timedelta(days=1):
                raise Exception('n=T - diff is not >= 2 days: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('first thursday of november')
            if next.month != 11 or next.weekday() != 3 or next.day > 7:
                raise Exception('n=T - next is not first thursday of november: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=350):
                raise Exception('n=T - diff is not ~1 year apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st,2nd,5th,eighteenth,22nd')
            if next.day not in (1,2,5,18,22):
                raise Exception('n=T - next is not 1st,2nd,5th,eighteenth,22nd: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('2nd monday')
            if next.weekday() != 0 or next.day <= 7 or next.day > 14:
                raise Exception('n=T - next is not 2nd monday: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=27) or diff > timedelta(days=42):
                raise Exception('n=T - diff is not ~1 month apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st of month')
            if next.day != 1:
                raise Exception('n=T - next is not 1st of month: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=27) or diff > timedelta(days=42):
                raise Exception('n=T - diff is not ~1 month apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st,3rd sat,sun of feb')
            if next.month != 2 or next.weekday() not in (5,6) or (next.day > 7 and next.day < 14) or next.day > 21:
                raise Exception('n=T - next is not 1st,3rd sat,sun of feb: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=1) and diff != timedelta(days=6) and (diff < timedelta(days=300) or diff > timedelta(days=400)):
                raise Exception('n=T - diff is not ~1 year apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            
            # test times
            (next,diff) = cron('every monday 10:10')
            if next.weekday() != 0 or next.hour != 10 or next.minute != 10:
                raise Exception('n=T - next is not every monday 10:10: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=7):
                raise Exception('n=T - diff is not 1 week apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('every mon at 10:59')
            if next.weekday() != 0 or next.hour != 10 or next.minute != 59:
                raise Exception('n=T - next is not every monday at 10:59: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=7):
                raise Exception('n=T - diff is not 1 week apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('every 2 tue from 10:59 to 11:03')
            if next.weekday() != 1 or next.hour not in (10,11) or next.minute not in (59,0,1,2,3):
                raise Exception('n=T - next is not every 2 tue from 10:59 to 11:03: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=13) or diff > timedelta(days=15):
                raise Exception('n=T - diff is not 2 weeks apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('03:00')
            if next.hour != 3 or next.minute != 0:
                raise Exception('n=T - next is not 03:00: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff != timedelta(days=1):
                raise Exception('n=T - diff is not 1 day apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            (next,diff) = cron('1st of sep,mar 01:00')
            if next.hour != 1 or next.day != 1 or next.month not in (3,9):
                raise Exception('n=T - next is not 1st hour of sep,mar: now=%r,next=%r,diff=%r',(now2,next,diff))
            if diff < timedelta(days=30*5) or diff > timedelta(days=30*7):
                raise Exception('n=T - diff is not 6 months apart: now=%r,next=%r,diff=%r',(now2,next,diff))
            
        except Exception, e:
            logger.error('Error running Scheduler.parsecron test - %s',str(e))
            printer('Test schedule.Scheduler.parsecron',False)
            raise
        else:
            printer('Test schedule.Scheduler.parsecron')
    
    def test_10_Scheduler(self):
        try:
            sched = schedule.Scheduler()
            def foo():
                foo.time = time.time()
                foo.cnt += 1
            def bar():
                bar.cnt += 1
                bar.time = time.time()
                time.sleep(150) # make it skip every other instance
                bar.time2 = time.time()
            foo.cnt = 0
            bar.cnt = 0
            sched.schedule('every 1 min',foo)
            sched.start()
            sched.schedule('every 2 min',bar)
            time.sleep(60-datetime.now().second) # start at beginning of minute
            time.sleep(4*60+10) # sleep for 4:10
            sched.finish()
            if foo.cnt != 5:
                raise Exception('foo not run correct number of times: 5 != %d'%foo.cnt)
            if bar.cnt != 2:
                raise Exception('bar not run correct number of times: 2 != %d'%bar.cnt)
        except Exception, e:
            logger.error('Error running Scheduler test - %s',str(e))
            printer('Test schedule.Scheduler',False)
            raise
        else:
            printer('Test schedule.Scheduler')
        
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()    
    alltests = glob_tests(loader.getTestCaseNames(schedule_test))
    suite.addTests(loader.loadTestsFromNames(alltests,schedule_test))
    return suite
