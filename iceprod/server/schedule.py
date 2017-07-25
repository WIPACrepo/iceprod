"""
Scheduler
"""

from __future__ import absolute_import, division, print_function

from functools import partial
import heapq
import time
from datetime import datetime,timedelta
import calendar
import logging
import inspect

import concurrent.futures

import tornado.concurrent
import tornado.gen
import tornado.ioloop

logger = logging.getLogger('schedule')

class Scheduler(object):
    """
    Schedules future tasks based on time.
    Uses a threadpool to execute tasks.
    """

    MAXWAIT = 60 # in seconds

    # task format: (next_run_time, id, task_function, recurring_cron_pattern)
    #    next_run_time is first so it sorts by soonest first

    def __init__(self, io_loop=None):
        self.sched = [] # heap of things to run on
        self.running = set() # the ids of currently running jobs, so we don't get duplicates
        self.idcount = 0

        if io_loop:
            self.io_loop = io_loop
        else:
            self.io_loop = tornado.ioloop.IOLoop.current()

    @tornado.gen.coroutine
    def _wrapper(self,id,task):
        """run task, then remove from running when done"""
        self.running.add(id)
        try:
            f = task()
            if isinstance(f, (tornado.concurrent.Future, concurrent.futures.Future)):
                yield f
        except Exception:
            logging.warning('Error in scheduled task',exc_info=True)
        finally:
            self.running.discard(id)
    
    @tornado.gen.coroutine
    def run(self):
        """run the scheduler"""
        # check if it's time to run the next event
        tasktime = time.time()+Scheduler.MAXWAIT
        while self.sched:
            (tasktime,id,task,recurring) = self.sched[0] # peek at heap
            if time.time() > tasktime:
                if id not in self.running:
                    # start task
                    yield self._wrapper(id,task)
                if recurring is not None:
                    # check for when to next run task
                    nextrun = Scheduler.parsecron(recurring,tasktime)
                    if nextrun is not None:
                        # remove old task from heap and add new task
                        heapq.heapreplace(self.sched,(nextrun,id,task,recurring))
                else:
                    # remove task from heap
                    heapq.heappop(self.sched)

                # look at next job
                continue
            else:
                # there's still time left
                break

        # get time to next task
        ntime = tasktime - time.time()
        waittime = Scheduler.MAXWAIT
        if waittime > ntime:
            waittime = ntime
        self.io_loop.call_later(waittime, self.run)
    
    def start(self):
        self.io_loop.add_callback(self.run)

    def schedule(self, cron, task, oneshot=False):
        """Add event to schedule"""
        id = self.idcount
        self.idcount += 1
        nextrun = Scheduler.parsecron(cron,time.time(),True)
        if oneshot is True:
            cron = None # only run this once
        heapq.heappush(self.sched,(nextrun,id,task,cron))
    
    @staticmethod
    def parsecron(cron,prevtime,new=False):
        """
        Find next time based on cron string and previous time.
        
        The cron string format is based on the `Google schedule format 
        <https://developers.google.com/appengine/docs/python/config/cron?csw=1#Python_app_yaml_The_schedule_format>`_::
        
            ("every"|ordinal) [N] (hours|mins|minutes|days) ["of" (monthspec)] (time|["from" (time) "to" (time)]|"synchronized")
        
        :param cron: String with scheduling info in an English-like format.
        :param prevtime: last runtime or now.
        :param new: if this is the first run, set to True.
        """
        # some definitions
        daysofweek = ('monday','tuesday','wednesday','thursday','friday','saturday','sunday')        
        daysabbr = ('mon','tue','wed','thu','fri','sat','sun')      
        monthsofyear = ('january','february','march','april','may','june','july','august','september','october','november','december')             
        monthsabbr = ('jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec')
        
        # convert prev to datetime
        prev = datetime.utcfromtimestamp(prevtime)
        try:
            words = cron.split(' ')
            
            # get cron parts
            ordinal,n,days,monthspec,timespec = (['every'],1,['day'],['month'],'synchronized')
            try:
                if words[0] not in ('of','from','at') and ':' not in words[0]:
                    ordinals = words.pop(0).split(',')
                    # strip endings and convert to numbers
                    if 'every' not in ordinals:
                        ordinal = []
                        for o in ordinals:
                            if o[0].isdigit():
                                if o[-2:] in ('st','nd','rd','th'):
                                    ordinal.append(int(o[:-2]))
                                else:
                                    ordinal.append(int(o))
                            else:
                                o2 = 0
                                if o.endswith('first') or 'eleven' == o:
                                    o2 += 1
                                elif o.endswith('second') or 'twelve' == o:
                                    o2 += 2
                                elif o.endswith('third') or 'thirteen' == o:
                                    o2 += 3
                                elif o.endswith('fourth') or 'fourteen' == o:
                                    o2 += 4
                                elif o.endswith('fifth') or 'fifteen' == o:
                                    o2 += 5
                                elif o.endswith('sixth') or 'sixteen' == o:
                                    o2 += 6
                                elif o.endswith('seventh') or 'seventeen' == o:
                                    o2 += 7
                                elif o.endswith('eighth') or 'eighteen' == o:
                                    o2 += 8
                                elif o.endswith('ninth') or 'nineteen' == o:
                                    o2 += 9
                                if o.endswith('teen') or o in ('ten','eleven','twelve'):
                                    o2 += 10
                                elif 'twenty' in o:
                                    o2 += 20
                                elif 'thirty' in o:
                                    o2 += 30
                                ordinal.append(o2)
                try: # try getting N
                    n = int(words[0])
                except Exception:
                    pass
                else: # we already got n, so just pop it and throw it away
                    words.pop(0)
                # try getting hr,min,day 
                if words[0] not in ('of','from','at') and ':' not in words[0]:
                    days = words.pop(0).split(',')                
                if words[0] == 'of':
                    words.pop(0)
                    monthspec = []
                    monthspecs = words.pop(0).split(',')
                    for m in monthspecs:
                        if m.isdigit() or m in ('month','months'):
                            monthspec.append(m)
                        elif m in monthsofyear:
                            monthspec.append(monthsofyear.index(m)+1)
                        elif m in monthsabbr:
                            monthspec.append(monthsabbr.index(m)+1)
                        else:
                            raise Exception('unknown month')
                if words[0] == 'from':
                    words.pop(0)
                    start = words.pop(0)
                    words.pop(0)
                    end = words.pop(0)
                    timespec = (start,end)
                else:
                    if words[0] == 'at':
                        words.pop(0) # remove at
                    timespec = words.pop(0)
                    if words[0].lower() == 'pm':
                        h = int(timespec.split(':')[0])
                        if h < 12:
                            timespec = str(h+12)+':'+timespec.split(':')[1]
            except IndexError:
                pass # indexerror likely indicates the string was shorter than the full version, so ignore
            
            # do some basic validation
            for o in ordinal:
                if o == 'every':
                    continue
                if days[0] in ('mins','minutes','min','minute') and (o < 0 or o >= 60):
                    raise Exception('ordinal %d is not valid for %s'%(o,days[0]))
                elif days[0] in ('hrs','hours','hr','hour') and (o < 0 or o >= 24):
                    raise Exception('ordinal %d is not valid for %s'%(o,days[0]))
                elif days[0] in ('day','days') and (o < 0 or o >= 32):
                    raise Exception('ordinal %d is not valid for %s'%(o,days[0]))
                elif days[0] in ('week','weeks') and (o < 0 or o >= 6):
                    raise Exception('ordinal %d is not valid for %s'%(o,days[0]))
                elif days[0] in ('month','months') and (o < 0 or o >= 13):
                    raise Exception('ordinal %d is not valid for %s'%(o,days[0]))            
            
            # start at prevdate and advance until we match the cron
            if new is True:
                now = start = prev + timedelta(seconds=59-prev.second,
                                   microseconds=1000000-prev.microsecond)
            else:
                now = start = prev - timedelta(seconds=prev.second,
                                   microseconds=prev.microsecond)
            flag = False
            class FlagException(Exception):
                pass
            iterations = 0
            while iterations < 1000000000: # do max of 1B loops
                iterations += 1

                incmin = 1
                inchour = 0
                incday = 0
                # check for match
                try:
                    if 'month' not in monthspec:
                        # month is specified, check if we're in the right one
                        if now.month not in monthspec:
                            # not in right month
                            if (now+timedelta(days=28)).month not in monthspec:
                                incday = 28
                            else:
                                incday = 28-now.day
                                if incday < 1:
                                    incday = 1
                            raise FlagException()
                    
                    if 'every' not in ordinal:
                        # ordinal is specified as list of numbers
                        # N should not be present
                        # monthspec assumed to be fulfilled
                        # days assumed to be singular
                        d = days[0]
                        week = (now.day - 1)//7+1
                        match = False
                        if d in ('mins','minutes','min','minute'):
                            if now.minute in ordinal and (start.minute != now.minute or start.month != now.month or start.year != now.year or new):
                                match = True
                            else:
                                incmin = 1
                        elif d in ('hrs','hours','hr','hour'):
                            if now.hour in ordinal and (start.hour != now.hour or start.month != now.month or start.year != now.year or new):
                                match = True
                            else:
                                inchour = 1
                        elif d in ('day','days'):
                            if now.day in ordinal and (start.day != now.day or start.month != now.month or start.year != now.year or new):
                                match = True
                            else:
                                incday = 1
                        elif d in daysofweek or d in daysabbr:
                            # days could be plural
                            for d in days:
                                # match day and ordinal
                                if d in (daysofweek[now.weekday()],daysabbr[now.weekday()]) and week in ordinal and (start != now or new):
                                    match = True
                                    break
                            if match is False:
                                incday = 1
                        elif d in ('weeks','week'):
                            if week in ordinal and (start.day != now.day or start.month != now.month or start.year != now.year or new):
                                match = True
                            else:
                                incday = 1
                        elif d in ('month','months'):
                            if now.month in ordinal and (start.month != now.month or start.year != now.year or new):
                                match = True
                            else:
                                incday = 1
                        if match is False:
                            raise FlagException()
                    else:
                        # find interval
                        match = None
                        for d in days:
                            if d in ('mins','minutes','min','minute'):
                                incmin = n
                            elif d in ('hrs','hours','hr','hour'):
                                inchour = n
                            elif d in ('day','days'):
                                incday = n
                            elif (d[-1] == 's' and (d[:-1] in daysofweek or d[:-1] in daysabbr)) or (d in daysofweek or d in daysabbr):
                                incday = n
                                # match day
                                if ((d[-1] == 's' and d[:-1] in (daysofweek[now.weekday()],daysabbr[now.weekday()])) or d in (daysofweek[now.weekday()],daysabbr[now.weekday()])) and (start != now or new):
                                    match = True
                                    break
                                else:
                                    match = False
                            elif d in ('weeks','week'):
                                incday = n * 7
                            elif d in ('month','months'):
                                incday = n
                                if start.month == now.month and start.year == now.year and not new:
                                    match = False
                            else:
                                raise Exception('Bad days specifier')
                        if match is None and (start != now or new or
                                (incday > 0 and (start + timedelta(minutes=60-start.minute,hours=23-start.hour,days=incday-1) == now-timedelta(minutes=now.minute,hours=now.hour))) or
                                (inchour > 0 and (start + timedelta(minutes=60-start.minute,hours=inchour-1) == now-timedelta(minutes=now.minute))) or
                                (incmin > 0 and (start + timedelta(minutes=incmin) == now))):
                            match = True
                        if match is not True:
                            raise FlagException()
                        else:
                            # reset increments for next checks
                            incmin = 1
                            inchour = 0
                            incday = 0
                    
                    if isinstance(timespec,tuple):
                        # check that we're in a proper time of day
                        tmp = timespec[0].split(':')
                        h1,m1 = int(tmp[0]),int(tmp[1])
                        tmp = timespec[1].split(':')
                        h2,m2 = int(tmp[0]),int(tmp[1])
                        if h1 > h2 or (h1 == h2  and m1 > m2):
                            # strange math occurs here, since our interval includes midnight
                            if ((now.hour < h1 and now.hour > h2) or 
                                (now.hour == h1 and now.minute < m1) or 
                                (now.hour == h2 and now.minute > m2)):
                                # missed the interval
                                if (now+timedelta(hours=1)).hour < h1:
                                    inchour = 1
                                else:
                                    inchour = 0
                                    incmin = 1
                                raise FlagException()
                        else:
                            # normal interval
                            if ((now.hour < h1 or now.hour > h2) or 
                                (now.hour == h1 and now.minute < m1) or 
                                (now.hour == h2 and now.minute > m2)):
                                # missed the interval
                                if (now+timedelta(hours=1)).hour < h1:
                                    inchour = 1
                                else:
                                    inchour = 0
                                    incmin = 1
                                raise FlagException()
                    elif 'synchronized' != timespec:
                        # check that we are at the correct time
                        h = int(timespec.split(':')[0])
                        m = int(timespec.split(':')[1])
                        if now.hour != h or now.minute != m:
                            # not at right time, figure out advance
                            h = int(timespec.split(':')[0])
                            if (now+timedelta(hours=1)).hour < h:
                                inchour = 1
                            else:
                                inchour = 0
                                incmin = 1     
                            raise FlagException()
                    
                except FlagException,e:
                    # break directly to incrementing phase
                    pass
                else:
                    # actually found a match
                    flag = True
                    break
                
                # increment now
                if incday > 0:
                    # go to beginning of next day
                    now = now + timedelta(minutes=60-now.minute,hours=23-now.hour,days=incday-1)
                elif inchour > 0:
                    # go to beginning of next hour
                    now = now + timedelta(minutes=60-now.minute,hours=inchour-1)
                else:
                    # go to next minute
                    now = now + timedelta(minutes=incmin)
            if flag is False:
                # no match, return None
                return None
        except Exception:
            # invalid cron
            logger.warn('Invalid cron',exc_info=True)
            return None
        else:
            # return time
            return calendar.timegm(now.timetuple())
