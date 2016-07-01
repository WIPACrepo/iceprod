"""
Test script for thread pools
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('pool_test')
import os, sys, time
import unittest

from threading import Thread

from iceprod.server import pool


class pool_test(unittest.TestCase):
    def setUp(self):
        super(pool_test,self).setUp()

    def tearDown(self):
        super(pool_test,self).tearDown()

    @unittest_reporter
    def test_01_deque2(self):
        """Test deque2"""
        import Queue
        # create max queues
        q = Queue.Queue(4)
        d = pool.deque2(4)

        # create max queues
        q = Queue.Queue(maxsize=4)
        d = pool.deque2(maxsize=4)

        # create normal queues
        q = Queue.Queue()
        d = pool.deque2()
        def cmp():
            return list(q.queue) == list(d)

        # test put
        q.put(1)
        d.put(1)
        if cmp() is not True:
            raise Exception('put() failed')

        # test full
        qq = q.full()
        dd = d.full()
        if qq != dd:
            raise Exception('full() failed')

        # test qsize
        qq = q.qsize()
        dd = d.qsize()
        if qq != dd:
            raise Exception('qsize failed - Queue has size %s and Deque2 has size %s'%(str(qq),str(dd)))
        if qq < 1:
            raise Exception('nothing on the queue')

        # test maxsize
        qq = q.maxsize
        dd = d.maxsize
        if qq != dd:
            raise Exception('maxsize failed - Queue has size %s and Deque2 has size %s'%(str(qq),str(dd)))

        # test queue
        qq = q.queue
        dd = d.queue
        if qq != dd:
            raise Exception('queue failed')

        # test get
        qq = q.get()
        dd = d.get()
        if cmp() is not True or qq != dd:
            raise Exception('get() failed')

        # test get_nowwait
        q.put(2)
        d.put(2)
        qq = q.get_nowait()
        dd = d.get_nowait()
        if cmp() is not True or qq != dd:
            raise Exception('get_nowait() failed')

        # test unfinished_tasks
        try:
            qq = q.unfinished_tasks
            dd = d.unfinished_tasks()
            if qq is None or dd is None:
                raise Exception()
        except:
            raise Exception('unfinished_tasks failed - Queue.unfinished_tasks = %s and deque2.unfinished_tasks = %s'%(str(qq),str(dd)))

        # test task_done
        qq = q.task_done()
        dd = d.task_done()
        if qq != dd:
            raise Exception('task_done() failed')

        # test join
        if not q.join or not d.join:
            raise Exception('join() is bad')
        t = Thread(target=q.join)
        q.task_done()
        q.put(3)
        t.start()
        time.sleep(0.1)
        q.get_nowait()
        q.task_done()
        time.sleep(0.1)
        # check that join is finished
        if t.is_alive():
            raise Exception('Queue.join() failed')
        t = Thread(target=d.join)
        d.task_done()
        d.put(3)
        t.start()
        time.sleep(0.1)
        d.get_nowait()
        d.task_done()
        time.sleep(0.1)
        # check that join is finished
        if t.is_alive():
            raise Exception('Deque2.join() failed')

        # test empty
        q.empty()
        qq = q.qsize()
        d.empty()
        dd = d.qsize()
        if qq != 0 or qq != dd:
            raise Exception('empty() failed')

    @unittest_reporter
    def test_02_Threadpool(self):
        """Test Threadpool"""
        # create default threadpool
        th = pool.ThreadPool()
        def foo(x):
            foo.cnt += x
        foo.cnt = 0
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('add_task failed to run all tasks')

        foo.cnt = 0
        th.start()
        th.map(foo,xrange(2000))
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(2000)):
            raise Exception('map failed to run all tasks')

        # create threadpool with init
        def init():
            return 'init'
        def bar(x,init):
            if init != 'init':
                bar.fail = True
            bar.cnt += x
        th = pool.ThreadPool(init=init)
        bar.cnt = 0
        bar.fail = False
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init: add_task failed to run all tasks')

        # create threadpool with exit
        def exit():
            exit.success = True
        th = pool.ThreadPool(exit=exit)
        foo.cnt = 0
        exit.success = False
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if exit.success is not True:
            raise Exception('exit() failed')
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('exit: add_task failed to run all tasks')

        # create threadpool with init and exit
        def exit2(init):
            if init != 'init':
                exit2.success = False
            exit2.success = True
        th = pool.ThreadPool(init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with init and exit in start function
        th = pool.ThreadPool()
        th.finish()
        th.start(num_threads=10,init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with return values
        th = pool.ThreadPool()
        def foo2(x):
            foo2.cnt += x
            return x
        foo2.cnt = 0
        ret = th.map(foo2,xrange(100))
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all tasks')
        if len(ret) != 100:
            raise Exception('ret: incorrect number of rets')
        if foo2.cnt != reduce(lambda a,b:a+b,ret):
            raise Exception('ret: ret is not the same as cnt')

        # create threadpool with callbacks
        th = pool.ThreadPool()
        def cb(x):
            cb.cnt += x
        foo2.cnt = 0
        cb.cnt = 0
        ret = th.map(foo2,xrange(100),[{'callback':cb} for _ in xrange(100)])
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('callback: add_task failed to run all tasks')
        if foo2.cnt != cb.cnt:
            raise Exception('callback: ret is not the same as cnt')

        # test disabling output
        th = pool.ThreadPool()
        th.finish()
        th.disable_output_queue()
        th.start()
        def foo2(x):
            foo2.cnt += x
            return x
        foo2.cnt = 0
        ret = th.map(foo2,xrange(100))
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('dis_output: add_task failed to run all tasks')
        if len(ret) != 0:
            raise Exception('dis_output: ret has a length')

        # and re-enabling output
        th.enable_output_queue()
        th.start()
        foo2.cnt = 0
        ret = th.map(foo2,xrange(100))
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all tasks')
        if len(ret) != 100:
            raise Exception('ret: incorrect number of rets')
        if foo2.cnt != reduce(lambda a,b:a+b,ret):
            raise Exception('ret: ret is not the same as cnt')

    @unittest_reporter
    def test_03_ThreadPoolDeque(self):
        """Test threadpool with deque"""
        # create default threadpool
        th = pool.ThreadPoolDeque()
        def foo(x):
            foo.cnt += x
        foo.cnt = 0
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('add_task failed to run all tasks')

        foo.cnt = 0
        th.start()
        th.map(foo,xrange(2000))
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(2000)):
            raise Exception('map failed to run all tasks')

        # create threadpool with init
        def init():
            return 'init'
        def bar(x,init):
            if init != 'init':
                bar.fail = True
            bar.cnt += x
        th = pool.ThreadPoolDeque(init=init)
        bar.cnt = 0
        bar.fail = False
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init: add_task failed to run all tasks')

        # create threadpool with exit
        def exit():
            exit.success = True
        th = pool.ThreadPoolDeque(exit=exit)
        foo.cnt = 0
        exit.success = False
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if exit.success is not True:
            raise Exception('exit() failed')
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('exit: add_task failed to run all tasks')

        # create threadpool with init and exit
        def exit2(init):
            if init != 'init':
                exit2.success = False
            exit2.success = True
        th = pool.ThreadPoolDeque(init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with init and exit in start function
        th = pool.ThreadPoolDeque()
        th.finish()
        th.start(num_threads=10,init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with return values
        th = pool.ThreadPoolDeque()
        def foo2(x):
            foo2.cnt += x
            return x
        foo2.cnt = 0
        ret = th.map(foo2,xrange(100))
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all tasks')
        if len(ret) != 100:
            raise Exception('ret: incorrect number of rets')
        if foo2.cnt != reduce(lambda a,b:a+b,ret):
            raise Exception('ret: ret is not the same as cnt')

        # create threadpool with callbacks
        th = pool.ThreadPoolDeque()
        def cb(x):
            cb.cnt += x
        foo2.cnt = 0
        cb.cnt = 0
        ret = th.map(foo2,xrange(100),[{'callback':cb} for _ in xrange(100)])
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('callback: add_task failed to run all tasks')
        if foo2.cnt != cb.cnt:
            raise Exception('callback: ret is not the same as cnt')

    @unittest_reporter
    def test_04_PriorityThreadPool(self):
        """Test priority threadpool"""
        # create default threadpool
        th = pool.PriorityThreadPool()
        def foo(x):
            foo.cnt += x
        foo.cnt = 0
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('add_task failed to run all tasks')

        foo.cnt = 0
        th.start()
        th.map(foo,xrange(2000))
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(2000)):
            raise Exception('map failed to run all tasks')

        # create threadpool with init
        def init():
            return 'init'
        def bar(x,init):
            if init != 'init':
                bar.fail = True
            bar.cnt += x
        th = pool.PriorityThreadPool(init=init)
        bar.cnt = 0
        bar.fail = False
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init: add_task failed to run all tasks')

        # create threadpool with exit
        def exit():
            exit.success = True
        th = pool.PriorityThreadPool(exit=exit)
        foo.cnt = 0
        exit.success = False
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if exit.success is not True:
            raise Exception('exit() failed')
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('exit: add_task failed to run all tasks')

        # create threadpool with init and exit
        def exit2(init):
            if init != 'init':
                exit2.success = False
            exit2.success = True
        th = pool.PriorityThreadPool(init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with init and exit in start function
        th = pool.PriorityThreadPool()
        th.finish()
        th.start(num_threads=10,init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with return values
        th = pool.PriorityThreadPool()
        def foo2(x):
            foo2.cnt += x
            return x
        foo2.cnt = 0
        ret = th.map(foo2,xrange(100))
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all tasks')
        if len(ret) != 100:
            raise Exception('ret: incorrect number of rets')
        if foo2.cnt != reduce(lambda a,b:a+b,ret):
            raise Exception('ret: ret is not the same as cnt')

        # create threadpool with callbacks
        th = pool.PriorityThreadPool()
        def cb(x):
            cb.cnt += x
        foo2.cnt = 0
        cb.cnt = 0
        ret = th.map(foo2,xrange(100),[{'callback':cb} for _ in xrange(100)])
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('callback: add_task failed to run all tasks')
        if foo2.cnt != cb.cnt:
            raise Exception('callback: ret is not the same as cnt')

    @unittest_reporter
    def test_05_GroupingThreadPool(self):
        """Test grouping threadpool"""
        # create default threadpool
        th = pool.GroupingThreadPool()
        def foo(x,tasks=[]):
            def foo2(x):
                foo.num += 1
                foo.cnt += x
            if x is not None:
                foo2(x)
            if len(tasks) > 0:
                for args,kwargs in tasks:
                    foo2(*args,**kwargs)
        foo.cnt = 0
        foo.num = 0
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('add_task failed to run all tasks')

        foo.cnt = 0
        foo.num = 0
        th.start()
        th.map(foo,xrange(2000))
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(2000)):
            logging.info('cnt = %d',foo.cnt)
            logging.info('num = %d',foo.num)
            raise Exception('map failed to run all tasks')

        # create threadpool with init
        def init():
            return 'init'
        def bar(x,init,tasks=[]):
            if init != 'init':
                bar.fail = True
            def foo2(x):
                bar.cnt += x
            if x is not None:
                foo2(x)
            if len(tasks) > 0:
                for args,kwargs in tasks:
                    foo2(*args,**kwargs)
        th = pool.GroupingThreadPool(init=init)
        bar.cnt = 0
        bar.fail = False
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init: add_task failed to run all tasks')

        # create threadpool with exit
        def exit():
            exit.success = True
        th = pool.GroupingThreadPool(exit=exit)
        foo.cnt = 0
        foo.num = 0
        exit.success = False
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if exit.success is not True:
            raise Exception('exit() failed')
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('exit: add_task failed to run all tasks')

        # create threadpool with init and exit
        def exit2(init):
            if init != 'init':
                exit2.success = False
            exit2.success = True
        th = pool.GroupingThreadPool(init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with init and exit in start function
        th = pool.GroupingThreadPool()
        th.finish()
        th.start(num_threads=10,init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with return values
        th = pool.GroupingThreadPool()
        def foo2(x,tasks=[]):
            def foo2_hp(x):
                foo2.cnt += x
                return x
            ret = []
            if x is not None:
                ret.append(foo2_hp(x))
            if len(tasks) > 0:
                for args,kwargs in tasks:
                    ret.append(foo2_hp(*args,**kwargs))
            if not ret:
                return None
            elif len(ret) == 1:
                return ret[0]
            else:
                return ret
        foo2.cnt = 0
        ret = th.map(foo2,xrange(100))
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all tasks')
        if len(ret) != 100:
            raise Exception('ret: incorrect number of rets')
        if foo2.cnt != reduce(lambda a,b:a+b,ret):
            raise Exception('ret: ret is not the same as cnt')

        # create threadpool with callbacks
        th = pool.GroupingThreadPool()
        def cb(x):
            cb.cnt += x
        foo2.cnt = 0
        cb.cnt = 0
        ret = th.map(foo2,xrange(100),[{'callback':cb} for _ in xrange(100)])
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('callback: add_task failed to run all tasks')
        if foo2.cnt != cb.cnt:
            raise Exception('callback: ret is not the same as cnt')

    @unittest_reporter
    def test_06_SingleGrouping(self):
        """Test single grouping threadpool"""
        # create default threadpool
        th = pool.SingleGrouping()
        def foo(x,tasks=[]):
            def foo2(x):
                foo.cnt += x
            if x is not None:
                foo2(x)
            if len(tasks) > 0:
                for args,kwargs in tasks:
                    foo2(*args,**kwargs)
        foo.cnt = 0
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('add_task failed to run all tasks')

        foo.cnt = 0
        th.start()
        th.map(foo,xrange(2000))
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(2000)):
            raise Exception('map failed to run all tasks')

        # create threadpool with init
        def init():
            return 'init'
        def bar(x,init,tasks=[]):
            if init != 'init':
                bar.fail = True
            def foo2(x):
                bar.cnt += x
            if x is not None:
                foo2(x)
            if len(tasks) > 0:
                for args,kwargs in tasks:
                    foo2(*args,**kwargs)
        th = pool.SingleGrouping(init=init)
        bar.cnt = 0
        bar.fail = False
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init: add_task failed to run all tasks')

        # create threadpool with exit
        def exit():
            exit.success = True
        th = pool.SingleGrouping(exit=exit)
        foo.cnt = 0
        exit.success = False
        for i in xrange(100):
            th.add_task(foo,i)
        th.finish()
        if exit.success is not True:
            raise Exception('exit() failed')
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('exit: add_task failed to run all tasks')

        # create threadpool with init and exit
        def exit2(init):
            if init != 'init':
                exit2.success = False
            exit2.success = True
        th = pool.SingleGrouping(init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with init and exit in start function
        th = pool.SingleGrouping()
        th.finish()
        th.start(num_threads=10,init=init,exit=exit2)
        bar.cnt = 0
        bar.fail = False
        exit2.success = None
        for i in xrange(100):
            th.add_task(bar,i)
        th.finish()
        if bar.fail == True:
            raise Exception('init+exit: init() failed')
        if exit2.success is False:
            raise Exception('init+exit: exit() does not have init')
        elif exit2.success is None:
            raise Exception('init+exit: exit() failed')
        if bar.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('init+exit: add_task failed to run all tasks')

        # create threadpool with return values
        th = pool.SingleGrouping()
        def foo2(x,tasks=[]):
            def foo2_hp(x):
                foo2.cnt += x
                return x
            ret = []
            if x is not None:
                ret.append(foo2_hp(x))
            if len(tasks) > 0:
                for args,kwargs in tasks:
                    ret.append(foo2_hp(*args,**kwargs))
            if not ret:
                return None
            elif len(ret) == 1:
                return ret[0]
            else:
                return ret
        foo2.cnt = 0
        ret = th.map(foo2,xrange(100))
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all tasks')
        if len(ret) != 100:
            raise Exception('ret: incorrect number of rets')
        if foo2.cnt != reduce(lambda a,b:a+b,ret):
            raise Exception('ret: ret is not the same as cnt')

        # create threadpool with callbacks
        th = pool.SingleGrouping()
        def cb(x):
            cb.cnt += x
        foo2.cnt = 0
        cb.cnt = 0
        ret = th.map(foo2,xrange(100),[{'callback':cb} for _ in xrange(100)])
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('callback: add_task failed to run all tasks')
        if foo2.cnt != cb.cnt:
            raise Exception('callback: ret is not the same as cnt')

        # create threadpool with return values and differing functions
        th = pool.SingleGrouping()
        def foo3(x,tasks=[]):
            def foo2_hp(x):
                foo3.cnt += x
                return x
            ret = []
            if x is not None:
                ret.append(foo2_hp(x))
            if len(tasks) > 0:
                for args,kwargs in tasks:
                    ret.append(foo2_hp(*args,**kwargs))
            if not ret:
                return None
            elif len(ret) == 1:
                return ret[0]
            else:
                return ret
        foo2.cnt = 0
        foo3.cnt = 0
        for i in xrange(100):
            th.add_task(foo2,i)
            if i%5 == 0:
                th.add_task(foo3,i)
        th.finish()
        ret = []
        while len(th.output) > 0:
            ret.append(th.output.get_nowait())
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all foo2 tasks')
        if foo3.cnt != reduce(lambda a,b:a+b,xrange(0,100,5)):
            raise Exception('ret: add_task failed to run all foo3 tasks')
        if len(ret) != 120:
            raise Exception('ret: incorrect number of rets')
        if foo2.cnt+foo3.cnt != reduce(lambda a,b:a+b,ret):
            raise Exception('ret: ret is not the same as cnt')

        # create threadpool with callbacks and differing functions
        th = pool.SingleGrouping()
        def cb(x):
            cb.cnt += x
        cb.cnt = 0
        foo2.cnt = 0
        foo3.cnt = 0
        for i in xrange(100):
            th.add_task(foo2,i,callback=cb)
            if i%5 == 0:
                th.add_task(foo3,i,callback=cb)
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('ret: add_task failed to run all foo2 tasks')
        if foo3.cnt != reduce(lambda a,b:a+b,xrange(0,100,5)):
            raise Exception('ret: add_task failed to run all foo3 tasks')
        if foo2.cnt+foo3.cnt != cb.cnt:
            raise Exception('callback: ret is not the same as cnt')

    @unittest_reporter
    def test_07_NamedThreadPool(self):
        """Test NamedThreadPool"""
        # create NamedThreadPool
        th = pool.NamedThreadPool()
        def foo(x):
            foo.cnt += x
        foo.cnt = 0
        for i in xrange(100):
            th.add_task('fooey',foo,i)
        th.finish()
        if foo.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('add_task failed to run all tasks')

        # create NamedThreadPool with callbacks
        th = pool.NamedThreadPool()
        def cb(x):
            cb.cnt += x
        def foo2(x,callback=None):
            foo2.cnt += x
            if callback:
                callback(x)
        foo2.cnt = 0
        cb.cnt = 0
        for i in xrange(100):
            th.add_task('fooey',foo2,i,callback=cb)
        th.finish()
        if foo2.cnt != reduce(lambda a,b:a+b,xrange(100)):
            raise Exception('callback: add_task failed to run all tasks')
        if foo2.cnt != cb.cnt:
            raise Exception('callback: ret is not the same as cnt')

        # try calling two same named tasks at once
        th = pool.NamedThreadPool(num_threads=3)
        def bar():
            bar.t = time.time()
            time.sleep(1)
        bar.t = None
        def bar2():
            bar2.t = time.time()
            time.sleep(1)
        bar2.t = None
        th.add_task('bar',bar)
        th.add_task('bar',bar2)
        th.finish()
        if not bar.t or not bar2.t:
            raise Exception('named: add_task failed to run all tasks')
        if abs(bar2.t - bar.t) < 0.9:
            raise Exception('named tasks occurred at same time')

        # try calling different named tasks at once
        th = pool.NamedThreadPool(num_threads=3)
        bar.t = None
        bar2.t = None
        th.add_task('bar',bar)
        th.add_task('bar2',bar2)
        th.finish()
        if not bar.t or not bar2.t:
            raise Exception('named diff: add_task failed to run all tasks')
        if abs(bar2.t - bar.t) > 0.9:
            raise Exception('diff named tasks were called sequentially')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(pool_test))
    suite.addTests(loader.loadTestsFromNames(alltests,pool_test))
    return suite
