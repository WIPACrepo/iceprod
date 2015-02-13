"""
  Simple Thread Pools
  
  copyright (c) 2012 the icecube collaboration
"""

import Queue
from time import sleep
from collections import deque
from threading import Thread,Event,RLock
from functools import partial
import logging

logger = logging.getLogger('pool')

# make a deque class with Queue semantics
class deque2(deque):
    put = deque.append
    put_nowait = deque.append
    get = deque.popleft
    get_nowait = deque.popleft
    qsize = deque.__len__
    unfinished_tasks = deque.__len__
    mutex = None
    empty = deque.clear
    _sleep_wait_time = 0.006 # anything in the range of 0.25 - 0.002 will do
    def full(self):
        return self.__len__() == self.maxlen
    def task_done(self,*args,**kwargs):
        pass
    def join(self):
        while self.__len__() != 0:
            sleep(deque2._sleep_wait_time)
    def __init__(self,*args,**kwargs):
        if 'maxsize' in kwargs:
            maxlen = kwargs.pop('maxsize')
        elif len(args) > 0:
            maxlen = args[0]
            args = args[1:]
        else:
            maxlen = None
        if maxlen:
            super(deque2,self).__init__(*args,maxlen=maxlen,**kwargs)
        else:
            super(deque2,self).__init__(*args,**kwargs)
        self.queue = self
        if self.maxlen is not None:
            self.maxsize = deque.maxlen
        else:
            self.maxsize = 0

class ThreadPool():
    """Pool of threads consuming tasks from a queue"""
    
    class _Worker(Thread):
        """Thread executing tasks from a given tasks queue"""
        def __init__(self, input, output, event=None, init=None, exit=None):
            Thread.__init__(self)
            self.input = input
            self.output = output
            self.event = event
            if init is not None:
                self.init = init() # get initial values to pass to worker task
            else:
                self.init = None
            if exit is not None:
                self.exit = exit # get exit function
            else:
                self.exit = None
            self.daemon = True
            self.start()
        
        def run(self):
            self.event[1].acquire()
            self.event[2] += 1
            self.event[1].release()
            while True:
                # get next task
                if self.event[0].is_set() is False:
                    break # pause worker
                try:
                    task = self.input.get()
                except (EOFError, IOError):
                    break # error in input queue
                except IndexError:
                    sleep(deque2._sleep_wait_time) # for deque, wait for a bit before trying again
                    continue
                if task is None:
                    break # sentinel found, so end worker
                func, args, kwargs = task
                callback = None
                if 'callback' in kwargs:
                    callback = kwargs['callback']
                    del kwargs['callback']
                if self.init is not None and 'init' not in kwargs:
                    kwargs['init'] = self.init
                
                # actually do task
                try:
                    ret = func(*args, **kwargs)
                    if callback is not None:
                        callback(ret)
                    else:
                        self.output.put(ret)
                except Exception as e:
                    logger.info('error in pool task',exc_info=True)
                    if callback is not None:
                        callback(e)
                finally:
                    self.input.task_done()
            if self.exit is not None:
                if self.init is not None:
                    self.exit(init=self.init)
                else:
                    self.exit()
            self.event[1].acquire()
            self.event[2] -= 1
            if self.event[2] <= 0:
                self.event[0].set()
            self.event[1].release()
    
    def __init__(self, num_threads=1, init=None, exit=None):
        self._init_queues()
        self.output_bak = None
        self.num_threads = num_threads
        self.init = init
        self.exit = exit
        self.event = [Event(),RLock(),0]
        self.start()
    
    def pause(self):
        self._help_stuff_finish()
        self.event[0].clear()
        self.event[0].wait()
    
    def start(self,num_threads=None,init=None,exit=None):
        if num_threads is None:
            num_threads = self.num_threads
        else:
            self.num_threads = num_threads
        if init is None:
            init = self.init
        else:
            self.init = init
        if exit is None:
            exit = self.exit
        else:
            self.exit = exit
        self.event[0].set()
        self.threads = [self._Worker(self.input,self.output,self.event,init,exit) for _ in xrange(num_threads)]
    
    def finish(self,blocking=True):
        """Wait for completion of all the tasks in the queue"""
        self._help_stuff_finish()
        if blocking:
            for t in self.threads:
                t.join()
    
    def disable_output_queue(self):
        if self.output_bak:
            return
        class dis(deque2):
            def put(*args,**kwargs):
                pass
            put_nowait = put
        self.output_bak = self.output
        self.output = dis()
    
    def enable_output_queue(self):
        if self.output_bak:
            self.output = self.output_bak
            self.output_bak = None

    def add_task(self, func, *args, **kwargs):
        """Add a task to the queue"""
        self._add_task(func,args,kwargs)
        
    def map(self, func, iterargs=None, iterkwargs=None):
        """Map a function to iterables containing any arguments"""
        if iterargs is None and iterkwargs is None:
            raise Exception('Must specify at least one regular or keyword argument iterator')
        
        if not self.input.empty():
            # wait for anything else that is running to finish
            self.finish()
            self.start()
        
        # empty output queue
        self.output.queue.clear()        
        
        # add tasks
        if iterargs is None:
            # if only kwargs are available, queue them
            for kwargs in iterkwargs:
                if not isinstance(kwargs,dict):
                    raise Exception('iterkwargs contains an entry that is not a dict')
                self._add_task(func,[],kwargs)
        elif iterkwargs is None:
            # if only args are available, queue them
            for args in iterargs:
                if not isinstance(args,(list,tuple)):
                    args = (args,)
                self._add_task(func,args,{})
        else:
            # if both args and kwargs are available, queue both
            for args,kwargs in zip(iterargs,iterkwargs):
                if not isinstance(args,(list,tuple)):
                    args = (args,)
                if not isinstance(kwargs,dict):
                    raise Exception('iterkwargs contains an entry that is not a dict')
                self._add_task(func, args, kwargs)
        
        # wait for completion
        self.finish()
        ret = list(self.output.queue)
        self.output.queue.clear()
        
        # restart
        self.start()
        
        # return result
        return ret
    
    def _init_queues(self):
        self.input = Queue.Queue()
        self.output = Queue.Queue()
        
    def _help_stuff_finish(self):
        # put sentinels at head of inqueue to make workers finish
        for _ in xrange(self.num_threads): self.input.put(None)
    
    def _add_task(self,func,args,kwargs):
        # internal add task which can be overridden by subclasses
        self.input.put((func, args, kwargs))


class ThreadPoolDeque(ThreadPool):
    """Thread Pool with deque as base.  Good for single-process work.  Do not use with multiprocessing."""
    def _init_queues(self):
        # use a raw deque without locking for increased speed
        self.input = deque2()
        self.output = deque2()

class PriorityThreadPool(ThreadPool):
    """A priority queue backed ThreadPool"""
    
    from sys import maxint
    from time import time
    
    class _Worker(Thread):
        """Thread executing tasks from a given tasks queue"""
        def __init__(self, input, output, event=None, init=None, exit=None):
            Thread.__init__(self)
            self.input = input
            self.output = output
            self.event = event
            if init is not None:
                self.init = init() # get initial values to pass to worker task
            else:
                self.init = None
            if exit is not None:
                self.exit = exit # get exit function
            else:
                self.exit = None
            self.daemon = True
            self.start()

        def run(self):
            self.event[1].acquire()
            self.event[2] += 1
            self.event[1].release()
            while True:
                # get next task
                if self.event[0].is_set() is False:
                    break # pause worker
                try:
                    task = self.input.get()
                except (EOFError, IOError):
                    break # error in input queue
                
                weight, func, args, kwargs = task
                if func is None:
                    break # sentinel found, so end worker
                callback = None
                if 'callback' in kwargs:
                    callback = kwargs['callback']
                    del kwargs['callback']
                if self.init is not None and 'init' not in kwargs:
                    kwargs['init'] = self.init
                
                # actually do task
                try:
                    ret = func(*args, **kwargs)
                    if callback is not None:
                        callback(ret)
                    else:
                        self.output.put(ret)
                except Exception as e:
                    logger.info('error in pool task',exc_info=True)
                    if callback is not None:
                        callback(e)
                finally:
                    self.input.task_done()
            if self.exit is not None:
                if self.init is not None:
                    self.exit(init=self.init)
                else:
                    self.exit()
            self.event[1].acquire()
            self.event[2] -= 1
            if self.event[2] <= 0:
                self.event[0].set()
            self.event[1].release()

    def _init_queues(self):
        self.input = Queue.PriorityQueue()
        self.output = Queue.Queue()
        
    def _help_stuff_finish(self):
        # put sentinels at head of inqueue to make workers finish
        for _ in xrange(self.num_threads): self.input.put((self.maxint,None,None,None))
    
    def _add_task(self,func,args,kwargs):
        # internal add task which can be overridden by subclasses
        try:
            # try getting a weight as a keyword argument
            weight = kwargs.pop('weight')
        except KeyError:
            # set standard weight as current time
            weight = self.time()
        # add to input
        self.input.put((weight, func, args, kwargs))


class GroupingThreadPool(ThreadPool):
    """Thread Pool that groups multiple tasks into a single task.  Good for combining db transactions."""
    class _Worker(ThreadPool._Worker):
        """Thread executing tasks from a given tasks queue"""
        def run(self):
            self.event[1].acquire()
            self.event[2] += 1
            self.event[1].release()
            while True:
                # get next task
                if self.event[0].is_set() is False:
                    break # pause worker
                try:
                    task = self.input.get()
                except (EOFError, IOError):
                    break # error in input queue
                except IndexError:
                    sleep(deque2._sleep_wait_time) # for deque, wait for a bit before trying again
                    continue
                if task is None:
                    break # sentinel found, so end worker
                func, args, kwargs = task
                callbacks = []
                if 'callback' in kwargs:
                    callbacks.append(kwargs['callback'])
                    del kwargs['callback']
                else:
                    callbacks.append(None)
                tasks = [(args,kwargs)]
                flag = None
                for i in xrange(1000): # do groupings of 1000
                    try:
                        task = self.input.get_nowait()
                    except (EOFError, IOError):
                        flag = 'end'
                        break # error in input queue
                    except (Queue.Empty, IndexError):
                        break # queue is now empty
                    if task is None:
                        flag = 'end'
                        break
                    else:
                        func2, args2, kwargs2 = task
                        if func == func2:
                            if 'callback' in kwargs2:
                                callbacks.append(kwargs2['callback'])
                                del kwargs2['callback']
                            else:
                                callbacks.append(None)
                            tasks.append((args2,kwargs2))
                        else:
                            flag = 'diff'
                            break
                
                if len(tasks) > 0:
                    kwargs['tasks'] = tasks[1:]
                if self.init is not None and 'init' not in kwargs:
                    kwargs['init'] = self.init
                # actually do task
                try:
                    ret = func(*args, **kwargs)
                    # do callback function
                    if 'tasks' in kwargs:
                        if isinstance(ret,list):
                            for i,c in enumerate(callbacks):
                                if c is not None:
                                    c(ret[i])
                                else:
                                    self.output.put(ret[i])
                        else:
                            for c in callbacks:
                                if c is not None:
                                    c(ret)
                                else:
                                    self.output.put(ret)
                    elif callbacks[0] is not None:
                        callbacks[0](ret)
                    else:
                        self.output.put(ret)
                except Exception as e:
                    logger.info('error in pool task',exc_info=True)
                    # do callback function
                    if 'tasks' in kwargs:
                        for c in callbacks:
                            if c is not None:
                                c(e)
                    elif callbacks[0] is not None:
                        callbacks[0](e)
                finally:
                    for i in xrange(len(tasks)):
                        self.input.task_done()
                    if flag == 'end':
                        break
                    elif flag == 'diff':
                        # there was another function to run, which we took off the queue early
                        if self.init is not None and 'init' not in kwargs2:
                            kwargs2['init'] = self.init
                        # actually do task
                        try:
                            if 'callback' in kwargs2:
                                callback = kwargs2['callback']
                                del kwargs2['callback']
                            else:
                                callback = None
                            ret = func2(*args2, **kwargs2)
                            if callback is not None:
                                callback(ret)
                            else:
                                self.output.put(ret)
                        except Exception as e:
                            logger.info('error in pool task',exc_info=True)
                            if callback is not None:
                                callback(e)
                        finally:
                            self.input.task_done()
            if self.exit is not None:
                if self.init is not None:
                    self.exit(init=self.init)
                else:
                    self.exit()
            self.event[1].acquire()
            self.event[2] -= 1
            if self.event[2] <= 0:
                self.event[0].set()
            self.event[1].release()


class SingleGrouping(GroupingThreadPool):
    """Single Threaded Grouping Thread Pool.  Good for DB writes, since only one can happen at a time."""
    def __init__(self, init=None, exit=None):
        GroupingThreadPool.__init__(self,1,init,exit)
    def _init_queues(self):
        # use a raw deque without locking for increased speed
        self.input = deque2()
        self.output = deque2()


class NamedThreadPool():
    """Thread Pool that groups tasks by name, running only one "name" at once."""
    def __init__(self, *args, **kwargs):
        self._pool = ThreadPoolDeque(*args, **kwargs)
        self._names = {} # {name:deque2(func,args,kwargs)}
        self._name_lock = RLock()
    
    def pause(self):
        self._pool.pause()
    
    def start(self, **kwargs):
        self._pool.start(**kwargs)
    
    def finish(self, *args, **kwargs):
        self._pool.finish(*args, **kwargs)
    
    def disable_output_queue(self):
        self._pool.disable_output_queue()
    
    def enable_output_queue(self):
        self._pool.enable_output_queue()

    def add_task(self, name, func, *args, **kwargs):
        """Add a task to the queue"""
        with self._name_lock:
            if name not in self._names:
                self._names[name] = deque2()
                self._pool.add_task(partial(self._runner,name))
            self._names[name].put((func,args,kwargs))
    
    def _runner(self,name,*args,**kwargs):
        while True:
            latest = None
            with self._name_lock:
                try:
                    latest = self._names[name].get()
                except IndexError, KeyError:
                    del self._names[name]
                    break
            if not latest:
                break
            try:
                f,a,k = latest
                f(*a,**k)
            except Exception:
                logger.info('error in NamedThreadPool runner',exc_info=True)
