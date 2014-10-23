
The website is the main way to communicate with IceProd.  It handles several
different jobs:

* Human
    * dataset submission
    * dataset editing, other in progress actions
    * viewing progress
* Computer
    * running task communications
    * site-to-site communications
    * file downloads
    * file proxying

In order to handle the many hundreds or thousands of requests it could get, 
the website was built on asynchronous principles.

Async
^^^^^

For IceProd, the main method of going asynchronous is by `callback functions <http://en.wikipedia.org/wiki/Asynchronous_I/O#Callback_functions>`_.
Instead of waiting for a long function to return, it is instead given a 
callback which it calls when finished.  This allows the main thread to 
continue processing additional requests.  While this would not work for 
compute-bound applications, network I/O lends itself very well to this because
of the high latencies between network events.  Thus, as long as the main 
thread can check for a new request every few microseconds, it can also run a 
large number of shorter functions at the same time.

In practice, the main benefit comes when interacting with the filesystem or 
database.  Both of these actions would normally spend a large number of clock 
cycles waiting for a return value, but can now do useful work in the meantime.
Eventually the original action will be finished and call the callback; this 
gets scheduled on the main thread and runs shortly.

Tornado
"""""""

IceProd's main access to asynchronous I/O is through `Tornado <http://www.tornadoweb.org>`_.
This is the basic framework for the website, and provides utility functions 
for asynchronous sockets that are used in several other places.  Tornado 
also allows us to make http downloads asynchronously.

GridFTP
"""""""

Using the C API for the `Globus Toolkit <http://www.globus.org/toolkit>`_ 
allows us to use GridFTP asynchronously as well.  This has been integrated 
into Tornado's callback mechanism in Python with mostly complete pybindings 
for ease of use.

Callback Details
""""""""""""""""

Functions that accept callbacks generally take the form of::

    def foo(arg1,arg2,callback):
        # do some work
        callback(result)

The callback argument should be a function capable of handling the result it 
is given.

For I/O applications, it is often necessary to pass connection details to the 
callback function.  There are two approaches:

1. Quick and Dirty:

    .. code-block:: python

        def foo(arg1,callback,callback_args):
            # do work
            callback(result,callback_args)
        def cb(result,args):
            # handle result
        foo(1,cb,{'handle':None})

|
|     This passes callback arguments through the worker function using a second argument.  It is used often in lower level languages where things must be compiled to binary before running.

2. Functional Programming:

    .. code-block:: python

        def foo(arg1,callback):
            # do work
            callback(result)
        def cb(result,args={}):
            # handle result
        foo(1,partial(cb,args={'handle':None}))

|
|     In functional programming, function signatures can be changed by filling in only some of the arguments and treating that as a new function.  Python allows this with the ``functools.partial()`` built-in.

Much of the code in the IceProd server uses the functional programming 
style, though there is some of the first style in the GridFTP python bindings.

Futures
"""""""

Starting in python 3.2, and available as a backport with the ``futures`` 
package, asynchronous actions have more official support::

    # Run slow operations in parallel using threads.
    # Instead of taking 5 seconds, this should only take 1 second.

    import time
    import concurrent.futures

    def slow_operation():
        time.sleep(1)

    # make a futures executor that launches 5 worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # queue the operations
        queue_operations = [executor.submit(slow_operation) for _ in range(5)]
        for future in queue_operations:
            try:
                # wait for this one to finish
                future.result()
            except Exception:
                pass

IceProd already has Tornado to do the heavy lifting that the
``ThreadPoolExecutor`` would do.  And in fact, it gets even easier.

In the Tornado request handler, where the get or post method is usually
defined, ``tornado.gen.coroutine`` and ``tornado.concurrent.run_on_executor``
can be used to provide a yield-like syntax for callback functions::

    # Using Tornado, run slow operations in parallel using threads.
    # Instead of taking 5 seconds, this should only take 1 second.
    import time
    import concurrent.futures
    import tornado.web
    import tornado.ioloop
    import tornado.concurrent

    class slow_op:
        def __init__(self):
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
            self.io_loop = tornado.ioloop.IOLoop.instance()

        # wrap this function such that it returns a Future
        @tornado.concurrent.run_on_executor
        def slow_operation(self):
            time.sleep(1)
            return True

    class MyHandler(tornado.web.RequestHandler):
        # get the global slop_op instance
        def initialize(self,ops):
            self.ops = ops

        # handle Futures inline with yield
        @tornado.gen.coroutine
        def get(self):
            ret = yield self.ops.slow_operation()
            self.write(str(ret))


    # make a futures executor that launches 5 worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        # launch tornado
        app = tornado.web.Application([
            (r"/.*", MyHandler, {'ops':slow_op()}),
        ])
        app.listen(8888)
        tornado.ioloop.IOLoop.instance().start()

Test this with:

.. code-block:: bash

    time (curl http://localhost:8888 & curl http://localhost:8888 & curl http://localhost:8888 & curl http://localhost:8888 & curl http://localhost:8888 & wait)

Or, if you already have an asynchronous function with a callback, you can use
``tornado.concurrent.return_future`` to make it return a Future. Note that
the function should be truly asynchronous, with no blocking before the 
function returns. Good examples of this are network calls where you expect
the result to be returned in the callback whenever it happens.


Task Communication
^^^^^^^^^^^^^^^^^^

Tasks communicate with the server using a json-rpc interface built into the 
website.  For most communications, this involves talking with the database 
using the internal RPC.

Site-to-Site Communication
^^^^^^^^^^^^^^^^^^^^^^^^^^

Communication between sites also use the json-rpc interface in the website.

Human Interaction
^^^^^^^^^^^^^^^^^

The website can modify things in the database using ajax and the json-rpc 
interface in the website.

Nginx
^^^^^

For security, the website uses nginx as a front end.  Nginx handles all SSL 
certificate checking, static files, and file uploading before proxying the 
request to Tornado.  Nginx has been proven to be a very robust web server, 
with over 10% of the web (and growing) using it.  It is also the recommended 
front end for production Tornado sites.
