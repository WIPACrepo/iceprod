"""
A `Futures <https://docs.python.org/dev/library/concurrent.futures.html>`_-based 
file IO library using the Tornado 
`concurrent.run_on_executor <http://tornado.readthedocs.org/en/latest/concurrent.html#tornado.concurrent.run_on_executor>`_
decorator. Requires either Python >= 3.2 or the `futures` package from pip.

Tornado applications should be able to use it directly in a 
`gen.coroutine <http://tornado.readthedocs.org/en/latest/gen.html#tornado.gen.coroutine>`_
decorator, like so::

    from iceprod.server.file_io import AsyncFileIO
    class MyHandler(tornado.web.RequestHandler):
        @tornado.gen.coroutine
        def get(self):
            io = AsyncFileIO()
            file = yield io.open('my_file.txt')
            data = yield io.read(file)
            self.write(data)
            yield AsyncFileIO.close(file)
"""

from __future__ import absolute_import, division, print_function

import os
import shutil

from concurrent.futures import ThreadPoolExecutor

from tornado.concurrent import run_on_executor
from tornado.ioloop import IOLoop

class AsyncFileIO(object):
    """
    Async File IO hidden behind threads using concurrent.futures.
    
    :param executor: A concurrent.futures.Executor object, optional.
            Defaults to a 1 thread Executor.
    :param io_loop: A tornado.ioloop.IOLoop object, optional.
            Defaults to the current IOLoop.
    """
    def __init__(self, executor=None, io_loop=None):
        self.executor = executor or ThreadPoolExecutor(1)
        self.io_loop = io_loop or IOLoop.current()
    
    @run_on_executor
    def open(self, filename, mode=None):
        """
        Open a file.
        
        :param filename: Filename of a file.
        :param mode: Mode to open a file in, optional.
        :returns: file object.
        :raises: IOError if file cannot be opened.
        """
        if mode:
            return open(filename, mode)
        else:
            return open(filename)
    
    @run_on_executor
    def read(self, file, bytes=None, callback=None):
        """
        Read from an open file object.
        
        :param file: Open file object.
        :param bytes: Number of bytes to read, defaults to 64k.
        :returns: Data read from file.
        :raises: IOError if file cannot be read from.
        """
        if bytes is None:
            bytes = 65536
        return file.read(bytes)
    
    @run_on_executor
    def readline(self, file):
        """
        Read a line from an open file object.
        
        :param file: Open file object.
        :returns: A line from the file.
        :raises: IOError if file cannot be read from.
        """
        return file.readline()
    
    @run_on_executor
    def write(self, file, data):
        """
        Write some data to an open file object.
        
        :param file: Open file object.
        :param data: Some data to write.
        :raises: IOError if file cannot be written to.
        """
        file.write(data)
    
    @run_on_executor
    def close(self, file):
        """
        Close an open file object.
        
        :param file: Open file object.
        :raises: IOError if file cannot be closed.
        """
        file.close()
