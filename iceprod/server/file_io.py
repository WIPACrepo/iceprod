"""
A `Futures <https://docs.python.org/dev/library/concurrent.futures.html>`_-based 
file IO library using the Tornado 
`concurrent.return_future <http://tornado.readthedocs.org/en/latest/concurrent.html#tornado.concurrent.return_future>`_
decorator.

Tornado applications should be able to use it directly in a 
`gen.coroutine <http://tornado.readthedocs.org/en/latest/gen.html#tornado.gen.coroutine>`_
decorator, like so::

    from iceprod.server.file_io import AsyncFileIO
    class MyHandler(tornado.web.RequestHandler):
        @tornado.gen.coroutine
        def get(self):
            file = yield AsyncFileIO.open('my_file.txt')
            data = yield AsyncFileIO.read(file)
            self.write(data)
            yield AsyncFileIO.close(file)
"""

from __future__ import absolute_import, division, print_function

import os
import shutil
from tornado.concurrent import return_future

class AsyncFileIO:
    # put things in a class so we don't override the built-in functions
    
    @staticmethod
    @return_future
    def open(filename, mode=None, callback=None):
        """
        Open a file.
        
        :param filename: Filename of a file.
        :param mode: Mode to open a file in, optional.
        :returns: file object.
        :raises: IOError if file cannot be opened.
        """
        if mode:
            callback(open(filename, mode))
        else:
            callback(open(filename))
    
    @staticmethod
    @return_future
    def read(file, bytes=None, callback=None):
        """
        Read from an open file object.
        
        :param file: Open file object.
        :param bytes: Number of bytes to read, defaults to 64k.
        :returns: Data read from file.
        :raises: IOError if file cannot be read from.
        """
        if bytes is None:
            bytes = 65536
        callback(file.read(bytes))
    
    @staticmethod
    @return_future
    def readline(file, callback=None):
        """
        Read a line from an open file object.
        
        :param file: Open file object.
        :returns: A line from the file.
        :raises: IOError if file cannot be read from.
        """
        callback(file.readline())
    
    @staticmethod
    @return_future
    def close(file, callback=None):
        """
        Close an open file object.
        
        :param file: Open file object.
        :raises: IOError if file cannot be closed.
        """
        file.close()
        callback()
