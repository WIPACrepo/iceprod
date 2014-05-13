"""
  db client class

  copyright (c) 2013 the icecube collaboration
"""

from iceprod.server import module


class MetaDB(type):
    """metaclass for DB.  Allows for static class usage."""
    __db = None
    
    @classmethod
    def start(cls,address=None,ssl_options=None):
        cls.__address = address
        cls.__ssl_options = ssl_options
        cls.__db = module.get_db_handle(cls.__address,cls.__ssl_options)
        
    @classmethod
    def stop(cls):
        cls.__db.close()
        cls.__db = None
    
    @classmethod
    def restart(cls):
        cls.stop()
        cls.start()
    
    @classmethod
    def __getattr__(cls,name):
        if cls.__db is None:
            raise Exception('DB connection not started yet')
        class _Method(object):
            def __init__(self,db,name):
                self.__db = db
                self.__name = name
            def __getattr__(self,name):
                return _Method(self.__db,"%s.%s"%(self.__name,name))
            def __call__(self,*args,**kwargs):
                return getattr(self.__db,self.__name)(*args,**kwargs)
        return _Method(cls.__db,name)

class DB(object):
    """DB connection.
       Call DB.start(address,ssl_options) to start.
       Call DB.stop() to stop.
       Call DB.restart()
       Call DB functions as regular function calls.
       
       Example:
           DB.set_task_status(task_id,'waiting')
    """
    __metaclass__ = MetaDB
    def __getattr__(self,name):
        return getattr(DB,name)