"""
The website module uses `Tornado <http://www.tornadoweb.org>`_,
a fast and scalable python web server.

There are three handlers:

* Main website
    This is the external website users will see when interacting with IceProd.

* JSONRPC
    This is the machine-readable portion of the website. Jobs talk to the
    server through this mechanism. The main website also uses this for
    various actions.

* lib downloads
    This is a directory of downloads local to the IceProd instance.
    Usually this will be the necessary software to run the iceprod core.

"""
from __future__ import absolute_import, division, print_function

import sys
import os
import time
from threading import Thread,Event,Condition
import logging
from contextlib import contextmanager
from functools import partial
from urlparse import urlparse

from concurrent.futures import ThreadPoolExecutor

# override tornado json encoder and decoder so we can use dataclasses objects
import iceprod.core.jsonUtil
import tornado.escape
tornado.escape.json_encode = iceprod.core.jsonUtil.json_encode
tornado.escape.json_decode = iceprod.core.jsonUtil.json_decode

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen

from iceprod.server import get_pkgdata_filename
from iceprod.server import module
from iceprod.server.nginx import Nginx
from iceprod.server.file_io import AsyncFileIO
import iceprod.core.functions

logger = logging.getLogger('website')

class website(module.module):
    """
    The main website module.
    
    Run the website, which is required for anything to work.
    """
    
    def __init__(self,*args,**kwargs):
        # run default init
        super(website,self).__init__(*args,**kwargs)        
        self.service_class = WebsiteService(self)
        
        # set up local variables
        self.nginx = None
        
        # start website
        self.start()
        
    def start(self):
        """Start thread"""
        super(website,self).start(callback=self._start)
        
    def stop(self):
        """Stop thread"""
        # stop nginx
        try:
            if self.nginx:
                self.nginx.stop()
        except Exception as e:
            logger.error('cannot stop Nginx: %r',e)
        super(website,self).stop()
    
    def kill(self):
        """Kill thread"""
        # kill nginx
        try:
            if self.nginx:
                self.nginx.kill()
        except Exception as e:
            logger.error('cannot kill Nginx: %r',e)
        super(website,self).kill()
        
    
    def _start(self):
        """Run the website"""
        # make sure directories are set up properly
        for d in self.cfg['webserver']:
            if '_dir' in d:
                path = self.cfg['webserver'][d]
                path = os.path.expanduser(os.path.expandvars(path))
                try:
                    os.makedirs(path)
                except:
                    pass
        
        # get package data
        static_path = get_pkgdata_filename('iceprod.server','data/www')
        if static_path is None:
            raise Exception('bad static path')
        template_path = get_pkgdata_filename('iceprod.server','data/www_templates')
        if template_path is None:
            raise Exception('bad template path')
        
        # configure nginx
        kwargs = {'request_timeout': self.cfg['webserver']['request_timeout']}
        if ('download' in self.cfg and 'http_username' in self.cfg['download']
            and self.cfg['download']['http_username']):
            kwargs['username'] = self.cfg['download']['http_username']
        if ('download' in self.cfg and 'http_password' in self.cfg['download']
            and self.cfg['download']['http_password']):
            kwargs['password'] = self.cfg['download']['http_password']
        if ('system' in self.cfg and 'ssl_cert' in self.cfg['system'] and
            self.cfg['system']['ssl_cert'] and 'ssl_key' in self.cfg['system']
            and self.cfg['system']['ssl_key']):
            kwargs['sslcert'] = self.cfg['system']['ssl_cert']
            kwargs['sslkey'] = self.cfg['system']['ssl_key']
        if ('system' in self.cfg and 'ssl_cacert' in self.cfg['system'] and
            self.cfg['system']['ssl_cacert']):
            kwargs['cacert'] = self.cfg['system']['ssl_cacert']
        kwargs['port'] = self.cfg['webserver']['port']
        kwargs['proxy_port'] = self.cfg['webserver']['tornado_port']
        kwargs['static_dir'] = static_path
        
        # start nginx
        try:
            self.nginx = Nginx(**kwargs)
        except Exception:
            logger.error('Nginx not present, running Tornado directly')
            logger.error('(Note that this mode is not very secure)')
            self.nginx = None
        else:
            try:
                self.nginx.start()
            except Exception:
                logger.critical('cannot start Nginx:',exc_info=True)
                raise
        
        # configure tornado
        def tornado_logger(handler):
            if handler.get_status() < 400:
                log_method = logger.debug
            elif handler.get_status() < 500:
                log_method = logger.warning
            else:
                log_method = logger.error
            request_time = 1000.0 * handler.request.request_time()
            log_method("%d %s %.2fms", handler.get_status(),
                    handler._request_summary(), request_time)
        #UploadHandler.upload_prefix = '/upload'
        handler_args = {
            'messaging':self.messaging,
            'fileio':AsyncFileIO(executor=ThreadPoolExecutor(10)),
        }
        lib_args = handler_args.copy()
        lib_args['prefix'] = '/lib/'
        lib_args['directory'] = os.path.expanduser(os.path.expandvars(
                self.cfg['webserver']['lib_dir']))
        self.application = tornado.web.Application([
            (r"/jsonrpc", JSONRPCHandler, handler_args),
            (r"/lib/.*", LibHandler, lib_args),
            (r"/.*", MainHandler, handler_args),
        ],static_path=static_path,
          template_path=template_path,
          log_function=tornado_logger)
        self.http_server = tornado.httpserver.HTTPServer(
                self.application,
                xheaders=True)
        
        # start tornado
        if self.nginx is None:
            tornado_port = self.cfg['webserver']['port']
            tornado_address = '0.0.0.0' # bind to all
        else:
            tornado_port = self.cfg['webserver']['tornado_port']
            tornado_address = 'localhost' # bind locally
        numcpus = self.cfg['webserver']['numcpus']
        logger.warn('tornado bound to port %d with %d cpus',tornado_port,numcpus)
        self.http_server.bind(tornado_port,address=tornado_address)
        self.http_server.start(numcpus)
        logger.warn('tornado starting')
    
    def logrotate(self):
        """Rotate the Nginx logs."""
        logger.warn('got a logrotate() call')
        try:
            if self.nginx:
                # rotate nginx logs
                self.nginx.logrotate()
            # tornado uses regular python logs, which rotate automatically
        except:
            pass # ignore errors in favor of continuous running

class WebsiteService(module.Service):
    """
    Override the basic :class:`Service` handler.
    """
    def logrotate(self,callback=None):
        self.mod.logrotate()
        if callback:
            callback()


class JSONRPCHandler(tornado.web.RequestHandler):
    """JSONRPC 2.0 Handler.
    
       Call DB methods using RPC over json.
    """
    def initialize(self, messaging, fileio):
        """
        Get some params from the website module
        
        :param messaging: the messaging handle
        :param fileio: AsyncFileIO object
        """
        self.messaging = messaging
        self.fileio = fileio
        
    def get(self):
        """GET is invalid and returns an error"""
        raise tornado.web.HTTPError(400,'GET is invalid.  Use POST')

    @tornado.web.asynchronous
    def post(self):
        """Parses json in the jsonrpc format, returning results in
           jsonrpc format as well.
        """
        # parse JSON
        try:
            request = tornado.escape.json_decode(self.request.body)
        except Exception as e:
            raise tornado.web.HTTPError(400,'POST request is not valid json')
        
        # check for all parts of jsonrpc 2.0 spec
        if 'jsonrpc' not in request or (request['jsonrpc'] != '2.0' and
            request['jsonrpc'] != 2.0):
            self.json_error({'code':-32600,'message':'Invalid Request',
                'data':'jsonrpc is not 2.0'})
            return
        if 'method' not in request:
            self.json_error({'code':-32600,'message':'Invalid Request',
                'data':'method not in request'})
            return
        if request['method'].startswith('_'):
            self.json_error({'code':-32600,'message':'Invalid Request',
                'data':'method name cannot start with underscore'})
            return
            
        # add rpc_ to method name to prevent calling other DB methods
        method = 'rpc_'+request['method']
        if 'params' in request:
            params = request['params']
        else:
            params = {}
        if 'id' in request:
            id = request['id']
        else:
            id = None

        # check for auth
        if 'passkey' not in params:
            self.json_error({'code':403,'message':'Not Authorized',
                'data':'missing passkey'})
            return
        passkey = params.pop('passkey')
        
        if 'site_id' in params:
            # authorize site
            site_id = params.pop('site_id')
            cb = partial(self.auth_callback,method,params,id,self.set_status,
                         self.write,self.finish,self.json_error)
            self.messaging.db.authorize_site(site_id,passkey,callback=cb)
        else:
            # authorize task
            cb = partial(self.auth_callback,method,params,id,self.set_status,
                         self.write,self.finish,self.json_error)
            self.messaging.db.authorize_task(passkey=passkey,callback=cb)
        
    def auth_callback(self,method,params,id,set_status,write,finish,
                      error,auth=False):
        """Callback after perforing authorization on the request"""
        if isinstance(auth,Exception) or auth is not True:
            error({'code':403,'message':'Not Authorized',
                   'data':'passkey invalid'},
                  set_status=set_status,write=write,finish=finish)
            return
    
        # check for args and kwargs
        if 'args' in params:
            args = params.pop('args')
        else:
            args = []
        
        # call method on DB if exists
        try:
            func = getattr(self.messaging.db,method)
        except:
            error({'code':-32601,'message':'Method not found'},
                  set_status=set_status,write=write,finish=finish)
        else:
            try:
                params['callback'] = partial(self.callback,id,set_status,
                                             write,finish,error)
                func(*args,**params)
            except Exception:
                logger.info('error in DB method',exc_info=True)
                error({'code':-32602,'message':'Invalid  params'},
                      set_status=set_status,write=write,finish=finish)
    
    def callback(self,id,set_status,write,finish,error,ret):
        """Callback after running the request"""
        if isinstance(ret,Exception):
            error({'code':-32602,'message':'Invalid  params','data':str(ret)},
                  set_status=set_status,write=write,finish=finish)
        else:
            # return response
            write({'jsonrpc':'2.0','result':ret,'id':id})
            finish()

    def json_error(self,error,status=400,id=None,set_status=None,
                   write=None,finish=None):
        """Create a proper jsonrpc error message"""
        if not set_status:
            set_status = self.set_status
        set_status(status)
        if not write:
            write = self.write
        if isinstance(error,Exception):
            error = str(error)
        logger.info('json_error: %r',error)
        write({'jsonrpc':'2.0','error':error,'id':id})
        if not finish:
            finish = self.finish
        finish()


class LibHandler(tornado.web.RequestHandler):
    """Handler for iceprod library downloads.
    
       These are straight http downloads like normal.
    """
    
    def initialize(self, messaging, fileio, prefix, directory):
        """
        Get some params from the website module
        
        :param messaging: the messaging handle
        :param fileio: AsyncFileIO object
        :param prefix: library url prefix
        :param directory: library directory on disk
        """
        self.messaging = messaging
        self.fileio = fileio
        self.prefix = prefix
        self.directory = directory
    
    @tornado.gen.coroutine
    def get(self):
        """Look up a library"""
        try:
            url = self.request.uri[len(self.prefix):]
        except:
            url = ''
        if not url:
            # TODO: make this human-browsable in future
            raise tornado.web.HTTPError(404,'not browsable')
        else:
            # TODO: make this work better for multi-site
            filename = os.path.join(self.directory,url)
            # open the file and send it
            file = yield self.fileio.open(filename)
            num = 65536
            data = yield self.fileio.read(file,bytes=num)
            self.write(data)
            self.flush()
            while len(data) >= num:
                data = yield self.fileio.read(file,bytes=num)
                self.write(data)
                self.flush()
            yield self.fileio.close(file)

class MainHandler(tornado.web.RequestHandler):
    """Handler for public facing website"""
    def initialize(self, messaging, fileio):
        """
        Get some params from the website module
        
        :param messaging: the messaging handle
        :param fileio: AsyncFileIO object
        """
        self.messaging = messaging
        self.fileio = fileio
    
    @tornado.concurrent.return_future
    def db_call(self,func_name,**kwargs):
        """Turn a DB messaging call into a `Futures` object"""
        getattr(self.messaging.db,func_name)(**kwargs)
    
    def render_handle(self,*args,**kwargs):
        """Handle renderer exceptions properly"""
        try:
            self.render(*args,**kwargs)
        except Exception as e:
            logger.error('render error',exc_info=True)
            self.send_error(message='render error')
    
    @tornado.gen.coroutine
    def get(self):
        # get correct template
        url = self.request.uri[1:]
        url_parts = [x for x in url.split('/') if x]
        if not url:
            status = yield self.db_call('get_datasets_by_status')
            if isinstance(status,Exception):
                raise status
            self.render_handle('main.html',status=status)
        elif url.startswith('submit'):
            passkey = yield self.db_call('new_passkey')
            if isinstance(passkey,Exception):
                raise passkey
            gridspec = yield self.db_call('get_gridspec')
            if isinstance(gridspec,Exception):
                raise gridspec
            self.render_handle('submit.html',passkey=passkey,gridspec=gridspec)
        elif url.startswith('dataset'):
            status = self.get_argument('status',default=None)
            if len(url_parts) > 1:
                dataset_id = url_parts[1]
                ret = yield self.db_call('get_datasets_details',dataset_id=dataset_id)
                if isinstance(ret,Exception):
                    raise ret
                if ret:
                    dataset = ret.values()[0]
                else:
                    dataset = None
                tasks = yield self.db_call('get_tasks_by_status',dataset_id=dataset_id)
                if isinstance(tasks,Exception):
                    raise tasks
                self.render_handle('dataset_detail.html',dataset_id=dataset_id,
                                   dataset=dataset,tasks=tasks)
            elif status:
                dataset = yield self.db_call('get_datasets_details',status=status)
                if isinstance(dataset,Exception):
                    raise dataset
                self.render_handle('dataset_browse.html',dataset=dataset)
            else:
                status = yield self.db_call('get_datasets_by_status')
                if isinstance(status,Exception):
                    raise status
                self.render_handle('main.html',status=status)
        elif url.startswith('task'):
            dataset_id = self.get_argument('dataset_id',default=None)
            status = self.get_argument('status',default=None)
            if len(url_parts) > 1:
                task_id = url_parts[1]
                ret = yield self.db_call('get_tasks_details',task_id=task_id,
                                         dataset_id=dataset_id)
                if isinstance(ret,Exception):
                    raise ret
                if ret:
                    task_details = ret.values()[0]
                else:
                    task_details = None
                logs = yield self.db_call('get_logs',task_id=task_id,lines=20) #TODO: make lines adjustable
                if isinstance(logs,Exception):
                    raise logs
                self.render_handle('task_detail.html',task=task_details,logs=logs)
            elif status:
                tasks = yield self.db_call('get_tasks_details',status=status,
                                           dataset_id=dataset_id)
                if isinstance(tasks,Exception):
                    raise tasks
                self.render_handle('task_browse.html',tasks=tasks)
            else:
                status = yield self.db_call('get_tasks_by_status',dataset_id=dataset_id)
                if isinstance(status,Exception):
                    raise status
                self.render_handle('tasks.html',status=status)
        else:
            self.set_status(404)
    
    def write_error(self,status_code=500,**kwargs):
        self.set_status(status_code)
        self.write('<h2>Internal Error</h2>')
        if 'message' in kwargs:
            self.write('<br />'.join(kwargs['message'].split('\n')))
        self.finish()
    
    def post(self):
        #TODO: actually do something here
        self.write("Hello, world")

