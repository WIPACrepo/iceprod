"""
  website module

  copyright (c) 2012 the icecube collaboration
"""

import sys
import os
import time
from threading import Thread,Event,Condition
import logging
from contextlib import contextmanager
from functools import partial
from urlparse import urlparse

# override tornado json encoder and decoder so we can use dataclasses objects
import iceprod.core.jsonUtil
import tornado.escape
tornado.escape.json_encode = iceprod.core.jsonUtil.json_encode
tornado.escape.json_decode = iceprod.core.jsonUtil.json_decode

import tornado.ioloop
import tornado.web
import tornado.httpserver

from pyuv_tornado import fs

from iceprod.server import module
from iceprod.server.dbclient import DB
from iceprod.server.proxy import Proxy
from iceprod.server.nginx import Nginx
import iceprod.core.functions




class JSONRPCHandler(tornado.web.RequestHandler):
    """JSONRPC 2.0 Handler.
    
       Call DB methods using RPC over json.
    """
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
            DB.authorize_site(site_id,passkey,callback=cb)
        else:
            # authorize task
            cb = partial(self.auth_callback,method,params,id,self.set_status,
                         self.write,self.finish,self.json_error)
            DB.authorize_task(passkey,callback=cb)
        
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
            func = getattr(DB,method)
        except:
            error({'code':-32601,'message':'Method not found'},
                  set_status=set_status,write=write,finish=finish)
        else:
            try:
                params['callback'] = partial(self.callback,id,set_status,
                                             write,finish,error)
                func(*args,**params)
            except:
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
        write({'jsonrpc':'2.0','error':error,'id':id})
        if not finish:
            finish = self.finish
        finish()


class LibHandler(tornado.web.RequestHandler):
    """Handler for iceprod library downloads.
    
       These are straight http downloads like normal.
    """
    prefix = '/lib/'
    directory = '$I3PROD/var/lib/iceprod'
    
    @tornado.web.asynchronous
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
            # TODO: make this work better for multi-site,
            #       possibly hook into caching system
            filename = os.path.join(self.directory,url)
            # open the file and send it
            fs.open(filename,os.O_RDONLY,0,tornado=True,callback=self.pre_send)
    def pre_send(self,path,fd,errno):
        """Start reading the file"""
        if errno:
            self.set_status(404)
            self.write('does not exist')
            self.finish()
            return
        num = 65536
        offset = 0
        fs.read(fd,num,offset,tornado=True,
                callback=partial(self.send,fd,offset+num))
    def send(self,fd,offset,path,data,errno):
        """Send a file in pieces"""
        num = 65536
        if errno:
            fs.close(fd,tornado=True,callback=self.post_send)
        else:
            if len(data) < num:
                cb = partial(fs.close,fd,tornado=True,callback=self.post_send)
            else:
                cb = partial(fs.read,fd,num,offset,tornado=True,
                             callback=partial(self.send,fd,offset+num))
            self.write(data)
            self.flush(callback=cb)
    def post_send(self,path,errno):
        self.finish()


class DownloadHandler(tornado.web.RequestHandler):
    """Handler for downloads"""
    def get(self):
        """GET is invalid and returns an error"""
        raise tornado.web.HTTPError(405,'GET is invalid.  Use POST')
        # TODO: make this human-browsable in future
    
    @tornado.web.asynchronous
    def post(self):
        """POST request should be json
           
           parameters
             :type: type of result requested (download, size, checksum)
             :url: the url requested
             :key: authorization key
             :site_id: designate this request as coming from another site
        
           return values (by request type)
             :download: response will be the file or 404 error
             :size: response will be json of the type {url,size} or 404 error
             :checksum: response will be json of the type {url,checksum} or 404 error
        """
        # parse JSON
        try:
            request = tornado.escape.json_decode(self.request.body)
        except Exception as e:
            raise tornado.web.HTTPError(400,'POST request is not valid json')
        
        # look for authorization data
        if not isinstance(request,dict):
            self.json_error('json is not a dict')
        if 'url' not in request:
            self.json_error('json does not contain url')
        if 'key' not in request:
            self.json_error('json does not contain key')
        
        # try to authorize
        if 'site_id' in request:
            # authorize site
            cb = partial(self.post_after_auth,request,self.write,self.flush,self.finish,self.json_error)
            DB.authorize_site(request['site_id'],request['key'],callback=cb)
        else:
            # authorize task
            cb = partial(self.post_after_auth,request,self.write,self.flush,self.finish,self.json_error)
            DB.authorize_task(request['key'],callback=cb)
    
    def json_error(self,error,status=400):
        """Create a proper jsonrpc error message"""
        self.set_status(status)
        self.write({'error':error}) # autoconverts to json and sets content-type
        self.finish()
    
    def post_after_auth(self,request,write,flush,finish,error,auth=False):
        """Callback after perforing authorization on the request"""
        # send response
        if auth is True:
            # make request from url
            h = urlparse(request['url'])[1]
            url = tornado.httpserver.HTTPRequest('GET',request['url'],host=h)
            # get current host
            host = self.request.host
            # check request setting
            type='download'
            if 'type' in request:
                type = str(request['type'])
            if type == 'size':
                # only send file size
                Proxy.size_request(url,
                                   host=host,
                                   writer=write,
                                   error=error,
                                   callback=finish)
            elif type == 'checksum':
                # only send checksum
                Proxy.checksum_request(url,
                                       host=host,
                                       writer=write,
                                       error=error,
                                       callback=finish)
            
            elif type == 'download':
                # check cache setting
                cache=True
                if 'cache' in request:
                    cache = request['cache']
                # send the file
                Proxy.download_request(url,
                                       host=host,
                                       cache=cache,
                                       writer=write,
                                       flusher=flush,
                                       error=error,
                                       callback=finish)
            else:
                error('invalid type in json request')                
        else:
            # send error message
            error('authorization invalid: %r'%(auth))


class UploadHandler(tornado.web.RequestHandler):
    """Superclass for Upload Handlers

       Start with a POST json request to prepare for upload.
       The POST should return a url to upload to or an error.
       Then send a POST request with the actual file. This request
       is authenticated before uploading.
       Finally, send a POST json request to make sure upload
       was successful.
    """
    upload_prefix = '/upload'

class UploadFileHandler(UploadHandler):
    """Handle step 2 of upload"""
    def get(self):
        """GET is invalid and returns an error"""
        raise tornado.web.HTTPError(405,'GET is invalid. Use POST')
    
    @tornado.web.asynchronous
    def post(self):
        """POST request gets buffered by Nginx, then sent here to be delt with.
           File is checked against size and checksum before returning success.
           If final destination is not here, file will be forwarded.
        """
        url = self.request.uri[len(self.upload_prefix)+1:]
        name = self.get_argument('name', default=None)
        content_type = self.get_argument('content-type', default=None)
        path = self.get_argument('path', default=None)
        host = self.request.host
        
        cb = partial(self.post_after,self.write,self.set_status,self.finish)
        DB.handle_upload(url,name,content_type,path,host,callback=cb)
    
    def post_after(self,write,set_status,finish,ret):
        if ret and not isinstance(ret,Exception):
            set_status(200)
            write("upload complete")
        else:
            set_status(500)
            write("upload failed")
        finish()

class UploadAuthHandler(UploadHandler):
    """Handle authentication for step 2 of upload"""
    @tornado.web.asynchronous
    def get(self):
        """Call auth()"""
        self.auth()
    
    @tornado.web.asynchronous
    def post(self):
        """Call auth()"""
        self.auth()
    
    def auth(self):
        """Authenticate request by checking url to see if it is on the list"""
        if 'X-Original-URI' in self.request.headers:
            url = self.request.headers['X-Original-URI']
            if self.upload_prefix+'/' in url:
                url = url[len(self.upload_prefix)+1:]
                cb = partial(self.post_after,self.write,self.set_status,self.finish)
                DB.is_upload_addr(url,callback=cb)
                return
        self.set_status(403)
        self.write('Request denied')
        self.finish()
    
    def post_after(self,write,set_status,finish,auth):
        if auth and not isinstance(auth,Exception):
            set_status(200)
            write("OK")
            finish()
        else:
            set_status(403)
            write('Request denied')
            finish()

class UploadJSONHandler(UploadHandler):
    """Handle steps 1,3 of upload"""
    def get(self):
        """GET is invalid and returns an error"""
        raise tornado.web.HTTPError(405,'GET is invalid. Use POST')
    
    @tornado.web.asynchronous
    def post(self):
        """POST request should be json
           
           json parameters
             :type: type of result requested (upload, check)
             :url: the url requested
             :size: (if upload type) the size of the file to upload
             :checksum: (if upload type) the checksum of the file to upload
             :checksum_type: (if upload type) the checksum type (md5,sha1,sha256,sha512)
             :key: authorization key
             :site_id: designate this request as coming from another site
           
           Upload response will be json
             :type: upload
             :url: the url requested
             :upload: the url to upload the file to
           
           Check response will be json
             :type: check
             :url: the url requested
             :result: True/False/"Still Uploading"
           
        """
        # parse JSON
        try:
            request = tornado.escape.json_decode(self.request.body)
        except Exception as e:
            raise tornado.web.HTTPError(400,'POST request is not valid json')
        
        # look for authorization data
        if not isinstance(request,dict):
            self.json_error('json is not a dict')
        if 'url' not in request:
            self.json_error('json does not contain url')
        if 'key' not in request:
            self.json_error('json does not contain key')
        
        # try to authorize
        if 'site_id' in request:
            # authorize site
            cb = partial(self.post_after_auth,request,self.write,self.flush,self.finish,self.json_error)
            DB.authorize_site(request['site_id'],request['key'],callback=cb)
        else:
            # authorize task
            cb = partial(self.post_after_auth,request,self.write,self.flush,self.finish,self.json_error)
            DB.authorize_task(request['key'],callback=cb)
    
    def json_error(self,error,status=400):
        """Create a proper jsonrpc error message"""
        self.set_status(status)
        self.write({'error':error}) # autoconverts to json and sets content-type
        self.finish()
    
    def post_after_auth(self,request,write,flush,finish,error,auth=False):
        """Callback after perforing authorization on the request"""
        # send response
        if auth is True:
            # check request setting
            type='upload'
            if 'type' in request:
                type = str(request['type'])
            if type == 'upload':
                if ('url' not in request or
                    'size' not in request or 
                    'checksum' not in request or
                    'checksum_type' not in request):
                    error('missing url, size, checksum, or checksum_type')
                    return
                # ask for a new upload url
                cb = partial(self.new_upload,write,flush,finish,
                             error,request['url'])
                DB.new_upload(request['url'],
                              request['size'],
                              request['checksum'],
                              request['checksum_type'],
                              callback=cb)
            elif type == 'check':
                if 'url' not in request:
                    error('missing url')
                    return
                cb = partial(self.check_upload,write,flush,finish,
                             error,request['url'])
                DB.check_upload(request['url'],callback=cb)
            else:
                error('type is invalid')
        else:
            # send error message
            json_error('authorization invalid: %r'%(auth))
    
    def new_upload(self,write,flush,finish,error,oldurl,uid=None):
        if uid:
            newurl = self.upload_prefix+'/'+uid
            write({'type':'upload','url':oldurl,'upload':newurl})
            finish()
        else:
            error('could not upload')
    
    def check_upload(self,write,flush,finish,error,oldurl,result=None):
        if result is not None:
            write({'type':'check','url':oldurl,'result':result})
            finish()
        else:
            error('could not check upload')

class MainHandler(tornado.web.RequestHandler):
    """Handler for public facing website"""
    def render_handle(self,*args,**kwargs):
        """Handle renderer exceptions properly"""
        try:
            self.render(*args,**kwargs)
        except Exception as e:
            module.logger.error('render error',exc_info=True)
            self.send_error(message='render error')
    
    @tornado.web.asynchronous
    def get(self):
        # get correct template
        url = self.request.uri[1:]
        url_parts = [x for x in url.split('/') if x]
        if url.startswith('submit'):
            DB.new_passkey(callback=self.display_submit)
        elif url.startswith('dataset'):
            status = self.get_argument('status',default=None)
            if len(url_parts) > 1:
                dataset_id = url_parts[1]
                cb = partial(self.dataset_detail,dataset_id=dataset_id)
                DB.get_datasets_details(dataset_id=dataset_id,callback=cb)
            elif status:
                DB.get_datasets_details(status=status,callback=self.dataset_browse)
            else:
                DB.get_datasets_by_status(callback=self.display_dataset_status)
        elif url.startswith('task'):
            dataset_id = self.get_argument('dataset_id',default=None)
            status = self.get_argument('status',default=None)
            if len(url_parts) > 1:
                task_id = url_parts[1]
                cb = partial(self.task_detail,task_id=task_id)
                DB.get_tasks_details(task_id=task_id,dataset_id=dataset_id,
                                     callback=cb)
            elif status:
                DB.get_tasks_details(status=status,dataset_id=dataset_id,
                                     callback=self.task_browse)
            else:
                DB.get_tasks_by_status(dataset_id=dataset_id,
                                       callback=self.display_task_status)
        else:
            DB.get_datasets_by_status(callback=self.display_dataset_status)
    def display_dataset_status(self,ret):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_datasets_by_status: %r',ret)
            self.send_error(message='DB.get_datasets_by_status error') #DEBUG
        else:
            self.render_handle('main.html',status=ret)
    def display_task_status(self,ret):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_tasks_by_status: %r',ret)
            self.send_error(message='DB.get_tasks_by_status error') #DEBUG
        else:
            self.render_handle('tasks.html',status=ret)
    def display_submit(self,ret):
        if isinstance(ret,Exception):
            logging.error('error in DB.new_passkey: %r',ret)
            self.send_error(message='DB.new_passkey error') #DEBUG
        else:
            cb = partial(self.display_submit2,passkey=ret)
            DB.get_gridspec(callback=cb)
    def display_submit2(self,ret,passkey=None):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_gridspec: %r',ret)
            self.send_error(message='DB.get_gridspec error') #DEBUG
        else:
            self.render_handle('submit.html',passkey=passkey,gridspec=ret)
    def dataset_detail(self,ret,dataset_id=''):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_datasets_details: %r',ret)
            self.send_error(message='DB.get_datasets_details error') #DEBUG
        else:
            if ret:
                ret = ret.values()[0]
            cb = partial(self.dataset_detail2,dataset_id=dataset_id,dataset=ret)
            DB.get_tasks_by_status(dataset_id=dataset_id,callback=cb)
    def dataset_detail2(self,ret,dataset_id='',dataset={}):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_tasks_by_status: %r',ret)
            self.send_error(message='DB.get_tasks_by_status error') #DEBUG
        else:
            self.render_handle('dataset_detail.html',dataset_id=dataset_id,dataset=dataset,tasks=ret)
    def dataset_browse(self,ret):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_datasets_details: %r',ret)
            self.send_error(message='DB.get_datasets_details error') #DEBUG
        else:
            self.render_handle('dataset_browse.html',dataset=ret)
    def task_detail(self,ret,task_id=None):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_tasks_details: %r',ret)
            self.send_error(message='DB.get_tasks_details error') #DEBUG
        else:
            if ret:
                ret = ret.values()[0]
            cb = partial(self.task_detail2,ret)
            DB.get_logs(task_id=task_id,lines=20,callback=cb) #TODO: make lines adjustable
    def task_detail2(self,task_details,ret):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_logs: %r',ret)
            self.send_error(message='DB.get_logs error') #DEBUG
        else:
            self.render_handle('task_detail.html',task=task_details,logs=ret)
    def task_browse(self,ret):
        if isinstance(ret,Exception):
            logging.error('error in DB.get_tasks_details: %r',ret)
            self.send_error(message='DB.get_tasks_details error') #DEBUG
        else:
            self.render_handle('task_browse.html',tasks=ret)
    
    def write_error(self,status_code=500,**kwargs):
        self.set_status(status_code)
        self.write('<h2>Internal Error</h2>')
        if 'message' in kwargs:
            self.write('<br />'.join(kwargs['message'].split('\n')))
        self.finish()
    
    def post(self):
        #TODO: actually do something here
        self.write("Hello, world")


class website(module.module):
    """The main website module.
    
       Run the website, which is required for anything to work.
    """
    
    def __init__(self,args):
        # run default init
        super(website,self).__init__(args)
        
        # set up local variables
        self.thread_running = 0
        self.thread_running_cv = Condition()
        
        # start website
        self.start()
        
        # start message loop
        self.message_handling_loop()
    
    def website(self):
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
        
        with self.check_wait():
            # start DB connection
            db_address = self.cfg['db']['address']
            ssl = self.cfg['db']['ssl']
            if ssl is True:
                ssl_cert = self.cfg['system']['ssl_cert']
                ssl_key = self.cfg['system']['ssl_key']
                DB.start(db_address,ssl_options={'keyfile':ssl_key,'cert_file':ssl_cert})
            else:
                DB.start(db_address)
            module.logger.warn('DB connected to address %s  SSL is %s',db_address,str(ssl))
            
            # configure Proxy
            kwargs = {'request_timeout': self.cfg['webserver']['proxy_request_timeout'],
                      'download_dir': self.cfg['webserver']['proxycache_dir'],
                     }
            if self.cfg['download']['http_username']:
                kwargs['username'] = self.cfg['download']['http_username']
            if self.cfg['download']['http_password']:
                kwargs['password'] = self.cfg['download']['http_password']
            if ssl is True:
                kwargs['sslcert'] = self.cfg['system']['ssl_cert']
                kwargs['sslkey'] = self.cfg['system']['ssl_key']
                kwargs['cacert'] = self.cfg['system']['ssl_cacert']
            Proxy.configure(**kwargs)
            
            # start nginx
            nginx_kwargs = kwargs.copy()
            nginx_kwargs['port'] = self.cfg['webserver']['port']
            nginx_kwargs['proxy_port'] = self.cfg['webserver']['tornado_port']
            nginx_kwargs['static_dir'] = self.cfg['webserver']['static_dir']
            nginx_kwargs['upload_dir'] = self.cfg['webserver']['tmp_upload_dir']
            self.nginx = Nginx(**nginx_kwargs)
            try:
                self.nginx.start()
            except Exception as e:
                module.logger.critical('cannot start Nginx: %r',e)
                raise
            
            # configure tornado
            def logger(handler):
                if handler.get_status() < 400:
                    log_method = module.logger.debug
                elif handler.get_status() < 500:
                    log_method = module.logger.warning
                else:
                    log_method = module.logger.error
                request_time = 1000.0 * handler.request.request_time()
                log_method("%d %s %.2fms", handler.get_status(),
                        handler._request_summary(), request_time)
            UploadHandler.upload_prefix = '/upload'
            LibHandler.prefix = '/lib/'
            LibHandler.directory = os.path.expanduser(os.path.expandvars(
                              self.cfg['webserver']['lib_dir']))
            static_path = os.path.expanduser(os.path.expandvars(
                              self.cfg['webserver']['static_dir']))
            template_path = os.path.expanduser(os.path.expandvars(
                              self.cfg['webserver']['template_dir']))
            self.application = tornado.web.Application([
                (r"/jsonrpc", JSONRPCHandler),
                (r"/download", DownloadHandler),
                (r"/upload", UploadJSONHandler),
                (r"/upload/.*", UploadFileHandler),
                (r"/uploadauth", UploadAuthHandler),
                (r"/lib/.*", LibHandler),
                (r"/.*", MainHandler),
            ],static_path=static_path,
              template_path=template_path,
              log_function=logger)
            self.http_server = tornado.httpserver.HTTPServer(
                    self.application,
                    xheaders=True)
            
            # start tornado
            tornado_port = self.cfg['webserver']['tornado_port']
            numcpus = self.cfg['webserver']['numcpus']
            module.logger.warn('tornado bound to port %d with %d cpus',tornado_port,numcpus)
            self.http_server.bind(tornado_port,address='localhost') # bind locally
            self.http_server.start(numcpus)
            module.logger.warn('tornado starting')
            tornado.ioloop.IOLoop.instance().start()
            
            # thread waits until tornado is stopped
            module.logger.warn('tornado stopped')
        
    def start(self):
        """Start thread"""
        module.logger.warn('got a start() call')
        # start proxy server thread
        self.website_thread = Thread(target=self.website)
        self.website_thread.start()
        
    def stop(self):
        """Stop thread"""
        module.logger.warn('got a stop() call')
        # tell threads to stop
        tornado.ioloop.IOLoop.instance().stop()
        self.thread_running_cv.acquire()
        while self.thread_running:
            # wait until current threads have finished
            self.thread_running_cv.wait(1) 
        self.thread_running_cv.release()
        DB.stop()
        # stop nginx
        try:
            if hasattr(self,'nginx') and self.nginx:
                self.nginx.stop()
        except Exception as e:
            module.logger.error('cannot stop Nginx: %r',e)
            raise
    
    def kill(self):
        """Kill thread"""
        module.logger.warn('got a kill() call')
        tornado.ioloop.IOLoop.instance().stop()
        time.sleep(0.01) # let the immediate thread finish if possible
        DB.stop()
        # kill nginx
        try:
            if hasattr(self,'nginx') and self.nginx:
                self.nginx.kill()
        except Exception as e:
            module.logger.error('cannot kill Nginx: %r',e)
        # then just let the killed process eat the hanging threads
    
    def logrotate(self):
        module.logger.warn('got a logrotate() call')
        try:
            # rotate nginx logs
            self.nginx.logrotate()
            # tornado uses regular python logs, which rotate automatically
        except:
            pass # ignore errors in favor of continuous running
    
    def handle_message(self,msg):
        """Handle a non-default message"""
        if msg == 'stop':
            try:
                self.stop()
            except:
                pass
        elif msg == 'kill':
            self.kill()
        elif msg == 'start':
            self.start()
        elif msg == 'rotatelogs':
            self.logrotate()
    
    def update_cfg(self,newcfg):
        """Update the cfg, making any necessary changes"""
        self.cfg = newcfg
        try:
            self.stop()
        except Exception as e:
            module.logger.error('cannot stop website',exc_info=True)
            self.kill()
            time.sleep(10)
        self.start()
    
    @contextmanager
    def check_wait(self):
        """A context manager which keeps track of # of running threads"""
        self.thread_running_cv.acquire()
        self.thread_running += 1
        self.thread_running_cv.release()
        try:
            yield
        finally:
            self.thread_running_cv.acquire()
            self.thread_running -= 1
            self.thread_running_cv.notify_all()
            self.thread_running_cv.release()
