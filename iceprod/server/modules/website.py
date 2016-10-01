"""
The website module uses `Tornado <http://www.tornadoweb.org>`_,
a fast and scalable python web server.

There are three groups of handlers:

* Main website
    This is the external website users will see when interacting with IceProd.
    It has been broken down into several sub-handlers for easier maintenance.

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
import random
import binascii
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

import iceprod
from iceprod.server import get_pkgdata_filename
from iceprod.server import module
from iceprod.server.nginx import Nginx, find_nginx
from iceprod.server.ssl_cert import create_cert, verify_cert
from iceprod.server.file_io import AsyncFileIO
from iceprod.server.modules.db import DBAPI
import iceprod.core.functions
from iceprod.server import documentation

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
        self.http_server = None

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
        # stop tornado
        try:
            if self.http_server:
                self.http_server.stop()
        except Exception as e:
            logger.error('cannot stop tornado: %r',e)
        super(website,self).stop()

    def restart(self):
        """restart website"""
        logger.warn('restarting website')
        # stop nginx
        try:
            if self.nginx:
                self.nginx.stop()
        except Exception as e:
            logger.error('cannot stop Nginx: %r',e)
        try:
            if self.http_server:
                self.http_server.stop()
            def cb(*args,**kwargs):
                self._start()
            t = time.time()+0.5
            tornado.ioloop.IOLoop.current().add_timeout(t,cb)
        except Exception:
            logger.warn('error during restart',exc_info=True)
            raise

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
        try:
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

            # detect nginx
            try:
                if ('nginx' in self.cfg['webserver'] and
                    not self.cfg['webserver']['nginx']):
                    raise Exception('nginx explicitly disabled')
                find_nginx()
            except Exception:
                if ('system' in self.cfg and 'ssl' in self.cfg['system']
                    and self.cfg['system']['ssl'] is not False):
                    logger.error('Nginx not present when SSL requested')
                    raise
                logger.error('Nginx not present, running Tornado directly')
                logger.error('(Note that this mode is not secure)')
                self.nginx = None
            else:
                # configure nginx
                kwargs = {}
                if (self.cfg and 'webserver' in self.cfg and
                    'request_timeout' in self.cfg['webserver']):
                    try:
                        timeout = int(self.cfg['webserver']['request_timeout'])
                    except Exception:
                        pass
                    else:
                        kwargs['request_timeout'] = timeout
                        self.messaging.timeout = timeout
                if ('download' in self.cfg and 'http_username' in self.cfg['download']
                    and self.cfg['download']['http_username']):
                    kwargs['username'] = self.cfg['download']['http_username']
                if ('download' in self.cfg and 'http_password' in self.cfg['download']
                    and self.cfg['download']['http_password']):
                    kwargs['password'] = self.cfg['download']['http_password']
                if ('system' in self.cfg and 'ssl' in self.cfg['system']
                    and self.cfg['system']['ssl'] is not False):
                    cert = None
                    key = None
                    if ('autogen' in self.cfg['system']['ssl']
                        and self.cfg['system']['ssl']['autogen']):
                        if (os.path.exists(self.cfg['system']['ssl']['cert'])
                            and os.path.exists(self.cfg['system']['ssl']['key'])):
                            cert = self.cfg['system']['ssl']['cert']
                            key = self.cfg['system']['ssl']['key']
                            if not verify_cert(cert,key):
                                cert = None
                                key = None
                    elif ('cert' in self.cfg['system']['ssl']
                          and 'key' in self.cfg['system']['ssl']):
                        if (os.path.exists(self.cfg['system']['ssl']['cert'])
                            and os.path.exists(self.cfg['system']['ssl']['key'])):
                            cert = self.cfg['system']['ssl']['cert']
                            key = self.cfg['system']['ssl']['key']
                        else:
                            raise Exception('Bad ssl cert or key')
                    if not cert:
                        # auto-generate self-signed cert
                        create_cert('$PWD/cert','$PWD/key',days=365)
                        cert = os.path.expandvars('$PWD/cert')
                        key = os.path.expandvars('$PWD/key')
                        self.cfg['system']['ssl']['autogen'] = True
                        self.cfg['system']['ssl']['cert'] = cert
                        self.cfg['system']['ssl']['key'] = key
                        self.messaging.config.set(key='system',value=self.cfg['system'])
                        logger.warn('prepare for cfg reload')
                        return
                    else:
                        kwargs['sslcert'] = cert
                        kwargs['sslkey'] = key

                    if 'cacert' in self.cfg['system']['ssl']:
                        if not os.path.exists(self.cfg['system']['ssl']['cacert']):
                            raise Exception('Bad path to cacert')
                        kwargs['cacert'] = self.cfg['system']['ssl']['cacert']
                kwargs['port'] = self.cfg['webserver']['port']
                kwargs['proxy_port'] = self.cfg['webserver']['tornado_port']
                kwargs['static_dir'] = static_path

                # start nginx
                try:
                    self.nginx = Nginx(**kwargs)
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
                'cfg':self.cfg,
                'messaging':self.messaging,
                'fileio':AsyncFileIO(executor=ThreadPoolExecutor(10)),
            }
            if 'debug' in self.cfg['webserver'] and self.cfg['webserver']['debug']:
                handler_args['debug'] = True
            lib_args = handler_args.copy()
            lib_args['prefix'] = '/lib/'
            lib_args['directory'] = os.path.expanduser(os.path.expandvars(
                    self.cfg['webserver']['lib_dir']))
            if 'cookie_secret' in self.cfg['webserver']:
                cookie_secret = self.cfg['webserver']['cookie_secret']
            else:
                cookie_secret = ''.join(hex(random.randint(0,15))[-1] for _ in range(64))
                self.cfg['webserver']['cookie_secret'] = cookie_secret
                self.messaging.config.set(key='webserver',
                                          value=self.cfg['webserver'],
                                          update=False)
            self.application = tornado.web.Application([
                (r"/jsonrpc", JSONRPCHandler, handler_args),
                (r"/lib/.*", LibHandler, lib_args),
                (r"/", Default, handler_args),
                (r"/submit", Submit, handler_args),
                (r"/config", Config, handler_args),
                (r"/dataset(/.*)?", Dataset, handler_args),
                (r"/task(/.*)?", Task, handler_args),
                (r"/site(/.*)?", Site, handler_args),
                (r"/help", Help, handler_args),
                (r"/docs/(.*)", Documentation, handler_args),
                (r"/log/(.*)/(.*)", Log, handler_args),
                (r"/login", Login, handler_args),
                (r"/logout", Logout, handler_args),
                (r"/.*", Other, handler_args),
            ],static_path=static_path,
              template_path=template_path,
              log_function=tornado_logger,
              xsrf_cookies=True,
              cookie_secret=binascii.unhexlify(cookie_secret),
              login_url='/login')
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
            logger.warn('tornado bound to port %d', tornado_port)
            self.http_server.listen(tornado_port, address=tornado_address)
            logger.warn('tornado starting')
        except Exception:
            logger.error('website startup error',exc_info=True)
            self.messaging.daemon.stop()

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
    def restart(self,callback=None):
        self.mod.restart()
        if callback:
            callback()
    def logrotate(self,callback=None):
        self.mod.logrotate()
        if callback:
            callback()

class MyHandler(tornado.web.RequestHandler):
    """Default Handler"""
    def initialize(self, cfg, messaging, fileio, debug=False):
        """
        Get some params from the website module

        :param cfg: the global config
        :param messaging: the messaging handle
        :param fileio: AsyncFileIO object
        :param debug: debug flag (optional)
        """
        self.cfg = cfg
        self.messaging = messaging
        self.fileio = fileio
        self.debug = debug

    @tornado.concurrent.return_future
    def db_call(self,func_name,**kwargs):
        """Turn a DB messaging call into a `Futures` object"""
        logger.debug('db_call for %s',func_name)
        try:
            getattr(self.messaging.db,func_name)(**kwargs)
        except Exception:
            logger.warn('db_call error for %s',func_name,exc_info=True)
            raise

    @tornado.concurrent.return_future
    def daemon_call(self, func_name, **kwargs):
        try:
            getattr(self.messaging.daemon,func_name)(**kwargs)
        except Exception:
            logger.warn('daemon_call error for %s',func_name,exc_info=True)
            raise

    @tornado.concurrent.return_future
    def config_call(self, func_name, **kwargs):
        try:
            getattr(self.messaging.config,func_name)(**kwargs)
        except Exception:
            logger.warn('config_call error for %s',func_name,exc_info=True)
            raise


    def get(self):
        """GET is invalid and returns an error"""
        raise tornado.web.HTTPError(400,'GET is invalid.  Use POST')

    def post(self):
        """POST is invalid and returns an error"""
        raise tornado.web.HTTPError(400,'POST is invalid.  Use GET')


class JSONRPCHandler(MyHandler):
    """JSONRPC 2.0 Handler.

       Call DB methods using RPC over json.
    """
    def check_xsrf_cookie(self):
        pass

    @tornado.gen.coroutine
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

        if not method.startswith("rpc_public"):
            # check for auth
            if 'passkey' not in params:
                self.json_error({'code':403,'message':'Not Authorized',
                                 'data':'missing passkey'})
                return
            passkey = params.pop('passkey')

            if 'site_id' in params:
                # authorize site
                site_id = params.pop('site_id')
                auth = yield self.db_call('auth_authorize_site',site=site_id,key=passkey)
            else:
                # authorize task
                auth = yield self.db_call('auth_authorize_task',key=passkey)
            if isinstance(auth,Exception) or auth is not True:
                self.json_error({'code':403,'message':'Not Authorized',
                                 'data':'passkey invalid'})
                return

        # check for args and kwargs
        if 'args' in params:
            args = params.pop('args')
        else:
            args = []

        # call method on DB if exists
        try:
            ret = yield self.db_call(method,*args,**params)

        except AttributeError:
            self.json_error({'code':-32601,'message':'Method not found'})
            return
        except Exception:
            logger.info('error in DB method',exc_info=True)
            self.json_error({'code':-32000,'message':'Server error'},
                            status=500, id=id)
            return
        if isinstance(ret,Exception):
            self.json_error({'code':-32000,'message':'Server error',
                             'data':str(ret)}, status=500, id=id)
        else:
            # return response
            self.write({'jsonrpc':'2.0','result':ret,'id':id})

    def json_error(self,error,status=400,id=None):
        """Create a proper jsonrpc error message"""
        self.set_status(status)
        if isinstance(error,Exception):
            error = str(error)
        logger.info('json_error: %r',error)
        self.write({'jsonrpc':'2.0','error':error,'id':id})
        self.finish()

class LibHandler(MyHandler):
    """Handler for iceprod library downloads.

       These are straight http downloads like normal.
    """

    def initialize(self, prefix, directory, **kwargs):
        """
        Get some params from the website module

        :param messaging: the messaging handle
        :param fileio: AsyncFileIO object
        :param prefix: library url prefix
        :param directory: library directory on disk
        """
        super(LibHandler,self).initialize(**kwargs)
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

class PublicHandler(MyHandler):
    """Handler for public facing website"""
    def render_handle(self,*args,**kwargs):
        """Handle renderer exceptions properly"""
        try:
            self.render(*args,**kwargs)
        except Exception as e:
            logger.error('render error',exc_info=True)
            self.send_error(message='render error')

    def get_template_namespace(self):
        namespace = super(MyHandler,self).get_template_namespace()
        namespace['version'] = iceprod.__version__
        namespace['section'] = self.request.uri.lstrip('/').split('?')[0].split('/')[0]
        namespace['master'] = ('master' in self.cfg and
                               'status' in self.cfg['master'] and
                               self.cfg['master']['status'])
        namespace['master_url'] = ('master' in self.cfg and
                                   'url' in self.cfg['master'] and
                                   self.cfg['master']['url'])
        namespace['site_id'] = (self.cfg['site_id'] if 'site_id' in self.cfg else None)
        namespace['sites'] = (self.cfg['webserver']['sites'] if (
                              'webserver' in self.cfg and
                              'sites' in self.cfg['webserver']) else None)
        return namespace

    def get_current_user(self):
        return self.get_secure_cookie("user")

    def write_error(self,status_code=500,**kwargs):
        """Write out custom error page."""
        self.set_status(status_code)
        if status_code >= 500:
            self.write('<h2>Internal Error</h2>')
        else:
            self.write('<h2>Request Error</h2>')
        if 'message' in kwargs:
            self.write('<br />'.join(kwargs['message'].split('\n')))
        self.finish()

    @contextmanager
    def catch_error(self,message='Error generating page'):
        """Context manager for catching, logging, and displaying errors."""
        try:
            yield
        except Exception as e:
            logger.warn('Error in public website',exc_info=True)
            if self.debug:
                message = message + '\n' + str(e)
            self.write_error(500,message=message)

class Default(PublicHandler):
    """Handle / urls"""
    @tornado.gen.coroutine
    def get(self):
        with self.catch_error():
            datasets = yield self.db_call('web_get_datasets',groups=['status'])
            if isinstance(datasets,Exception):
                raise datasets
            if not datasets:
                logger.info('no datasets to display: %r',datasets)
                datasets = [] # set to iterable to prevent None error
            self.render_handle('main.html',datasets=datasets)

class Submit(PublicHandler):
    """Handle /submit urls"""
    @tornado.web.authenticated
    @tornado.gen.coroutine
    def get(self):
        with self.catch_error(message='error generating submit page'):
            url = self.request.uri[1:]
            passkey = yield self.db_call('auth_new_passkey')
            if isinstance(passkey,Exception):
                raise passkey
            grids = yield self.db_call('web_get_gridspec')
            if isinstance(grids,Exception):
                raise grids
            render_args = {
                'passkey':passkey,
                'grids':grids,
                'edit':False,
                'dataset':None,
                'config':None,
            }
            self.render_handle('submit.html',**render_args)

class Config(PublicHandler):
    """Handle /submit urls"""
    @tornado.web.authenticated
    @tornado.gen.coroutine
    def get(self):
        with self.catch_error(message='error generating config page'):
            dataset_id = self.get_argument('dataset_id',default=None)
            if not dataset_id:
                self.write_error(400,message='must provide dataset_id')
                return
            dataset = yield self.db_call('web_get_datasets_details',dataset_id=dataset_id)
            if isinstance(dataset,Exception):
                raise dataset
            if dataset_id not in dataset:
                raise Exception('get_dataset_details does not have dataset_id '+dataset_id)
            dataset = dataset[dataset_id]
            edit = self.get_argument('edit',default=False)
            if edit:
                passkey = yield self.db_call('auth_new_passkey')
                if isinstance(passkey,Exception):
                    raise passkey
            else:
                passkey = None
            config = yield self.db_call('queue_get_cfg_for_dataset',dataset_id=dataset_id)
            if isinstance(config,Exception):
                raise config
            render_args = {
                'edit':edit,
                'passkey':passkey,
                'grids':None,
                'dataset':dataset,
                'config':config,
            }
            self.render_handle('submit.html',**render_args)

class Site(PublicHandler):
    """Handle /site urls"""
    @tornado.web.authenticated
    @tornado.gen.coroutine
    def get(self,url):
        #self.write_error(404,message='Not yet implemented')
        #return
        with self.catch_error(message='error generating site page'):
            if url:
                url_parts = [x for x in url.split('/') if x]

            def cb(m):
                print(m)

            ret = yield self.daemon_call('get_running_modules')

            if isinstance(ret,Exception):
                raise ret

            available_modules = {}
            for mod in iceprod.server.listmodules('iceprod.server.modules'):
                mod_name = mod.rsplit('.',1)[1]
                available_modules[mod_name] = mod


            module_state = []
            for mod in available_modules.keys():
                state = mod in ret
                module_state.append([mod, state])
            #self.messaging.daemon.get_running_modules(callback=cb)
            #print('11111')

            passkey = yield self.db_call('auth_new_passkey')
            if isinstance(passkey,Exception):
                raise passkey

            config = yield self.config_call('get_config_string')

            self.render_handle('site.html', url = url[1:], modules = module_state, passkey=passkey, config = config)
            '''
            filter_options = {}
            filter_results = {n:self.get_arguments(n) for n in filter_options}
            if url and url_parts:
                site_id = url_parts[0]
                ret = yield self.db_call('web_get_site_details',dataset_id=dataset_id)
                if isinstance(ret,Exception):
                    raise ret
                if ret:
                    site = ret.values()[0]
                else:
                    site = None
                tasks = yield self.db_call('web_get_tasks_by_status',site_id=site_id)
                if isinstance(tasks,Exception):
                    raise tasks
                self.render_handle('site_detail.html',site_id=site_id,
                                   site=site,tasks=tasks)
            else:
                sites = yield self.db_call('web_get_sites',**filter_results)
                if isinstance(sites,Exception):
                    raise sites
                self.render_handle('site_browse.html',sites=sites,
                                   filter_options=filter_options,
                                   filter_results=filter_results)
            '''

class Dataset(PublicHandler):
    """Handle /dataset urls"""
    @tornado.gen.coroutine
    def get(self,url):
        with self.catch_error(message='error generating dataset page'):
            if url:
                url_parts = [x for x in url.split('/') if x]
            filter_options = {'status':DBAPI.status_options['dataset']}
            filter_results = {n:self.get_arguments(n) for n in filter_options}
            if url and url_parts:
                dataset_id = url_parts[0]
                if dataset_id.isdigit():
                    try:
                        if int(dataset_id) < 10000000:
                            ret = yield self.db_call('web_get_dataset_by_name',
                                                     name=dataset_id)
                            if ret and not isinstance(ret,Exception):
                                dataset_id = ret
                    except:
                        pass
                ret = yield self.db_call('web_get_datasets_details',dataset_id=dataset_id)
                if isinstance(ret,Exception):
                    raise ret
                if ret:
                    dataset = ret.values()[0]
                else:
                    dataset = None
                tasks = yield self.db_call('web_get_tasks_by_status',dataset_id=dataset_id)
                task_info = yield self.db_call('web_get_task_completion_stats', dataset_id=dataset_id)
                task_info2 = []
                for t in task_info:
                    requirements = tornado.escape.json_decode(t[1])
                    type = 'CPU'
                    if 'gpu' in requirements and requirements['gpu']: type = 'GPU'
                    q = 0
                    r = 0
                    task_info2.append([t[0], type, q, r])
                if isinstance(tasks,Exception):
                    raise tasks
                self.render_handle('dataset_detail.html',dataset_id=dataset_id,
                                   dataset=dataset,tasks=tasks,task_info=task_info2)
            else:
                datasets = yield self.db_call('web_get_datasets',**filter_results)
                if isinstance(datasets,Exception):
                    raise datasets
                self.render_handle('dataset_browse.html',datasets=datasets,
                                   filter_options=filter_options,
                                   filter_results=filter_results)

class Task(PublicHandler):
    """Handle /task urls"""
    @tornado.gen.coroutine
    def get(self,url):
        with self.catch_error(message='error generating dataset page'):
            if url:
                url_parts = [x for x in url.split('/') if x]
            dataset_id = self.get_argument('dataset_id',default=None)
            status = self.get_argument('status',default=None)

            passkey = yield self.db_call('auth_new_passkey')
            if isinstance(passkey,Exception):
                raise passkey

            if url and url_parts:
                if dataset_id and dataset_id.isdigit():
                    try:
                        if int(dataset_id) < 10000000:
                            ret = yield self.db_call('web_get_dataset_by_name',
                                                     name=dataset_id)
                            if ret and not isinstance(ret,Exception):
                                dataset_id = ret
                    except:
                        pass
                task_id = url_parts[0]
                ret = yield self.db_call('web_get_tasks_details',task_id=task_id,
                                         dataset_id=dataset_id)
                if isinstance(ret,Exception):
                    raise ret
                if ret:
                    task_details = ret.values()[0]
                else:
                    task_details = None
                logs = yield self.db_call('web_get_logs',task_id=task_id,lines=40) #TODO: make lines adjustable
                if isinstance(logs,Exception):
                    raise logs
                self.render_handle('task_detail.html',task=task_details,logs=logs,passkey=passkey)
            elif status:

                tasks = yield self.db_call('web_get_tasks_details',status=status,
                                           dataset_id=dataset_id)
                if isinstance(tasks,Exception):
                    raise tasks
                self.render_handle('task_browse.html',tasks=tasks, passkey=passkey)
            else:
                status = yield self.db_call('web_get_tasks_by_status',dataset_id=dataset_id)
                if isinstance(status,Exception):
                    raise status
                self.render_handle('tasks.html',status=status)

class Documentation(PublicHandler):
    def get(self, url):
        doc_path = get_pkgdata_filename('iceprod.server','data/docs')
        self.write(documentation.load_doc(doc_path+'/' + url))
        self.flush()

class Log(PublicHandler):
    @tornado.gen.coroutine
    def get(self, url, log):
        logs = yield self.db_call('web_get_logs',task_id=url)
        log_text = logs[log]
        html = '<html><body>'
        html += log_text.replace('\n', '<br/>')
        html += '</body></html>'
        self.write(html)
        self.flush()


class Help(PublicHandler):
    """Help Page"""
    def get(self):
        with self.catch_error(message='error generating site page'):
            self.render_handle('help.html')



class Other(PublicHandler):
    """Handle any other urls - this is basically all 404"""
    def get(self):
        path = self.request.path
        self.set_status(404)
        self.render_handle('404.html',path=path)

class Login(PublicHandler):
    """Handle the login url"""
    def get(self):
        n = self.get_argument('next', default='/')
        if 'password' in self.cfg['webserver']:
            self.render_handle('login.html', status=None, next=n)
        else:
            self.set_secure_cookie('user', 'admin', expires_days=1)
            self.redirect(n)

    def post(self):
        n = self.get_argument('next', default='/')
        if ('password' in self.cfg['webserver'] and
            self.get_argument('pwd') == self.cfg['webserver']['password']):
            self.set_secure_cookie('user', 'admin', expires_days=1)
            self.redirect(n)
        else:
            self.render_handle('login.html', status='failed', next=n)

class Logout(PublicHandler):
    def get(self):
        self.clear_cookie("user")
        self.current_user = None
        self.render_handle('logout.html', status=None)
