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
from functools import partial, wraps
from urlparse import urlparse
from datetime import timedelta

# override tornado json encoder and decoder so we can use dataclasses objects
import iceprod.core.jsonUtil
import tornado.escape
tornado.escape.json_encode = iceprod.core.jsonUtil.json_encode
tornado.escape.json_decode = iceprod.core.jsonUtil.json_decode

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen

import tornado.concurrent
import concurrent.futures

import iceprod
from iceprod.server import GlobalID, get_pkgdata_filename
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
        self.service['logrotate'] = self.logrotate

        # set up local variables
        self.nginx = None
        self.http_server = None

    def stop(self):
        """Stop website"""
        # stop nginx
        try:
            if self.nginx:
                self.nginx.stop()
        except:
            logger.error('cannot stop Nginx', exc_info=True)
        # stop tornado
        try:
            if self.http_server:
                self.http_server.stop()
        except:
            logger.error('cannot stop tornado', exc_info=True)
        super(website,self).stop()

    def kill(self):
        """Kill website"""
        # kill nginx
        try:
            if self.nginx:
                self.nginx.kill()
        except:
            logger.error('cannot kill Nginx', exc_info=True)
        super(website,self).kill()

    def logrotate(self):
        """Rotate the Nginx logs."""
        logger.warn('got a logrotate() call')
        try:
            if self.nginx:
                # rotate nginx logs
                self.nginx.logrotate()
            # tornado uses regular python logs, which rotate automatically
        except:
            logger.warn('error in logrotate', exc_info=True)

    def start(self):
        """Run the website"""
        super(website,self).start()

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
            if static_path is None or not os.path.exists(static_path):
                logger.info('static path: %r',static_path)
                raise Exception('bad static path')
            template_path = get_pkgdata_filename('iceprod.server','data/www_templates')
            if template_path is None or not os.path.exists(template_path):
                logger.info('template path: %r',template_path)
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
                'modules':self.modules,
                'fileio':AsyncFileIO(executor=self.executor),
                'statsd':self.statsd,
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
            self.application = tornado.web.Application([
                (r"/jsonrpc", JSONRPCHandler, handler_args),
                (r"/lib/.*", LibHandler, lib_args),
                (r"/", Default, handler_args),
                (r"/submit", Submit, handler_args),
                (r"/config", Config, handler_args),
                (r"/dataset(/.*)?", Dataset, handler_args),
                (r"/task(/.*)?", Task, handler_args),
                (r"/job(/.*)?", Job, handler_args),
                (r"/site(/.*)?", Site, handler_args),
                (r"/help", Help, handler_args),
                (r"/docs/(.*)", Documentation, handler_args),
                (r"/log/(.*)/(.*)", Log, handler_args),
                (r"/groups", GroupsHandler, handler_args),
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
            raise


def catch_error(method):
    """Decorator to catch and handle errors on handlers"""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            self.statsd.incr(self.__class__.__name__+'.error')
            logger.warn('Error in website handler', exc_info=True)
            message = 'Error generating page for '+self.__class__.__name__
            if self.debug:
                message = message + '\n' + str(e)
            self.send_error(500, message=message)
    return wrapper

def authenticated_secure(method):
    """Decorate methods with this to require that the user be logged in
    to a secure area.

    If the user is not logged in, they will be redirected to the configured
    `login url <RequestHandler.get_login_url>`.

    If you configure a login url with a query parameter, Tornado will
    assume you know what you're doing and use it as-is.  If not, it
    will add a `next` parameter so the login page knows where to send
    you once you're logged in.
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.current_user_secure:
            if self.request.method in ("GET", "HEAD"):
                url = self.get_login_url()
                if "?" not in url:
                    if urlparse.urlsplit(url).scheme:
                        # if login url is absolute, make next absolute too
                        next_url = self.request.full_url()
                    else:
                        next_url = self.request.uri
                    url += "?" + urlencode(dict(next=next_url,secure=True))
                self.redirect(url)
                return
            raise HTTPError(403)
        return method(self, *args, **kwargs)
    return wrapper


class MyHandler(tornado.web.RequestHandler):
    """Default Handler"""
    def initialize(self, cfg, modules, fileio, debug=False, statsd=None):
        """
        Get some params from the website module

        :param cfg: the global config
        :param modules: modules handle
        :param fileio: AsyncFileIO object
        :param debug: debug flag (optional)
        """
        self.cfg = cfg
        self.modules = modules
        self.fileio = fileio
        self.debug = debug
        self.statsd = statsd

    @tornado.gen.coroutine
    def db_call(self,func_name,*args,**kwargs):
        """Make a database call, returning the result"""
        logger.info('db_call for %s',func_name)
        try:
            f = self.modules['db'][func_name](*args,**kwargs)
            if isinstance(f, (tornado.concurrent.Future, concurrent.futures.Future)):
                f = yield tornado.gen.with_timeout(timedelta(seconds=120),f)
        except:
            logger.warn('db_call error for %s',func_name,exc_info=True)
            raise
        raise tornado.gen.Return(f)

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
            self.json_error({'code':-32700,'message':'Parse Error',
                             'data':'invalid json'})

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

        self.statsd.incr('jsonrpc.'+request['method'])

        # add rpc_ to method name to prevent calling other DB methods
        method = 'rpc_'+request['method']
        if 'params' in request:
            params = request['params']
        else:
            params = {}
        if 'id' in request:
            request_id = request['id']
        else:
            request_id = None

        if not method.startswith("rpc_public"):
            # check for auth
            if (isinstance(params,dict) and 'passkey' not in params) or (not params):
                self.json_error({'code':403,'message':'Not Authorized',
                                 'data':'missing passkey'},
                                request_id=request_id)
                return
            passkey = params.pop('passkey') if isinstance(params,dict) else params.pop(0)

            try:
                if ((isinstance(params,dict) and 'site_id' in params) or
                    isinstance(params,(tuple,list))):
                    # authorize site
                    site_id = params.pop('site_id') if isinstance(params,dict) else params.pop(0)
                    yield self.db_call('auth_authorize_site',site_id=site_id,key=passkey)
                else:
                    # authorize task
                    yield self.db_call('auth_authorize_task',key=passkey)
            except:
                logger.info('auth error', exc_info=True)
                self.json_error({'code':403,'message':'Not Authorized',
                                 'data':'passkey invalid'},
                                request_id=request_id)
                return

        # check for args and kwargs
        if isinstance(params,dict):
            args = params.pop('args') if 'args' in params else []
        else:
            args = params
            params = {}

        # call method on DB if exists
        try:
            ret = yield self.db_call(method,*args,**params)
        except KeyError:
            logger.info('DB method not found: %r', method)
            self.json_error({'code':-32601,'message':'Method not found'},
                            request_id=request_id)
        except tornado.gen.TimeoutError:
            logger.info('Timeout error in DB method: %r', method, exc_info=True)
            self.json_error({'code':-32001,'message':'Server error'},
                            status=503, request_id=request_id)
        except Exception:
            logger.info('error in DB method: %r', method, exc_info=True)
            self.json_error({'code':-32000,'message':'Server error'},
                            status=500, request_id=request_id)
        else:
            if request_id is not None:
                logger.debug('jsonrpc response: %r', ret)
                self.write({'jsonrpc':'2.0', 'result':ret, 'id':request_id})

    def json_error(self,error,status=400,request_id=None):
        """Create a proper jsonrpc error message"""
        self.statsd.incr('jsonrpc_error')
        self.set_status(status)
        if isinstance(error,Exception):
            error = str(error)
        logger.info('json_error: %r',error)
        if request_id is not None:
            self.write({'jsonrpc':'2.0', 'error':error, 'id':request_id})

class LibHandler(MyHandler):
    """Handler for iceprod library downloads.

       These are straight http downloads like normal.
    """

    def initialize(self, prefix, directory, **kwargs):
        """
        Get some params from the website module

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
        self.statsd.incr('lib')
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
        user = self.get_secure_cookie("user", max_age_days=1)
        user_secure = self.get_secure_cookie("user_secure", max_age_days=0.01)
        self.current_user_secure = (user_secure is not None)
        if user_secure is None or user == user_secure:
            return user
        else:
            return None

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

class Default(PublicHandler):
    """Handle / urls"""
    @catch_error
    @tornado.gen.coroutine
    def get(self):
        self.statsd.incr('default')
        datasets = yield self.db_call('web_get_datasets',groups=['status'])
        if isinstance(datasets,Exception):
            raise datasets
        if not datasets:
            logger.info('no datasets to display: %r',datasets)
            datasets = [] # set to iterable to prevent None error
        self.render('main.html',datasets=datasets)

class Submit(PublicHandler):
    """Handle /submit urls"""
    @catch_error
    @tornado.web.authenticated
    @tornado.gen.coroutine
    def get(self):
        self.statsd.incr('submit')
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
        self.render('submit.html',**render_args)

class Config(PublicHandler):
    """Handle /submit urls"""
    @catch_error
    @tornado.web.authenticated
    @tornado.gen.coroutine
    def get(self):
        self.statsd.incr('config')
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
        self.render('submit.html',**render_args)

class Site(PublicHandler):
    """Handle /site urls"""
    @catch_error
    @tornado.web.authenticated
    @tornado.gen.coroutine
    def get(self,url):
        self.statsd.incr('site')
        if url:
            url_parts = [x for x in url.split('/') if x]

        def cb(m):
            print(m)

        running_modules = self.modules['daemon']['get_running_modules']()

        module_state = []
        for mod in self.cfg['modules']:
            state = mod in running_modules
            module_state.append([mod, state])

        passkey = yield self.db_call('auth_new_passkey')
        if isinstance(passkey,Exception):
            raise passkey

        config = self.config.save_to_string()

        self.render('site.html', url = url[1:], modules = module_state, passkey=passkey, config = config)
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
            self.render('site_detail.html',site_id=site_id,
                               site=site,tasks=tasks)
        else:
            sites = yield self.db_call('web_get_sites',**filter_results)
            if isinstance(sites,Exception):
                raise sites
            self.render('site_browse.html',sites=sites,
                               filter_options=filter_options,
                               filter_results=filter_results)
        '''

class Dataset(PublicHandler):
    """Handle /dataset urls"""
    @catch_error
    @tornado.gen.coroutine
    def get(self,url):
        self.statsd.incr('dataset')
        if url:
            url_parts = [x for x in url.split('/') if x]
        filter_options = {'status':DBAPI.status_options['dataset']}
        filter_results = {n:self.get_arguments(n) for n in filter_options}
        if url and url_parts:
            dataset_id = url_parts[0]
            ret = None
            if dataset_id.isdigit():
                try:
                    if int(dataset_id) < 10000000:
                        try_dataset_id = GlobalID.globalID_gen(int(dataset_id),self.cfg['site_id'])
                        ret = yield self.db_call('web_get_datasets_details',
                                                 dataset_id=try_dataset_id)
                        if isinstance(ret,Exception):
                            ret = None
                        elif ret:
                            dataset_num = dataset_id
                            dataset_id = try_dataset_id
                except:
                    pass
            if not ret:
                ret = yield self.db_call('web_get_datasets_details',dataset_id=dataset_id)
                if isinstance(ret,Exception):
                    raise ret
                dataset_num = GlobalID.localID_ret(dataset_id,type='int')
            if ret:
                dataset = ret.values()[0]
            else:
                raise Exception('dataset not found')

            passkey = yield self.db_call('auth_new_passkey')
            if isinstance(passkey,Exception):
                raise passkey

            tasks = yield self.db_call('web_get_tasks_by_status',dataset_id=dataset_id)
            task_info = yield self.db_call('web_get_task_completion_stats', dataset_id=dataset_id)
            self.render('dataset_detail.html',dataset_id=dataset_id,dataset_num=dataset_num,
                        dataset=dataset,tasks=tasks,task_info=task_info,passkey=passkey)
        else:
            datasets = yield self.db_call('web_get_datasets',**filter_results)
            if isinstance(datasets,Exception):
                raise datasets
            self.render('dataset_browse.html',datasets=datasets,
                        filter_options=filter_options,
                        filter_results=filter_results)

class Task(PublicHandler):
    """Handle /task urls"""
    @catch_error
    @tornado.gen.coroutine
    def get(self,url):
        self.statsd.incr('task')
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
            self.render('task_detail.html',task=task_details,logs=logs,passkey=passkey)
        elif status:

            tasks = yield self.db_call('web_get_tasks_details',status=status,
                                       dataset_id=dataset_id)
            if isinstance(tasks,Exception):
                raise tasks
            self.render('task_browse.html',tasks=tasks, passkey=passkey)
        else:
            status = yield self.db_call('web_get_tasks_by_status',dataset_id=dataset_id)
            if isinstance(status,Exception):
                raise status
            self.render('tasks.html',status=status)

class Job(PublicHandler):
    """Handle /job urls"""
    @catch_error
    @tornado.gen.coroutine
    def get(self,url):
        self.statsd.incr('job')
        if url:
            url_parts = [x for x in url.split('/') if x]
        dataset_id = self.get_argument('dataset_id',default=None)
        status = self.get_argument('status',default=None)

        passkey = yield self.db_call('auth_new_passkey')
        if isinstance(passkey,Exception):
            raise passkey

        if url and url_parts:
            job_id = url_parts[0]
            ret = yield self.db_call('web_get_jobs_details',job_id=job_id)
            if isinstance(ret,Exception):
                raise ret
            if ret:
                job_details = ret
            else:
                job_details = {}
            self.render('job_detail.html', job=job_details, passkey=passkey)
        else:
            if dataset_id and dataset_id.isdigit():
                try:
                    if int(dataset_id) < 10000000:
                        ret = yield self.db_call('web_get_dataset_by_name',
                                                 name=dataset_id)
                        if ret and not isinstance(ret,Exception):
                            dataset_id = ret
                except:
                    pass
            jobs = yield self.db_call('web_get_jobs_by_status', status=status,
                                       dataset_id=dataset_id)
            if isinstance(jobs,Exception):
                raise jobs
            self.render('job_browse.html', jobs=jobs, passkey=passkey)

class Documentation(PublicHandler):
    @catch_error
    def get(self, url):
        self.statsd.incr('documentation')
        doc_path = get_pkgdata_filename('iceprod.server','data/docs')
        self.write(documentation.load_doc(doc_path+'/' + url))
        self.flush()

class Log(PublicHandler):
    @catch_error
    @tornado.gen.coroutine
    def get(self, url, log):
        self.statsd.incr('log')
        logs = yield self.db_call('web_get_logs',task_id=url)
        log_text = logs[log]
        html = '<html><body>'
        html += log_text.replace('\n', '<br/>')
        html += '</body></html>'
        self.write(html)
        self.flush()

class GroupsHandler(PublicHandler):
    """View/modify groups"""
    @catch_error
    @tornado.gen.coroutine
    def get(self):
        render_args = {
            'edit': True if self.current_user else False,
        }
        render_args['groups'] = yield self.db_call('rpc_get_groups')
        if render_args['edit']:
            passkey = yield self.db_call('auth_new_passkey')
            if isinstance(passkey,Exception):
                raise passkey
            render_args['passkey'] = passkey
        self.render('groups.html', **render_args)

class UserAccount(PublicHandler):
    """View/modify a user account"""
    @catch_error
    @authenticated_secure
    @tornado.gen.coroutine
    def get(self):
        username = self.get_argument('username', default=self.current_user)
        account = yield self.db_call('website_get_user_account')
        if isinstance(account, Exception):
            raise account
        self.render('user_account.html', account=account)

    @catch_error
    @authenticated_secure
    @tornado.gen.coroutine
    def post(self):
        username = self.get_argument('username', default=self.current_user)
        password = self.get_argument('password', default=None)
        if not password:
            raise Exception('invalid password')
        ret = yield self.db_call('website_edit_user_account', password=password)
        if isinstance(ret, Exception):
            raise ret
        self.get()

class Help(PublicHandler):
    """Help Page"""
    @catch_error
    def get(self):
        self.statsd.incr('help')
        self.render('help.html')

class Other(PublicHandler):
    """Handle any other urls - this is basically all 404"""
    @catch_error
    def get(self):
        self.statsd.incr('other')
        path = self.request.path
        self.set_status(404)
        self.render('404.html',path=path)

class Login(PublicHandler):
    """Handle the login url"""
    @catch_error
    def get(self):
        self.statsd.incr('login')
        n = self.get_argument('next', default='/')
        secure = self.get_argument('secure', default=None)
        if 'password' in self.cfg['webserver']:
            self.render('login.html', status=None, next=n)
        else:
            # TODO: remove this entirely
            if secure:
                self.set_secure_cookie('user_secure', 'admin', expires_days=0.01)
            self.set_secure_cookie('user', 'admin', expires_days=1)
            self.redirect(n)

    @catch_error
    def post(self):
        n = self.get_argument('next', default='/')
        secure = self.get_argument('secure', default=None)
        if ('password' in self.cfg['webserver'] and
            self.get_argument('pwd') == self.cfg['webserver']['password']):
            if secure:
                self.set_secure_cookie('user_secure', 'admin', expires_days=0.01)
            self.set_secure_cookie('user', 'admin', expires_days=1)
            self.redirect(n)
        else:
            self.render('login.html', status='failed', next=n)

class Logout(PublicHandler):
    @catch_error
    def get(self):
        self.statsd.incr('logout')
        self.clear_cookie("user")
        self.clear_cookie("user_secure")
        self.current_user = None
        self.render('logout.html', status=None)
