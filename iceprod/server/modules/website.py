"""
The website module uses `Tornado <http://www.tornadoweb.org>`_,
a fast and scalable python web server.

Main website
------------

This is the external website users will see when interacting with IceProd.
It has been broken down into several sub-handlers for easier maintenance.

"""
from __future__ import absolute_import, division, print_function

import sys
import os
import time
import random
import binascii
import socket
from threading import Thread,Event,Condition
import logging
from contextlib import contextmanager
from functools import partial, wraps
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from datetime import timedelta

from iceprod.core.jsonUtil import json_encode,json_decode

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
from iceprod.server.tornado import tornado_logger, startup

from iceprod.core import rest_client

logger = logging.getLogger('website')

class website(module.module):
    """
    The main website module.

    Run the website, which is required for anything to work.
    """

    def __init__(self,*args,**kwargs):
        # run default init
        super(website,self).__init__(*args,**kwargs)

        # set up local variables
        self.http_server = None

    def stop(self):
        """Stop website"""
        # stop tornado
        try:
            if self.http_server:
                self.http_server.stop()
        except Exception:
            logger.error('cannot stop tornado', exc_info=True)
        super(website,self).stop()

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
                    except Exception:
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

            handler_args = {
                'cfg':self.cfg,
                'modules':self.modules,
                'fileio':AsyncFileIO(executor=self.executor),
                'statsd':self.statsd,
                'rest_api':self.cfg['rest_api'],
            }
            login_handler_args = handler_args.copy()
            login_handler_args['module_rest_client'] = self.rest_client
            if 'debug' in self.cfg['webserver'] and self.cfg['webserver']['debug']:
                handler_args['debug'] = True
            if 'cookie_secret' in self.cfg['webserver']:
                cookie_secret = self.cfg['webserver']['cookie_secret']
            else:
                cookie_secret = ''.join(hex(random.randint(0,15))[-1] for _ in range(64))
                self.cfg['webserver']['cookie_secret'] = cookie_secret

            routes = [
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
                (r"/login", Login, login_handler_args),
                (r"/logout", Logout, handler_args),
                (r"/.*", Other, handler_args),
            ]
            self.application = tornado.web.Application(
                routes,
                static_path=static_path,
                template_path=template_path,
                log_function=tornado_logger,
                xsrf_cookies=True,
                cookie_secret=binascii.unhexlify(cookie_secret),
                login_url='/login',
            )

            # start tornado
            self.http_server = startup(self.application,
                port=self.cfg['webserver']['port'],
                address='0.0.0.0', # bind to all
            )
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
            logger.warning('Error in website handler', exc_info=True)
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
                    url += "?" + urlencode({'next':next_url,'secure':True})
                self.redirect(url)
                return
            raise HTTPError(403)
        return method(self, *args, **kwargs)
    return wrapper


class PublicHandler(tornado.web.RequestHandler):
    """Default Handler"""
    def initialize(self, cfg, modules, fileio, debug=False, statsd=None,
                   rest_api=None):
        """
        Get some params from the website module

        :param cfg: the global config
        :param modules: modules handle
        :param fileio: AsyncFileIO object
        :param debug: debug flag (optional)
        :param rest_api: the rest api url
        """
        self.cfg = cfg
        self.modules = modules
        self.fileio = fileio
        self.debug = debug
        self.statsd = statsd
        self.rest_api = rest_api
        self.current_user = None
        self.rest_client = None

    def set_default_headers(self):
        self._headers['Server'] = 'IceProd/' + iceprod.__version__

    def get_template_namespace(self):
        namespace = super(PublicHandler,self).get_template_namespace()
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

    def prepare(self):
        try:
            data = self.get_secure_cookie("user", max_age_days=1)
            if not data:
                raise Exception('user cookie is missing/empty')
            data = json_decode(data)
            user_secure = self.get_secure_cookie("user_secure", max_age_days=0.01)
            if user_secure is not None and data['username'] != user_secure:
                raise Exception('mismatch between user_secure and username')
            self.current_user = data['username']
            self.current_user_data = data
            self.current_user_secure = (user_secure is not None)
            self.rest_client = rest_client.Client(self.rest_api, data['token'])
        except Exception:
            logger.info('error getting current user', exc_info=True)
            self.clear_cookie("user")
            self.clear_cookie("user_secure")
            self.current_user = None

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
    @tornado.web.authenticated
    async def get(self):
        self.statsd.incr('default')
        ret = await self.rest_client.request('POST','/dataset_summaries/status')
        datasets = {k:len(ret[k]) for k in ret}
        self.render('main.html',datasets=datasets)

class Submit(PublicHandler):
    """Handle /submit urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self):
        self.statsd.incr('submit')
        url = self.request.uri[1:]
        ret = await self.rest_client.request('POST','/create_token')
        token = ret['result']
        grids = await self.db_call('web_get_gridspec')
        if isinstance(grids,Exception):
            raise grids
        render_args = {
            'passkey':token,
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
                except Exception:
                    pass
            if not ret:
                ret = yield self.db_call('web_get_datasets_details',dataset_id=dataset_id)
                if isinstance(ret,Exception):
                    raise ret
                dataset_num = GlobalID.localID_ret(dataset_id,type='int')
            if ret:
                dataset = list(ret.values())[0]
            else:
                raise Exception('dataset not found')

            passkey = yield self.db_call('auth_new_passkey')
            if isinstance(passkey,Exception):
                raise passkey

            jobs = yield self.db_call('web_get_job_counts_by_status',
                                      dataset_id=dataset_id)
            tasks = yield self.db_call('web_get_tasks_by_status',
                                       dataset_id=dataset_id)
            task_info = yield self.db_call('web_get_task_completion_stats', dataset_id=dataset_id)
            self.render('dataset_detail.html',dataset_id=dataset_id,dataset_num=dataset_num,
                        dataset=dataset,jobs=jobs,tasks=tasks,task_info=task_info,passkey=passkey)
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
                        try_dataset_id = GlobalID.globalID_gen(int(dataset_id),self.cfg['site_id'])
                        ret = yield self.db_call('web_get_datasets_details',
                                                 dataset_id=try_dataset_id)
                        if isinstance(ret,Exception):
                            ret = None
                        elif ret:
                            dataset_num = dataset_id
                            dataset_id = try_dataset_id
                except Exception:
                    pass
            task_id = url_parts[0]
            ret = yield self.db_call('web_get_tasks_details',task_id=task_id,
                                     dataset_id=dataset_id)
            if ret:
                task_details = list(ret.values())[0]
            else:
                task_details = None
            logs = yield self.db_call('web_get_logs',task_id=task_id,lines=40) #TODO: make lines adjustable
            del task_details['task_status'] # task_status and status are repeats. Remove task_status.
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
                        try_dataset_id = GlobalID.globalID_gen(int(dataset_id),self.cfg['site_id'])
                        ret = yield self.db_call('web_get_datasets_details',
                                                 dataset_id=try_dataset_id)
                        if isinstance(ret,Exception):
                            ret = None
                        elif ret:
                            dataset_num = dataset_id
                            dataset_id = try_dataset_id
                except Exception:
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
    def initialize(self, module_rest_client, *args, **kwargs):
        """
        Get some params from the website module

        :param module_rest_client: a REST Client
        """
        super(Login, self).initialize(*args, **kwargs)
        self.module_rest_client = module_rest_client

    @catch_error
    def get(self):
        self.statsd.incr('login')
        n = self.get_argument('next', default='/')
        secure = self.get_argument('secure', default=None)
        self.clear_cookie("user")
        self.clear_cookie("user_secure")
        self.render('login.html', status=None, next=n)

    @catch_error
    async def post(self):
        n = self.get_argument('next', default='/')
        secure = self.get_argument('secure', default=None)
        username = self.get_argument('username')
        password = self.get_argument('password')
        self.clear_cookie("user")
        self.clear_cookie("user_secure")
        try:
            data = await self.module_rest_client.request('POST','/ldap',{'username':username,'password':password})
            cookie = json_encode(data)
            if secure:
                self.set_secure_cookie('user_secure', username, expires_days=0.01)
            self.set_secure_cookie('user', cookie, expires_days=1)
            self.redirect(n)
        except Exception:
            logger.info('failed', exc_info=True)
            self.render('login.html', status='failed', next=n)

class Logout(PublicHandler):
    @catch_error
    def get(self):
        self.statsd.incr('logout')
        self.clear_cookie("user")
        self.clear_cookie("user_secure")
        self.current_user = None
        self.render('logout.html', status=None)
