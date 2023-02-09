"""
The website module uses `Tornado <http://www.tornadoweb.org>`_,
a fast and scalable python web server.

Main website
------------

This is the external website users will see when interacting with IceProd.
It has been broken down into several sub-handlers for easier maintenance.

"""

import os
import random
import logging
from collections import defaultdict

from iceprod.core.jsonUtil import json_encode

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.gen

import tornado.concurrent
from rest_tools.client import RestClient
from rest_tools.server import (catch_error, RestServer, RestHandlerSetup, RestHandler,
                               OpenIDLoginHandler, OpenIDWebHandlerMixin, KeycloakUsernameMixin)
from rest_tools import telemetry as wtt

import iceprod
from iceprod.server import get_pkgdata_filename
from iceprod.server import module
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

        # set up local variables
        self.http_server = None

    async def stop(self):
        """Stop website"""
        # stop tornado
        try:
            if self.http_server:
                await self.http_server.stop()
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

            if 'url' in self.cfg['rest_api']:
                rest_address = self.cfg['rest_api']['url']
            else:
                # for local systems
                rest_address = 'http://{}:{}'.format(
                    self.cfg['rest_api']['address'],
                    self.cfg['rest_api']['port'],
                )

            rest_cfg = {
                'debug': False,
                'server_header': 'IceProd/' + iceprod.__version__,
            }
            if os.environ.get('CI_TESTING', None):
                logger.warning('CI TESTING MODE!')
                rest_cfg['auth'] = {
                    'secret': 'secret'
                }
            else:
                if not ('rest_api' in self.cfg and 'oauth_url' in self.cfg['rest_api']
                        and 'oauth_client_id' in self.cfg['rest_api']
                        and 'oauth_client_secret' in self.cfg['rest_api']):
                    raise Exception('must set oauth_ params in cfg[rest_api]')
                rest_cfg['auth'] = {
                    'audience': self.cfg['rest_api'].get('oauth_audience', 'iceprod'),
                    'openid_url': self.cfg['rest_api']['oauth_url'],
                }
            handler_args = RestHandlerSetup(rest_cfg)

            login_handler_args = handler_args.copy()
            if not os.environ.get('CI_TESTING', None):
                login_handler_args['oauth_client_id'] = self.cfg['rest_api']['oauth_client_id']
                login_handler_args['oauth_client_secret'] = self.cfg['rest_api']['oauth_client_secret']
                login_handler_args['oauth_client_scope'] = 'offline_access posix profile'

            handler_args.update({
                'cfg': self.cfg,
                'modules': self.modules,
                'statsd': self.statsd,
                'rest_api': rest_address,
            })
            if 'debug' in self.cfg['webserver'] and self.cfg['webserver']['debug']:
                handler_args['debug'] = True
            if 'cookie_secret' in self.cfg['webserver']:
                cookie_secret = self.cfg['webserver']['cookie_secret']
                logger.info('using supplied cookie secret %r', cookie_secret)
            else:
                cookie_secret = ''.join(hex(random.randint(0,15))[-1] for _ in range(64))
                self.cfg['webserver']['cookie_secret'] = cookie_secret

            routes = [
                (r"/", Default, handler_args),
                (r"/submit", Submit, handler_args),
                (r"/config", Config, handler_args),
                (r"/dataset", DatasetBrowse, handler_args),
                (r"/dataset/(\w+)", Dataset, handler_args),
                (r"/dataset/(\w+)/task", TaskBrowse, handler_args),
                (r"/dataset/(\w+)/task/(\w+)", Task, handler_args),
                (r"/dataset/(\w+)/job", JobBrowse, handler_args),
                (r"/dataset/(\w+)/job/(\w+)", Job, handler_args),
                (r"/help", Help, handler_args),
                (r"/docs/(.*)", Documentation, handler_args),
                (r"/dataset/(\w+)/log/(\w+)", Log, handler_args),
                (r'/profile', Profile, handler_args),
                (r"/login", OpenIDLoginHandler, login_handler_args),
                (r"/logout", Logout, handler_args),
                (r"/.*", Other, handler_args),
            ]
            self.http_server = RestServer(
                static_path=static_path,
                template_path=template_path,
                cookie_secret=cookie_secret,
                login_url='/login',
                debug=handler_args['debug'],
            )
            for r in routes:
                self.http_server.add_route(*r)

            # start tornado
            self.http_server.startup(
                port=self.cfg['webserver']['port'],
                address='0.0.0.0',  # bind to all
            )
        except Exception:
            logger.error('website startup error',exc_info=True)
            raise


class PublicHandler(KeycloakUsernameMixin, OpenIDWebHandlerMixin, RestHandler):
    """Default Handler"""
    def initialize(self, cfg=None, modules=None, statsd=None, rest_api=None, **kwargs):
        """
        Get some params from the website module

        :param cfg: the global config
        :param modules: modules handle
        :param statsd: statsd client
        :param rest_api: the rest api url
        """
        super().initialize(**kwargs)
        self.cfg = cfg
        self.modules = modules
        self.statsd = statsd
        self.rest_api = rest_api
        self.rest_client = None

    def get_template_namespace(self):
        namespace = super().get_template_namespace()
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
        namespace['json_encode'] = json_encode
        return namespace

    def get_current_user(self):
        ret = super().get_current_user()
        try:
            if ret:
                self.rest_client = RestClient(self.rest_api, self.auth_key, timeout=50, retries=1)
        except Exception:
            pass
        return ret

    @wtt.evented(all_args=True)
    def write_error(self, status_code=500, **kwargs):
        """Write out custom error page."""
        self.set_status(status_code)
        if status_code >= 500:
            self.write('<h2>Internal Error</h2>')
        else:
            self.write('<h2>Request Error</h2>')
        if 'message' in kwargs:
            self.write('<br />'.join(kwargs['message'].split('\n')))
        elif 'reason' in kwargs:
            self.write('<br />'.join(kwargs['reason'].split('\n')))
        elif self._reason:
            self.write('<br />'.join(self._reason.split('\n')))
        self.finish()


class Default(PublicHandler):
    """Handle / urls"""
    @catch_error
    async def get(self):
        self.statsd.incr('default')
        self.render('main.html')


class Submit(PublicHandler):
    """Handle /submit urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self):
        logger.info('here')
        self.statsd.incr('submit')
        token = self.auth_key
        groups = []
        if self.auth_data and 'groups' in self.auth_data:
            groups = self.auth_data['groups']
        default_config = {
            "categories": [],
            "dataset": 0,
            "description": "",
            "difplus": None,
            "options": {},
            "parent_id": 0,
            "steering": None,
            "tasks": [],
            "version": 3
        }
        render_args = {
            'passkey':token,
            'edit':False,
            'dataset_id':'',
            'config':default_config,
            'groups':groups,
            'description':'',
        }
        self.render('submit.html',**render_args)


class Config(PublicHandler):
    """Handle /config urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self):
        self.statsd.incr('config')
        dataset_id = self.get_argument('dataset_id',default=None)
        if not dataset_id:
            self.write_error(400,message='must provide dataset_id')
            return
        dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
        edit = self.get_argument('edit',default=False)
        if edit:
            passkey = self.auth_key
        else:
            passkey = None
        config = await self.rest_client.request('GET','/config/{}'.format(dataset_id))
        render_args = {
            'edit':edit,
            'passkey':passkey,
            'dataset_id':dataset_id,
            'config':config,
            'description':dataset.get('description',''),
        }
        self.render('submit.html',**render_args)


class DatasetBrowse(PublicHandler):
    """Handle /dataset urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self):
        self.statsd.incr('dataset_browse')
        filter_options = {'status':['processing','suspended','errors','complete','truncated']}
        filter_results = {n:self.get_arguments(n) for n in filter_options}

        args = []
        for name in filter_results:
            val = filter_results[name]
            if any(v not in filter_options[name] for v in val):
                raise tornado.web.HTTPError(400, reason='Bad filter '+name+' value')
            args.append(name+'='+('|'.join(val)))

        url = '/datasets'
        if args:
            url += '?'+('&'.join(args))

        ret = await self.rest_client.request('GET', url)
        datasets = sorted(ret.values(), key=lambda x:x['dataset'], reverse=True)
        self.render('dataset_browse.html',datasets=datasets,
                    filter_options=filter_options,
                    filter_results=filter_results)


class Dataset(PublicHandler):
    """Handle /dataset urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self, dataset_id):
        self.statsd.incr('dataset')

        if dataset_id.isdigit():
            try:
                d_num = int(dataset_id)
                if d_num < 10000000:
                    all_datasets = await self.rest_client.request('GET','/datasets')
                    for d in all_datasets.values():
                        if d['dataset'] == d_num:
                            dataset_id = d['dataset_id']
                            break
            except Exception:
                pass
        try:
            dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
        except Exception:
            raise tornado.web.HTTPError(404, reason='Dataset not found')
        dataset_num = dataset['dataset']

        ret = await self.rest_client.request('POST','/create_token')
        passkey = ret['result']

        jobs = await self.rest_client.request('GET','/datasets/{}/job_counts/status'.format(dataset_id))
        tasks = await self.rest_client.request('GET','/datasets/{}/task_counts/status'.format(dataset_id))
        task_info = await self.rest_client.request('GET','/datasets/{}/task_counts/name_status'.format(dataset_id))
        task_stats = await self.rest_client.request('GET','/datasets/{}/task_stats'.format(dataset_id))
        config = await self.rest_client.request('GET','/config/{}'.format(dataset_id))
        for t in task_info:
            logger.info('task_info[%s] = %r', t, task_info[t])
            for s in ('waiting','queued','processing','complete'):
                if s not in task_info[t]:
                    task_info[t][s] = 0
            error = 0
            for s in ('reset','resume','failed'):
                if s in task_info[t]:
                    error += task_info[t][s]
            task_info[t]['error'] = error
            for task in config['tasks']:
                if 'name' in task and task['name'] == t:
                    task_info[t]['type'] = 'GPU' if 'requirements' in task and 'gpu' in task['requirements'] and task['requirements']['gpu'] else 'CPU'
                    break
            else:
                task_info[t]['type'] = 'UNK'
        self.render('dataset_detail.html',dataset_id=dataset_id,dataset_num=dataset_num,
                    dataset=dataset,jobs=jobs,tasks=tasks,task_info=task_info,task_stats=task_stats,passkey=passkey)


class TaskBrowse(PublicHandler):
    """Handle /task urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self, dataset_id):
        self.statsd.incr('task_browse')
        status = self.get_argument('status',default=None)

        if status:
            tasks = await self.rest_client.request('GET','/datasets/{}/tasks?status={}'.format(dataset_id,status))
            for t in tasks:
                if 'job_index' not in tasks[t]:
                    job = await self.rest_client.request('GET', '/datasets/{}/jobs/{}'.format(dataset_id, tasks[t]['job_id']))
                    tasks[t]['job_index'] = job['job_index']
            ret = await self.rest_client.request('POST','/create_token')
            passkey = ret['result']
            self.render('task_browse.html',tasks=tasks, passkey=passkey)
        else:
            status = await self.rest_client.request('GET','/datasets/{}/task_counts/status'.format(dataset_id))
            self.render('tasks.html',status=status)


class Task(PublicHandler):
    """Handle /task urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self, dataset_id, task_id):
        self.statsd.incr('task')
        status = self.get_argument('status', default=None)

        ret = await self.rest_client.request('POST','/create_token')
        passkey = ret['result']

        dataset = await self.rest_client.request('GET', '/datasets/{}'.format(dataset_id))
        task_details = await self.rest_client.request('GET','/datasets/{}/tasks/{}?status={}'.format(dataset_id, task_id, status))
        task_stats = await self.rest_client.request('GET','/datasets/{}/tasks/{}/task_stats?last=true'.format(dataset_id, task_id))
        if task_stats:
            task_stats = list(task_stats.values())[0]
        try:
            ret = await self.rest_client.request('GET','/datasets/{}/tasks/{}/logs?group=true'.format(dataset_id, task_id))
            logs = ret['logs']
            # logger.info("logs: %r", logs)
            ret2 = await self.rest_client.request('GET','/datasets/{}/tasks/{}/logs?keys=log_id|name|timestamp'.format(dataset_id, task_id))
            logs2 = ret2['logs']
            logger.info("logs2: %r", logs2)
            log_by_name = defaultdict(list)
            for log in sorted(logs2,key=lambda lg:lg['timestamp'] if 'timestamp' in lg else '',reverse=True):
                log_by_name[log['name']].append(log)
            for log in logs:
                if 'data' not in log or not log['data']:
                    log['data'] = ''
                log_by_name[log['name']][0] = log
        except Exception:
            log_by_name = {}
        self.render('task_detail.html', dataset=dataset, task=task_details, task_stats=task_stats, logs=log_by_name, passkey=passkey)


class JobBrowse(PublicHandler):
    """Handle /job urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self, dataset_id):
        self.statsd.incr('job')
        status = self.get_argument('status',default=None)

        ret = await self.rest_client.request('POST','/create_token')
        passkey = ret['result']

        jobs = await self.rest_client.request('GET', '/datasets/{}/jobs'.format(dataset_id))
        if status:
            for t in list(jobs):
                if jobs[t]['status'] != status:
                    del jobs[t]
                    continue
        self.render('job_browse.html', jobs=jobs, passkey=passkey)


class Job(PublicHandler):
    """Handle /job urls"""
    @catch_error
    @tornado.web.authenticated
    async def get(self, dataset_id, job_id):
        self.statsd.incr('job')
        status = self.get_argument('status',default=None)

        ret = await self.rest_client.request('POST','/create_token')
        passkey = ret['result']

        dataset = await self.rest_client.request('GET', '/datasets/{}'.format(dataset_id))
        job = await self.rest_client.request('GET', '/datasets/{}/jobs/{}'.format(dataset_id,job_id))
        tasks = await self.rest_client.request('GET','/datasets/{}/tasks?job_id={}&status={}'.format(dataset_id,job_id,status))
        job['tasks'] = list(tasks.values())
        job['tasks'].sort(key=lambda x:x['task_index'])
        self.render('job_detail.html', dataset=dataset, job=job, passkey=passkey)


class Documentation(PublicHandler):
    @catch_error
    async def get(self, url):
        self.statsd.incr('documentation')
        doc_path = get_pkgdata_filename('iceprod.server','data/docs')
        full_path = os.path.join(doc_path, url)
        if not full_path.startswith(doc_path):
            self.set_status(404)
            self.render('404.html', path='bad docs path')
            return
        self.write(documentation.load_doc(full_path))
        self.flush()


class Log(PublicHandler):
    @catch_error
    @tornado.web.authenticated
    async def get(self, dataset_id, log_id):
        self.statsd.incr('log')
        ret = await self.rest_client.request('GET','/datasets/{}/logs/{}'.format(dataset_id, log_id))
        log_text = ret['data']
        html = '<html><head><title>' + ret['name'] + '</title></head><body>'
        html += log_text.replace('\n', '<br/>')
        html += '</body></html>'
        self.write(html)
        self.flush()


class Help(PublicHandler):
    """Help Page"""
    @catch_error
    async def get(self):
        self.statsd.incr('help')
        self.render('help.html')


class Other(PublicHandler):
    """Handle any other urls - this is basically all 404"""
    @catch_error
    async def get(self):
        self.statsd.incr('other')
        path = self.request.path
        self.set_status(404)
        self.render('404.html', path=path)


class Profile(PublicHandler):
    """Handle user profile page"""
    @catch_error
    @tornado.web.authenticated
    async def get(self):
        self.statsd.incr('profile')
        token = self.auth_key
        groups = []
        logger.info('user_data: %r', self.auth_data)
        logger.info('token: %r', token)
        if self.auth_data and 'groups' in self.auth_data:
            groups = self.auth_data['groups']
        self.render('profile.html', username=self.current_user, groups=groups,
                    token=token)


class Logout(PublicHandler):
    @catch_error
    async def get(self):
        self.statsd.incr('logout')
        self.clear_cookie("access_token")
        self.clear_cookie("refresh_token")
        self.clear_cookie("identity")
        self.current_user = None
        self.render('logout.html', status=None)
