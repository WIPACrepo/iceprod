"""
Main website
------------

This is the external website users will see when interacting with IceProd.
It has been broken down into several sub-handlers for easier maintenance.
"""

from collections import defaultdict
from datetime import datetime, timedelta
import functools
import importlib.resources
import logging
import os
import random
import re
from urllib.parse import urlencode

from iceprod.core.jsonUtil import json_encode

from cachetools.func import ttl_cache
import tornado.web
import tornado.httpserver
import tornado.gen
import jwt
import tornado.concurrent
from rest_tools.client import RestClient, ClientCredentialsAuth
from rest_tools.server import catch_error, RestServer, RestHandlerSetup, RestHandler, OpenIDLoginHandler
from rest_tools import telemetry as wtt
from wipac_dev_tools import from_environment

import iceprod
from iceprod.roles_groups import GROUPS
import iceprod.core.functions
from iceprod.server import documentation
from iceprod.server.module import FakeStatsClient, StatsClientIgnoreErrors
import iceprod.server.states
from iceprod.server.util import nowstr

logger = logging.getLogger('website')


def authenticated(method):
    """Decorate methods with this to require that the user be logged in.

    If the user is not logged in, they will be redirected to the configured
    `login url <RequestHandler.get_login_url>`.

    If you configure a login url with a query parameter, Tornado will
    assume you know what you're doing and use it as-is.  If not, it
    will add a `next` parameter so the login page knows where to send
    you once you're logged in.
    """
    @catch_error
    @functools.wraps(method)
    async def wrapper(self, *args, **kwargs):
        if not await self.get_current_user_async():
            if self.request.method in ("GET", "HEAD"):
                try:
                    url = self.get_login_url()
                    if "?" not in url:
                        next_url = self.request.uri
                        url += "?" + urlencode(dict(next=next_url))
                    self.redirect(url)
                    return None
                except Exception:
                    logger.warning('failed to make redirect', exc_info=True)
                    raise tornado.web.HTTPError(403, reason='auth failed')
            raise tornado.web.HTTPError(403, reason='auth failed')
        return await method(self, *args, **kwargs)
    return wrapper


def eval_expression(token, e):
    """from rest_tools.server.decorators.token_attribute_role_mapping_auth"""
    name, val = e.split('=',1)
    if name == 'scope':
        # special handling to split into string
        token_val = token.get('scope','').split()
    else:
        prefix = name.split('.')[:-1]
        while prefix:
            token = token.get(prefix[0], {})
            prefix = prefix[1:]
        token_val = token.get(name.split('.')[-1], None)

    logger.debug('token_val = %r', token_val)
    if token_val is None:
        return []

    prog = re.compile(val)
    if isinstance(token_val, list):
        ret = (prog.fullmatch(v) for v in token_val)
    else:
        ret = [prog.fullmatch(token_val)]
    return [r for r in ret if r]


class TokenStorageMixin:
    """
    Store/load current user's `OpenIDLoginHandler` tokens in iceprod credentials API.
    """
    def get_current_user(self):
        return None

    async def get_current_user_async(self):
        """Get the current user, and set auth-related attributes."""
        try:
            username = self.get_secure_cookie('iceprod_username')
            if not username:
                return None
            if isinstance(username, bytes):
                username = username.decode('utf-8')
            creds = await self.cred_rest_client.request('GET', f'/users/{username}/credentials', {'url': self.full_url})
            cred = creds[self.full_url]
            access_token = cred['access_token']
            try:
                data = self.auth.validate(access_token)
            except jwt.ExpiredSignatureError:
                logger.debug('user access_token expired')
                return None
            self.auth_data = data

            # lookup groups
            auth_groups = set()
            try:
                for name in GROUPS:
                    for expression in GROUPS[name]:
                        ret = eval_expression(data, expression)
                        auth_groups.update(match.expand(name) for match in ret)
            except Exception:
                logger.info('cannot determine groups', exc_info=True)
            self.auth_groups = sorted(auth_groups)

            self.auth_access_token = access_token
            self.auth_refresh_token = cred.get('refresh_token', '')
            return username

        except Exception:
            logger.debug('failed auth', exc_info=True)
        return None

    def store_tokens(
        self,
        access_token,
        access_token_exp,
        refresh_token=None,
        refresh_token_exp=None,
        user_info=None,
        user_info_exp=None,
    ):
        """
        Store jwt tokens and user info from OpenID-compliant auth source.

        Args:
            access_token (str): jwt access token
            access_token_exp (int): access token expiration in seconds
            refresh_token (str): jwt refresh token
            refresh_token_exp (int): refresh token expiration in seconds
            user_info (dict): user info (from id token or user info lookup)
            user_info_exp (int): user info expiration in seconds
        """
        if not user_info:
            user_info = self.auth.validate(access_token)
        username = user_info.get('preferred_username')
        if not username:
            username = user_info.get('upn')
        if not username:
            raise tornado.web.HTTPError(400, reason='no username in token')
        args = {
            'url': self.full_url,
            'type': 'oauth',
            'access_token': access_token,
        }
        if refresh_token:
            args['refresh_token'] = refresh_token

        self.cred_rest_client.request_seq('POST', f'/users/{username}/credentials', args)

        self.set_secure_cookie('iceprod_username', username, expires_days=30)

    def clear_tokens(self):
        """
        Clear token data, usually on logout.
        """
        self.clear_cookie('iceprod_username')


class Login(TokenStorageMixin, OpenIDLoginHandler):
    def initialize(self, cred_rest_client=None, full_url=None, **kwargs):
        super().initialize(**kwargs)
        self.cred_rest_client = cred_rest_client
        self.full_url = full_url


class PublicHandler(TokenStorageMixin, RestHandler):
    """Default Handler"""
    def initialize(self, statsd=None, rest_api=None, cred_rest_client=None, system_rest_client=None, full_url=None, **kwargs):
        """
        Get some params from the website module

        :param statsd: statsd client
        :param rest_api: the rest api url
        :param cred_rest_client: the rest api url for the cred service
        :param system_rest_client: the rest client for the system role
        :param full_url: the full base url for the website
        """
        super().initialize(**kwargs)
        self.statsd = statsd
        self.rest_api = rest_api
        self.cred_rest_client = cred_rest_client
        self.system_rest_client = system_rest_client
        self.rest_client = None
        self.full_url = full_url

    def get_template_namespace(self):
        namespace = super().get_template_namespace()
        namespace['version'] = iceprod.__version__
        namespace['section'] = self.request.uri.lstrip('/').split('?')[0].split('/')[0]
        namespace['json_encode'] = json_encode
        namespace['states'] = iceprod.server.states
        namespace['rest_api'] = self.rest_api
        return namespace

    async def get_current_user_async(self):
        ret = await super().get_current_user_async()
        try:
            if ret:
                self.rest_client = RestClient(self.rest_api, self.auth_access_token, timeout=50, retries=1)
        except Exception:
            pass

        self.current_user = ret
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
        # try to get the user, if available
        await self.get_current_user_async()
        self.statsd.incr('default')
        self.render('main.html')


class Submit(PublicHandler):
    """Handle /submit urls"""
    @authenticated
    async def get(self):
        logger.info('here')
        self.statsd.incr('submit')
        token = self.auth_access_token
        groups = self.auth_groups
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
        self.render('submit.html', **render_args)


class Config(PublicHandler):
    """Handle /config urls"""
    @authenticated
    async def get(self):
        self.statsd.incr('config')
        dataset_id = self.get_argument('dataset_id',default=None)
        if not dataset_id:
            self.write_error(400,message='must provide dataset_id')
            return
        dataset = await self.rest_client.request('GET','/datasets/{}'.format(dataset_id))
        edit = self.get_argument('edit',default=False)
        if edit:
            passkey = self.auth_access_token
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
    @ttl_cache(maxsize=256, ttl=600)
    async def get_usernames(self):
        ret = await self.system_rest_client.request('GET', '/users')
        return [x['username'] for x in ret['results']]

    @authenticated
    async def get(self):
        self.statsd.incr('dataset_browse')
        usernames = await self.get_usernames()
        filter_options = {
            'status': ['processing', 'suspended', 'errors', 'complete', 'truncated'],
            'groups': list(GROUPS.keys()),
            'users': usernames,
        }
        filter_results = {n: self.get_arguments(n) for n in filter_options}

        args = {'keys': 'dataset_id|dataset|status|description|group|username'}
        for name in filter_results:
            val = filter_results[name]
            if not val:
                continue
            if any(v not in filter_options[name] for v in val):
                raise tornado.web.HTTPError(400, reason='Bad filter '+name+' value')
            args[name] = '|'.join(val)

        url = '/datasets'

        ret = await self.rest_client.request('GET', url, args)
        datasets = sorted(ret.values(), key=lambda x:x.get('dataset',0), reverse=True)
        logger.debug('datasets: %r', datasets)
        datasets = filter(lambda x: 'dataset' in x, datasets)
        self.render(
            'dataset_browse.html',
            datasets=datasets,
            filter_options=filter_options,
            filter_results=filter_results,
        )


class Dataset(PublicHandler):
    """Handle /dataset urls"""
    @authenticated
    async def get(self, dataset_id):
        self.statsd.incr('dataset')

        if dataset_id.isdigit():
            try:
                d_num = int(dataset_id)
                if d_num < 10000000:
                    all_datasets = await self.rest_client.request('GET', '/datasets', {'keys': 'dataset_id|dataset'})
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

        passkey = self.auth_access_token

        jobs = await self.rest_client.request('GET','/datasets/{}/job_counts/status'.format(dataset_id))
        tasks = await self.rest_client.request('GET','/datasets/{}/task_counts/status'.format(dataset_id))
        task_info = await self.rest_client.request('GET','/datasets/{}/task_counts/name_status'.format(dataset_id))
        task_stats = await self.rest_client.request('GET','/datasets/{}/task_stats'.format(dataset_id))
        config = await self.rest_client.request('GET','/config/{}'.format(dataset_id))
        for t in task_info:
            logger.info('task_info[%s] = %r', t, task_info[t])
            type_ = 'UNK'
            for task in config['tasks']:
                if 'name' in task and task['name'] == t:
                    type_ = 'GPU' if 'requirements' in task and 'gpu' in task['requirements'] and task['requirements']['gpu'] else 'CPU'
                    break
            task_info[t] = {
                'name': t,
                'type': type_,
                'waiting': task_info[t].get('waiting', 0) + task_info[t].get('idle', 0),
                'queued': task_info[t].get('queued', 0),
                'running': task_info[t].get('processing', 0),
                'complete': task_info[t].get('complete', 0),
                'error': task_info[t].get('failed', 0) + task_info[t].get('suspended', 0),
            }
        self.render('dataset_detail.html', dataset_id=dataset_id, dataset_num=dataset_num,
                    dataset=dataset, jobs=jobs, tasks=tasks, task_info=task_info,
                    task_stats=task_stats, passkey=passkey)


class TaskBrowse(PublicHandler):
    """Handle /task urls"""
    @authenticated
    async def get(self, dataset_id):
        self.statsd.incr('task_browse')
        status = self.get_argument('status',default=None)

        if status:
            tasks = await self.rest_client.request('GET','/datasets/{}/tasks?status={}'.format(dataset_id,status))
            for t in tasks:
                if 'job_index' not in tasks[t]:
                    job = await self.rest_client.request('GET', '/datasets/{}/jobs/{}'.format(dataset_id, tasks[t]['job_id']))
                    tasks[t]['job_index'] = job['job_index']
            passkey = self.auth_access_token
            self.render('task_browse.html',tasks=tasks, passkey=passkey)
        else:
            status = await self.rest_client.request('GET','/datasets/{}/task_counts/status'.format(dataset_id))
            self.render('tasks.html',status=status)


class Task(PublicHandler):
    """Handle /task urls"""
    @authenticated
    async def get(self, dataset_id, task_id):
        self.statsd.incr('task')
        status = self.get_argument('status', default=None)

        passkey = self.auth_access_token

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
    @authenticated
    async def get(self, dataset_id):
        self.statsd.incr('job')
        status = self.get_argument('status',default=None)

        passkey = self.auth_access_token

        jobs = await self.rest_client.request('GET', '/datasets/{}/jobs'.format(dataset_id))
        if status:
            for t in list(jobs):
                if jobs[t]['status'] != status:
                    del jobs[t]
                    continue
        self.render('job_browse.html', jobs=jobs, passkey=passkey)


class Job(PublicHandler):
    """Handle /job urls"""
    @authenticated
    async def get(self, dataset_id, job_id):
        self.statsd.incr('job')
        status = self.get_argument('status',default=None)

        passkey = self.auth_access_token

        dataset = await self.rest_client.request('GET', '/datasets/{}'.format(dataset_id))
        job = await self.rest_client.request('GET', '/datasets/{}/jobs/{}'.format(dataset_id,job_id))
        args = {'job_id': job_id}
        if status:
            args['status'] = status
        tasks = await self.rest_client.request('GET', f'/datasets/{dataset_id}/tasks', args)
        job['tasks'] = list(tasks.values())
        job['tasks'].sort(key=lambda x:x['task_index'])
        self.render('job_detail.html', dataset=dataset, job=job, passkey=passkey)


class Documentation(PublicHandler):
    @catch_error
    async def get(self, url):
        # try to get the user, if available
        await self.get_current_user_async()
        self.statsd.incr('documentation')
        doc_path = str(importlib.resources.files('iceprod.server')/'data'/'docs')
        full_path = os.path.join(doc_path, url)
        if not full_path.startswith(doc_path):
            self.set_status(404)
            self.render('404.html', path='bad docs path')
            return
        self.write(documentation.load_doc(full_path))
        self.flush()


class Log(PublicHandler):
    @authenticated
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
        # try to get the user, if available
        await self.get_current_user_async()
        self.statsd.incr('help')
        self.render('help.html')


class Other(PublicHandler):
    """Handle any other urls - this is basically all 404"""
    @catch_error
    async def get(self):
        # try to get the user, if available
        await self.get_current_user_async()
        self.statsd.incr('other')
        path = self.request.path
        self.set_status(404)
        self.render('404.html', path=path)


class Profile(PublicHandler):
    """Handle user profile page"""
    @authenticated
    async def get(self):
        self.statsd.incr('profile')
        username = self.current_user
        groups = self.auth_groups
        group_creds = {}
        for g in groups:
            if g != 'users':
                ret = await self.cred_rest_client.request('GET', f'/groups/{g}/credentials')
                group_creds[g] = ret
        user_creds = await self.cred_rest_client.request('GET', f'/users/{username}/credentials')
        self.render('profile.html', username=username, groups=groups,
                    group_creds=group_creds, user_creds=user_creds)

    @authenticated
    async def post(self):
        username = self.current_user

        if self.get_argument('add_icecube_token', ''):
            args = {
                'url': 'https://data.icecube.aq',
                'type': 'oauth',
                'access_token': self.auth_access_token,
            }
            if self.auth_refresh_token:
                args['refresh_token'] = self.auth_refresh_token
                args['expiration'] = (datetime.utcnow() + timedelta(days=30)).isoformat()
            await self.cred_rest_client.request('POST', f'/users/{username}/credentials', args)

        else:
            type_ = self.get_argument('type')
            args = {
                'url': self.get_argument('url'),
                'type': type_,
            }
            if type_ == 's3':
                args['buckets'] = [x for x in self.get_argument('buckets').split('\n') if x]
                args['access_key'] = self.get_argument('access_key')
                args['secret_key'] = self.get_argument('secret_key')
            elif type_ == 'oauth':
                if acc := self.get_argument('refresh_token', ''):
                    args['refresh_token'] = acc
                if ref := self.get_argument('refresh_token', ''):
                    args['refresh_token'] = ref
                if not (acc or ref):
                    raise tornado.web.HTTPError(400, reason='need access or refresh token')
            else:
                raise tornado.web.HTTPError(400, reason='bad cred type')

            if self.get_argument('add_user_cred', ''):
                await self.cred_rest_client.request('POST', f'/users/{username}/credentials', args)
            elif self.get_argument('add_group_cred', ''):
                groupname = self.get_argument('group')
                if groupname not in self.auth_groups or groupname == 'users':
                    raise tornado.web.HTTPError(400, reason='bad group name')
                await self.cred_rest_client.request('POST', f'/groups/{groupname}/credentials', args)

        # now show the profile page
        await self.get()


class Logout(PublicHandler):
    @catch_error
    async def get(self):
        self.statsd.incr('logout')
        self.clear_tokens()
        self.current_user = None
        self.request.uri = '/'  # for login redirect, fake the main page
        self.render('logout.html', status=None)


class HealthHandler(PublicHandler):
    """
    Handle health requests.
    """
    async def get(self):
        """
        Get health status.

        Returns based on exit code, 200 = ok, 400 = failure
        """
        status = {
            'now': nowstr(),
        }

        try:
            await self.system_rest_client.request('GET', '/dataset_summaries/status')
            status['rest_api'] = 'OK'
        except Exception:
            logger.info('error from REST API', exc_info=True)
            self.send_error(500, reason='error from REST API')
            return

        try:
            await self.cred_rest_client.request('GET', '/healthz')
            status['cred_service'] = 'OK'
        except Exception:
            logger.info('error from REST API', exc_info=True)
            self.send_error(500, reason='error from credential service')
            return

        self.write(status)


class Server:
    def __init__(self):
        default_config = {
            'HOST': 'localhost',
            'PORT': 8080,
            'DEBUG': False,
            'OPENID_URL': '',
            'OPENID_AUDIENCE': '',
            'ICEPROD_WEB_URL': 'https://iceprod2.icecube.wisc.edu',
            'ICEPROD_API_ADDRESS': 'https://iceprod2-api.icecube.wisc.edu',
            'ICEPROD_API_CLIENT_ID': '',
            'ICEPROD_API_CLIENT_SECRET': '',
            'ICEPROD_CRED_ADDRESS': 'https://credentials.iceprod.icecube.aq',
            'ICEPROD_CRED_CLIENT_ID': '',
            'ICEPROD_CRED_CLIENT_SECRET': '',
            'COOKIE_SECRET': '',
            'STATSD_ADDRESS': '',
            'STATSD_PREFIX': 'rest_api',
            'CI_TESTING': '',
        }
        config = from_environment(default_config)

        # get package data
        static_path = str(importlib.resources.files('iceprod.website')/'data'/'www')
        if static_path is None or not os.path.exists(static_path):
            logger.info('static path: %r',static_path)
            raise Exception('bad static path')
        template_path = str(importlib.resources.files('iceprod.website')/'data'/'www_templates')
        if template_path is None or not os.path.exists(template_path):
            logger.info('template path: %r',template_path)
            raise Exception('bad template path')

        # set IceProd REST API
        if config['ICEPROD_API_ADDRESS']:
            rest_address = config['ICEPROD_API_ADDRESS']
        else:
            raise RuntimeError('ICEPROD_API_ADDRESS not specified')

        rest_config = {
            'debug': config['DEBUG'],
            'server_header': 'IceProd/' + iceprod.__version__,
        }

        if config['OPENID_URL']:
            logging.info(f'enabling auth via {config["OPENID_URL"]} for aud "{config["OPENID_AUDIENCE"]}"')
            rest_config.update({
                'auth': {
                    'openid_url': config['OPENID_URL'],
                    'audience': config['OPENID_AUDIENCE'],
                }
            })
        elif config['CI_TESTING']:
            rest_config.update({
                'auth': {
                    'secret': 'secret',
                }
            })
        else:
            raise RuntimeError('OPENID_URL not specified, and CI_TESTING not enabled!')

        statsd = FakeStatsClient()
        if config['STATSD_ADDRESS']:
            try:
                addr = config['STATSD_ADDRESS']
                port = 8125
                if ':' in addr:
                    addr,port = addr.split(':')
                    port = int(port)
                statsd = StatsClientIgnoreErrors(addr, port=port, prefix=config['STATSD_PREFIX'])
            except Exception:
                logger.warning('failed to connect to statsd: %r', config['STATSD_ADDRESS'], exc_info=True)

        if config['ICEPROD_CRED_CLIENT_ID'] and config['ICEPROD_CRED_CLIENT_SECRET']:
            logging.info(f'enabling auth via {config["OPENID_URL"]} for aud "{config["OPENID_AUDIENCE"]}"')
            cred_client = ClientCredentialsAuth(
                address=config['ICEPROD_CRED_ADDRESS'],
                token_url=config['OPENID_URL'],
                client_id=config['ICEPROD_CRED_CLIENT_ID'],
                client_secret=config['ICEPROD_CRED_CLIENT_SECRET'],
            )
        elif config['CI_TESTING']:
            cred_client = RestClient(config['ICEPROD_CRED_ADDRESS'], timeout=1, retries=0)
        else:
            raise RuntimeError('ICEPROD_CRED_CLIENT_ID or ICEPROD_CRED_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

        handler_args = RestHandlerSetup(rest_config)
        handler_args['cred_rest_client'] = cred_client

        full_url = config['ICEPROD_WEB_URL']
        handler_args['full_url'] = full_url
        login_url = full_url+'/login'

        login_handler_args = handler_args.copy()
        if config['ICEPROD_API_CLIENT_ID'] and config['ICEPROD_API_CLIENT_SECRET']:
            logging.info('enabling system rest client and website logins"')
            rest_client = ClientCredentialsAuth(
                address=config['ICEPROD_API_ADDRESS'],
                token_url=config['OPENID_URL'],
                client_id=config['ICEPROD_API_CLIENT_ID'],
                client_secret=config['ICEPROD_API_CLIENT_SECRET'],
            )
            login_handler_args['oauth_client_id'] = config['ICEPROD_API_CLIENT_ID']
            login_handler_args['oauth_client_secret'] = config['ICEPROD_API_CLIENT_SECRET']
            login_handler_args['oauth_client_scope'] = 'offline_access posix profile'
        elif config['CI_TESTING']:
            logger.info('CI_TESTING: no login for testing')
            rest_client = RestClient(config['ICEPROD_API_ADDRESS'], timeout=1, retries=0)
        else:
            raise RuntimeError('ICEPROD_API_CLIENT_ID or ICEPROD_API_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

        handler_args.update({
            'statsd': statsd,
            'rest_api': rest_address,
            'system_rest_client': rest_client,
        })
        if config['COOKIE_SECRET']:
            cookie_secret = config['COOKIE_SECRET']
            log_cookie_secret = cookie_secret[:4] + 'X'*(len(cookie_secret)-8) + cookie_secret[-4:]
            logger.info('using supplied cookie secret %r', log_cookie_secret)
        else:
            cookie_secret = ''.join(hex(random.randint(0,15))[-1] for _ in range(64))

        server = RestServer(
            debug=config['DEBUG'],
            cookie_secret=cookie_secret,
            login_url=login_url,
            template_path=template_path,
            static_path=static_path,
        )

        server.add_route(r"/", Default, handler_args)
        server.add_route(r"/submit", Submit, handler_args)
        server.add_route(r"/config", Config, handler_args)
        server.add_route(r"/dataset", DatasetBrowse, handler_args)
        server.add_route(r"/dataset/(\w+)", Dataset, handler_args)
        server.add_route(r"/dataset/(\w+)/task", TaskBrowse, handler_args)
        server.add_route(r"/dataset/(\w+)/task/(\w+)", Task, handler_args)
        server.add_route(r"/dataset/(\w+)/job", JobBrowse, handler_args)
        server.add_route(r"/dataset/(\w+)/job/(\w+)", Job, handler_args)
        server.add_route(r"/help", Help, handler_args)
        server.add_route(r"/docs/(.*)", Documentation, handler_args)
        server.add_route(r"/dataset/(\w+)/log/(\w+)", Log, handler_args)
        server.add_route(r'/profile', Profile, handler_args)
        server.add_route(r"/login", Login, login_handler_args)
        server.add_route(r"/logout", Logout, handler_args)
        server.add_route('/healthz', HealthHandler, handler_args)
        server.add_route(r"/.*", Other, handler_args)

        server.startup(address=config['HOST'], port=config['PORT'])

        self.server = server

    async def start(self):
        pass

    async def stop(self):
        await self.server.stop()
