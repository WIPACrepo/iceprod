"""
A helper to auto-configure a `Squid Cache <http://www.squid-cache.org/>`_
server and control starts, stops, and reloads.
"""

from __future__ import absolute_import, division, print_function

import os
import time
import subprocess
import crypt
import string
import random
import signal
import glob
from datetime import datetime,timedelta
import logging
from functools import partial

from iceprod.server import salt,KwargConfig

logger = logging.getLogger('squid')


class Squid(KwargConfig):
    """
    Wrapper around the `Squid` server. `Squid` is used to proxy and
    cache web requests by IceProd running on worker nodes.
    """
    def __init__(self, *args, **kwargs):
        # defaults
        self._cfg = {
            'username': None,
            'password': None,
            #'sslcert': None,
            #'sslkey': None,
            #'cacert': os.path.expandvars('$I3PROD/etc/cacerts.crt'),
            'request_timeout': 10000,
            'cache_dir': os.path.expandvars('$I3PROD/var/cache/squid'),
            'cache_size': 200, # in MB
            'port': 8082,
            # space separated networks for valid nodes (default: private IPv4)
            'localnet': '10.0.0.0/8 172.16.0.0/12 192.168.0.0/16',
            'pid_file': os.path.expandvars('$I3PROD/var/run/squid.pid'),
            'cfg_file': os.path.expandvars('$I3PROD/etc/squid.conf'),
            'cache_log': os.path.expandvars('$I3PROD/var/log/squid/cache.log'),
            'access_log': os.path.expandvars('$I3PROD/var/log/squid/access.log'),
            'squid_bin': os.path.expandvars('$I3PROD/sbin/squid'),
        }
        self._cfg_types = {
            'username': 'str',
            'password': 'str',
            #'sslcert': 'file',
            #'sslkey': 'file',
            #'cacert': 'file',
            'request_timeout': 'int',
            'cache_dir': 'dir',
            'cache_size': 'int',
            'port': 'int',
            'localnet': 'str',
            'pid_file': 'file',
            'cfg_file': 'file',
            'cache_log': 'file',
            'access_log': 'file',
            'squid_bin': 'file',
        }
        self.update(**kwargs)
        self.process = None

    def update(self,**kwargs):
        # setup cfg variables
        self.validate(kwargs)

        if self._cfg['username'] is not None and self._cfg['password'] is not None:
            logger.info('enabling auth_basic')
            self.auth_basic = True
            self.authbasicfile = os.path.join(os.getcwd(),'authbasic_squid.htpasswd')
            with open(self.authbasicfile,'w') as file:
                passwd = crypt.crypt(self._cfg['password'],salt())
                file.write('{}:{}\n'.format(self._cfg['username'],passwd))
        else:
            logger.info('disabling auth_basic')
            self.auth_basic = False

        #if self._cfg['sslcert'] is not None and self._cfg['sslkey'] is not None:
        #    logger.info('enabling SSL for squid')
        #    self.ssl = True
        #else:
        #    logger.info('disabling SSL for squid')
        #    self.ssl = False

        # write config file
        self.cfgfile = os.path.abspath(self._cfg['cfg_file'])
        with open(self.cfgfile,'w') as file:
            p = partial(print,sep='',file=file)
            # core squid options
            p('http_port {}'.format(self._cfg['port']))
            p('pid_filename {}'.format(self._cfg['pid_file']))
            # general options
            p('cache_mgr not_to_be_disturbed')
            p('client_db off')
            p('detect_broken_pconn on')
            p('dns_timeout 5 minutes')
            p('forwarded_for off')
            p('half_closed_clients off')
            p('httpd_suppress_version_string on')
            p('pipeline_prefetch on')
            p('retry_on_error on')
            p('strip_query_terms off')
            p('uri_whitespace strip')
            p('visible_hostname localhost')
            # timeouts
            p('forward_timeout {} seconds'.format(self._cfg['request_timeout']))
            p('connect_timeout {} seconds'.format(self._cfg['request_timeout']))
            p('read_timeout {} seconds'.format(self._cfg['request_timeout']))
            p('request_timeout {} seconds'.format(self._cfg['request_timeout']))
            p('persistent_request_timeout {} seconds'.format(self._cfg['request_timeout']))
            p('client_lifetime 20 hours')
            # acl definitions
            #p('acl all src 0.0.0.0/0')
            #p('acl localhost src 127.0.0.1/255.255.255.255')
            #p('acl to_localhost dst 127.0.0.0/8')
            p('acl localnet src {}'.format(self._cfg['localnet']))
            # access rules
            p('http_access allow localhost manager')
            p('http_access deny manager')
            p('http_access deny to_localhost')
            p('http_access allow localnet')
            p('http_access allow localhost')
            p('http_access deny all')
            # max connections per ip
            p('acl maxuserconn src 127.0.0.0/8 {}'.format(self._cfg['localnet']))
            p('acl limitusercon maxconn 500')
            p('http_access deny maxuserconn limitusercon')
            # caching
            p('cache allow all')
            p('cache_dir ufs {} {} 16 256'.format(self._cfg['cache_dir'],self._cfg['cache_size']))
            # logs
            p('logformat squid [%tl] %>A %{Host}>h "%rm %ru HTTP/%rv" %Hs %<st "%{Referer}>h" "%{User-Agent}>h" %Ss:%Sh')
            p('logfile_rotate 4')
            p('access_log daemon:{} squid'.format(self._cfg['access_log']))
            p('cache_log {} squid'.format(self._cfg['cache_log']))

    def start(self):
        """Start server"""
        if self.process:
            raise Exception('Squid already running')

        logger.warning('starting Squid...')
        self.process = subprocess.Popen([self._cfg['squid_bin'],'-Nzf',self.cfgfile])
        time.sleep(1)
        logger.warning('Squid running on %d',self._cfg['port'])

    def stop(self):
        """Stop server"""
        if self.process:
            logger.warning('stopping Squid...')
            self.process.send_signal(signal.SIGTERM)
            self.process = None
            time.sleep(0.5)
        else:
            raise Exception('Squid not running')

    def kill(self):
        """Stop server"""
        if self.process:
            logger.warning('killing Squid...')
            self.process.send_signal(signal.SIGINT)
            self.process = None
        else:
            raise Exception('Squid not running')

    def restart(self):
        """Restart (reconfigure) the server"""
        if self.process:
            logger.warning('reconfiguring Squid...')
            self.process.send_signal(signal.SIGHUP)
            time.sleep(0.5)
        else:
            self.start()

    def logrotate(self):
        """Rotate log files"""
        if self.process:
            logger.warning('rotating Squid log files...')
            try:
                self.process.send_signal(signal.SIGUSR1)
            except Exception as e:
                logger.error('error rotating Squid log files: %r',e)
                raise
        else:
            raise Exception('Squid not running')
