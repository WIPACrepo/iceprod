"""
Nginx is used to handle static content and proxy all other requests to 
the `website module <iceprod.server.modules.website>`_. It also handles 
most of the web security as a SSL/TLS front-end and more generally as 
a hardened attack surface.
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

logger = logging.getLogger('nginx')

def salt():
    """Returns a string of 2 random letters"""
    letters = string.letters+string.digits
    return random.choice(letters)+random.choice(letters)

def rotate(filename):
    """Rotate a filename.  Useful for log files."""
    # move log
    date = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.rename(filename,filename+'_'+date)

def deleteoldlogs(filename,days=30):
    """Delete old log files"""
    # delete old logs
    for file in glob.iglob(filename+'_*'):
        filedate = datetime.fromtimestamp(os.path.getmtime(file))
        if datetime.now()-filedate > timedelta(days=days):
            os.remove(file)

class Nginx(object):
    """Wrapper around the Nginx webserver."""
    def __init__(self, *args, **kwargs):
        """Set up Nginx"""
        # make sure nginx exists
        subprocess.check_call(['which','nginx'])
        
        # defaults
        self._cfg = {
            'username': None,
            'password': None,
            'sslcert': None,
            'sslkey': None,
            'cacert': os.path.expandvars('$I3PREFIX/etc/cacerts.crt'),
            'request_timeout': 10000,
            'upload_dir': os.path.expandvars('$I3PROD/var/www_uploads'),
            'static_dir': os.path.expandvars('$I3PROD/var/www'),
            'port': 8080,
            'proxy_port':8081,
            'pid_file': os.path.expandvars('$I3PROD/var/run/nginx.pid'),
            'cfg_file': os.path.expandvars('$I3PROD/conf/nginx.conf'),
            'access_log': os.path.expandvars('$I3PROD/var/log/nginx/access.log'),
            'error_log': os.path.expandvars('$I3PROD/var/log/nginx/error.log'),
            'nginx_bin': os.path.expandvars('$I3PREFIX/sbin/nginx'),
        }
        self._cfg_types = {
            'username': 'str',
            'password': 'str',
            'sslcert': 'file',
            'sslkey': 'file',
            'cacert': 'file',
            'request_timeout': 'int',
            'upload_dir': 'dir',
            'static_dir': 'dir',
            'port': 'int',
            'proxy_port': 'int',
            'pid_file': 'file',
            'cfg_file': 'file',
            'access_log': 'file',
            'error_log': 'file',
            'nginx_bin': 'file',
        }
        
        # setup cfg variables
        for s in kwargs.keys():
            v = kwargs[s]
            if not isinstance(s,str):
                raise Exception('parameter name %s is not a string'%(str(s)))
            if not s in self._cfg:
                logger.warn('%s is not a valid arg',s)
                continue
            t = self._cfg_types[s]
            if t in ('str','file'):
                if not isinstance(v,str):
                    raise Exception('%s is not a string'%(str(s)))
                if t == 'file':
                    v = os.path.expanduser(os.path.expandvars(v))
                    if not ('_file' in s or '_log' in s):
                        try:
                            os.path.exists(v)
                        except Exception:
                            raise Exception('parameter %s with filepath %s does not exist'%(s,v))
            elif t == 'int':
                if not isinstance(v,int):
                    raise Exception('%s is not an int'%(str(s)))
            elif t == 'float':
                if not isinstance(v,float):
                    raise Exception('%s is not a float'%(str(s)))
            else:
                raise Exception('%s has an unknown type'%(str(s)))
            self._cfg[s] = v
        
        if self._cfg['username'] is not None and self._cfg['password'] is not None:
            logger.info('enabling auth_basic')
            self.auth_basic = True
            self.authbasicfile = os.path.join(os.getcwd(),'authbasic.htpasswd')
            with open(self.authbasicfile,'w') as file:
                passwd = crypt.crypt(self._cfg['password'],salt())
                file.write('{}:{}\n'.format(self._cfg['username'],passwd))
        else:
            logger.info('disabling auth_basic')
            self.auth_basic = False
        
        if self._cfg['sslcert'] is not None and self._cfg['sslkey'] is not None:
            logger.info('enabling SSL for nginx')
            self.ssl = True
        else:
            logger.info('disabling SSL for nginx')
            self.ssl = False
        
        # create upload dirs
        for x in xrange(0,10):
            try:
                os.makedirs(os.path.join(self._cfg['upload_dir'],x))
            except:
                pass
        
        # write config file
        self.cfgfile = os.path.abspath(self._cfg['cfg_file'])
        with open(self.cfgfile,'w') as file:
            p = partial(print,sep='',file=file)
            # core nginx options
            p('daemon off;')
            p('pid {};'.format(self._cfg['pid_file']))
            p('worker_processes 1;')
            p('events {')
            p('  worker_connections 1024;')
            p('}')
            # http options
            p('http {')
            p('  include {};'.format(os.path.abspath(os.path.expandvars('$I3PROD/conf/mime.types'))))
            p('  default_type application/octet-stream;')
            p('  ignore_invalid_headers on;')
            p('  keepalive_timeout 300;')
            # logging
            p('  log_format main  \'$remote_addr $host $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"\';')
            p('  access_log {} main buffer=32k;'.format(self._cfg['access_log']))
            p('  error_log {} error;'.format(self._cfg['error_log']))
            # gzip if possible
            p('  gzip on;')
            p('  gzip_vary on;')
            p('  gzip_min_length 1000;')
            p('  sendfile off;') # don't use sendfile because of proxies
            p('  root {}/;'.format(self._cfg['static_dir'])) # direct random queries to static dir
            # ssl options
            if self.ssl is True:
                p('  ssl on;')  
                p('  ssl_ciphers ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:AES256-GCM-SHA384:AES256-SHA256:AES256-SHA:AES128-SHA;')
                p('  ssl_prefer_server_ciphers on;')
                p('  ssl_protocols TLSv1 TLSv1.1 TLSv1.2;')
                p('  ssl_session_timeout 5m;')
                p('  ssl_certificate {};'.format(self._cfg['sslcert']))
                p('  ssl_certificate_key {};'.format(self._cfg['sslkey']))
                p('  ssl_client_certificate {};'.format(self._cfg['cacert']))
            # server options
            p('  server {')
            p('    listen {:d};'.format(self._cfg['port']))
            p('    access_log {} main buffer=32k;'.format(self._cfg['access_log']))
            p('    proxy_connect_timeout 30s;')
            p('    proxy_send_timeout 30s;')
            p('    proxy_read_timeout 600s;')
            p('    proxy_set_header Host $http_host;')
            p('    proxy_redirect off;')
            p('    proxy_http_version 1.1;')
            p('    proxy_next_upstream error;')
            if self.ssl is True: # redirect http to https
                p('    error_page  497 =307 https://$http_host$request_uri;')
            # static files
            p('    location /static/ {')
            p('      alias {}/;'.format(self._cfg['static_dir']))
            p('      index index.html index.htm;')
            p('      sendfile on;') # turn sendfile on for lower resource usage
            p('      max_ranges 1;') # allow partial downloads
            p('    }')
            # tornado proxy
            p('    location / {')
            p('      proxy_pass http://localhost:{:d};'.format(self._cfg['proxy_port']))
            p('    }')
            # upload proxy
            p('    location /upload {')
            if self.auth_basic is True:
                p('      auth_basic "iceprod";')
                p('      auth_basic_user_file {};'.format(self.authbasicfile))
            p('      proxy_pass http://localhost:{:d};'.format(self._cfg['proxy_port']))
            p('    }')
            p('    location /uploadauth {')
            p('      proxy_pass http://localhost:{:d};'.format(self._cfg['proxy_port']))
            p('      proxy_pass_request_body off;')
            p('      proxy_set_header Content-Length "";')
            p('      proxy_set_header X-Original-URI $request_uri;')
            p('      client_max_body_size 10g;')
            p('    }')
            p('    location @after_upload {')
            p('      proxy_pass http://localhost:{:d};'.format(self._cfg['proxy_port']))
            p('    }')
            p('    location /upload/ {')
            if self.auth_basic is True:
                p('      auth_basic "iceprod";')
                p('      auth_basic_user_file {};'.format(self.authbasicfile))
            p('      auth_request /uploadauth;')
            p('      keepalive_disable msie6 safari;')
            p('      client_max_body_size 10g;')
            p('      upload_pass @after_upload;')
            p('      upload_buffer_size 1048576;') # set buffer to 1M
            p('      upload_store {}/;'.format(self._cfg['upload_dir']))
            p('      upload_store_access user:r;')
            p('      upload_set_form_field name "$upload_file_name";')
            p('      upload_set_form_field content_type "$upload_content_type";')
            p('      upload_set_form_field path "$upload_tmp_path";')
            p('      upload_cleanup 400 403 404 499 500-505;')
            #if self.ssl is True:
            #    p('      error_page 497   error.html;')
            p('    }')
            p('  }')
            p('}')
        
        self.process = None
        
    def start(self):
        """Start server"""
        if self.process:
            raise Exception('Nginx already running')
        
        logger.warn('starting Nginx...')
        self.process = subprocess.Popen([self._cfg['nginx_bin'],'-c',self.cfgfile])
        time.sleep(1)
        logger.warn('Nginx running on %d, proxying to %d',self._cfg['port'],
                     self._cfg['proxy_port'])
    
    def stop(self):
        """Stop server"""
        if self.process:
            logger.warn('stopping Nginx...')
            self.process.send_signal(signal.SIGQUIT)
            self.process = None
            time.sleep(0.5)
        else:
            raise Exception('Nginx not running')
    
    def kill(self):
        """Stop server"""
        if self.process:
            logger.warn('killing Nginx...')
            self.process.send_signal(signal.SIGTERM)
            self.process = None
        else:
            raise Exception('Nginx not running')

    def logrotate(self):
        """Rotate log files"""
        if self.process:
            logger.warn('rotating Nginx log files...')
            try:
                rotate(self._cfg['access_log'])
                rotate(self._cfg['error_log'])
                self.process.send_signal(signal.SIGUSR1)
                deleteoldlogs(self._cfg['access_log'])
                deleteoldlogs(self._cfg['error_log'])
            except Exception as e:
                logger.error('error rotating Nginx log files: %r',e)
                raise
        else:
            raise Exception('Nginx not running')
