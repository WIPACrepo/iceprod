"""
Tools to help manage Globus proxies
"""

import os
import subprocess
import logging

from iceprod.server.config import IceProdConfig

logger = logging.getLogger('globus')

class SiteGlobusProxy(object):
    """
    Manage site-wide globus proxy

    :param cfgfile: cfgfile location (optional)
    :param duration: proxy duration (optional, default 72 hours)
    """
    def __init__(self, cfgfile=None, duration=None):
        if not cfgfile:
            cfgfile = os.path.join(os.getcwd(),'globus_proxy.json')
        self.cfg = IceProdConfig(filename=cfgfile, defaults=False,
                                 validate=False)
        if duration:
            self.cfg['duration'] = duration
        elif 'duration' not in self.cfg:
            self.cfg['duration'] = 72

    def set_passphrase(self, p):
        """Set the passphrase"""
        self.cfg['passphrase'] = p

    def set_duration(self, d):
        """Set the duration"""
        self.cfg['duration'] = d

    def update_proxy(self):
        """Update the proxy"""
        if 'passphrase' not in self.cfg:
            raise Exception('passphrase missing')
        if 'duration' not in self.cfg:
            raise Exception('duration missing')
        FNULL = open(os.devnull, 'w')
        logger.info('duration: %r',self.cfg['duration'])
        if subprocess.call(['grid-proxy-info','-e',
                            '-valid','%d:0'%self.cfg['duration'],
                           ], stdout=FNULL, stderr=FNULL):
            # proxy needs updating
            p = subprocess.Popen(['grid-proxy-init','-pwstdin',
                                  '-valid','%d:0'%(self.cfg['duration']+1),
                                 ], stdin=subprocess.PIPE)
            p.communicate(input=self.cfg['passphrase']+'\n')
            p.wait()
            if p.returncode > 0:
                raise Exception('grid-proxy-init failed')

    def get_proxy(self):
        """Get the proxy location"""
        FNULL = open(os.devnull, 'w')
        return subprocess.check_output(['grid-proxy-info','-path'],
                                       stderr=FNULL).strip()
