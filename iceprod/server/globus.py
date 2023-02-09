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

    def set_voms_vo(self, vo):
        """Set the voms VO"""
        self.cfg['voms_vo'] = vo

    def set_voms_role(self, r):
        """Set the voms role"""
        self.cfg['voms_role'] = r

    def update_proxy(self):
        """Update the proxy"""
        if 'passphrase' not in self.cfg:
            raise Exception('passphrase missing')
        if 'duration' not in self.cfg:
            raise Exception('duration missing')
        logger.info('duration: %r',self.cfg['duration'])
        if subprocess.call(
            [
                'grid-proxy-info',
                '-e',
                '-valid',
                '%d:0'%self.cfg['duration'],
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        ):
            # proxy needs updating
            if 'voms_vo' in self.cfg and self.cfg['voms_vo']:
                cmd = ['voms-proxy-init']
                if 'voms_role' in self.cfg and self.cfg['voms_role']:
                    vo = self.cfg['voms_vo']
                    role = self.cfg['voms_role']
                    cmd.extend(['-voms', '{0}:/{0}/Role={1}'.format(vo, role)])
                else:
                    cmd.extend(['-voms', self.cfg['voms_vo']])
            else:
                cmd = ['grid-proxy-init']
            cmd.extend(['-pwstdin','-valid','%d:0'%(self.cfg['duration']+1)])
            if 'out' in self.cfg:
                cmd.extend(['-out', self.cfg['out']])
            inputbytes = (self.cfg['passphrase']+'\n').encode('utf-8')
            p = subprocess.run(cmd, input=inputbytes, capture_output=True, timeout=60, check=False)
            logger.info('proxy cmd: %r', p.args)
            logger.info('stdout: %s', p.stdout)
            logger.info('stderr: %s', p.stderr)
            if 'voms_vo' in self.cfg and self.cfg['voms_vo']:
                for line in p.stdout.decode('utf-8').split('\n'):
                    if line.startswith('Creating proxy') and line.endswith('Done'):
                        break  # this is a good proxy
                else:
                    raise Exception('voms-proxy-init failed')
            elif p.returncode > 0:
                raise Exception('grid-proxy-init failed')

    def get_proxy(self):
        """Get the proxy location"""
        if 'out' in self.cfg:
            return self.cfg['out']
        FNULL = open(os.devnull, 'w')
        return subprocess.check_output(['grid-proxy-info','-path'],
                                       stderr=FNULL).decode('utf-8').strip()
