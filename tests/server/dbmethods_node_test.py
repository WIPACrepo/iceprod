"""
Test script for dbmethods.node
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests, cmp_dict

import logging
logger = logging.getLogger('dbmethods_test')

import os, sys, time
import shutil
import tempfile
import random
import stat
import StringIO
from itertools import izip
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import tornado.escape

from flexmock import flexmock

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base


class dbmethods_node_test(dbmethods_base):
    @unittest_reporter
    def test_01_node_update(self):
        """Test node_update"""
        self.mock.setup({'node':[]})
        hostname = 'host'
        domain = 'domain'
        stats = {'stat1':1,'stat2':2}

        self._db.node_update(hostname=hostname,domain=domain,**stats)

        endtables = self.mock.get(['node'])
        if len(endtables['node']) != 1:
            logger.info('nodes: %r',endtables['node'])
            raise Exception('node table does not have 1 entry')
        if endtables['node'][0]['hostname'] != hostname:
            logger.info('nodes: %r',endtables['node'])
            raise Exception('bad hostname')
        if endtables['node'][0]['domain'] != domain:
            logger.info('nodes: %r',endtables['node'])
            raise Exception('bad domain')
        if json_decode(endtables['node'][0]['stats']) != stats:
            logger.info('nodes: %r',endtables['node'])
            raise Exception('bad stats')

        # add more stats to same node
        morestats = {'stat2':4,'stat3':3}
        self._db.node_update(hostname=hostname,domain=domain,**morestats)

        endtables = self.mock.get(['node'])
        if len(endtables['node']) != 1:
            logger.info('nodes: %r',endtables['node'])
            raise Exception('node table does not have 1 entry')
        retstats = stats.copy()
        retstats.update(morestats)
        if json_decode(endtables['node'][0]['stats']) != retstats:
            logger.info('stats: %s',endtables['node'][0]['stats'])
            logger.info('expecting: %r',retstats)
            raise Exception('bad more stats')

        # failed updates
        self.mock.setup({'node':[]})
        self._db.node_update()
        endtables = self.mock.get(['node'])
        if endtables['node']:
            raise Exception('node updated when nothing to update')

        # failed updates
        for i in range(1,3):
            self.mock.setup({'node':[]})
            self.mock.failures = i
            self._db.node_update(hostname=hostname,domain=domain,**stats)
            endtables = self.mock.get(['node'])
            if endtables['node']:
                raise Exception('node updated when failure occurred')

    @unittest_reporter
    def test_02_node_collate_resources(self):
        """Test node_collate_resources"""
        site_id = 'thesite'
        gridspec = 'gspec'
        tables = {
            'site':[
                {'site_id':site_id,'name':'n','institution':'inst',
                 'queues':'{"'+gridspec+'":{"type":"condor","description":"desc","resources":{}}}',
                 'auth_key':None,'website_url':'','version':'2',
                 'last_update':dbmethods.nowstr(),
                 'admin_name':'','admin_email':''},
            ],
            'node':[
                {'node_id':'a','hostname':'host1','domain':'d1',
                 'last_update':dbmethods.nowstr(),
                 'stats':'{"gridspec":"'+gridspec+'","memory":4}'},
            ],
        }

        self.mock.setup(tables)
        self._db.node_collate_resources(site_id)

        endtables = self.mock.get(['site'])
        if len(endtables['site']) != 1:
            logger.info('site: %r',endtables['site'])
            raise Exception('site table does not have 1 entry')
        queues = json_decode(endtables['site'][0]['queues'])
        if gridspec not in queues:
            logger.info('queues: %r',queues)
            raise Exception('bad queues')
        if 'memory' not in queues[gridspec]['resources']:
            logger.info('queues: %r',queues)
            raise Exception('no memory')
        if queues[gridspec]['resources']['memory'] != [4,4]:
            logger.info('queues: %r',queues)
            raise Exception('memory != [4,4]')

        # test for no site_id
        self.mock.setup(tables)
        self._db.node_collate_resources()
        endtables = self.mock.get(['site'])
        if len(endtables['site']) != 1:
            logger.info('site: %r',endtables['site'])
            raise Exception('site table does not have 1 entry')
        if not cmp_dict(endtables['site'][0],tables['site'][0]):
            logger.info('begin: %r',tables['site'][0])
            logger.info('end: %r',endtables['site'][0])
            raise Exception('site table modified')

        # test for no nodes
        self.mock.setup({'site':tables['site'],'node':[]})
        self._db.node_collate_resources(site_id)
        endtables = self.mock.get(['site'])
        if len(endtables['site']) != 1:
            logger.info('site: %r',endtables['site'])
            raise Exception('site table does not have 1 entry')
        if not cmp_dict(endtables['site'][0],tables['site'][0]):
            logger.info('begin: %r',tables['site'][0])
            logger.info('end: %r',endtables['site'][0])
            raise Exception('site table modified')

        tables2 = {
            'site':tables['site'],
            'node':[
                {'node_id':'a','hostname':'host1','domain':'d1',
                 'last_update':dbmethods.nowstr(),
                 'stats':'{"gridspec":"'+gridspec+'","memory":4,"cpu":0}'},
            ],
        }

        # test for 0 or false resources
        self.mock.setup(tables2)
        self._db.node_collate_resources(site_id)
        endtables = self.mock.get(['site'])
        if len(endtables['site']) != 1:
            logger.info('site: %r',endtables['site'])
            raise Exception('site table does not have 1 entry')
        queues = json_decode(endtables['site'][0]['queues'])
        if 'memory' not in queues[gridspec]['resources']:
            logger.info('queues: %r',queues)
            raise Exception('no memory')
        if 'cpu' in queues[gridspec]['resources']:
            logger.info('queues: %r',queues)
            raise Exception('cpu appeared but was 0')

        tables3 = {
            'site':tables['site'],
            'node':[
                {'node_id':'a','hostname':'host1','domain':'d1',
                 'last_update':dbmethods.nowstr(),
                 'stats':'{"gridspec":"'+gridspec+'","memory":4}'},
                {'node_id':'b','hostname':'host1','domain':'d1',
                 'last_update':dbmethods.nowstr(),
                 'stats':'{"gridspec":"'+gridspec+'","memory":4}'},
                {'node_id':'c','hostname':'host1','domain':'d1',
                 'last_update':dbmethods.nowstr(),
                 'stats':'{"gridspec":"'+gridspec+'","memory":4}'},
            ],
        }

        # test for summing resources
        self.mock.setup(tables3)
        self._db.node_collate_resources(site_id)
        endtables = self.mock.get(['site'])
        if len(endtables['site']) != 1:
            logger.info('site: %r',endtables['site'])
            raise Exception('site table does not have 1 entry')
        queues = json_decode(endtables['site'][0]['queues'])
        if 'memory' not in queues[gridspec]['resources']:
            logger.info('queues: %r',queues)
            raise Exception('no memory')
        if queues[gridspec]['resources']['memory'] != [12,12]:
            logger.info('queues: %r',queues)
            raise Exception('memory != [12,12]')

        tables4 = {
            'site':tables['site'],
            'node':[
                {'node_id':'a','hostname':'host1','domain':'d1',
                 'last_update':dbmethods.nowstr(),
                 'stats':'{"gridspec":"'+gridspec+'","gpu":"gtx980"}'},
                {'node_id':'b','hostname':'host1','domain':'d1',
                 'last_update':dbmethods.nowstr(),
                 'stats':'{"gridspec":"'+gridspec+'","gpu":"gtx980"}'},
            ],
        }

        # test for non-number resources
        self.mock.setup(tables4)
        self._db.node_collate_resources(site_id)
        endtables = self.mock.get(['site'])
        if len(endtables['site']) != 1:
            logger.info('site: %r',endtables['site'])
            raise Exception('site table does not have 1 entry')
        queues = json_decode(endtables['site'][0]['queues'])
        if 'gpu' not in queues[gridspec]['resources']:
            logger.info('queues: %r',queues)
            raise Exception('missing gpu')
        if queues[gridspec]['resources']['gpu'] != ["gtx980",0]:
            logger.info('queues: %r',queues)
            raise Exception('gpu != ["gtx980",0]')

        # test for sql errors
        for i in range(1,4):
            self.mock.setup(tables)
            self.mock.failures = i
            self._db.node_collate_resources(site_id)
            endtables = self.mock.get(['site'])
            if len(endtables['site']) != 1:
                logger.info('site: %r',endtables['site'])
                raise Exception('site table does not have 1 entry')
            if not cmp_dict(endtables['site'][0],tables['site'][0]):
                logger.info('begin: %r',tables['site'][0])
                logger.info('end: %r',endtables['site'][0])
                raise Exception('site table modified')

    @unittest_reporter
    def test_03_node_get_site_resources(self):
        """Test node_get_site_resources"""
        site_id = 'thesite'
        gridspec = 'gspec'
        resources = '{"memory":[4,2],"cpu":[8,6]}'
        tables = {
            'site':[
                {'site_id':site_id,'name':'n','institution':'inst',
                 'queues':'{"'+gridspec+'":{"type":"condor","description":"desc","resources":'+resources+'}}',
                 'auth_key':None,'website_url':'','version':'2',
                 'last_update':dbmethods.nowstr(),
                 'admin_name':'','admin_email':''},
            ],
        }

        def cb(ret):
            cb.ret = ret

        self.mock.setup(tables)
        cb.ret = False
        self._db.node_get_site_resources(site_id,callback=cb)

        if isinstance(cb.ret,Exception):
            logger.info(cb.ret)
            raise Exception('exception raised in get_site_resources')
        ret_resources = {"memory":2,"cpu":6}
        if cb.ret != ret_resources:
            logger.info('expected: %r',ret_resources)
            logger.info('got: %r',cb.ret)
            raise Exception('did not get expected resources')

        # test no site_id
        self.mock.setup(tables)
        cb.ret = False
        self._db.node_get_site_resources(callback=cb)
        if not isinstance(cb.ret,Exception):
            raise Exception('did not return exception')

        # test no sites
        self.mock.setup({'site':[]})
        cb.ret = False
        self._db.node_get_site_resources(site_id,callback=cb)
        if not isinstance(cb.ret,Exception):
            raise Exception('did not return exception')

        # test no resources
        tables2 = {
            'site':[
                {'site_id':site_id,'name':'n','institution':'inst',
                 'queues':'{"'+gridspec+'":{"type":"condor","description":"desc"}}',
                 'auth_key':None,'website_url':'','version':'2',
                 'last_update':dbmethods.nowstr(),
                 'admin_name':'','admin_email':''},
            ],
        }
        self.mock.setup(tables2)
        cb.ret = False
        self._db.node_get_site_resources(site_id,callback=cb)
        if isinstance(cb.ret,Exception):
            logger.info(cb.ret)
            raise Exception('exception raised in get_site_resources')
        if cb.ret != {}:
            logger.info('got: %r',cb.ret)
            raise Exception('did not no resources')

        # test multiple queues
        resources2 = '{"memory":[4,2],"gpu":["gtx980",0]}'
        tables = {
            'site':[
                {'site_id':site_id,'name':'n','institution':'inst',
                 'queues':'{"'+gridspec+'":{"type":"condor","description":"desc","resources":'+resources+'},'
                           '"gs2":{"type":"condor","description":"desc","resources":'+resources2+'}}',
                 'auth_key':None,'website_url':'','version':'2',
                 'last_update':dbmethods.nowstr(),
                 'admin_name':'','admin_email':''},
            ],
        }
        self.mock.setup(tables)
        cb.ret = False
        self._db.node_get_site_resources(site_id,callback=cb)
        if isinstance(cb.ret,Exception):
            logger.info(cb.ret)
            raise Exception('exception raised in get_site_resources')
        ret_resources = {"memory":4,"cpu":6,"gpu":"gtx980"}
        if cb.ret != ret_resources:
            logger.info('expected: %r',ret_resources)
            logger.info('got: %r',cb.ret)
            raise Exception('did not get expected resources')


        # test sql error
        self.mock.setup(tables)
        self.mock.failures = 1
        cb.ret = False
        self._db.node_get_site_resources(site_id,callback=cb)
        if not isinstance(cb.ret,Exception):
            raise Exception('did not return exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_node_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_node_test))
    return suite
