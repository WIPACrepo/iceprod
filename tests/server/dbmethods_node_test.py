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
try:
    import StringIO
except ImportError:
    from io import StringIO
from datetime import datetime,timedelta
from collections import OrderedDict, Iterable
import unittest

import tornado.escape

from iceprod.core import functions
from iceprod.core.jsonUtil import json_encode,json_decode
from iceprod.server import dbmethods

from .dbmethods_test import dbmethods_base


class dbmethods_node_test(dbmethods_base):
    @unittest_reporter
    def test_01_node_update(self):
        """Test node_update"""
        yield self.set_tables({'node':[]})

        hostname = 'host'
        domain = 'domain'
        stats = {'stat1':1,'stat2':2}

        yield self.db['node_update'](hostname=hostname,domain=domain,**stats)

        endtables = yield self.get_tables(['node'])
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
        yield self.db['node_update'](hostname=hostname,domain=domain,**morestats)

        endtables = yield self.get_tables(['node'])
        if len(endtables['node']) != 1:
            logger.info('nodes: %r',endtables['node'])
            raise Exception('node table does not have 1 entry')
        retstats = stats.copy()
        retstats.update(morestats)
        if json_decode(endtables['node'][0]['stats']) != retstats:
            logger.info('stats: %s',endtables['node'][0]['stats'])
            logger.info('expecting: %r',retstats)
            raise Exception('bad more stats')

        # combine stats
        yield self.set_tables({'node':[]})
        stats2 = {'a': {'stat1':1,'stat2':2} }
        morestats2 = {'a': {'stat2':4,'stat3':3} }
        yield self.db['node_update'](hostname=hostname,domain=domain,**stats2)
        yield self.db['node_update'](hostname=hostname,domain=domain,**morestats2)

        endtables = yield self.get_tables(['node'])
        if len(endtables['node']) != 1:
            logger.info('nodes: %r',endtables['node'])
            raise Exception('node table does not have 1 entry')
        retstats = {'a': {'stat1':1,'stat2':4,'stat3':3} }
        if json_decode(endtables['node'][0]['stats']) != retstats:
            logger.info('stats: %s',endtables['node'][0]['stats'])
            logger.info('expecting: %r',retstats)
            raise Exception('bad more stats')

        # failed updates
        yield self.set_tables({'node':[]})
        yield self.db['node_update']()
        endtables = yield self.get_tables(['node'])
        if endtables['node']:
            raise Exception('node updated when nothing to update')

        # failed query
        for i in range(2):
            yield self.set_tables({'node':[]})
            self.set_failures([False for _ in range(i)]+[True])
            try:
                yield self.db['node_update'](hostname=hostname,domain=domain,**stats)
            except:
                pass
            else:
                raise Exception('did not raise Exception')
            endtables = yield self.get_tables(['node'])
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

        yield self.set_tables(tables)
        yield self.db['node_collate_resources'](site_id)

        endtables = yield self.get_tables(['site'])
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
        yield self.set_tables(tables)
        yield self.db['node_collate_resources']()
        endtables = yield self.get_tables(['site'])
        if len(endtables['site']) != 1:
            logger.info('site: %r',endtables['site'])
            raise Exception('site table does not have 1 entry')
        if not cmp_dict(endtables['site'][0],tables['site'][0]):
            logger.info('begin: %r',tables['site'][0])
            logger.info('end: %r',endtables['site'][0])
            raise Exception('site table modified')

        # test for no nodes
        yield self.set_tables({'site':tables['site'],'node':[]})
        yield self.db['node_collate_resources'](site_id)
        endtables = yield self.get_tables(['site'])
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
        yield self.set_tables(tables2)
        yield self.db['node_collate_resources'](site_id)
        endtables = yield self.get_tables(['site'])
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
        yield self.set_tables(tables3)
        yield self.db['node_collate_resources'](site_id)
        endtables = yield self.get_tables(['site'])
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
        yield self.set_tables(tables4)
        yield self.db['node_collate_resources'](site_id)
        endtables = yield self.get_tables(['site'])
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
        for i in range(3):
            yield self.set_tables(tables)
            self.set_failures([False for _ in range(i)]+[True])
            yield self.db['node_collate_resources'](site_id)

            endtables = yield self.get_tables(['site'])
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

        yield self.set_tables(tables)
        ret = yield self.db['node_get_site_resources'](site_id)
        ret_resources = {"memory":2,"cpu":6}
        self.assertEqual(ret, ret_resources)

        # test no site_id
        yield self.set_tables(tables)
        try:
            ret = yield self.db['node_get_site_resources']()
        except Exception:
            pass
        else:
            raise Exception('did not raise exception')

        # test no sites
        yield self.set_tables({'site':[]})
        try:
            ret = yield self.db['node_get_site_resources'](site_id)
        except Exception:
            pass
        else:
            raise Exception('did not raise exception')

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
        yield self.set_tables(tables2)
        ret = yield self.db['node_get_site_resources'](site_id)
        self.assertEqual(ret, {})

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
        yield self.set_tables(tables)
        ret = yield self.db['node_get_site_resources'](site_id)
        ret_resources = {"memory":4,"cpu":6,"gpu":"gtx980"}
        self.assertEqual(ret, ret_resources)

        # query error
        yield self.set_tables(tables)
        self.set_failures(True)
        try:
            ret = yield self.db['node_get_site_resources'](site_id)
        except Exception:
            pass
        else:
            raise Exception('did not raise exception')

        # resource error
        tables = {
            'site':[
                {'site_id':site_id,'name':'n','institution':'inst',
                 'queues':'{"'+gridspec+'":{"type":"condor","description":"desc","resources":}}',
                 'auth_key':None,'website_url':'','version':'2',
                 'last_update':dbmethods.nowstr(),
                 'admin_name':'','admin_email':''},
            ],
        }
        self.set_failures(False)
        yield self.set_tables(tables)
        try:
            ret = yield self.db['node_get_site_resources'](site_id)
        except Exception:
            pass
        else:
            raise Exception('did not raise exception')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dbmethods_node_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dbmethods_node_test))
    return suite
