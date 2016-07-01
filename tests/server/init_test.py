"""
Test script for server init scripts
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('server_init_test')

import os, sys, time
import shutil
import tempfile
import random
import unittest
import iceprod.server


class init_test(unittest.TestCase):
    def setUp(self):
        super(init_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(init_test,self).tearDown()

    @unittest_reporter
    def test_10_GlobalID_int2char(self):
        """Test GlobalID_int2char"""
        inputs = [0,26,61,62,100,124,126,59428801]
        outputs = ['a','A','9','aa','aM','ba','bc','david']
        for i,input in enumerate(inputs):
            output = iceprod.server.GlobalID.int2char(input)
            if outputs[i] != output:
                raise Exception('input=%d, given output=%s, correct output=%s'%(input,str(output),outputs[i]))

    @unittest_reporter
    def test_11_GlobalID_char2int(self):
        """Test GlobalID_char2int"""
        inputs = ['a','A','9','aa','aM','ba','bc','david']
        outputs = [0,26,61,62,100,124,126,59428801]
        for i,input in enumerate(inputs):
            output = iceprod.server.GlobalID.char2int(input)
            if outputs[i] != output:
                raise Exception('input=%s, given output=%s, correct output=%d'%(input,str(output),outputs[i]))

    @unittest_reporter
    def test_12_GlobalID_siteID_gen(self):
        """Test GlobalID_siteID_gen"""
        for i in range(1000):
            ret = iceprod.server.GlobalID.siteID_gen()
            ret2 = iceprod.server.GlobalID.char2int(ret)
            if ret2 < 0 or ret2 >= iceprod.server.GlobalID.MAXSITEID:
                raise Exception('outsite permissible range: %s : %d'%(ret,ret2))

    @unittest_reporter
    def test_13_GlobalID_globalID_gen(self):
        """Test GlobalID_globalID_gen"""
        for i in range(1000):
            ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
            ret = iceprod.server.GlobalID.globalID_gen(ran,iceprod.server.GlobalID.siteID_gen())
            ret2 = iceprod.server.GlobalID.char2int(ret)
            if ret2 < ran or ret2 >= (iceprod.server.GlobalID.MAXSITEID*iceprod.server.GlobalID.MAXLOCALID):
                raise Exception('outsite permissible range: (0<x<%d)  %d = %s'%((iceprod.server.GlobalID.MAXSITEID*iceprod.server.GlobalID.MAXLOCALID),ret2,ret))

    @unittest_reporter
    def test_14_GlobalID_localID_ret(self):
        """Test GlobalID_localID_ret"""
        for i in range(1000):
            ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
            ret = iceprod.server.GlobalID.globalID_gen(ran,iceprod.server.GlobalID.siteID_gen())
            ret2 = iceprod.server.GlobalID.localID_ret(ret,type='int')
            if ret2 != ran:
                raise Exception('returned local id does not match initial id: %d != %d'%(ran,ret2))

    @unittest_reporter
    def test_15_GlobalID_siteID_ret(self):
        """Test GlobalID_siteID_ret"""
        for i in xrange(1000):
            ran = random.randint(0,iceprod.server.GlobalID.MAXLOCALID-1)
            ran2 = iceprod.server.GlobalID.siteID_gen()
            ret = iceprod.server.GlobalID.globalID_gen(ran,ran2)
            ret2 = iceprod.server.GlobalID.siteID_ret(ret)
            if ret2 != ran2:
                raise Exception('returned site id does not match initial id: %d != %d'%(ran,ret2))

    @unittest_reporter
    def test_20_calc_dataset_prio(self):
        """Test calc_dataset_prio"""
        site = iceprod.server.GlobalID.siteID_gen()

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':0,
                  }
        prio1 = iceprod.server.calc_dataset_prio(dataset)
        if not isinstance(prio1,(int,float)):
            raise Exception('dataset prio is not a number')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':1,
                  }
        prio2 = iceprod.server.calc_dataset_prio(dataset)
        if prio2 < prio1:
            raise Exception('priority is not winning')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':100,
                   'priority':1,
                  }
        prio3 = iceprod.server.calc_dataset_prio(dataset)
        if prio2 < prio3:
            raise Exception('greater # tasks submitted is not losing')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(2,site),
                   'tasks_submitted':0,
                   'priority':1,
                  }
        prio4 = iceprod.server.calc_dataset_prio(dataset)
        if prio2 < prio4:
            raise Exception('greater dataset_id is not losing')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':-1,
                  }
        prio5 = iceprod.server.calc_dataset_prio(dataset)
        if prio5 != prio1:
            raise Exception('negative prio not reset to 0')

    @unittest_reporter
    def test_21_calc_datasets_prios(self):
        """Test calc_datasets_prios"""
        site = iceprod.server.GlobalID.siteID_gen()

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':0,
                  }
        dataset2 = {'dataset_id':iceprod.server.GlobalID.globalID_gen(2,site),
                   'tasks_submitted':0,
                   'priority':0,
                  }
        datasets = {dataset['dataset_id']:dataset,
                   dataset2['dataset_id']:dataset2}

        prios = iceprod.server.calc_datasets_prios(datasets)
        for p in prios.values():
            if not isinstance(p,(int,float)):
                raise Exception('dataset prio is not a number')
        if prios[dataset['dataset_id']] != prios[dataset2['dataset_id']]:
            logger.info(prios)
            raise Exception('datasets not equal in priority')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':1,
                  }
        dataset2 = {'dataset_id':iceprod.server.GlobalID.globalID_gen(2,site),
                   'tasks_submitted':0,
                   'priority':1,
                  }
        datasets = {dataset['dataset_id']:dataset,
                   dataset2['dataset_id']:dataset2}

        prios = iceprod.server.calc_datasets_prios(datasets)
        for p in prios.values():
            if not isinstance(p,(int,float)):
                raise Exception('dataset prio is not a number')
        if prios[dataset['dataset_id']] <= prios[dataset2['dataset_id']]:
            logger.info(prios)
            raise Exception('datasets in wrong order')

    @unittest_reporter
    def test_30_salt(self):
        s = iceprod.server.salt()
        if not isinstance(s,basestring):
            raise Exception('not a string')

        for _ in range(100):
            for l in range(1,100):
                s = iceprod.server.salt(l)
                if len(s) != l:
                    logger.info('len: %d. salt: %s',l,s)
                    raise Exception('salt is not correct length')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(init_test))
    suite.addTests(loader.loadTestsFromNames(alltests,init_test))
    return suite
