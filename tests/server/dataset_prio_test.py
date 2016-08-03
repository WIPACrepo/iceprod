"""
Test script for dataset_prio
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('dataset_prio_test')

import os, sys, time
import shutil
import tempfile
import random
import unittest

import iceprod.server
from iceprod.server import dataset_prio

class dataset_prio_test(unittest.TestCase):
    def setUp(self):
        super(dataset_prio_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    @unittest_reporter
    def test_20_calc_dataset_prio(self):
        """Test calc_dataset_prio"""
        site = iceprod.server.GlobalID.siteID_gen()

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':0,
                  }
        prio1 = dataset_prio.calc_dataset_prio(dataset)
        if not isinstance(prio1,(int,float)):
            raise Exception('dataset prio is not a number')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':1,
                  }
        prio2 = dataset_prio.calc_dataset_prio(dataset)
        if prio2 < prio1:
            raise Exception('priority is not winning')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':100,
                   'priority':1,
                  }
        prio3 = dataset_prio.calc_dataset_prio(dataset)
        if prio2 < prio3:
            raise Exception('greater # tasks submitted is not losing')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(2,site),
                   'tasks_submitted':0,
                   'priority':1,
                  }
        prio4 = dataset_prio.calc_dataset_prio(dataset)
        if prio2 < prio4:
            raise Exception('greater dataset_id is not losing')

        dataset = {'dataset_id':iceprod.server.GlobalID.globalID_gen(1,site),
                   'tasks_submitted':0,
                   'priority':-1,
                  }
        prio5 = dataset_prio.calc_dataset_prio(dataset)
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

        prios = dataset_prio.calc_datasets_prios(datasets)
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

        prios = dataset_prio.calc_datasets_prios(datasets)
        for p in prios.values():
            if not isinstance(p,(int,float)):
                raise Exception('dataset prio is not a number')
        if prios[dataset['dataset_id']] <= prios[dataset2['dataset_id']]:
            logger.info(prios)
            raise Exception('datasets in wrong order')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dataset_prio_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dataset_prio_test))
    return suite
