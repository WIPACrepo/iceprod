"""
Test script for priority
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('priority_test')

import os, sys, time
import shutil
import tempfile
import random
import unittest

from tornado.testing import AsyncTestCase

import iceprod.server
from iceprod.server import priority

def prio_setup():
    datasets = {
        'd0': {
            'dataset_id': 'd0',
            'jobs_submitted': 10,
            'tasks_submitted': 20,
            'priority': 1,
            'group': 'g_a',
            'username': 'u_a',
            'tasks': {
                't0': {'task_id': 't0', 'task_index': 0, 'job_index': 0},
                't1': {'task_id': 't1', 'task_index': 1, 'job_index': 0},
            }
        },
        'd1': {
            'dataset_id': 'd1',
            'jobs_submitted': 10,
            'tasks_submitted': 20,
            'priority': 1,
            'group': 'g_a',
            'username': 'u_b',
            'tasks': {
                't2': {'task_id': 't2', 'task_index': 0, 'job_index': 0},
                't3': {'task_id': 't3', 'task_index': 1, 'job_index': 0},
            }
        },
        'd2': {
            'dataset_id': 'd2',
            'jobs_submitted': 1000,
            'tasks_submitted': 20000,
            'priority': 1,
            'group': 'g_a',
            'username': 'u_a',
            'tasks': {
                't4': {'task_id': 't4', 'task_index': 0, 'job_index': 400},
                't5': {'task_id': 't5', 'task_index': 1, 'job_index': 400},
            }
        },
    }
    users = {
        'u_a': {'username': 'u_a', 'priority': 1.},
        'u_b': {'username': 'u_b', 'priority': .5},
    }
    groups = {
        'g_a': {'name':'g_a', 'priority': 1.},
    }

    p = priority.Priority(None)
    p.dataset_cache = datasets
    p.user_cache = users
    p.group_cache = groups
    return p

class priority_test(AsyncTestCase):
    def setUp(self):
        super(priority_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    @unittest_reporter
    async def test_10_get_dataset_prio(self):
        """Test get_dataset_prio"""
        p = prio_setup()
        prio1 = await p.get_dataset_prio('d0')
        prio2 = await p.get_dataset_prio('d1')
        self.assertLess(prio2, prio1)

        prio3 = await p.get_dataset_prio('d2')
        self.assertLess(prio3, prio1)

    @unittest_reporter
    async def test_20_get_task_prio(self):
        """Test get_task_prio"""
        p = prio_setup()
        prio1 = await p.get_task_prio('d0', 't0')
        prio2 = await p.get_task_prio('d1', 't2')
        # equal at 1.0 because of boost
        self.assertEqual(prio2, prio1)

        prio3 = await p.get_task_prio('d2', 't4')
        prio4 = await p.get_task_prio('d2', 't5')
        self.assertLess(prio3, prio1) # 
        self.assertLess(prio3, prio4) # ordering of tasks in a job


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(priority_test))
    suite.addTests(loader.loadTestsFromNames(alltests,priority_test))
    return suite
