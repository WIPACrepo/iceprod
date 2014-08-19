"""
Test script for dataclasses
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('dataclasses')

import os
import sys
import json

try:
    import cPickle as pickle
except:
    import pickle

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from iceprod.core import to_log
import iceprod.core.dataclasses


class dataclasses_test(unittest.TestCase):
    def setUp(self):
        super(dataclasses_test,self).setUp()
    
    def tearDown(self):
        super(dataclasses_test,self).tearDown()

    def test_01_job(self):
        """Test the Job class"""
        try:
            j = iceprod.core.dataclasses.Job()
            
            if not j.valid():
                raise Exception('empty job not valid')
            
            j.convert()
            
            if not j.valid():
                raise Exception('converted empty job not valid')
            
        except Exception as e:
            logger.error('Error running Job class test: %s',str(e))
            printer('Test dataclasses.Job',False)
            raise
        else:
            printer('Test dataclasses.Job')

    def test_02_steering(self):
        """Test the Steering class"""
        try:
            s = iceprod.core.dataclasses.Steering()
            
            if not s.valid():
                raise Exception('empty steering not valid')
            
            s.convert()
            
            if not s.valid():
                raise Exception('converted empty steering not valid')
            
        except Exception as e:
            logger.error('Error running Steering class test: %s',str(e))
            printer('Test dataclasses.Steering',False)
            raise
        else:
            printer('Test dataclasses.Steering')

    def test_03_task(self):
        """Test the Task class"""
        try:
            t = iceprod.core.dataclasses.Task()
            
            if not t.valid():
                raise Exception('empty task not valid')
            
            t.convert()
            
            if not t.valid():
                raise Exception('converted empty task not valid')
            
        except Exception as e:
            logger.error('Error running Task class test: %s',str(e))
            printer('Test dataclasses.Task',False)
            raise
        else:
            printer('Test dataclasses.Task')

    def test_04_tray(self):
        """Test the Tray class"""
        try:
            t = iceprod.core.dataclasses.Tray()
            
            if not t.valid():
                raise Exception('empty tray not valid')
            
            t.convert()
            
            if not t.valid():
                raise Exception('converted empty tray not valid')
            
        except Exception as e:
            logger.error('Error running Tray class test: %s',str(e))
            printer('Test dataclasses.Tray',False)
            raise
        else:
            printer('Test dataclasses.Tray')

    def test_05_module(self):
        """Test the Module class"""
        try:
            m = iceprod.core.dataclasses.Module()
            
            if not m.valid():
                raise Exception('empty module not valid')
            
            m.convert()
            
            if not m.valid():
                raise Exception('converted empty module not valid')
            
        except Exception as e:
            logger.error('Error running Module class test: %s',str(e))
            printer('Test dataclasses.Module',False)
            raise
        else:
            printer('Test dataclasses.Module')

    def test_06_class(self):
        """Test the Class class"""
        try:
            c = iceprod.core.dataclasses.Class()
            
            if not c.valid():
                raise Exception('empty class not valid')
            
            c.convert()
            
            if not c.valid():
                raise Exception('converted empty class not valid')
            
        except Exception as e:
            logger.error('Error running Class class test: %s',str(e))
            printer('Test dataclasses.Class',False)
            raise
        else:
            printer('Test dataclasses.Class')

    def test_07_project(self):
        """Test the Project class"""
        try:
            p = iceprod.core.dataclasses.Project()
            
            if not p.valid():
                raise Exception('empty project not valid')
            
            p.convert()
            
            if not p.valid():
                raise Exception('converted empty project not valid')
            
        except Exception as e:
            logger.error('Error running Project class test: %s',str(e))
            printer('Test dataclasses.Project',False)
            raise
        else:
            printer('Test dataclasses.Project')

    def test_08_resource(self):
        """Test the Resource class"""
        try:
            r = iceprod.core.dataclasses.Resource()
            
            if not r.valid():
                raise Exception('empty resource not valid')
            
            r.convert()
            
            if not r.valid():
                raise Exception('converted empty resource not valid')
            
        except Exception as e:
            logger.error('Error running Resource class test: %s',str(e))
            printer('Test dataclasses.Resource',False)
            raise
        else:
            printer('Test dataclasses.Resource')

    def test_09_data(self):
        """Test the Data class"""
        try:
            d = iceprod.core.dataclasses.Data()
            
            if not d.valid():
                raise Exception('empty data not valid')
            
            d.convert()
            
            if not d.valid():
                raise Exception('converted empty data not valid')
            
        except Exception as e:
            logger.error('Error running Data class test: %s',str(e))
            printer('Test dataclasses.Data',False)
            raise
        else:
            printer('Test dataclasses.Data')

    def test_10_batchsys(self):
        """Test the Batchsys class"""
        try:
            b = iceprod.core.dataclasses.Batchsys()
            
            if not b.valid():
                raise Exception('empty batchsys not valid')
            
            b.convert()
            
            if not b.valid():
                raise Exception('converted empty batchsys not valid')
            
        except Exception as e:
            logger.error('Error running Batchsys class test: %s',str(e))
            printer('Test dataclasses.Batchsys',False)
            raise
        else:
            printer('Test dataclasses.Batchsys')

    def test_20_difplus(self):
        """Test the DifPlus class"""
        try:
            d = iceprod.core.dataclasses.DifPlus()
            
            if not d.valid():
                raise Exception('empty difplus not valid')
            
            d.convert()
            
            if not d.valid():
                raise Exception('converted empty difplus not valid')
            
        except Exception as e:
            logger.error('Error running DifPlus class test: %s',str(e))
            printer('Test dataclasses.DifPlus',False)
            raise
        else:
            printer('Test dataclasses.DifPlus')

    def test_21_dif(self):
        """Test the Dif class"""
        try:
            d = iceprod.core.dataclasses.Dif()
            
            if not d.valid():
                raise Exception('empty dif not valid')
            
            d.convert()
            
            if not d.valid():
                raise Exception('converted empty dif not valid')
            
        except Exception as e:
            logger.error('Error running Dif class test: %s',str(e))
            printer('Test dataclasses.Dif',False)
            raise
        else:
            printer('Test dataclasses.Dif')

    def test_22_plus(self):
        """Test the Plus class"""
        try:
            p = iceprod.core.dataclasses.Plus()
            
            if not p.valid():
                raise Exception('empty plus not valid')
            
            p.convert()
            
            if not p.valid():
                raise Exception('converted empty plus not valid')
            
        except Exception as e:
            logger.error('Error running Plus class test: %s',str(e))
            printer('Test dataclasses.Plus',False)
            raise
        else:
            printer('Test dataclasses.Plus')

    def test_23_personnel(self):
        """Test the Personnel class"""
        try:
            p = iceprod.core.dataclasses.Personnel()
            
            if not p.valid():
                raise Exception('empty personnel not valid')
            
            p.convert()
            
            if not p.valid():
                raise Exception('converted empty personnel not valid')
            
        except Exception as e:
            logger.error('Error running Personnel class test: %s',str(e))
            printer('Test dataclasses.Personnel',False)
            raise
        else:
            printer('Test dataclasses.Personnel')

    def test_24_datacenter(self):
        """Test the DataCenter class"""
        try:
            d = iceprod.core.dataclasses.DataCenter()
            
            if not d.valid():
                raise Exception('empty datacenter not valid')
            
            d.convert()
            
            if not d.valid():
                raise Exception('converted empty datacenter not valid')
            
        except Exception as e:
            logger.error('Error running DataCenter class test: %s',str(e))
            printer('Test dataclasses.DataCenter',False)
            raise
        else:
            printer('Test dataclasses.DataCenter')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dataclasses_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dataclasses_test))
    return suite
