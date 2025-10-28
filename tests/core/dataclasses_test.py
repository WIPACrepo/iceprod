"""
Test script for dataclasses
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('dataclasses')


try:
    pass
except:
    pass

import unittest
import iceprod.core.dataclasses


class dataclasses_test(unittest.TestCase):
    def setUp(self):
        super(dataclasses_test,self).setUp()

    def tearDown(self):
        super(dataclasses_test,self).tearDown()

    @unittest_reporter
    def test_01_Job(self):
        """Test the Job class"""
        j = iceprod.core.dataclasses.Job()

        if not j.valid():
            raise Exception('empty job not valid')

        j.convert()

        if not j.valid():
            raise Exception('converted empty job not valid')

    @unittest_reporter
    def test_02_Steering(self):
        """Test the Steering class"""
        s = iceprod.core.dataclasses.Steering()

        if not s.valid():
            raise Exception('empty steering not valid')

        s.convert()

        if not s.valid():
            raise Exception('converted empty steering not valid')

    @unittest_reporter
    def test_03_Task(self):
        """Test the Task class"""
        t = iceprod.core.dataclasses.Task()

        if not t.valid():
            raise Exception('empty task not valid')

        t.convert()

        if not t.valid():
            raise Exception('converted empty task not valid')

    @unittest_reporter
    def test_04_Tray(self):
        """Test the Tray class"""
        t = iceprod.core.dataclasses.Tray()

        if not t.valid():
            raise Exception('empty tray not valid')

        t.convert()

        if not t.valid():
            raise Exception('converted empty tray not valid')

    @unittest_reporter
    def test_05_Module(self):
        """Test the Module class"""
        m = iceprod.core.dataclasses.Module()

        if not m.valid():
            raise Exception('empty module not valid')

        m.convert()

        if not m.valid():
            raise Exception('converted empty module not valid')

    @unittest_reporter
    def test_06_Class(self):
        """Test the Class class"""
        c = iceprod.core.dataclasses.Class()

        if not c.valid():
            raise Exception('empty class not valid')

        c.convert()

        if not c.valid():
            raise Exception('converted empty class not valid')

    @unittest_reporter
    def test_08_Resource(self):
        """Test the Resource class"""
        r = iceprod.core.dataclasses.Resource()

        if not r.valid():
            raise Exception('empty resource not valid')

        r.convert()

        if not r.valid():
            raise Exception('converted empty resource not valid')

        r['transfer'] = False
        self.assertIs(r.do_transfer(), False)
        r['transfer'] = 'f'
        self.assertIs(r.do_transfer(), False)
        r['transfer'] = 'N'
        self.assertIs(r.do_transfer(), False)
        r['transfer'] = 0
        self.assertIs(r.do_transfer(), False)
        r['transfer'] = True
        self.assertIs(r.do_transfer(), True)
        r['transfer'] = 'T'
        self.assertIs(r.do_transfer(), True)
        r['transfer'] = 'Yes'
        self.assertIs(r.do_transfer(), True)
        r['transfer'] = 1
        self.assertIs(r.do_transfer(), True)
        r['transfer'] = 'maybe'
        self.assertEqual(r.do_transfer(), 'maybe')
        r['transfer'] = 'If'
        self.assertEqual(r.do_transfer(), 'maybe')
        r['transfer'] = 'if needed'
        self.assertEqual(r.do_transfer(), 'maybe')
        r['transfer'] = 'exists'
        self.assertEqual(r.do_transfer(), 'maybe')
        r['transfer'] = 'blah'
        self.assertIs(r.do_transfer(), True)
        r['transfer'] = 1234
        self.assertIs(r.do_transfer(), True)
        r['transfer'] = [1,2,3]
        self.assertIs(r.do_transfer(), True)

    @unittest_reporter
    def test_09_Data(self):
        """Test the Data class"""
        d = iceprod.core.dataclasses.Data()

        if not d.valid():
            raise Exception('empty data not valid')

        d.convert()

        if not d.valid():
            raise Exception('converted empty data not valid')

    @unittest_reporter
    def test_10_Batchsys(self):
        """Test the Batchsys class"""
        b = iceprod.core.dataclasses.Batchsys()

        if not b.valid():
            raise Exception('empty batchsys not valid')

        b.convert()

        if not b.valid():
            raise Exception('converted empty batchsys not valid')

    @unittest_reporter
    def test_20_DifPlus(self):
        """Test the DifPlus class"""
        d = iceprod.core.dataclasses.DifPlus()

        if not d.valid():
            raise Exception('empty difplus not valid')

        d.convert()

        if not d.valid():
            raise Exception('converted empty difplus not valid')

    @unittest_reporter
    def test_21_Dif(self):
        """Test the Dif class"""
        d = iceprod.core.dataclasses.Dif()

        if not d.valid():
            raise Exception('empty dif not valid')

        d.convert()

        if not d.valid():
            raise Exception('converted empty dif not valid')

    @unittest_reporter
    def test_22_Plus(self):
        """Test the Plus class"""
        p = iceprod.core.dataclasses.Plus()

        if not p.valid():
            raise Exception('empty plus not valid')

        p.convert()

        if not p.valid():
            raise Exception('converted empty plus not valid')

    @unittest_reporter
    def test_23_Personnel(self):
        """Test the Personnel class"""
        p = iceprod.core.dataclasses.Personnel()

        if not p.valid():
            raise Exception('empty personnel not valid')

        p.convert()

        if not p.valid():
            raise Exception('converted empty personnel not valid')

    @unittest_reporter
    def test_24_Datacenter(self):
        """Test the DataCenter class"""
        d = iceprod.core.dataclasses.DataCenter()

        if not d.valid():
            raise Exception('empty datacenter not valid')

        d.convert()

        if not d.valid():
            raise Exception('converted empty datacenter not valid')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dataclasses_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dataclasses_test))
    return suite
