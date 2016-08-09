"""
Test script for parser
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('parser_test')

import os, sys, time
import shutil
import random
import string
import subprocess
import threading
import unittest

from flexmock import flexmock

from iceprod.core import to_log
from iceprod.core import parser, dataclasses

class parser_test(unittest.TestCase):
    def setUp(self):
        super(parser_test,self).setUp()

    def tearDown(self):
        super(parser_test,self).tearDown()

    @unittest_reporter
    def test_01_steering(self):
        """Test parser steering"""
        job = dataclasses.Job()
        job['steering'] = dataclasses.Steering()
        job['steering']['parameters'] = {
            'test': 1,
            'test2': 't2',
        }

        p = parser.ExpParser()
        p.job = job

        # run tests
        ret = p.steering_func('test')
        expected = str(job['steering']['parameters']['test'])
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test: ret != expected')

        ret = p.steering_func('test2')
        expected = str(job['steering']['parameters']['test2'])
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')

        try:
            p.steering_func('test3')
        except parser.GrammarException:
            pass
        else:
            raise Exception('test3: did not raise GrammarException')

    @unittest_reporter
    def test_02_system(self):
        """Test parser system"""
        job = dataclasses.Job()
        job['steering'] = dataclasses.Steering()
        job['steering']['system'] = {
            'test': 1,
            'test2': 't2',
        }

        p = parser.ExpParser()
        p.job = job

        # run tests
        ret = p.system_func('test')
        expected = str(job['steering']['system']['test'])
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test: ret != expected')

        ret = p.system_func('test2')
        expected = str(job['steering']['system']['test2'])
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')

        try:
            p.system_func('test3')
        except parser.GrammarException:
            pass
        else:
            raise Exception('test3: did not raise GrammarException')

    @unittest_reporter
    def test_03_options(self):
        """Test parser options"""
        job = dataclasses.Job()
        job['options'] = {
            'test': 1,
            'test2': 't2',
        }

        p = parser.ExpParser()
        p.job = job

        # run tests
        ret = p.options_func('test')
        expected = str(job['options']['test'])
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test: ret != expected')

        ret = p.options_func('test2')
        expected = str(job['options']['test2'])
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')

        try:
            p.options_func('test3')
        except parser.GrammarException:
            pass
        else:
            raise Exception('test3: did not raise GrammarException')

    @unittest_reporter
    def test_04_difplus(self):
        """Test parser difplus"""
        job = dataclasses.Job()
        job['difplus'] = dataclasses.DifPlus()
        job['difplus']['dif'] = dataclasses.Dif()
        job['difplus']['plus'] = dataclasses.Plus()
        job['difplus']['plus']['category'] = 'filtered'

        p = parser.ExpParser()
        p.job = job

        # run tests
        ret = p.difplus_func('sensor_name')
        expected = job['difplus']['dif']['sensor_name']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('dif: ret != expected')

        ret = p.difplus_func('category')
        expected = job['difplus']['plus']['category']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('plus: ret != expected')

        try:
            p.difplus_func('test')
        except parser.GrammarException:
            pass
        else:
            raise Exception('not present: did not raise GrammarException')

    @unittest_reporter
    def test_05_eval(self):
        """Test parser eval"""
        p = parser.ExpParser()

        # run tests
        ret = p.eval_func('4+4')
        expected = '8'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('simple: ret != expected')

        ret = p.eval_func('(4+3*2)%3')
        expected = '1'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('nested: ret != expected')

        try:
            p.eval_func('import os')
        except parser.GrammarException:
            pass
        else:
            raise Exception('import: did not raise GrammarException')

        try:
            p.eval_func('os.remove("/")')
        except parser.GrammarException:
            pass
        else:
            raise Exception('remove: did not raise GrammarException')

    @unittest_reporter
    def test_06_sprintf(self):
        """Test parser sprintf"""
        p = parser.ExpParser()

        # run tests
        ret = p.sprintf_func('"%d",5')
        expected = '5'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('"d": ret != expected')

        ret = p.sprintf_func('\'%d\',5')
        expected = '5'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('\'d\': ret != expected')

        ret = p.sprintf_func('%d,5')
        expected = '5'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('d: ret != expected')

        ret = p.sprintf_func('"%s %06d","testing",12')
        expected = 'testing 000012'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('s 06d: ret != expected')

        try:
            p.sprintf_func('"%s,12')
        except parser.GrammarException:
            pass
        else:
            raise Exception('missing quote: did not raise GrammarException')

        try:
            p.sprintf_func('%s')
        except parser.GrammarException:
            pass
        else:
            raise Exception('no args: did not raise GrammarException')

        try:
            p.sprintf_func('"%s"')
        except parser.GrammarException:
            pass
        else:
            raise Exception('no args2: did not raise GrammarException')

        try:
            p.sprintf_func('"%f","test"')
        except parser.GrammarException:
            pass
        else:
            raise Exception('bad type: did not raise GrammarException')

    @unittest_reporter
    def test_07_choice(self):
        """Test parser choice"""
        p = parser.ExpParser()

        # run tests
        ret = p.choice_func('1,2,3,4')
        expected = ('1','2','3','4')
        if ret not in expected:
            logger.info('multi: ret=%r, expected=%r',ret,expected)
            raise Exception('ret != expected')
        ret = p.choice_func([1,2,3,4])
        if ret not in expected:
            logger.info('multi: ret=%r, expected=%r',ret,expected)
            raise Exception('ret != expected')

        ret = p.choice_func('1')
        expected = '1'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('single: ret != expected')

        ret = p.choice_func('')
        expected = ''
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('empty: ret != expected')

        try:
            p.choice_func(123)
        except parser.GrammarException:
            pass
        else:
            raise Exception('number: did not raise GrammarException')

    @unittest_reporter(name='parse() steering')
    def test_10_steering(self):
        """Test parser parse steering"""
        job = dataclasses.Job()
        job['steering'] = dataclasses.Steering()
        job['steering']['parameters'] = {
            'test': 1,
            'test2': 't2',
        }

        p = parser.ExpParser()

        # run tests
        ret = p.parse('$steering(test)',job=job)
        expected = job['steering']['parameters']['test']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test: ret != expected')

        ret = p.parse('$steering(test2)',job=job)
        expected = job['steering']['parameters']['test2']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')


        ret = p.parse('$steering(test3)',job=job)
        expected = '$steering(test3)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test3: ret != expected')

    @unittest_reporter(name='parse() system')
    def test_11_system(self):
        """Test parser parse system"""
        job = dataclasses.Job()
        job['steering'] = dataclasses.Steering()
        job['steering']['system'] = {
            'test': 1,
            'test2': 't2',
        }

        p = parser.ExpParser()

        # run tests
        ret = p.parse('$system(test)',job=job)
        expected = job['steering']['system']['test']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test: ret != expected')

        ret = p.parse('$system(test2)',job=job)
        expected = job['steering']['system']['test2']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')

        ret = p.parse('$system(test3)',job=job)
        expected = '$system(test3)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test3: ret != expected')

    @unittest_reporter(name='parse() options')
    def test_12_options(self):
        """Test parser parse options"""
        job = dataclasses.Job()
        job['options'] = {
            'test': 1,
            'test2': 't2',
        }

        p = parser.ExpParser()

        # run tests
        ret = p.parse('$options(test)',job=job)
        expected = job['options']['test']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test: ret != expected')

        ret = p.parse('$options(test2)',job=job)
        expected = job['options']['test2']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')

        ret = p.parse('$(test2)',job=job)
        expected = job['options']['test2']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2 general search: ret != expected')

        ret = p.parse('$(test3)',job=job)
        expected = '$(test3)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test3: ret != expected')

        ret = p.parse('$args(test)',job=job)
        expected = job['options']['test']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('args: ret != expected')

    @unittest_reporter(name='parse() difplus')
    def test_13_difplus(self):
        """Test parser difplus"""
        job = dataclasses.Job()
        job['difplus'] = dataclasses.DifPlus()
        job['difplus']['dif'] = dataclasses.Dif()
        job['difplus']['plus'] = dataclasses.Plus()
        job['difplus']['plus']['category'] = 'filtered'

        p = parser.ExpParser()

        # run tests
        ret = p.parse('$metadata(sensor_name)',job=job)
        expected = job['difplus']['dif']['sensor_name']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('dif: ret != expected')

        ret = p.parse('$metadata(category)',job=job)
        expected = job['difplus']['plus']['category']
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('plus: ret != expected')

        ret = p.parse('$metadata(test)',job=job)
        expected = '$metadata(test)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('plus: ret != expected')

    @unittest_reporter(name='parse() eval')
    def test_14_eval(self):
        """Test parser parse eval"""
        p = parser.ExpParser()

        # run tests
        ret = p.parse('$eval(4+4)')
        expected = 8
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('simple: ret != expected')

        ret = p.parse('$eval(\(4+3*2\)%3)')
        expected = 1
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('nested: ret != expected')

        ret = p.parse('$eval(import os)')
        expected = '$eval(import os)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('import: ret != expected')

        ret = p.parse('$eval(os.remove\("/"\))')
        expected = '$eval(os.remove("/"))'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('remove: ret != expected')

    @unittest_reporter(name='parse() sprintf')
    def test_15_sprintf(self):
        """Test parser parse sprintf"""
        p = parser.ExpParser()

        # run tests
        ret = p.parse('$sprintf("%d",5)')
        expected = 5
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('"d": ret != expected')

        ret = p.parse('$sprintf(\'%d\',5)')
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('\'d\': ret != expected')

        ret = p.parse('$sprintf(%d,5)')
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('d: ret != expected')

        ret = p.parse('$sprintf("%s %06d","testing",12)')
        expected = 'testing 000012'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('s 06d: ret != expected')

        ret = p.parse('$sprintf("%s,12)')
        expected = '$sprintf("%s,12)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('missing quote: ret != expected')

        ret = p.parse('$sprintf(%s)')
        expected = '$sprintf(%s)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('no args: ret != expected')

        ret = p.parse('$sprintf("%s")')
        expected = '$sprintf("%s")'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('no args2: ret != expected')

        ret = p.parse('$sprintf("%f","test")')
        expected = '$sprintf("%f","test")'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('bad type: ret != expected')

    @unittest_reporter(name='parse() choice')
    def test_16_choice(self):
        """Test parser parse choice"""
        p = parser.ExpParser()

        # run tests
        ret = p.parse('$choice(1,2,3,4)')
        expected = (1,2,3,4)
        if ret not in expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('multi: ret != expected')

        ret = p.parse('$choice(1)')
        expected = 1
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('single: ret != expected')

        ret = p.parse('$choice()')
        expected = '$choice()'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('empty: ret != expected')

    @unittest_reporter(name='parse() env')
    def test_20_env(self):
        """Test parser parse env"""
        p = parser.ExpParser()
        env = {'parameters':{'test':1}}

        # run tests
        ret = p.parse('$(test)',env=env)
        expected = 1
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('sentence: ret != expected')

        ret = p.parse('$test()',env=env)
        expected = '$test()'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('keyword: ret != expected')

        ret = p.parse('$test',env=env)
        expected = '$test'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('bare: ret != expected')

        ret = p.parse('$eval(os.remove("/"))',env=env)
        expected = '$eval(os.remove("/"))'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('remove: ret != expected')

    @unittest_reporter(name='parse() job')
    def test_21_job(self):
        """Test parser parse job"""
        job = dataclasses.Job()
        job['test'] = 1
        job['test2'] = 'test'

        p = parser.ExpParser()

        # run tests
        ret = p.parse('$(test)',job=job)
        expected = 1
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test: ret != expected')

        ret = p.parse('$(test2)',job=job)
        expected = 'test'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')

        ret = p.parse('$(test3)',job=job)
        expected = '$(test3)'
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test3: ret != expected')

    @unittest_reporter
    def test_22_parse(self):
        """Test parser parse"""
        job = dataclasses.Job()
        job['steering'] = dataclasses.Steering()
        job['steering']['parameters'] = {
            'FILTER_filtered': 1,
            'SIM_filtered': 2,
        }
        job['steering']['system'] = {
            'test': 'FILTER',
            'test2': 'SIM',
        }
        job['options'] = {
            'FILTER_filtered': 3,
            'SIM_filtered': 4,
        }
        job['difplus'] = dataclasses.DifPlus()
        job['difplus']['dif'] = dataclasses.Dif()
        job['difplus']['plus'] = dataclasses.Plus()
        job['difplus']['plus']['category'] = 'filtered'

        p = parser.ExpParser()

        # run tests
        ret = p.parse('$sprintf("$%s(%s_%s)","steering",$system(test),$metadata(category))',job=job)
        expected = 1
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test1: ret != expected')

        ret = p.parse('$sprintf("$%s(%s_%s)","steering",$system(test2),$metadata(category))',job=job)
        expected = 2
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test2: ret != expected')

        ret = p.parse('$sprintf("$%s(%s_%s)","options",$system(test),$metadata(category))',job=job)
        expected = 3
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test3: ret != expected')

        ret = p.parse('$sprintf("$%s(%s_%s)","options",$system(test2),$metadata(category))',job=job)
        expected = 4
        if ret != expected:
            logger.info('ret=%r, expected=%r',ret,expected)
            raise Exception('test4: ret != expected')

    @unittest_reporter
    def test_30_parse_job_bin(self):
        """Test parsing the job binning. A bug found during prod-test."""
        for j in (0,15,248,1389,10000,10001,20482,83727,493837,1393728):
            job = dataclasses.Job()
            job['steering'] = dataclasses.Steering()
            job['options'] = {
                'job': j,
            }
            p = parser.ExpParser()

            # run tests
            ret = p.parse("$sprintf('%06d-%06d',$eval($(job)//10000*10000),$eval($eval($(job)//10000+1)*10000))",job=job)
            expected = '%06d-%06d'%(j//10000*10000,(j//10000+1)*10000)
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('ret != expected')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(parser_test))
    suite.addTests(loader.loadTestsFromNames(alltests,parser_test))
    return suite
