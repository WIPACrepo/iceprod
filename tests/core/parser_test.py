"""
  Test script for parser

  copyright (c) 2013 the icecube collaboration
"""

from __future__ import print_function,absolute_import
try:
    from core_tester import printer,glob_tests
    import logging
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    import logging
    logging.basicConfig()
logger = logging.getLogger('parser')

import os, sys, time
import shutil
import random
import string
import subprocess
import threading

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from flexmock import flexmock

from iceprod.core import to_log
from iceprod.core import parser, dataclasses

class parser_test(unittest.TestCase):
    def setUp(self):
        super(parser_test,self).setUp()
    
    def tearDown(self):
        super(parser_test,self).tearDown()

    def test_01_steering(self):
        """Test parser steering"""
        try:
            job = dataclasses.Job()
            job.steering = dataclasses.Steering()
            job.steering.parameters = {
                'test': dataclasses.Parameter('test',1),
                'test2': dataclasses.Parameter('test2','2'),
            }
            
            p = parser.ExpParser()
            p.job = job
            
            # run tests
            ret = p.steering_func('test')
            expected = job.steering.parameters['test'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test: ret != expected')
            
            ret = p.steering_func('test2')
            expected = job.steering.parameters['test2'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2: ret != expected')
            
            try:
                p.steering_func('test3')
            except parser.GrammarException:
                pass
            else:
                raise Exception('test3: did not raise GrammarException')
            
        except Exception, e:
            logger.error('Error running parser steering test: %s',str(e))
            printer('Test parser steering',False)
            raise
        else:
            printer('Test parser steering')

    def test_02_system(self):
        """Test parser system"""
        try:
            job = dataclasses.Job()
            job.steering = dataclasses.Steering()
            job.steering.system = {
                'test': dataclasses.Parameter('test',1),
                'test2': dataclasses.Parameter('test2','2'),
            }
            
            p = parser.ExpParser()
            p.job = job
            
            # run tests
            ret = p.system_func('test')
            expected = job.steering.system['test'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test: ret != expected')
            
            ret = p.system_func('test2')
            expected = job.steering.system['test2'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2: ret != expected')
            
            try:
                p.system_func('test3')
            except parser.GrammarException:
                pass
            else:
                raise Exception('test3: did not raise GrammarException')
            
        except Exception, e:
            logger.error('Error running parser system test: %s',str(e))
            printer('Test parser system',False)
            raise
        else:
            printer('Test parser system')

    def test_03_options(self):
        """Test parser options"""
        try:
            job = dataclasses.Job()
            job.options = {
                'test': dataclasses.Parameter('test',1),
                'test2': dataclasses.Parameter('test2','2'),
            }
            
            p = parser.ExpParser()
            p.job = job
            
            # run tests
            ret = p.options_func('test')
            expected = job.options['test'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test: ret != expected')
            
            ret = p.options_func('test2')
            expected = job.options['test2'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2: ret != expected')
            
            try:
                p.options_func('test3')
            except parser.GrammarException:
                pass
            else:
                raise Exception('test3: did not raise GrammarException')
            
        except Exception, e:
            logger.error('Error running parser options test: %s',str(e))
            printer('Test parser options',False)
            raise
        else:
            printer('Test parser options')

    def test_04_difplus(self):
        """Test parser difplus"""
        try:
            job = dataclasses.Job()
            job.difplus = dataclasses.DifPlus()
            job.difplus.dif = dataclasses.Dif()
            job.difplus.plus = dataclasses.Plus()
            job.difplus.plus.category = 'filtered'
            
            p = parser.ExpParser()
            p.job = job
            
            # run tests
            ret = p.difplus_func('sensor_name')
            expected = job.difplus.dif.sensor_name
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('dif: ret != expected')
            
            ret = p.difplus_func('category')
            expected = job.difplus.plus.category
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('plus: ret != expected')
            
            try:
                p.difplus_func('test')
            except parser.GrammarException:
                pass
            else:
                raise Exception('not present: did not raise GrammarException')
            
        except Exception, e:
            logger.error('Error running parser difplus test: %s',str(e))
            printer('Test parser difplus',False)
            raise
        else:
            printer('Test parser difplus')

    def test_05_eval(self):
        """Test parser eval"""
        try:
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
            
        except Exception, e:
            logger.error('Error running parser eval test: %s',str(e))
            printer('Test parser eval',False)
            raise
        else:
            printer('Test parser eval')

    def test_06_sprintf(self):
        """Test parser sprintf"""
        try:
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
            
        except Exception, e:
            logger.error('Error running parser sprintf test: %s',str(e))
            printer('Test parser sprintf',False)
            raise
        else:
            printer('Test parser sprintf')

    def test_07_choice(self):
        """Test parser choice"""
        try:
            p = parser.ExpParser()
            
            # run tests
            ret = p.choice_func('1,2,3,4')
            expected = ('1','2','3','4')
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
            
        except Exception, e:
            logger.error('Error running parser choice test: %s',str(e))
            printer('Test parser choice',False)
            raise
        else:
            printer('Test parser choice')

    def test_10_steering(self):
        """Test parser parse steering"""
        try:
            job = dataclasses.Job()
            job.steering = dataclasses.Steering()
            job.steering.parameters = {
                'test': dataclasses.Parameter('test',1),
                'test2': dataclasses.Parameter('test2','2'),
            }
            
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$steering(test)',job=job)
            expected = job.steering.parameters['test'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test: ret != expected')
            
            ret = p.parse('$steering(test2)',job=job)
            expected = job.steering.parameters['test2'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2: ret != expected')
            
            
            ret = p.parse('$steering(test3)',job=job)
            expected = '$steering(test3)'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test3: ret != expected')
            
        except Exception, e:
            logger.error('Error running parser parse steering test: %s',str(e))
            printer('Test parser parse steering',False)
            raise
        else:
            printer('Test parser parse steering')

    def test_11_system(self):
        """Test parser parse system"""
        try:
            job = dataclasses.Job()
            job.steering = dataclasses.Steering()
            job.steering.system = {
                'test': dataclasses.Parameter('test',1),
                'test2': dataclasses.Parameter('test2','2'),
            }
            
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$system(test)',job=job)
            expected = job.steering.system['test'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test: ret != expected')
            
            ret = p.parse('$system(test2)',job=job)
            expected = job.steering.system['test2'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2: ret != expected')
            
            ret = p.parse('$system(test3)',job=job)
            expected = '$system(test3)'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test3: ret != expected')
            
        except Exception, e:
            logger.error('Error running parser parse system test: %s',str(e))
            printer('Test parser parse system',False)
            raise
        else:
            printer('Test parser parse system')

    def test_12_options(self):
        """Test parser parse options"""
        try:
            job = dataclasses.Job()
            job.options = {
                'test': dataclasses.Parameter('test',1),
                'test2': dataclasses.Parameter('test2','2'),
            }
            
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$options(test)',job=job)
            expected = job.options['test'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test: ret != expected')
            
            ret = p.parse('$options(test2)',job=job)
            expected = job.options['test2'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2: ret != expected')
            
            ret = p.parse('$(test2)',job=job)
            expected = job.options['test2'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2 general search: ret != expected')
            
            ret = p.parse('$(test3)',job=job)
            expected = '$(test3)'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test3: ret != expected')
            
            ret = p.parse('$args(test)',job=job)
            expected = job.options['test'].value
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('args: ret != expected')
            
        except Exception, e:
            logger.error('Error running parser parse options test: %s',str(e))
            printer('Test parser parse options',False)
            raise
        else:
            printer('Test parser parse options')

    def test_13_difplus(self):
        """Test parser difplus"""
        try:
            job = dataclasses.Job()
            job.difplus = dataclasses.DifPlus()
            job.difplus.dif = dataclasses.Dif()
            job.difplus.plus = dataclasses.Plus()
            job.difplus.plus.category = 'filtered'
            
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$metadata(sensor_name)',job=job)
            expected = job.difplus.dif.sensor_name
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('dif: ret != expected')
            
            ret = p.parse('$metadata(category)',job=job)
            expected = job.difplus.plus.category
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('plus: ret != expected')
            
            ret = p.parse('$metadata(test)',job=job)
            expected = '$metadata(test)'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('plus: ret != expected')
            
        except Exception, e:
            logger.error('Error running parser parse difplus test: %s',str(e))
            printer('Test parser parse difplus',False)
            raise
        else:
            printer('Test parser parse difplus')

    def test_14_eval(self):
        """Test parser parse eval"""
        try:
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$eval(4+4)')
            expected = '8'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('simple: ret != expected')
            
            ret = p.parse('$eval(\(4+3*2\)%3)')
            expected = '1'
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
            
        except Exception, e:
            logger.error('Error running parser parse eval test: %s',str(e))
            printer('Test parser parse eval',False)
            raise
        else:
            printer('Test parser parse eval')

    def test_15_sprintf(self):
        """Test parser parse sprintf"""
        try:
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$sprintf("%d",5)')
            expected = '5'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('"d": ret != expected')
            
            ret = p.parse('$sprintf(\'%d\',5)')
            expected = '5'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('\'d\': ret != expected')
            
            ret = p.parse('$sprintf(%d,5)')
            expected = '5'
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
            
        except Exception, e:
            logger.error('Error running parser parse sprintf test: %s',str(e))
            printer('Test parser parse sprintf',False)
            raise
        else:
            printer('Test parser parse sprintf')

    def test_16_choice(self):
        """Test parser parse choice"""
        try:
            p = parser.ExpParser()
            
            
            # run tests
            ret = p.parse('$choice(1,2,3,4)')
            expected = ('1','2','3','4')
            if ret not in expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('multi: ret != expected')
            
            ret = p.parse('$choice(1)')
            expected = '1'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('single: ret != expected')
            
            ret = p.parse('$choice()')
            expected = ''
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('empty: ret != expected')
            
            
        except Exception, e:
            logger.error('Error running parser parse sprintf test: %s',str(e))
            printer('Test parser parse sprintf',False)
            raise
        else:
            printer('Test parser parse sprintf')


    def test_20_env(self):
        """Test parser parse env"""
        try:
            p = parser.ExpParser()
            env = {'parameters':{'test':dataclasses.Parameter('test',1)}}
            
            # run tests
            ret = p.parse('$(test)',env=env)
            expected = '1'
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
            
        except Exception, e:
            logger.error('Error running parser parse env test: %s',str(e))
            printer('Test parser parse env',False)
            raise
        else:
            printer('Test parser parse env')

    def test_21_job(self):
        """Test parser parse job"""
        try:
            job = dataclasses.Job()
            job.test = 1
            job.test2 = 'test'
            
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$(test)',job=job)
            expected = '1'
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
            
        except Exception, e:
            logger.error('Error running parser parse job test: %s',str(e))
            printer('Test parser parse job',False)
            raise
        else:
            printer('Test parser parse job')

    def test_22_parse(self):
        """Test parser parse"""
        try:
            job = dataclasses.Job()
            job.steering = dataclasses.Steering()
            job.steering.parameters = {
                'FILTER_filtered': dataclasses.Parameter('FILTER_filtered',1),
                'SIM_filtered': dataclasses.Parameter('SIM_filtered',2),
            }
            job.steering.system = {
                'test': dataclasses.Parameter('test','FILTER'),
                'test2': dataclasses.Parameter('test2','SIM'),
            }
            job.options = {
                'FILTER_filtered': dataclasses.Parameter('FILTER_filtered',3),
                'SIM_filtered': dataclasses.Parameter('SIM_filtered',4),
            }
            job.difplus = dataclasses.DifPlus()
            job.difplus.dif = dataclasses.Dif()
            job.difplus.plus = dataclasses.Plus()
            job.difplus.plus.category = 'filtered'
            
            p = parser.ExpParser()
            
            # run tests
            ret = p.parse('$sprintf("$%s(%s_%s)","steering",$system(test),$metadata(category))',job=job)
            expected = '1'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test1: ret != expected')
            
            ret = p.parse('$sprintf("$%s(%s_%s)","steering",$system(test2),$metadata(category))',job=job)
            expected = '2'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test2: ret != expected')
            
            ret = p.parse('$sprintf("$%s(%s_%s)","options",$system(test),$metadata(category))',job=job)
            expected = '3'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test3: ret != expected')
            
            ret = p.parse('$sprintf("$%s(%s_%s)","options",$system(test2),$metadata(category))',job=job)
            expected = '4'
            if ret != expected:
                logger.info('ret=%r, expected=%r',ret,expected)
                raise Exception('test4: ret != expected')
            
        except Exception, e:
            logger.error('Error running parser parse test: %s',str(e))
            printer('Test parser parse',False)
            raise
        else:
            printer('Test parser parse')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(parser_test))
    suite.addTests(loader.loadTestsFromNames(alltests,parser_test))
    return suite
