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
try:
    import builtins
except ImportError:
    import __builtin__ as builtins

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
            'test3': False,
        }

        p = parser.ExpParser()
        p.job = job

        # run tests
        ret = p.steering_func('test')
        expected = str(job['steering']['parameters']['test'])
        self.assertEqual(ret,expected)

        ret = p.steering_func('test2')
        expected = str(job['steering']['parameters']['test2'])
        self.assertEqual(ret,expected)

        ret = p.steering_func('test3')
        expected = str(job['steering']['parameters']['test3'])
        self.assertEqual(ret,expected)

        with self.assertRaises(parser.GrammarException):
            p.steering_func('test4')

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
        self.assertEqual(ret,expected)

        ret = p.system_func('test2')
        expected = str(job['steering']['system']['test2'])
        self.assertEqual(ret,expected)

        with self.assertRaises(parser.GrammarException):
            p.system_func('test3')

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
        self.assertEqual(ret,expected)

        ret = p.options_func('test2')
        expected = str(job['options']['test2'])
        self.assertEqual(ret,expected)

        with self.assertRaises(parser.GrammarException):
            p.options_func('test3')

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
        self.assertEqual(ret,expected)

        ret = p.difplus_func('category')
        expected = job['difplus']['plus']['category']
        self.assertEqual(ret,expected)

        with self.assertRaises(parser.GrammarException):
            p.difplus_func('test')

    @unittest_reporter
    def test_05_eval(self):
        """Test parser eval"""
        p = parser.ExpParser()

        # run tests
        ret = p.eval_func('4+4')
        self.assertEqual(ret, '8')

        ret = p.eval_func('(4+3*2)%3')
        self.assertEqual(ret, '1')

        with self.assertRaises(parser.GrammarException):
            p.eval_func('import os')

        with self.assertRaises(parser.GrammarException):
            p.eval_func('os.remove("/")')

    @unittest_reporter
    def test_06_sprintf(self):
        """Test parser sprintf"""
        p = parser.ExpParser()

        # run tests
        ret = p.sprintf_func('"%d",5')
        self.assertEqual(ret, '5')

        ret = p.sprintf_func('\'%d\',5')
        self.assertEqual(ret, '5')

        ret = p.sprintf_func('%d,5')
        self.assertEqual(ret, '5')

        ret = p.sprintf_func('"%s %06d","testing",12')
        expected = 'testing 000012'
        self.assertEqual(ret,expected)

        ret = p.sprintf_func('"%07d", 12.0000')
        expected = '0000012'
        self.assertEqual(ret,expected)

        ret = p.sprintf_func('"%x", 12')
        self.assertEqual(ret, 'c')

        ret = p.sprintf_func('"%o", 12')
        self.assertEqual(ret, '14')

        with self.assertRaises(parser.GrammarException):
            p.sprintf_func('"%s,12')
            
        with self.assertRaises(parser.GrammarException):
            p.sprintf_func('%s')

        with self.assertRaises(parser.GrammarException):
            p.sprintf_func('"%s"')

        with self.assertRaises(parser.GrammarException):
            p.sprintf_func('"%f","test"')

    @unittest_reporter
    def test_07_choice(self):
        """Test parser choice"""
        p = parser.ExpParser()

        # run tests
        ret = p.choice_func('1,2,3,4')
        expected = ('1','2','3','4')
        self.assertIn(ret,expected)
        
        ret = p.choice_func([1,2,3,4])
        self.assertIn(ret,expected)

        ret = p.choice_func('1')
        self.assertEqual(ret, '1')

        with self.assertRaises(parser.GrammarException):
            p.choice_func('')

        with self.assertRaises(parser.GrammarException):
            p.choice_func([])

        with self.assertRaises(parser.GrammarException):
            p.choice_func(123)

    @unittest_reporter
    def test_08_system(self):
        """Test parser environ"""
        job = dataclasses.Job()
        environ = {
            'environment': {
                'test': "blah",
                'test2': 3.14,
                'test3': True,
            },
        }

        p = parser.ExpParser()
        p.job = job
        p.env = environ

        # run tests
        ret = p.environ_func('test')
        self.assertEqual(ret, environ['environment']['test'])

        ret = p.environ_func('test2')
        self.assertEqual(ret, str(environ['environment']['test2']))

        ret = p.environ_func('test3')
        self.assertEqual(ret, str(environ['environment']['test3']))

        with self.assertRaises(parser.GrammarException):
            p.environ_func('test4')

    @unittest_reporter(name='parse() steering')
    def test_10_steering(self):
        """Test parser parse steering"""
        job = dataclasses.Job()
        job['steering'] = dataclasses.Steering()
        job['steering']['parameters'] = {
            'test': 1,
            'test2': 't2',
            'test3': False,
            'list': [1,2,3,4.0],
        }

        p = parser.ExpParser()

        # run tests
        ret = p.parse('$steering(test)',job=job)
        expected = job['steering']['parameters']['test']
        self.assertEqual(ret,expected)

        ret = p.parse('$steering(test2)',job=job)
        expected = job['steering']['parameters']['test2']
        self.assertEqual(ret,expected)

        ret = p.parse('$steering(test3)',job=job)
        expected = job['steering']['parameters']['test3']
        self.assertEqual(ret,expected)

        ret = p.parse('$steering(list)',job=job)
        expected = job['steering']['parameters']['list']
        self.assertEqual(ret,expected)

        ret = p.parse('$steering(list)[$steering(test)]',job=job)
        expected = job['steering']['parameters']['list'][job['steering']['parameters']['test']]
        self.assertEqual(ret,expected)

        ret = p.parse('$steering(list)[10]',job=job)
        expected = str(job['steering']['parameters']['list'])+'[10]'
        self.assertEqual(ret,expected)

        for reduction in 'sum', 'len', 'min', 'max':
            ret = p.parse('${}($steering(list))'.format(reduction),job=job)
            expected = getattr(builtins, reduction)(job['steering']['parameters']['list'])
            self.assertEqual(ret,expected)

        ret = p.parse('$steering(test4)',job=job)
        expected = '$steering(test4)'
        self.assertEqual(ret,expected)

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
        self.assertEqual(ret,expected)

        ret = p.parse('$system(test2)',job=job)
        expected = job['steering']['system']['test2']
        self.assertEqual(ret,expected)

        ret = p.parse('$system(test3)',job=job)
        expected = '$system(test3)'
        self.assertEqual(ret,expected)

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
        self.assertEqual(ret,expected)

        ret = p.parse('$options(test2)',job=job)
        expected = job['options']['test2']
        self.assertEqual(ret,expected)

        ret = p.parse('$(test2)',job=job)
        expected = job['options']['test2']
        self.assertEqual(ret,expected)

        ret = p.parse('$(test3)',job=job)
        expected = '$(test3)'
        self.assertEqual(ret,expected)

        ret = p.parse('$args(test)',job=job)
        expected = job['options']['test']
        self.assertEqual(ret,expected)

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
        self.assertEqual(ret,expected)

        ret = p.parse('$metadata(category)',job=job)
        expected = job['difplus']['plus']['category']
        self.assertEqual(ret,expected)

        ret = p.parse('$metadata(test)',job=job)
        expected = '$metadata(test)'
        self.assertEqual(ret,expected)

    @unittest_reporter(name='parse() eval')
    def test_14_eval(self):
        """Test parser parse eval"""
        p = parser.ExpParser()

        # run tests
        ret = p.parse('$eval(4+4)')
        expected = 8
        self.assertEqual(ret,expected)

        ret = p.parse('$eval(\(4+3*2\)%3)')
        expected = 1
        self.assertEqual(ret,expected)

        ret = p.parse('$eval((4+3*2)%3)')
        expected = 1
        self.assertEqual(ret,expected)

        ret = p.parse('$eval(import os)')
        expected = '$eval(import os)'
        self.assertEqual(ret,expected)

        ret = p.parse('$eval(os.remove\("/"\))')
        expected = '$eval(os.remove("/"))'
        self.assertEqual(ret,expected)

        ret = p.parse('$eval(os.remove("/"))')
        expected = '$eval(os.remove("/"))'
        self.assertEqual(ret,expected)

    @unittest_reporter(name='parse() sprintf')
    def test_15_sprintf(self):
        """Test parser parse sprintf"""
        p = parser.ExpParser()

        # run tests
        ret = p.parse('$sprintf("%d",5)')
        self.assertEqual(ret, 5)

        ret = p.parse('$sprintf(\'%d\',5)')
        self.assertEqual(ret, 5)

        ret = p.parse('$sprintf(%d,5)')
        self.assertEqual(ret, 5)

        ret = p.parse('$sprintf("%s %06d","testing",12)')
        self.assertEqual(ret, 'testing 000012')

        ret = p.parse('$sprintf("%s,12)')
        self.assertEqual(ret, '$sprintf("%s,12)')

        ret = p.parse('$sprintf(%s)')
        self.assertEqual(ret, '$sprintf(%s)')

        ret = p.parse('$sprintf("%s")')
        self.assertEqual(ret, '$sprintf("%s")')

        ret = p.parse('$sprintf("%f","test")')
        self.assertEqual(ret, '$sprintf("%f","test")')

    @unittest_reporter(name='parse() choice')
    def test_16_choice(self):
        """Test parser parse choice"""
        p = parser.ExpParser()

        # run tests
        ret = p.parse('$choice(1,2,3,4)')
        expected = (1,2,3,4)
        self.assertIn(ret,expected)

        ret = p.parse('$choice(1)')
        expected = 1
        self.assertEqual(ret,expected)

        ret = p.parse('$choice()')
        expected = '$choice()'
        self.assertEqual(ret,expected)

    @unittest_reporter(name='parse() environ')
    def test_17_environ(self):
        """Test parser parse environ"""
        p = parser.ExpParser()
        env = {
            'environment':{
                'test': 'blah',
                'test2': 3.14,
                'test3': True,
            }
        }

        # run tests
        ret = p.parse('$environ(test)', env=env)
        self.assertEqual(ret, env['environment']['test'])
        
        ret = p.parse('$environ(test2)', env=env)
        self.assertEqual(ret, env['environment']['test2'])
        
        ret = p.parse('$environ(test3)', env=env)
        self.assertEqual(ret, env['environment']['test3'])

    @unittest_reporter(name='parse() env')
    def test_20_env(self):
        """Test parser parse env"""
        p = parser.ExpParser()
        env = {'parameters':{'test':1}}

        # run tests
        ret = p.parse('$(test)',env=env)
        expected = 1
        self.assertEqual(ret,expected)

        ret = p.parse('$test()',env=env)
        expected = '$test()'
        self.assertEqual(ret,expected)

        ret = p.parse('$test',env=env)
        expected = '$test'
        self.assertEqual(ret,expected)

        ret = p.parse('$eval(os.remove("/"))',env=env)
        expected = '$eval(os.remove("/"))'
        self.assertEqual(ret,expected)

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
        self.assertEqual(ret,expected)

        ret = p.parse('$(test2)',job=job)
        expected = 'test'
        self.assertEqual(ret,expected)

        ret = p.parse('ab$(test2)cd',job=job)
        expected = 'abtestcd'
        self.assertEqual(ret,expected)

        ret = p.parse('$(test3)',job=job)
        expected = '$(test3)'
        self.assertEqual(ret,expected)

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
        ret = p.parse('$steering($sprintf("%s_%s",$system(test),$metadata(category)))',job=job)
        expected = 1
        self.assertEqual(ret,expected)

        ret = p.parse('$steering($system(test2)_$metadata(category))',job=job)
        expected = 2
        self.assertEqual(ret,expected)

        ret = p.parse('$options($sprintf("%s_%s",$system(test),$metadata(category)))',job=job)
        expected = 3
        self.assertEqual(ret,expected)

        ret = p.parse('$options($system(test2)_$metadata(category))',job=job)
        expected = 4
        self.assertEqual(ret,expected)

        ret = p.parse('$foo(ba]]r)')
        expected = '$foo(ba]]r)'
        self.assertEqual(ret,expected)

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
        self.assertEqual(ret,expected)

    @unittest_reporter
    def test_100_scanner(self):
        ret = list(parser.scanner('$foo(bar)'))
        expected = [('starter','$'),('word','foo'),('scopeL','('),('word','bar'),('scopeR',')')]
        self.assertEqual(ret, expected)

        ret = list(parser.scanner('$foo(bar)[baz]'))
        expected = [('starter','$'),('word','foo'),('scopeL','('),('word','bar'),
                    ('scopeR',')'),('bracketL','['),('word','baz'),('bracketR',']')]
        self.assertEqual(ret, expected)

    @unittest_reporter
    def test_110_parser(self):
        ret = list(parser.parser('$foo(bar)'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('word','bar'),('scopeR',')')]
        self.assertEqual(ret, expected)

        ret = list(parser.parser('$foo(bar)[baz]'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('word','bar'),
                    ('scopeR',')'),('bracketL','['),('word','baz'),('bracketR',']')]
        self.assertEqual(ret, expected)

        ret = list(parser.parser('$foo(b[a]r)'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('word','b[a]r'),('scopeR',')')]
        self.assertEqual(ret, expected)

        ret = list(parser.parser('$foo(bar[$(baz)])'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('word','bar['),
                    ('starter','$'),('name',None),('scopeL','('),
                    ('word','baz'),('scopeR',')'),('word',']'),('scopeR',')')]
        self.assertEqual(ret, expected)

        ret = list(parser.parser('$foo($($(bar)[baz]))'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('starter','$'),
                    ('name',None),('scopeL','('),('starter','$'),
                    ('name',None),('scopeL','('),('word','bar'),('scopeR',')'),
                    ('bracketL','['),('word','baz'),('bracketR',']'),
                    ('word',''),('scopeR',')'),('word',''),('scopeR',')')]
        self.assertEqual(ret, expected)

        ret = list(parser.parser('$foo($(b[a]r))'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('starter','$'),
                    ('name',None),('scopeL','('),('word','b[a]r'),('scopeR',')'),
                    ('word',''),('scopeR',')')]
        self.assertEqual(ret, expected)

        ret = list(parser.parser('$foo(bar)[]'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('word','bar'),('scopeR',')'),
                    ('bracketL','['),('word',''),('bracketR',']'),]
        self.assertEqual(ret, expected)

        ret = list(parser.parser('$foo(ba]]r)'))
        expected = [('starter','$'),('name','foo'),('scopeL','('),('word','ba'),
                    ('bracketR',']'),('word',''),('bracketR',']'),('word','r'),('scopeR',')')]
        self.assertEqual(ret, expected)

        with self.assertRaises(SyntaxError):
            list(parser.parser('$)'))

        ret = list(parser.parser('()'))
        self.assertEqual(ret, [('word','()')])

        with self.assertRaises(SyntaxError):
            list(parser.parser('$(]'))

        with self.assertRaises(SyntaxError):
            list(parser.parser('$foo([]bar)'))

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(parser_test))
    suite.addTests(loader.loadTestsFromNames(alltests,parser_test))
    return suite
