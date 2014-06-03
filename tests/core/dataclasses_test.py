"""
  Test script for dataclasses
  
  copyright (c) 2013 the icecube collaboration  
"""

from __future__ import print_function
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

    def test_01_parameter_init(self):
        """Test the parameter init"""
        try:
            Parameter = iceprod.core.dataclasses.Parameter
            
            p = Parameter('t',True)
            if p.name != 't' or p.value != 'True' or p.type != 'bool':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad bool parameter')
            
            p = Parameter('t','str')
            if p.name != 't' or p.value != 'str' or p.type != 'basestring':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad str parameter')
            
            p = Parameter('t',2)
            if p.name != 't' or p.value != '2' or p.type != 'int':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad int parameter')
            
            p = Parameter('t',2.0)
            if p.name != 't' or p.value != '2.0' or p.type != 'float':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad float parameter')
            
            p = Parameter('t',(1,2))
            if p.name != 't' or p.value != '[1, 2]' or p.type != 'list':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad tuple parameter')
            
            p = Parameter('t',[1,2])
            if p.name != 't' or p.value != '[1, 2]' or p.type != 'list':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad list parameter')
            
            p = Parameter('t',{1,2})
            if p.name != 't' or p.value != '[1, 2]' or p.type != 'set':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad set parameter')
            
            p = Parameter('t',{1:3,2:4})
            if p.name != 't' or p.value != '{"1": 3, "2": 4}' or p.type != 'dict':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad dict parameter')
            
            p = Parameter('t',{'t':'testing'},type='pickle')
            if p.name != 't' or p.value != pickle.dumps({'t':'testing'}) or p.type != 'pickle':
                logger.debug('%r %r %r',p.name,p.value,p.type)
                raise Exception('bad pickle parameter')
            
        except Exception as e:
            logger.error('Error running parameter init test: %s',str(e))
            printer('Test dataclasses.Parameter init',False)
            raise
        else:
            printer('Test dataclasses.Parameter init')

    def test_02_parameter_get(self):
        """Test the parameter get"""
        try:
            Parameter = iceprod.core.dataclasses.Parameter
            
            p = Parameter('t',True)
            v = p.get()
            if v is not True:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad bool parameter')
            
            p = Parameter('t','str')
            v = p.get()
            if v != 'str':
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad str parameter')
            
            p = Parameter('t',2)
            v = p.get()
            if v != 2:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad int parameter')
            
            p = Parameter('t',2.0)
            v = p.get()
            if v != 2.0:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad float parameter')
            
            p = Parameter('t',(1,2))
            v = p.get()
            if v != [1,2]:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad tuple parameter')
            
            p = Parameter('t',[1,2])
            v = p.get()
            if v != [1,2]:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad list parameter')
            
            p = Parameter('t',{1,2})
            v = p.get()
            if v != {1,2}:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad set parameter')
            
            p = Parameter('t',{1:3,2:4})
            v = p.get()
            if v != {'1':3,'2':4}:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad dict parameter')
            
            p = Parameter('t',{'t':'testing'},type='pickle')
            v = p.get()
            if v != {'t':'testing'}:
                logger.debug('%r %r %r = %r',p.name,p.value,p.type,v)
                raise Exception('bad pickle parameter')
            
        except Exception as e:
            logger.error('Error running parameter get test: %s',str(e))
            printer('Test dataclasses.Parameter get',False)
            raise
        else:
            printer('Test dataclasses.Parameter get')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(dataclasses_test))
    suite.addTests(loader.loadTestsFromNames(alltests,dataclasses_test))
    return suite
