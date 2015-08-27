"""
Test script for serialization
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
logger = logging.getLogger('serialization_test')

import os
import sys
import json
import tempfile
import shutil

import unittest

from iceprod.core import to_log
import iceprod.core.serialization
from iceprod.core import dataclasses


class serialization_test(unittest.TestCase):
    def setUp(self):
        super(serialization_test,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(serialization_test,self).tearDown()

    @unittest_reporter
    def test_01_SerializationError(self):
        """Test the SerializationError class"""
        e = iceprod.core.serialization.SerializationError()

        if not isinstance(e,Exception):
            raise Exception('SerializationError is not an Exception')
        if e.value != '':
            raise Exception('Empty SerializationError has non-empty value')
        if str(e) != "SerializationError()":
            logger.info('repr: %s',str(e))
            raise Exception('bad string representation of SerializationError')

        e = iceprod.core.serialization.SerializationError('some text')

        if not isinstance(e,Exception):
            raise Exception('SerializationError is not an Exception')
        if e.value != 'some text':
            raise Exception('SerializationError has different value')
        if str(e) != "SerializationError('some text')":
            logger.info('repr: %s',str(e))
            raise Exception('bad string representation of SerializationError')

    @unittest_reporter
    def test_02_dict_to_dataclasses(self):
        """Test the dict_to_dataclasses function"""
        input_dict = {}
        ret = iceprod.core.serialization.dict_to_dataclasses(input_dict)
        if not isinstance(ret,dataclasses.Job):
            raise Exception('did not convert to Job')

        input_dict = {'dataset':10384,
                 'parent_id':10158,
                 'steering':{'parameters':{'test':1}},
                 'tasks':[{
                    'trays':[{
                        'modules':[{
                            'parameters':{'test2':2.0}
                        }],
                    }],
                 }],
                 'options':{'test3':'3'},
                }
        ret2 = iceprod.core.serialization.dict_to_dataclasses(input_dict)
        if not isinstance(ret2,dataclasses.Job):
            raise Exception('did not convert to Job')
        def dict_equal(d1,d2):
            if not isinstance(d1,dict) or not isinstance(d2,dict):
                return False
            for k in d1:
                if k not in d2:
                    return False
                elif isinstance(d1[k],dict) and isinstance(d2[k],dict):
                    if not dict_equal(d1[k],d2[k]):
                        return False
                else:
                    if d1[k] != d2[k]:
                        return False
            return True
        if not dict_equal(input_dict,ret2):
            logger.info('input: %r',input_dict)
            logger.info('ret2: %r',ret2)
            raise Exception('ret2 != input')

    @unittest_reporter
    def test_03_serialize_json(self):
        """Test the serialize_json class"""
        input_dict = dataclasses.Job()
        ret = iceprod.core.serialization.serialize_json.dumps(input_dict)
        if not isinstance(ret,dataclasses.String):
            raise Exception('did not convert to string')

        ret2 = iceprod.core.serialization.serialize_json.loads(ret)
        if not isinstance(ret2,dataclasses.Job):
            raise Exception('did not convert to Job')
        if input_dict != ret2:
            logger.info('input: %r',input_dict)
            logger.info('converted: %r',ret2)
            raise Exception('input != converted')

        filename = os.path.join(self.test_dir,'test.json')

        iceprod.core.serialization.serialize_json.dump(input_dict,filename)
        if not os.path.exists(filename):
            raise Exception('did not write to file')

        ret3 = iceprod.core.serialization.serialize_json.load(filename)
        if not isinstance(ret3,dataclasses.Job):
            raise Exception('did not load to Job')
        if input_dict != ret3:
            logger.info('input: %r',input_dict)
            logger.info('converted: %r',ret3)
            raise Exception('input != converted')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(serialization_test))
    suite.addTests(loader.loadTestsFromNames(alltests,serialization_test))
    return suite
